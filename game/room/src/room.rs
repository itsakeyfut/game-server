//! The Room actor: owned state, a bounded mailbox, typed message handlers, and the
//! [`RoomCtx`] a handler uses to reply and broadcast.
//!
//! A Room is one Tokio task that exclusively owns its state `S` (runtime §2). Inbound
//! messages arrive through a bounded mailbox and are processed **one at a time** on
//! `&mut S`, so handlers never race — the single consumer is the ordering guarantee, no
//! locks required. Handlers are registered per message type on a [`RoomBuilder`]; the
//! type is erased into the mailbox and recovered by a `TypeId` dispatch table inside the
//! actor (programming-model §1.7), so callers always see the concrete `&mut S`.
//!
//! # Backpressure & failure (minimal scope)
//!
//! Emitting an [`Outbound`] awaits the outbound sender, so the actor holds its processing
//! turn until the message is accepted: a slow or stalled outbound consumer applies
//! **head-of-line backpressure** to the whole Room (the actor stops draining its inbox).
//! The outbound consumer must therefore not synchronously feed back into this Room's own
//! mailbox, or the two backpressures would deadlock. Per-channel flow control (stale-drop
//! for unreliable channels, programming-model §1.1) arrives with the transport wiring.
//!
//! A handler **panic** unwinds and terminates the Room's task — isolated to that task, so
//! the node and other Rooms survive (programming-model §1.5), but this Room stops and
//! further sends return [`RoomClosed`]. Per-message panic recovery (`catch_unwind`) plus
//! structured metrics is deferred; today the panic is not caught here.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Bound on a Room's inbound mailbox. A full mailbox applies backpressure: a producer
/// awaiting [`RoomHandle::send`] parks until the actor drains a slot (runtime §2).
const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Identifies a participant within a Room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlayerId(pub u64);

/// Correlates a reply to the request that triggered it (programming-model §1.1 C2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId(pub u64);

/// One outbound message the Room emits toward the delivery layer (wired to transport in a
/// later issue). `correlation` is `Some` for a [`reply`](RoomCtx::reply) and `None` for a
/// [`send_to`](RoomCtx::send_to) or a [`broadcast`](RoomCtx::broadcast) fan-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound<Out> {
    /// The recipient player.
    pub to: PlayerId,
    /// The request id this message replies to, if it is a reply.
    pub correlation: Option<RequestId>,
    /// The message payload.
    pub message: Out,
}

/// A handler's boxed, borrowing future. Handlers `await` while holding `&mut S`, so the
/// future borrows the state and context for `'a`; boxing keeps the handler type uniform in
/// the dispatch table. See [`RoomBuilder::on`] for how to write one.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// A type-erased handler stored in the dispatch table: it downcasts the payload to the
/// registered message type and runs the user handler.
type BoxHandler<S, Out> = Box<
    dyn for<'a> Fn(&'a mut S, Box<dyn Any + Send>, &'a mut RoomCtx<Out>) -> HandlerFuture<'a>
        + Send,
>;

/// An effect a handler requests through its [`RoomCtx`]; the actor turns each into one or
/// more [`Outbound`] messages after the handler returns.
enum Effect<Out> {
    Reply(Out),
    Send { to: PlayerId, message: Out },
    Broadcast(Out),
}

/// The context a handler uses to respond. Effects are buffered here and applied by the
/// actor once the handler returns, so the context needs no access to the member set.
pub struct RoomCtx<Out> {
    player: PlayerId,
    request_id: RequestId,
    effects: Vec<Effect<Out>>,
}

impl<Out> RoomCtx<Out> {
    fn new(player: PlayerId, request_id: RequestId) -> Self {
        Self {
            player,
            request_id,
            effects: Vec::new(),
        }
    }

    /// Reply to the player whose request is being handled, correlated by its request id.
    pub fn reply(&mut self, message: Out) {
        self.effects.push(Effect::Reply(message));
    }

    /// Send a message to a specific player.
    pub fn send_to(&mut self, player: PlayerId, message: Out) {
        self.effects.push(Effect::Send {
            to: player,
            message,
        });
    }

    /// Broadcast a message to every current member of the Room.
    pub fn broadcast(&mut self, message: Out) {
        self.effects.push(Effect::Broadcast(message));
    }

    /// The player whose message is currently being handled.
    #[must_use]
    pub fn player(&self) -> PlayerId {
        self.player
    }

    /// The request id of the message currently being handled.
    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    fn take_effects(&mut self) -> Vec<Effect<Out>> {
        std::mem::take(&mut self.effects)
    }
}

/// An inbound mailbox item. The message payload is type-erased; the actor recovers the
/// concrete type via the dispatch table.
enum Envelope {
    Join(PlayerId),
    Leave(PlayerId),
    Message {
        from: PlayerId,
        request_id: RequestId,
        payload: Box<dyn Any + Send>,
    },
}

/// Returned when a message cannot be delivered because the Room actor has stopped (its
/// mailbox is closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the room has stopped and can no longer receive messages")]
pub struct RoomClosed;

/// Registers per-message handlers for a Room, then [`spawn`](RoomBuilder::spawn)s the actor.
///
/// `S` is the Room's state type and `Out` its outbound message type (the game's response
/// protocol). Handlers are `async fn(&mut S, M, &mut RoomCtx<Out>)`, but because such a
/// future borrows `&mut S` it cannot be a single generic future on stable Rust — so a
/// handler returns a boxed [`HandlerFuture`]:
///
/// ```
/// use gsf_room::{HandlerFuture, RoomBuilder, RoomCtx};
///
/// fn on_ping<'a>(state: &'a mut i64, add: i64, ctx: &'a mut RoomCtx<i64>) -> HandlerFuture<'a> {
///     Box::pin(async move {
///         *state += add;
///         ctx.reply(*state);
///     })
/// }
///
/// let builder = RoomBuilder::<i64, i64>::new().on::<i64, _>(on_ping);
/// ```
pub struct RoomBuilder<S, Out> {
    handlers: HashMap<TypeId, BoxHandler<S, Out>>,
}

impl<S, Out> RoomBuilder<S, Out>
where
    S: Send + 'static,
    Out: Send + Clone + 'static,
{
    /// An empty builder with no handlers registered.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register `handler` for inbound messages of type `M`. A later registration for the
    /// same `M` replaces the earlier one.
    #[must_use]
    pub fn on<M, F>(mut self, handler: F) -> Self
    where
        M: Send + 'static,
        F: for<'a> Fn(&'a mut S, M, &'a mut RoomCtx<Out>) -> HandlerFuture<'a>
            + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        let boxed: BoxHandler<S, Out> = Box::new(move |state, payload, ctx| -> HandlerFuture<'_> {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                // The payload was registered under `TypeId::of::<M>()`, so this
                // downcast succeeds; a mismatch is dropped rather than panicking.
                if let Ok(message) = payload.downcast::<M>() {
                    handler(state, *message, ctx).await;
                }
            })
        });
        self.handlers.insert(TypeId::of::<M>(), boxed);
        self
    }

    /// Spawn the Room actor with initial `state`, emitting [`Outbound`] messages to
    /// `outbound`, using a default inbound mailbox capacity. Dropping every [`RoomHandle`]
    /// closes the mailbox and ends the task.
    pub fn spawn(self, state: S, outbound: mpsc::Sender<Outbound<Out>>) -> RoomHandle {
        self.spawn_with_capacity(DEFAULT_MAILBOX_CAPACITY, state, outbound)
    }

    /// Like [`spawn`](Self::spawn), but with an explicit inbound mailbox `capacity`. A
    /// full mailbox parks a producer awaiting [`RoomHandle::send`] until the actor drains
    /// a slot (runtime §2).
    pub fn spawn_with_capacity(
        self,
        capacity: usize,
        state: S,
        outbound: mpsc::Sender<Outbound<Out>>,
    ) -> RoomHandle {
        let (inbound, mailbox) = mpsc::channel(capacity);
        let actor = Actor {
            state,
            handlers: self.handlers,
            members: HashSet::new(),
            mailbox,
            outbound,
        };
        let task = tokio::spawn(actor.run());
        RoomHandle { inbound, task }
    }
}

impl<S, Out> Default for RoomBuilder<S, Out>
where
    S: Send + 'static,
    Out: Send + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A handle to a spawned Room actor. Drive the mailbox with [`join`](Self::join) /
/// [`leave`](Self::leave) / [`send`](Self::send); dropping the handle closes the mailbox so
/// the actor drains any queued messages and then stops.
pub struct RoomHandle {
    inbound: mpsc::Sender<Envelope>,
    task: JoinHandle<()>,
}

impl RoomHandle {
    /// Add a player to the Room's membership (a subsequent broadcast reaches them).
    ///
    /// # Errors
    /// Returns [`RoomClosed`] if the actor has stopped.
    pub async fn join(&self, player: PlayerId) -> Result<(), RoomClosed> {
        self.inbound
            .send(Envelope::Join(player))
            .await
            .map_err(|_| RoomClosed)
    }

    /// Remove a player from the Room's membership.
    ///
    /// # Errors
    /// Returns [`RoomClosed`] if the actor has stopped.
    pub async fn leave(&self, player: PlayerId) -> Result<(), RoomClosed> {
        self.inbound
            .send(Envelope::Leave(player))
            .await
            .map_err(|_| RoomClosed)
    }

    /// Deliver a typed message from `from`, correlated by `request_id`. If no handler is
    /// registered for `M`, the actor drops it (see the module docs).
    ///
    /// # Errors
    /// Returns [`RoomClosed`] if the actor has stopped.
    pub async fn send<M>(
        &self,
        from: PlayerId,
        request_id: RequestId,
        message: M,
    ) -> Result<(), RoomClosed>
    where
        M: Send + 'static,
    {
        self.inbound
            .send(Envelope::Message {
                from,
                request_id,
                payload: Box::new(message),
            })
            .await
            .map_err(|_| RoomClosed)
    }

    /// Abort the actor task immediately, without draining the mailbox.
    pub fn abort(&self) {
        self.task.abort();
    }
}

/// The Room actor: exclusively owns `state`, drains the mailbox, and dispatches each
/// message to its handler on `&mut state`.
struct Actor<S, Out> {
    state: S,
    handlers: HashMap<TypeId, BoxHandler<S, Out>>,
    members: HashSet<PlayerId>,
    mailbox: mpsc::Receiver<Envelope>,
    outbound: mpsc::Sender<Outbound<Out>>,
}

impl<S, Out> Actor<S, Out>
where
    S: Send + 'static,
    Out: Send + Clone + 'static,
{
    async fn run(mut self) {
        while let Some(envelope) = self.mailbox.recv().await {
            match envelope {
                Envelope::Join(player) => {
                    self.members.insert(player);
                }
                Envelope::Leave(player) => {
                    self.members.remove(&player);
                }
                Envelope::Message {
                    from,
                    request_id,
                    payload,
                } => {
                    let type_id = (*payload).type_id();
                    let Some(handler) = self.handlers.get(&type_id) else {
                        tracing::warn!(
                            ?type_id,
                            "room received a message with no registered handler"
                        );
                        continue;
                    };
                    let mut ctx = RoomCtx::new(from, request_id);
                    handler(&mut self.state, payload, &mut ctx).await;
                    for effect in ctx.take_effects() {
                        match effect {
                            Effect::Reply(message) => {
                                emit(
                                    &self.outbound,
                                    Outbound {
                                        to: from,
                                        correlation: Some(request_id),
                                        message,
                                    },
                                )
                                .await;
                            }
                            Effect::Send { to, message } => {
                                emit(
                                    &self.outbound,
                                    Outbound {
                                        to,
                                        correlation: None,
                                        message,
                                    },
                                )
                                .await;
                            }
                            Effect::Broadcast(message) => {
                                for &member in &self.members {
                                    emit(
                                        &self.outbound,
                                        Outbound {
                                            to: member,
                                            correlation: None,
                                            message: message.clone(),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Forward one message to the delivery layer. Borrows only the [`mpsc::Sender`] (which is
/// `Sync`), not the whole actor, so the actor's `run` future stays `Send`. If the delivery
/// layer has gone away, the message is dropped rather than raising an error.
async fn emit<Out>(outbound: &mpsc::Sender<Outbound<Out>>, message: Outbound<Out>) {
    if outbound.send(message).await.is_err() {
        // The delivery layer has gone away; drop the message rather than failing the
        // actor. Logged so a vanished sink is observable, mirroring the
        // unregistered-handler path.
        tracing::debug!("room outbound closed; dropping message");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal outbound protocol for the tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Reply {
        Total(i64),
        Note(&'static str),
    }

    #[derive(Debug)]
    struct Incr(i64);

    #[derive(Debug)]
    struct Announce(&'static str);

    fn on_incr<'a>(
        state: &'a mut i64,
        msg: Incr,
        ctx: &'a mut RoomCtx<Reply>,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            *state += msg.0;
            ctx.reply(Reply::Total(*state));
        })
    }

    fn on_announce<'a>(
        _state: &'a mut i64,
        msg: Announce,
        ctx: &'a mut RoomCtx<Reply>,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.broadcast(Reply::Note(msg.0));
        })
    }

    // A read-modify-write that yields between the read and the write: if a later message
    // could be dispatched during the yield, the two would clobber each other (a lost
    // update). Sequential `&mut S` makes that impossible.
    fn on_read_yield_write<'a>(
        state: &'a mut i64,
        _msg: Incr,
        ctx: &'a mut RoomCtx<Reply>,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            let observed = *state;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            *state = observed + 1;
            ctx.reply(Reply::Total(*state));
        })
    }

    fn spawn_counter(state: i64) -> (RoomHandle, mpsc::Receiver<Outbound<Reply>>) {
        let (out_tx, out_rx) = mpsc::channel(64);
        let room = RoomBuilder::<i64, Reply>::new()
            .on::<Incr, _>(on_incr)
            .on::<Announce, _>(on_announce)
            .spawn(state, out_tx);
        (room, out_rx)
    }

    // Drop the handle (closing the mailbox) and collect every outbound the actor emits
    // before it stops, so tests observe a deterministic, complete set.
    async fn drain(
        room: RoomHandle,
        mut out_rx: mpsc::Receiver<Outbound<Reply>>,
    ) -> Vec<Outbound<Reply>> {
        drop(room);
        let mut collected = Vec::new();
        while let Some(outbound) = out_rx.recv().await {
            collected.push(outbound);
        }
        collected
    }

    #[tokio::test]
    async fn messages_should_be_processed_sequentially_on_mut_state() {
        let (room, out_rx) = spawn_counter(0);
        let p = PlayerId(1);
        for i in 1..=5 {
            room.send(p, RequestId(i), Incr(i as i64)).await.unwrap();
        }
        let out = drain(room, out_rx).await;
        let totals: Vec<Reply> = out.into_iter().map(|o| o.message).collect();
        // Running prefix sums, in order: 1, 3, 6, 10, 15.
        assert_eq!(
            totals,
            vec![
                Reply::Total(1),
                Reply::Total(3),
                Reply::Total(6),
                Reply::Total(10),
                Reply::Total(15),
            ]
        );
    }

    #[tokio::test]
    async fn a_handler_should_reply_to_the_requester() {
        let (room, out_rx) = spawn_counter(0);
        room.send(PlayerId(7), RequestId(42), Incr(3))
            .await
            .unwrap();
        let out = drain(room, out_rx).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].to, PlayerId(7));
        assert_eq!(out[0].correlation, Some(RequestId(42)));
        assert_eq!(out[0].message, Reply::Total(3));
    }

    #[tokio::test]
    async fn a_handler_should_broadcast_to_all_members() {
        let (room, out_rx) = spawn_counter(0);
        room.join(PlayerId(1)).await.unwrap();
        room.join(PlayerId(2)).await.unwrap();
        room.join(PlayerId(3)).await.unwrap();
        room.send(PlayerId(1), RequestId(1), Announce("hi"))
            .await
            .unwrap();
        let out = drain(room, out_rx).await;
        assert_eq!(out.len(), 3);
        let mut recipients: Vec<u64> = out.iter().map(|o| o.to.0).collect();
        recipients.sort_unstable();
        assert_eq!(recipients, vec![1, 2, 3]);
        assert!(out.iter().all(|o| o.correlation.is_none()));
        assert!(out.iter().all(|o| o.message == Reply::Note("hi")));
    }

    #[tokio::test]
    async fn send_to_should_target_a_specific_player() {
        fn on_whisper<'a>(
            _s: &'a mut i64,
            _m: Incr,
            ctx: &'a mut RoomCtx<Reply>,
        ) -> HandlerFuture<'a> {
            Box::pin(async move {
                ctx.send_to(PlayerId(99), Reply::Note("psst"));
            })
        }
        let (out_tx, out_rx) = mpsc::channel(64);
        let room = RoomBuilder::<i64, Reply>::new()
            .on::<Incr, _>(on_whisper)
            .spawn(0, out_tx);
        room.send(PlayerId(1), RequestId(1), Incr(0)).await.unwrap();
        let out = drain(room, out_rx).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].to, PlayerId(99));
        assert_eq!(out[0].correlation, None);
        assert_eq!(out[0].message, Reply::Note("psst"));
    }

    #[tokio::test]
    async fn leave_should_remove_a_member_from_broadcast() {
        let (room, out_rx) = spawn_counter(0);
        room.join(PlayerId(1)).await.unwrap();
        room.join(PlayerId(2)).await.unwrap();
        room.leave(PlayerId(1)).await.unwrap();
        room.send(PlayerId(2), RequestId(1), Announce("bye"))
            .await
            .unwrap();
        let out = drain(room, out_rx).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].to, PlayerId(2));
    }

    #[tokio::test]
    async fn dispatch_should_route_each_message_type_to_its_own_handler() {
        let (room, out_rx) = spawn_counter(10);
        let p = PlayerId(1);
        room.join(p).await.unwrap();
        room.send(p, RequestId(1), Incr(5)).await.unwrap(); // reply Total(15)
        room.send(p, RequestId(2), Announce("go")).await.unwrap(); // broadcast Note to p
        room.send(p, RequestId(3), Incr(2)).await.unwrap(); // reply Total(17)
        let out = drain(room, out_rx).await;
        assert_eq!(
            out.iter().map(|o| o.message.clone()).collect::<Vec<_>>(),
            vec![Reply::Total(15), Reply::Note("go"), Reply::Total(17),]
        );
    }

    #[tokio::test]
    async fn an_unregistered_message_type_should_be_dropped_without_panicking() {
        #[derive(Debug)]
        struct Unknown;
        let (room, out_rx) = spawn_counter(0);
        // No handler for `Unknown` — must not panic or stop the actor.
        room.send(PlayerId(1), RequestId(1), Unknown).await.unwrap();
        // A following registered message is still handled, proving the actor is alive.
        room.send(PlayerId(1), RequestId(2), Incr(4)).await.unwrap();
        let out = drain(room, out_rx).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].message, Reply::Total(4));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_yielding_handler_should_not_be_interrupted_across_await() {
        let (out_tx, out_rx) = mpsc::channel(64);
        let room = std::sync::Arc::new(
            RoomBuilder::<i64, Reply>::new()
                .on::<Incr, _>(on_read_yield_write)
                .spawn(0, out_tx),
        );
        // Fire many read-modify-write messages concurrently from several tasks. Even on a
        // multi-threaded runtime, the actor is one task: each handler holds `&mut S` across
        // its yields, so no two interleave.
        let mut joins = Vec::new();
        for i in 0..20u64 {
            let room = std::sync::Arc::clone(&room);
            joins.push(tokio::spawn(async move {
                room.send(PlayerId(1), RequestId(i), Incr(0)).await.unwrap();
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        let room = std::sync::Arc::try_unwrap(room)
            .ok()
            .expect("all sender tasks finished; sole owner");
        let out = drain(room, out_rx).await;
        let mut totals: Vec<i64> = out
            .into_iter()
            .map(|o| match o.message {
                Reply::Total(n) => n,
                Reply::Note(_) => unreachable!("counter only replies Total"),
            })
            .collect();
        totals.sort_unstable();
        // Sequential execution ⇒ each message increments exactly once, no lost updates:
        // the replied totals are precisely 1..=20. Any interleaving would clobber and
        // produce duplicates / gaps.
        assert_eq!(totals, (1..=20).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn dropping_the_outbound_receiver_should_not_panic_the_actor() {
        // Capacity 1 forces each `send` to wait for the actor to dequeue the previous
        // message, so a later `send` completing proves the actor survived the failed emit.
        let (out_tx, out_rx) = mpsc::channel(1);
        let room = RoomBuilder::<i64, Reply>::new()
            .on::<Incr, _>(on_incr)
            .spawn_with_capacity(1, 0, out_tx);
        drop(out_rx); // the delivery layer goes away; every emit now fails
        for i in 1..=5 {
            room.send(PlayerId(1), RequestId(i), Incr(1))
                .await
                .expect("actor stays alive despite a dropped outbound receiver");
        }
        room.abort();
    }

    #[tokio::test]
    async fn a_stalled_room_should_apply_backpressure_to_producers() {
        // A capacity-1 outbound that is never drained stalls the actor on its second emit
        // (the first reply fills the single slot). With the actor no longer draining its
        // inbound mailbox, producers must park — so a bulk of sends cannot all complete.
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let room = RoomBuilder::<i64, Reply>::new()
            .on::<Incr, _>(on_incr)
            .spawn_with_capacity(1, 0, out_tx);
        let pushed = tokio::time::timeout(std::time::Duration::from_millis(200), async {
            for i in 0..100u64 {
                room.send(PlayerId(1), RequestId(i), Incr(1)).await.unwrap();
            }
        })
        .await;
        assert!(
            pushed.is_err(),
            "sends should park under backpressure once the actor stalls, not all complete"
        );
        // Draining the outbound lets the actor make progress again (no permanent stall).
        assert_eq!(out_rx.recv().await.unwrap().message, Reply::Total(1));
        room.abort();
    }

    proptest::proptest! {
        #[test]
        fn any_increment_sequence_should_reply_prefix_sums(increments in proptest::collection::vec(-1000i64..1000, 0..64)) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("build current-thread runtime");
            rt.block_on(async {
                let (room, out_rx) = spawn_counter(0);
                let p = PlayerId(1);
                for (i, inc) in increments.iter().enumerate() {
                    room.send(p, RequestId(i as u64), Incr(*inc)).await.unwrap();
                }
                let out = drain(room, out_rx).await;
                let got: Vec<i64> = out
                    .into_iter()
                    .map(|o| match o.message {
                        Reply::Total(n) => n,
                        Reply::Note(_) => unreachable!("counter only replies Total"),
                    })
                    .collect();
                let mut running = 0i64;
                let expected: Vec<i64> = increments
                    .iter()
                    .map(|inc| {
                        running += inc;
                        running
                    })
                    .collect();
                proptest::prop_assert_eq!(got, expected);
                Ok(())
            })?;
        }
    }
}
