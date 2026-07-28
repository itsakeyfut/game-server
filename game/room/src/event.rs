//! The lifecycle [`EventBus`]: fire-and-subscribe for framework lifecycle events.
//!
//! The bus carries only **lifecycle** events (runtime §4) — a player joining or leaving, a
//! room opening or closing, a connection dropping — never game-specific events, which stay
//! in user code. Any number of consumers ([`subscribe`](EventBus::subscribe)) — the game,
//! service layers, the observability layer — each receive every [`RoomEvent`]
//! [`publish`](EventBus::publish)ed after they subscribed. Delivery is a
//! [`tokio::sync::broadcast`] fan-out: a bounded ring per subscriber, so a subscriber that
//! falls too far behind observes an [`EventStreamError::Lagged`] rather than growing memory
//! without bound.
//!
//! This provides the fire/subscribe mechanism; wiring producers (the Room actor and the
//! session emitting events) is a later integration step.

use tokio::sync::broadcast;

use gsf_session::{RoomId, SessionId};

use crate::PlayerId;

/// Number of events the bus buffers per subscriber before the oldest are overwritten (a
/// lagging subscriber then sees [`EventStreamError::Lagged`]).
const DEFAULT_EVENT_BUFFER: usize = 256;

/// A framework lifecycle event (runtime §4). Only lifecycle concerns belong here; game
/// events are the user's domain. `#[non_exhaustive]` — the set grows as the runtime does.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoomEvent {
    /// A room instance was created.
    RoomCreated { room: RoomId },
    /// A room instance was closed.
    RoomClosed { room: RoomId },
    /// A player joined a room.
    PlayerJoined { room: RoomId, player: PlayerId },
    /// A player's connection dropped but their seat is held during the resume window.
    PlayerDisconnected { room: RoomId, player: PlayerId },
    /// A disconnected player reconnected and was restored to their seat.
    Resumed { room: RoomId, player: PlayerId },
    /// A player left for good (the resume window elapsed), freeing their seat.
    PlayerLeft { room: RoomId, player: PlayerId },
    /// A session's connection dropped.
    Disconnected { session: SessionId },
    /// A session timed out (heartbeat / dead-connection detection).
    Timeout { session: SessionId },
    /// A game started in a room.
    GameStarted { room: RoomId },
    /// A game ended in a room.
    GameEnded { room: RoomId },
}

/// A cloneable publish handle for lifecycle [`RoomEvent`]s. Clone it so several producers
/// (later: the Room actor, the session) can each publish to the same subscribers.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<RoomEvent>,
}

impl EventBus {
    /// A bus with a default per-subscriber event buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_EVENT_BUFFER)
    }

    /// A bus buffering `capacity` events per subscriber. `capacity` is clamped to at least
    /// 1 (a zero-capacity broadcast channel is invalid), so this never panics.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _rx) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Subscribe to events published from now on. Each subscriber receives every event
    /// independently; events published before subscribing are not delivered.
    #[must_use]
    pub fn subscribe(&self) -> EventSubscriber {
        EventSubscriber(self.sender.subscribe())
    }

    /// Publish `event` to all current subscribers, returning how many it was sent to
    /// (a lagging subscriber may still miss it). Publishing with no subscribers is not an
    /// error (returns 0).
    pub fn publish(&self, event: RoomEvent) -> usize {
        self.sender.send(event).unwrap_or(0)
    }

    /// The number of active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// A subscription to an [`EventBus`]. Await [`recv`](Self::recv) for the next event.
pub struct EventSubscriber(broadcast::Receiver<RoomEvent>);

impl EventSubscriber {
    /// Await the next event.
    ///
    /// # Errors
    /// - [`EventStreamError::Lagged`] if this subscriber fell behind and the bus overwrote
    ///   `n` events it had not yet received (reception continues after this).
    /// - [`EventStreamError::Closed`] once every [`EventBus`] handle has been dropped.
    pub async fn recv(&mut self) -> Result<RoomEvent, EventStreamError> {
        match self.0.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(EventStreamError::Lagged(n)),
            Err(broadcast::error::RecvError::Closed) => Err(EventStreamError::Closed),
        }
    }
}

/// Why receiving the next event failed. `#[non_exhaustive]` — more reasons may be added.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventStreamError {
    /// The subscriber fell behind and missed this many events (delivery then resumes).
    #[error("subscriber lagged and missed {0} events")]
    Lagged(u64),
    /// Every [`EventBus`] handle has been dropped; no more events will arrive.
    #[error("the event bus has been dropped")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn created(id: u64) -> RoomEvent {
        RoomEvent::RoomCreated { room: RoomId(id) }
    }

    #[tokio::test]
    async fn a_subscriber_should_receive_a_published_event() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        assert_eq!(bus.publish(created(1)), 1);
        assert_eq!(sub.recv().await.unwrap(), created(1));
    }

    #[tokio::test]
    async fn multiple_subscribers_should_each_receive_the_event() {
        // The three subscriber roles the requirement names: user, service, observability.
        let bus = EventBus::new();
        let mut user = bus.subscribe();
        let mut service = bus.subscribe();
        let mut observability = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 3);
        assert_eq!(bus.publish(created(7)), 3);
        assert_eq!(user.recv().await.unwrap(), created(7));
        assert_eq!(service.recv().await.unwrap(), created(7));
        assert_eq!(observability.recv().await.unwrap(), created(7));
    }

    #[tokio::test]
    async fn a_subscriber_should_only_receive_events_published_after_it_subscribed() {
        let bus = EventBus::new();
        bus.publish(created(1)); // before anyone subscribes — dropped
        let mut sub = bus.subscribe();
        bus.publish(created(2));
        assert_eq!(sub.recv().await.unwrap(), created(2));
    }

    #[tokio::test]
    async fn events_should_be_delivered_in_publish_order() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        for id in 0..10 {
            bus.publish(created(id));
        }
        for id in 0..10 {
            assert_eq!(sub.recv().await.unwrap(), created(id));
        }
    }

    #[tokio::test]
    async fn publishing_with_no_subscribers_should_not_error() {
        let bus = EventBus::new();
        assert_eq!(bus.publish(created(1)), 0);
        // A subscriber added afterward still works.
        let mut sub = bus.subscribe();
        bus.publish(created(2));
        assert_eq!(sub.recv().await.unwrap(), created(2));
    }

    #[tokio::test]
    async fn a_lagged_subscriber_should_report_missed_event_count() {
        let bus = EventBus::with_capacity(2);
        let mut sub = bus.subscribe();
        // Overflow the 2-slot ring without draining: 4 published, 2 overwritten.
        for id in 0..4 {
            bus.publish(created(id));
        }
        assert_eq!(sub.recv().await, Err(EventStreamError::Lagged(2)));
        // After a lag the subscriber resumes with the oldest still-buffered events.
        assert_eq!(sub.recv().await.unwrap(), created(2));
        assert_eq!(sub.recv().await.unwrap(), created(3));
    }

    #[tokio::test]
    async fn dropping_the_bus_should_close_subscribers() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        drop(bus);
        assert_eq!(sub.recv().await, Err(EventStreamError::Closed));
    }

    #[tokio::test]
    async fn with_capacity_zero_should_not_panic() {
        let bus = EventBus::with_capacity(0); // clamped to 1, must not panic
        let mut sub = bus.subscribe();
        assert_eq!(bus.publish(created(1)), 1);
        assert_eq!(sub.recv().await.unwrap(), created(1));
    }
}
