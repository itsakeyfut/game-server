//! Channel reliability semantics, capability sets, and the concrete byte channel.

use std::fmt;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::TransportError;

/// The reliability semantics a channel provides.
///
/// A transport advertises the set of kinds it can honestly deliver (see
/// [`ChannelCapabilities`]); requesting a kind outside that set is a
/// [`TransportError::UnsupportedChannel`], never a silent downgrade.
///
/// This is a closed taxonomy — the two-axis matrix of {reliable, unreliable} ×
/// {ordered/sequenced, unordered} — so it is intentionally *not*
/// `#[non_exhaustive]`: adding a kind would be a deliberate breaking change that
/// every consumer should be forced to consider.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelKind {
    /// Every message is delivered, in send order (e.g. TCP, QUIC reliable stream).
    ReliableOrdered = 0,
    /// Every message is delivered, but order is not guaranteed.
    ReliableUnordered = 1,
    /// Messages may be dropped, but a message is never delivered after a newer one
    /// (stale messages are discarded rather than reordered).
    UnreliableSequenced = 2,
    /// Best-effort datagrams: messages may be dropped or reordered (e.g. raw UDP).
    Unreliable = 3,
}

impl ChannelKind {
    /// Every channel kind, in discriminant order. Used for iteration and for
    /// building [`ChannelCapabilities`].
    pub const ALL: [ChannelKind; 4] = [
        ChannelKind::ReliableOrdered,
        ChannelKind::ReliableUnordered,
        ChannelKind::UnreliableSequenced,
        ChannelKind::Unreliable,
    ];

    /// This kind's bit within a [`ChannelCapabilities`] bitset.
    const fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// The set of [`ChannelKind`]s a transport can provide, as a compact bitset.
///
/// Built additively from [`none`](Self::none) via [`with`](Self::with) (const-friendly)
/// or collected from an iterator of kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChannelCapabilities(u8);

impl ChannelCapabilities {
    /// An empty set — no channels supported.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// The set with `kind` added.
    #[must_use]
    pub const fn with(self, kind: ChannelKind) -> Self {
        Self(self.0 | kind.bit())
    }

    /// Whether `kind` is in the set.
    #[must_use]
    pub const fn supports(self, kind: ChannelKind) -> bool {
        self.0 & kind.bit() != 0
    }

    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The supported kinds, in [`ChannelKind::ALL`] order.
    pub fn kinds(self) -> impl Iterator<Item = ChannelKind> {
        ChannelKind::ALL
            .into_iter()
            .filter(move |&kind| self.supports(kind))
    }
}

impl FromIterator<ChannelKind> for ChannelCapabilities {
    fn from_iter<I: IntoIterator<Item = ChannelKind>>(iter: I) -> Self {
        iter.into_iter().fold(Self::none(), Self::with)
    }
}

/// What [`Channel::send`] does when the bounded send queue is full.
///
/// Chosen per channel — reliable channels flow-control by waiting, unreliable
/// channels never block and drop instead (see [`default_for`](Self::default_for)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePolicy {
    /// Wait for capacity — the sender is flow-controlled (reliable channels).
    Block,
    /// Never wait; if the queue is full, drop the message being sent (unreliable
    /// channels, best-effort). Strict latest-priority (dropping the *oldest*
    /// instead) is deferred to the reliable-UDP milestone.
    DropNewest,
    /// Never wait; if the queue is full, fail with [`TransportError::Backpressure`]
    /// so the caller can tear the connection down.
    Disconnect,
}

impl BackpressurePolicy {
    /// The default policy for a channel of `kind`: reliable kinds [`Block`](Self::Block),
    /// unreliable kinds [`DropNewest`](Self::DropNewest).
    #[must_use]
    pub const fn default_for(kind: ChannelKind) -> Self {
        match kind {
            ChannelKind::ReliableOrdered | ChannelKind::ReliableUnordered => Self::Block,
            ChannelKind::UnreliableSequenced | ChannelKind::Unreliable => Self::DropNewest,
        }
    }
}

/// A bidirectional byte channel with a fixed [`ChannelKind`] and [`BackpressurePolicy`].
///
/// This is the per-message hot path, so it is a concrete type — no trait object,
/// no dynamic dispatch on send/recv. It is backed by a pair of bounded mpsc queues
/// (one per direction); when the send queue is full, [`send`](Self::send) applies the
/// channel's [`policy`](Self::policy) — waiting (reliable) or dropping (unreliable).
pub struct Channel {
    kind: ChannelKind,
    policy: BackpressurePolicy,
    tx: mpsc::Sender<Bytes>,
    rx: mpsc::Receiver<Bytes>,
}

impl Channel {
    /// Build a channel from a matched sender/receiver pair, taking the
    /// [`BackpressurePolicy::default_for`] the `kind`. Backends (the loopback pair, or
    /// a socket bridge) construct the mpsc halves and wire them however they move bytes.
    pub(crate) fn new(
        kind: ChannelKind,
        tx: mpsc::Sender<Bytes>,
        rx: mpsc::Receiver<Bytes>,
    ) -> Channel {
        Channel::with_policy(kind, tx, rx, BackpressurePolicy::default_for(kind))
    }

    /// Build a channel with an explicit [`BackpressurePolicy`].
    pub(crate) fn with_policy(
        kind: ChannelKind,
        tx: mpsc::Sender<Bytes>,
        rx: mpsc::Receiver<Bytes>,
        policy: BackpressurePolicy,
    ) -> Channel {
        Channel {
            kind,
            policy,
            tx,
            rx,
        }
    }

    /// Create a connected, full-duplex pair of channels of the same `kind`.
    ///
    /// `capacity` bounds each direction's in-flight queue (the backpressure point).
    pub(crate) fn pair(kind: ChannelKind, capacity: usize) -> (Channel, Channel) {
        let (a_tx, b_rx) = mpsc::channel(capacity);
        let (b_tx, a_rx) = mpsc::channel(capacity);
        (
            Channel::new(kind, a_tx, a_rx),
            Channel::new(kind, b_tx, b_rx),
        )
    }

    /// The reliability semantics this channel provides.
    #[must_use]
    pub fn kind(&self) -> ChannelKind {
        self.kind
    }

    /// The policy [`send`](Self::send) applies when the send queue is full.
    #[must_use]
    pub fn policy(&self) -> BackpressurePolicy {
        self.policy
    }

    /// Send `payload` to the peer, applying the channel's [`BackpressurePolicy`] when
    /// the send queue is full: [`Block`](BackpressurePolicy::Block) waits,
    /// [`DropNewest`](BackpressurePolicy::DropNewest) drops (returning `Ok`), and
    /// [`Disconnect`](BackpressurePolicy::Disconnect) fails.
    ///
    /// # Errors
    /// [`TransportError::ConnectionClosed`] if the peer's receive half has been dropped;
    /// [`TransportError::Backpressure`] if the queue is full under the
    /// [`Disconnect`](BackpressurePolicy::Disconnect) policy.
    pub async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        match self.policy {
            BackpressurePolicy::Block => self
                .tx
                .send(payload)
                .await
                .map_err(|_| TransportError::ConnectionClosed),
            BackpressurePolicy::DropNewest => match self.tx.try_send(payload) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(()), // best-effort: dropped
                Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::ConnectionClosed),
            },
            BackpressurePolicy::Disconnect => match self.tx.try_send(payload) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err(TransportError::Backpressure),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::ConnectionClosed),
            },
        }
    }

    /// Receive the next bytes from the peer.
    ///
    /// Returns `None` once the peer's send half is dropped and the queue is drained —
    /// i.e. the channel is closed. For a socket-backed reliable-ordered channel the
    /// bytes are delivered in order and in full, but a single `recv` is *not*
    /// guaranteed to line up with a single peer `send` (message boundaries are the
    /// codec's concern); the in-memory loopback happens to preserve them 1:1.
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.rx.recv().await
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_should_report_supported_kinds() {
        let caps = ChannelCapabilities::none()
            .with(ChannelKind::ReliableOrdered)
            .with(ChannelKind::Unreliable);

        assert!(caps.supports(ChannelKind::ReliableOrdered));
        assert!(caps.supports(ChannelKind::Unreliable));
        assert!(!caps.supports(ChannelKind::ReliableUnordered));
        assert!(!caps.supports(ChannelKind::UnreliableSequenced));

        let kinds: Vec<_> = caps.kinds().collect();
        assert_eq!(
            kinds,
            vec![ChannelKind::ReliableOrdered, ChannelKind::Unreliable]
        );

        assert!(!caps.is_empty());
        assert!(ChannelCapabilities::none().is_empty());
    }

    #[test]
    fn capabilities_from_iter_should_equal_chained_with() {
        let from_iter: ChannelCapabilities =
            [ChannelKind::ReliableOrdered, ChannelKind::Unreliable]
                .into_iter()
                .collect();
        let chained = ChannelCapabilities::none()
            .with(ChannelKind::ReliableOrdered)
            .with(ChannelKind::Unreliable);
        assert_eq!(from_iter, chained);
    }

    #[tokio::test]
    async fn channel_pair_should_round_trip_both_directions() {
        let (mut a, mut b) = Channel::pair(ChannelKind::ReliableOrdered, 8);
        assert_eq!(a.kind(), ChannelKind::ReliableOrdered);

        a.send(Bytes::from_static(b"a->b")).await.unwrap();
        assert_eq!(b.recv().await, Some(Bytes::from_static(b"a->b")));

        b.send(Bytes::from_static(b"b->a")).await.unwrap();
        assert_eq!(a.recv().await, Some(Bytes::from_static(b"b->a")));
    }

    #[tokio::test]
    async fn channel_send_should_error_when_receiver_dropped() {
        let (a, b) = Channel::pair(ChannelKind::ReliableOrdered, 8);
        drop(b);
        let err = a.send(Bytes::from_static(b"x")).await.unwrap_err();
        assert!(matches!(err, TransportError::ConnectionClosed));
    }

    #[tokio::test]
    async fn channel_recv_should_return_none_when_sender_dropped() {
        let (a, mut b) = Channel::pair(ChannelKind::ReliableOrdered, 8);
        drop(a);
        assert_eq!(b.recv().await, None);
    }

    /// A connected pair with an explicit policy on both ends (for policy tests).
    fn pair_with_policy(
        kind: ChannelKind,
        capacity: usize,
        policy: BackpressurePolicy,
    ) -> (Channel, Channel) {
        let (a_tx, b_rx) = mpsc::channel(capacity);
        let (b_tx, a_rx) = mpsc::channel(capacity);
        (
            Channel::with_policy(kind, a_tx, a_rx, policy),
            Channel::with_policy(kind, b_tx, b_rx, policy),
        )
    }

    #[test]
    fn default_policy_should_match_channel_kind() {
        use BackpressurePolicy::{Block, DropNewest};
        assert_eq!(
            BackpressurePolicy::default_for(ChannelKind::ReliableOrdered),
            Block
        );
        assert_eq!(
            BackpressurePolicy::default_for(ChannelKind::ReliableUnordered),
            Block
        );
        assert_eq!(
            BackpressurePolicy::default_for(ChannelKind::UnreliableSequenced),
            DropNewest
        );
        assert_eq!(
            BackpressurePolicy::default_for(ChannelKind::Unreliable),
            DropNewest
        );
        // A reliable channel reports its derived policy.
        let (a, _b) = Channel::pair(ChannelKind::ReliableOrdered, 1);
        assert_eq!(a.policy(), Block);
    }

    #[tokio::test]
    async fn reliable_channel_send_should_block_when_full() {
        use std::time::Duration;
        use tokio::time::timeout;

        // Capacity 1: the second send has to wait for the receiver.
        let (a, mut b) = Channel::pair(ChannelKind::ReliableOrdered, 1);
        a.send(Bytes::from_static(b"1")).await.unwrap();

        // The queue is full, so this send blocks (times out).
        let blocked = timeout(Duration::from_millis(100), a.send(Bytes::from_static(b"2"))).await;
        assert!(
            blocked.is_err(),
            "reliable send must block when the queue is full"
        );

        // Draining a message frees capacity so a send can proceed.
        assert_eq!(b.recv().await, Some(Bytes::from_static(b"1")));
        a.send(Bytes::from_static(b"3")).await.unwrap();
    }

    #[tokio::test]
    async fn unreliable_channel_send_should_drop_when_full_without_blocking() {
        use std::time::Duration;
        use tokio::time::timeout;

        let (a, mut b) = Channel::pair(ChannelKind::Unreliable, 2);
        assert_eq!(a.policy(), BackpressurePolicy::DropNewest);

        // Far more sends than capacity, with nobody receiving: none may block.
        for i in 0..10u8 {
            let send = timeout(Duration::from_millis(100), a.send(Bytes::from(vec![i])));
            send.await
                .expect("unreliable send must never block")
                .unwrap();
        }

        // The queue held at most `capacity`, keeping the earliest (drop-newest).
        let mut got = Vec::new();
        while let Ok(Some(bytes)) = timeout(Duration::from_millis(50), b.recv()).await {
            got.push(bytes[0]);
        }
        assert!(
            got.len() <= 2,
            "unreliable channel must drop down to capacity"
        );
        assert_eq!(
            got.first(),
            Some(&0),
            "drop-newest keeps the oldest messages"
        );
    }

    #[tokio::test]
    async fn disconnect_policy_send_should_error_when_full() {
        let (a, _b) = pair_with_policy(
            ChannelKind::ReliableOrdered,
            1,
            BackpressurePolicy::Disconnect,
        );
        a.send(Bytes::from_static(b"1")).await.unwrap(); // fills the single slot

        let err = a.send(Bytes::from_static(b"2")).await.unwrap_err();
        assert!(matches!(err, TransportError::Backpressure));
    }
}
