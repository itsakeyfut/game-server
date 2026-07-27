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

/// A bidirectional byte channel with a fixed [`ChannelKind`].
///
/// This is the per-message hot path, so it is a concrete type — no trait object,
/// no dynamic dispatch on send/recv. It is backed by a pair of bounded mpsc queues
/// (one per direction); the bound is the backpressure seam a later issue tunes.
/// Every kind currently applies backpressure ([`send`](Self::send) awaits when the
/// queue is full); per-kind drop policies for the unreliable kinds are deferred to
/// that same backpressure issue.
pub struct Channel {
    kind: ChannelKind,
    tx: mpsc::Sender<Bytes>,
    rx: mpsc::Receiver<Bytes>,
}

impl Channel {
    /// Create a connected, full-duplex pair of channels of the same `kind`.
    ///
    /// `capacity` bounds each direction's in-flight queue (the backpressure point).
    pub(crate) fn pair(kind: ChannelKind, capacity: usize) -> (Channel, Channel) {
        let (a_tx, b_rx) = mpsc::channel(capacity);
        let (b_tx, a_rx) = mpsc::channel(capacity);
        let a = Channel {
            kind,
            tx: a_tx,
            rx: a_rx,
        };
        let b = Channel {
            kind,
            tx: b_tx,
            rx: b_rx,
        };
        (a, b)
    }

    /// The reliability semantics this channel provides.
    #[must_use]
    pub fn kind(&self) -> ChannelKind {
        self.kind
    }

    /// Send `payload` to the peer, waiting if the send queue is full (backpressure).
    ///
    /// # Errors
    /// [`TransportError::ConnectionClosed`] if the peer's receive half has been dropped.
    pub async fn send(&self, payload: Bytes) -> Result<(), TransportError> {
        self.tx
            .send(payload)
            .await
            .map_err(|_| TransportError::ConnectionClosed)
    }

    /// Receive the next message from the peer.
    ///
    /// Returns `None` once the peer's send half is dropped and the queue is drained —
    /// i.e. the channel is closed.
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
}
