//! The [`Request`] envelope that flows through the pipeline, and its [`Extensions`].

use std::any::{Any, TypeId};
use std::collections::HashMap;

use bytes::Bytes;
use gsf_session::RoomId;

/// Identifies the connection a message arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// A type-keyed bag of data that middleware attach to a [`Request`] for those downstream
/// (the http / tower `Extensions` pattern) — e.g. an auth middleware inserts an
/// [`Identity`](gsf_session::Identity).
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    /// An empty set of extensions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value, returning the previous one of the same type if any.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|prev| prev.downcast::<T>().ok().map(|boxed| *boxed))
    }

    /// Borrow the value of type `T`, if present.
    #[must_use]
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }

    /// Mutably borrow the value of type `T`, if present.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_mut::<T>())
    }

    /// How many values are attached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether no values are attached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The values are type-erased; show only how many are attached.
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish()
    }
}

/// An inbound message flowing through the [`Pipeline`](crate::Pipeline): the connection it
/// arrived on, its payload, an optional destination room, and middleware [`Extensions`].
#[derive(Debug)]
pub struct Request {
    connection: ConnectionId,
    payload: Bytes,
    destination: Option<RoomId>,
    extensions: Extensions,
}

impl Request {
    /// Build a request for `payload` received on `connection` (no destination yet).
    #[must_use]
    pub fn new(connection: ConnectionId, payload: Bytes) -> Self {
        Self {
            connection,
            payload,
            destination: None,
            extensions: Extensions::new(),
        }
    }

    /// The connection this message arrived on.
    #[must_use]
    pub fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// The message payload.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The destination room instance, once resolved.
    #[must_use]
    pub fn destination(&self) -> Option<RoomId> {
        self.destination
    }

    /// Set the destination room instance.
    pub fn set_destination(&mut self, room: RoomId) {
        self.destination = Some(room);
    }

    /// The middleware extensions.
    #[must_use]
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// The middleware extensions, mutably.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensions_should_round_trip_a_value() {
        let mut ext = Extensions::new();
        assert!(ext.get::<u32>().is_none());
        assert_eq!(ext.insert(7u32), None);
        assert_eq!(ext.get::<u32>(), Some(&7));
        *ext.get_mut::<u32>().unwrap() = 9;
        assert_eq!(ext.get::<u32>(), Some(&9));
    }

    #[test]
    fn extensions_insert_should_return_and_replace_the_previous_value() {
        let mut ext = Extensions::new();
        ext.insert(1u32);
        assert_eq!(ext.insert(2u32), Some(1)); // replaced, previous returned
        assert_eq!(ext.get::<u32>(), Some(&2));
        assert_eq!(ext.len(), 1);
    }

    #[test]
    fn extensions_should_key_by_type() {
        let mut ext = Extensions::new();
        ext.insert(1u32);
        ext.insert("hi".to_string());
        assert_eq!(ext.get::<u32>(), Some(&1));
        assert_eq!(ext.get::<String>(), Some(&"hi".to_string()));
        assert!(ext.get::<i64>().is_none());
    }
}
