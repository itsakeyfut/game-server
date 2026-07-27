//! The message registry: a type-safe map from a [`PacketId`] to the packet type
//! that claims it, rejecting id collisions.
//!
//! Two message types declared with the same [`PacketId`] is a protocol bug — a
//! received id would decode to whichever type happened to win. The registry catches
//! that class of bug at startup: register each [`Packet`] type, and a *different* type
//! reusing an already-claimed id is a [`RegistryError::DuplicateId`].
//!
//! ```
//! use gsf_codec::{ChannelKind, Packet, PacketId, Registry, Visibility};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Packet)]
//! #[packet(id = 1, channel = ReliableOrdered, visibility = Internal)]
//! struct Chat { text: String }
//!
//! #[derive(Serialize, Deserialize, Packet)]
//! #[packet(id = 2, channel = Unreliable, visibility = Public)]
//! struct Move { x: f32, y: f32 }
//!
//! let mut registry = Registry::new();
//! registry.register::<Chat>().unwrap().register::<Move>().unwrap();
//!
//! assert_eq!(registry.get(PacketId(1)).unwrap().channel, ChannelKind::ReliableOrdered);
//! assert_eq!(registry.get(PacketId(2)).unwrap().visibility, Visibility::Public);
//! ```

use std::any::{TypeId, type_name};
use std::collections::HashMap;

use crate::{ChannelKind, Packet, PacketId, Visibility};

/// The metadata a [`Registry`] holds for one registered packet type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredPacket {
    /// The id this type claims.
    pub id: PacketId,
    /// The type's [`TypeId`] — the registry's notion of "which type" claims [`id`](Self::id).
    pub type_id: TypeId,
    /// The type's name, for diagnostics (e.g. a collision message).
    pub type_name: &'static str,
    /// The reliability channel the type is sent on.
    pub channel: ChannelKind,
    /// Whether the type is internal or public.
    pub visibility: Visibility,
}

/// An error building a [`Registry`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegistryError {
    /// Two different packet types declared the same [`PacketId`].
    #[error("packet id {id:?} is claimed by both `{existing}` and `{new}`")]
    DuplicateId {
        /// The colliding id.
        id: PacketId,
        /// The type that claimed the id first.
        existing: &'static str,
        /// The type that tried to reuse it.
        new: &'static str,
    },
}

/// A type-safe map from [`PacketId`] to the [`Packet`] type that claims it, built by
/// registering types and rejecting id collisions.
///
/// Register with [`register`](Self::register); look an id up with [`get`](Self::get).
#[derive(Debug, Default, Clone)]
pub struct Registry {
    by_id: HashMap<PacketId, RegisteredPacket>,
}

impl Registry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register packet type `P` under its [`Packet::ID`].
    ///
    /// Returns `&mut Self` so registrations chain: `registry.register::<A>()?.register::<B>()?`.
    /// Re-registering the *same* type is idempotent (a no-op that returns `Ok`).
    ///
    /// # Errors
    ///
    /// [`RegistryError::DuplicateId`] if a *different* type already claimed `P::ID`.
    pub fn register<P: Packet + 'static>(&mut self) -> Result<&mut Self, RegistryError> {
        let type_id = TypeId::of::<P>();
        if let Some(existing) = self.by_id.get(&P::ID) {
            if existing.type_id == type_id {
                return Ok(self); // same type re-registered — idempotent
            }
            return Err(RegistryError::DuplicateId {
                id: P::ID,
                existing: existing.type_name,
                new: type_name::<P>(),
            });
        }
        self.by_id.insert(
            P::ID,
            RegisteredPacket {
                id: P::ID,
                type_id,
                type_name: type_name::<P>(),
                channel: P::CHANNEL,
                visibility: P::VISIBILITY,
            },
        );
        Ok(self)
    }

    /// Look up the type registered under `id`.
    #[must_use]
    pub fn get(&self, id: PacketId) -> Option<RegisteredPacket> {
        self.by_id.get(&id).copied()
    }

    /// The number of registered packet types.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether no packet types are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    // Test packets defined via the explicit (hand-written) `impl Packet` — so codec's
    // own tests need no `#[derive(Packet)]` (which would want `extern crate self`).
    #[derive(Serialize, Deserialize)]
    struct Alpha;
    impl Packet for Alpha {
        const ID: PacketId = PacketId(1);
        const CHANNEL: ChannelKind = ChannelKind::ReliableOrdered;
        const VISIBILITY: Visibility = Visibility::Internal;
    }

    #[derive(Serialize, Deserialize)]
    struct Beta;
    impl Packet for Beta {
        const ID: PacketId = PacketId(2);
        const CHANNEL: ChannelKind = ChannelKind::Unreliable;
        const VISIBILITY: Visibility = Visibility::Public;
    }

    // A *different* type claiming the same id as `Alpha` — the collision case.
    #[derive(Serialize, Deserialize)]
    struct AlphaClash;
    impl Packet for AlphaClash {
        const ID: PacketId = PacketId(1);
        const CHANNEL: ChannelKind = ChannelKind::Unreliable;
        const VISIBILITY: Visibility = Visibility::Internal;
    }

    #[test]
    fn register_should_map_id_to_the_packet_metadata() {
        let mut registry = Registry::new();
        registry.register::<Alpha>().unwrap();

        let entry = registry
            .get(PacketId(1))
            .expect("Alpha should be registered");
        assert_eq!(entry.id, PacketId(1));
        assert_eq!(entry.type_id, TypeId::of::<Alpha>());
        assert_eq!(entry.channel, ChannelKind::ReliableOrdered);
        assert_eq!(entry.visibility, Visibility::Internal);
    }

    #[test]
    fn register_should_detect_a_colliding_id() {
        let mut registry = Registry::new();
        registry.register::<Alpha>().unwrap();

        let err = registry
            .register::<AlphaClash>()
            .expect_err("a different type reusing id 1 should collide");
        match err {
            RegistryError::DuplicateId { id, existing, new } => {
                assert_eq!(id, PacketId(1));
                assert!(existing.contains("Alpha"));
                assert!(new.contains("AlphaClash"));
            }
        }
        // The first registration is untouched.
        assert_eq!(
            registry.get(PacketId(1)).unwrap().type_id,
            TypeId::of::<Alpha>()
        );
    }

    #[test]
    fn register_should_be_idempotent_for_the_same_type() {
        let mut registry = Registry::new();
        registry.register::<Alpha>().unwrap();
        registry
            .register::<Alpha>()
            .expect("re-registering the same type should be Ok");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn get_should_return_none_for_an_unregistered_id() {
        let mut registry = Registry::new();
        registry.register::<Alpha>().unwrap();
        assert!(registry.get(PacketId(999)).is_none());
    }

    #[test]
    fn register_should_chain_multiple_packets() {
        let mut registry = Registry::new();
        registry
            .register::<Alpha>()
            .unwrap()
            .register::<Beta>()
            .unwrap();
        assert_eq!(registry.len(), 2);
        assert!(registry.get(PacketId(1)).is_some());
        assert!(registry.get(PacketId(2)).is_some());
    }

    #[test]
    fn new_registry_should_be_empty() {
        let registry = Registry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }
}
