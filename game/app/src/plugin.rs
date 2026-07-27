//! The [`Plugin`] trait and the errors an [`App`] can raise while assembling one.

use std::any::{TypeId, type_name};

use async_trait::async_trait;

use crate::App;

/// A boxed, thread-safe error — what a plugin's async startup may fail with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Identifies a plugin type — its [`TypeId`] plus its name for diagnostics. Build one
/// with [`PluginId::of`] to name a plugin in [`Plugin::dependencies`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PluginId {
    id: TypeId,
    name: &'static str,
}

impl PluginId {
    /// The id of plugin type `P`.
    #[must_use]
    pub fn of<P: 'static>() -> Self {
        Self {
            id: TypeId::of::<P>(),
            name: type_name::<P>(),
        }
    }

    /// The identified type's [`TypeId`].
    #[must_use]
    pub fn type_id(&self) -> TypeId {
        self.id
    }

    /// The identified type's name (for diagnostics).
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// A unit of functionality composed into an [`App`] (programming-model §1.1). Genre
/// layers, services, LiveOps, and the cache client are all plugins.
///
/// A plugin declares the plugin types it [`depends on`](Plugin::dependencies); the app
/// builds it, starts it, and stops it **in dependency order** (§1.6). Only
/// [`build`](Plugin::build) is required — the rest default to no-ops.
///
/// ```
/// use gsf_app::{App, Plugin};
///
/// struct Logging;
/// impl Plugin for Logging {
///     fn build(&self, _app: &mut App) { /* register logging middleware, config, … */ }
/// }
/// ```
#[async_trait]
pub trait Plugin: Send + Sync {
    /// Register this plugin's features into `app`. Called once, in dependency order.
    ///
    /// Note: in this milestone `App` exposes no registration surface yet (handlers,
    /// rooms, transport, a resource store) and no server message loop consumes what
    /// `build` records — the registration API arrives in a later issue. Today `build`
    /// is the ordered hook; its `&mut App` mutations are not yet used.
    fn build(&self, app: &mut App);

    /// The plugin types this plugin depends on — built and started before it, stopped
    /// after it. Defaults to none. Name each with [`PluginId::of::<T>()`](PluginId::of).
    ///
    /// (Dependencies are declared here, rather than via a builder call inside
    /// [`build`](Self::build), because the app must know them *before* it can order
    /// `build` topologically — programming-model §1.6's `depends_on` is illustrative.)
    fn dependencies(&self) -> Vec<PluginId> {
        Vec::new()
    }

    /// Asynchronous startup (bind ports, open connections, …), run in dependency order
    /// after every plugin has been built.
    ///
    /// # Errors
    /// Any [`BoxError`]; the app then rolls back the plugins already started and aborts.
    async fn on_startup(&self) -> Result<(), BoxError> {
        Ok(())
    }

    /// Asynchronous shutdown, run in reverse dependency order when the app drains.
    async fn on_shutdown(&self) {}
}

/// An error assembling or running an [`App`]'s plugins.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AppError {
    /// The same plugin type was added more than once.
    #[error("plugin `{plugin}` is registered more than once")]
    DuplicatePlugin {
        /// The duplicated plugin's type name.
        plugin: &'static str,
    },
    /// A plugin depends on a plugin type that was never added.
    #[error("plugin `{plugin}` depends on an unregistered plugin `{dependency}`")]
    MissingDependency {
        /// The dependent plugin's type name.
        plugin: &'static str,
        /// The missing dependency's type name.
        dependency: &'static str,
    },
    /// The plugin dependency graph could not be ordered — a cycle, or a plugin downstream
    /// of one.
    #[error("plugins could not be ordered (a dependency cycle, or downstream of one): {plugins:?}")]
    CyclicDependency {
        /// The type names of the plugins that could not be ordered.
        plugins: Vec<&'static str>,
    },
    /// A plugin's [`on_startup`](Plugin::on_startup) failed.
    #[error("plugin `{plugin}` failed to start")]
    Startup {
        /// The plugin that failed to start.
        plugin: &'static str,
        /// The underlying startup error.
        #[source]
        source: BoxError,
    },
}
