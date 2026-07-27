//! The [`App`] builder and its dependency-ordered plugin lifecycle.

use std::any::{TypeId, type_name};
use std::collections::{HashMap, VecDeque};

use gsf_lifecycle::ShutdownListener;

use crate::plugin::{AppError, Plugin, PluginId};

struct PluginEntry {
    id: TypeId,
    name: &'static str,
    deps: Vec<PluginId>,
    plugin: Box<dyn Plugin>,
}

/// The center of a server: register [`Plugin`]s, then [`run`](App::run) them through a
/// dependency-ordered lifecycle (programming-model §1.1).
///
/// ```no_run
/// use gsf_app::App;
/// use gsf_lifecycle::Shutdown;
///
/// # async fn example() -> Result<(), gsf_app::AppError> {
/// let shutdown = Shutdown::new();
/// let mut app = App::new();
/// // app.add_plugin(TransportPlugin::default());
/// app.run(shutdown.subscribe()).await
/// # }
/// ```
#[derive(Default)]
pub struct App {
    plugins: Vec<PluginEntry>,
}

impl App {
    /// Create an empty app.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a plugin. Returns `&mut self` so calls chain.
    ///
    /// A plugin type added twice is rejected later, at [`run`](App::run), as
    /// [`AppError::DuplicatePlugin`].
    pub fn add_plugin<P: Plugin + 'static>(&mut self, plugin: P) -> &mut Self {
        self.plugins.push(PluginEntry {
            id: TypeId::of::<P>(),
            name: type_name::<P>(),
            deps: plugin.dependencies(),
            plugin: Box::new(plugin),
        });
        self
    }

    /// Resolve the plugins into a dependency-first build order.
    ///
    /// # Errors
    /// [`AppError::DuplicatePlugin`], [`AppError::MissingDependency`], or
    /// [`AppError::CyclicDependency`].
    fn plan(&self) -> Result<Vec<usize>, AppError> {
        // Map each plugin type to its index, rejecting duplicates.
        let mut index = HashMap::with_capacity(self.plugins.len());
        for (i, entry) in self.plugins.iter().enumerate() {
            if index.insert(entry.id, i).is_some() {
                return Err(AppError::DuplicatePlugin { plugin: entry.name });
            }
        }

        // Resolve each plugin's dependency type-ids to indices (missing → error).
        let mut deps: Vec<Vec<usize>> = Vec::with_capacity(self.plugins.len());
        for entry in &self.plugins {
            let mut resolved = Vec::with_capacity(entry.deps.len());
            for dep in &entry.deps {
                let &di = index
                    .get(&dep.type_id())
                    .ok_or(AppError::MissingDependency {
                        plugin: entry.name,
                        dependency: dep.name(),
                    })?;
                resolved.push(di);
            }
            deps.push(resolved);
        }

        topological_order(&deps).map_err(|cyclic| AppError::CyclicDependency {
            plugins: cyclic.iter().map(|&i| self.plugins[i].name).collect(),
        })
    }

    /// Build, start, run, and stop the registered plugins.
    ///
    /// Plugins are [built](Plugin::build) then [started](Plugin::on_startup) in dependency
    /// order; if a startup fails, the plugins already started are stopped in reverse and
    /// the error is returned. Otherwise the app waits for `shutdown`, then
    /// [stops](Plugin::on_shutdown) every plugin in reverse dependency order.
    ///
    /// **Cancellation:** this future is not cancellation-safe — if it is dropped after
    /// startup (e.g. cancelled by an outer `select!`), the reverse `on_shutdown` pass is
    /// skipped and started plugins leak their resources. Drive shutdown through the
    /// `shutdown` signal, not by cancelling `run`.
    ///
    /// # Errors
    /// Any [`AppError`] from resolving the dependency order (duplicate / missing / cyclic
    /// plugins) or from a plugin's startup.
    pub async fn run(mut self, mut shutdown: ShutdownListener) -> Result<(), AppError> {
        let order = self.plan()?;
        // Move the plugins out so `build(&mut self)` does not alias the plugin list.
        let plugins = std::mem::take(&mut self.plugins);

        for &i in &order {
            plugins[i].plugin.build(&mut self);
        }

        for (started, &i) in order.iter().enumerate() {
            if let Err(source) = plugins[i].plugin.on_startup().await {
                // Roll back the plugins already started, in reverse.
                for &j in order[..started].iter().rev() {
                    plugins[j].plugin.on_shutdown().await;
                }
                return Err(AppError::Startup {
                    plugin: plugins[i].name,
                    source,
                });
            }
        }

        shutdown.wait().await;

        for &i in order.iter().rev() {
            plugins[i].plugin.on_shutdown().await;
        }
        Ok(())
    }
}

/// Kahn's topological sort over a dependency graph: `deps[i]` lists the indices node `i`
/// depends on. Returns a dependency-first order (each node after all its dependencies),
/// or, on a cycle, the set of nodes that could not be ordered.
///
/// Ties are broken by ascending index, so the order is deterministic. Out-of-range
/// dependency indices are ignored rather than panicking (callers resolve real indices,
/// but the pure function stays total).
fn topological_order(deps: &[Vec<usize>]) -> Result<Vec<usize>, Vec<usize>> {
    let n = deps.len();
    let mut in_degree = vec![0usize; n];
    // `dependents[d]` = nodes that depend on `d`.
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (node, node_deps) in deps.iter().enumerate() {
        for &d in node_deps {
            if d < n {
                dependents[d].push(node);
                in_degree[node] += 1;
            }
        }
    }

    // Seed with dependency-free nodes in ascending index order (deterministic).
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    while let Some(node) = queue.pop_front() {
        order.push(node);
        for &dependent in &dependents[node] {
            in_degree[dependent] -= 1;
            if in_degree[dependent] == 0 {
                queue.push_back(dependent);
            }
        }
    }

    if order.len() == n {
        Ok(order)
    } else {
        // Whatever never reached in-degree 0 is part of (or downstream of) a cycle.
        Err((0..n).filter(|&i| in_degree[i] > 0).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use gsf_lifecycle::Shutdown;
    use proptest::prelude::*;

    use super::*;
    use crate::plugin::BoxError;

    /// A record of which plugin did what, in order (`"build:A"`, `"start:B"`, `"stop:C"`).
    type Log = Arc<Mutex<Vec<String>>>;

    // Distinct marker types → distinct plugin ids in the graph.
    struct A;
    struct B;
    struct C;
    struct D;

    /// A test plugin keyed to marker `M`, so its registered id and any dependency edge to
    /// it are the *same* `PluginId::of::<TestPlugin<M>>()`. Logs its lifecycle phases.
    struct TestPlugin<M: Send + Sync + 'static> {
        tag: &'static str,
        log: Log,
        deps: Vec<PluginId>,
        fail_startup: bool,
        _marker: std::marker::PhantomData<fn() -> M>,
    }

    impl<M: Send + Sync + 'static> TestPlugin<M> {
        fn new(tag: &'static str, log: &Log) -> Self {
            Self {
                tag,
                log: Arc::clone(log),
                deps: Vec::new(),
                fail_startup: false,
                _marker: std::marker::PhantomData,
            }
        }
        /// Declare a dependency on the plugin keyed to marker `D`.
        fn depends_on<D: Send + Sync + 'static>(mut self) -> Self {
            self.deps.push(PluginId::of::<TestPlugin<D>>());
            self
        }
        fn failing(mut self) -> Self {
            self.fail_startup = true;
            self
        }
        fn push(&self, phase: &str) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{phase}:{}", self.tag));
        }
    }

    #[async_trait]
    impl<M: Send + Sync + 'static> Plugin for TestPlugin<M> {
        fn build(&self, _app: &mut App) {
            self.push("build");
        }
        fn dependencies(&self) -> Vec<PluginId> {
            self.deps.clone()
        }
        async fn on_startup(&self) -> Result<(), BoxError> {
            self.push("start");
            if self.fail_startup {
                return Err("boom".into());
            }
            Ok(())
        }
        async fn on_shutdown(&self) {
            self.push("stop");
        }
    }

    /// Run `app` to completion with an already-triggered shutdown (so `run` builds,
    /// starts, and immediately drains).
    async fn run_draining(app: App) -> Result<(), AppError> {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        app.run(shutdown.subscribe()).await
    }

    #[tokio::test]
    async fn plugins_should_build_start_and_stop_in_dependency_order() {
        // C depends on B, B on A, added out of order → build/start A,B,C; stop C,B,A.
        let log: Log = Log::default();
        let mut app = App::new();
        app.add_plugin(
            TestPlugin::<C>::new("C", &log)
                .depends_on::<B>()
                .depends_on::<A>(),
        );
        app.add_plugin(TestPlugin::<A>::new("A", &log));
        app.add_plugin(TestPlugin::<B>::new("B", &log).depends_on::<A>());

        run_draining(app).await.unwrap();

        let events = log.lock().unwrap().clone();
        let phase = |p: &str| -> Vec<String> {
            events
                .iter()
                .filter(|e| e.starts_with(p))
                .cloned()
                .collect()
        };
        assert_eq!(phase("build"), ["build:A", "build:B", "build:C"]);
        assert_eq!(phase("start"), ["start:A", "start:B", "start:C"]);
        assert_eq!(phase("stop"), ["stop:C", "stop:B", "stop:A"]);
    }

    #[tokio::test]
    async fn run_should_error_on_a_dependency_cycle() {
        let log: Log = Log::default();
        let mut app = App::new();
        // A depends on B, B depends on A.
        app.add_plugin(TestPlugin::<A>::new("A", &log).depends_on::<B>());
        app.add_plugin(TestPlugin::<B>::new("B", &log).depends_on::<A>());
        let err = run_draining(app).await.unwrap_err();
        assert!(matches!(err, AppError::CyclicDependency { .. }));
    }

    #[tokio::test]
    async fn run_should_error_on_a_missing_dependency() {
        let log: Log = Log::default();
        let mut app = App::new();
        // A depends on C, which is never added.
        app.add_plugin(TestPlugin::<A>::new("A", &log).depends_on::<C>());
        let err = run_draining(app).await.unwrap_err();
        assert!(matches!(err, AppError::MissingDependency { .. }));
    }

    #[tokio::test]
    async fn run_should_error_on_a_duplicate_plugin() {
        let log: Log = Log::default();
        let mut app = App::new();
        app.add_plugin(TestPlugin::<A>::new("A1", &log));
        app.add_plugin(TestPlugin::<A>::new("A2", &log));
        let err = run_draining(app).await.unwrap_err();
        assert!(matches!(err, AppError::DuplicatePlugin { .. }));
    }

    #[tokio::test]
    async fn empty_app_should_run_cleanly() {
        // No plugins → build/startup/shutdown are all no-ops, `run` returns Ok.
        run_draining(App::new()).await.unwrap();
    }

    #[tokio::test]
    async fn run_should_wait_for_the_shutdown_signal_then_drain() {
        // Startup completes, but `run` must not finish until the shutdown fires; only then
        // does it drain (stop the plugins).
        let log: Log = Log::default();
        let mut app = App::new();
        app.add_plugin(TestPlugin::<A>::new("A", &log));
        app.add_plugin(TestPlugin::<B>::new("B", &log).depends_on::<A>());

        let shutdown = Shutdown::new();
        let handle = tokio::spawn(app.run(shutdown.subscribe()));

        // Let the spawned task build, start, and park at `shutdown.wait()`.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !handle.is_finished(),
            "run must wait for the shutdown signal"
        );
        {
            let events = log.lock().unwrap();
            assert!(events.contains(&"start:B".to_string()), "startup ran");
            assert!(
                !events.iter().any(|e| e.starts_with("stop")),
                "no drain before shutdown"
            );
        }

        shutdown.trigger();
        handle.await.unwrap().unwrap();
        // Drained in reverse after the signal.
        assert!(log.lock().unwrap().contains(&"stop:A".to_string()));
    }

    #[tokio::test]
    async fn startup_failure_should_roll_back_started_plugins() {
        let log: Log = Log::default();
        let mut app = App::new();
        // A then B start; C fails; D depends on C and must never start.
        app.add_plugin(TestPlugin::<A>::new("A", &log));
        app.add_plugin(TestPlugin::<B>::new("B", &log).depends_on::<A>());
        app.add_plugin(TestPlugin::<C>::new("C", &log).depends_on::<B>().failing());
        app.add_plugin(TestPlugin::<D>::new("D", &log).depends_on::<C>());

        let err = run_draining(app).await.unwrap_err();
        assert!(matches!(err, AppError::Startup { .. }));

        let events = log.lock().unwrap().clone();
        // A and B started; on C's failure they were stopped in reverse (B then A).
        let stops: Vec<&String> = events.iter().filter(|e| e.starts_with("stop")).collect();
        assert_eq!(stops, ["stop:B", "stop:A"]);
        // The failed plugin C is NOT shut down, and D (downstream) never started.
        assert!(!events.iter().any(|e| e == "stop:C"));
        assert!(!events.iter().any(|e| e == "start:D"));
    }

    #[test]
    fn topological_order_should_be_dependency_first_on_a_chain() {
        // 2 → 1 → 0 (node 2 depends on 1, 1 on 0).
        let deps = vec![vec![], vec![0], vec![1]];
        assert_eq!(topological_order(&deps), Ok(vec![0, 1, 2]));
    }

    #[test]
    fn topological_order_should_detect_a_self_cycle() {
        let deps = vec![vec![0]]; // node 0 depends on itself
        assert_eq!(topological_order(&deps), Err(vec![0]));
    }

    #[test]
    fn topological_order_should_detect_a_two_node_cycle() {
        let deps = vec![vec![1], vec![0]]; // 0↔1
        assert_eq!(topological_order(&deps), Err(vec![0, 1]));
    }

    #[test]
    fn topological_order_should_report_the_cycle_and_its_downstream() {
        // 0 is fine; 1↔2 cycle. The unorderable set is {1, 2}, not 0.
        let deps = vec![vec![], vec![2], vec![1]];
        assert_eq!(topological_order(&deps), Err(vec![1, 2]));
    }

    proptest! {
        /// An acyclic-by-construction graph (`deps[i] ⊆ 0..i`) always orders, is a
        /// permutation of `0..n`, and is dependency-first.
        #[test]
        fn acyclic_graphs_should_always_order_dependency_first(
            raw in prop::collection::vec(prop::collection::vec(any::<usize>(), 0..4), 0..12),
        ) {
            let n = raw.len();
            // Force each node to depend only on earlier nodes → acyclic.
            let deps: Vec<Vec<usize>> = raw
                .iter()
                .enumerate()
                .map(|(i, ds)| ds.iter().filter(|_| i > 0).map(|d| d % i).collect())
                .collect();

            let order = topological_order(&deps).expect("acyclic graph must order");
            prop_assert_eq!(order.len(), n);

            let position: std::collections::HashMap<usize, usize> =
                order.iter().enumerate().map(|(pos, &node)| (node, pos)).collect();
            prop_assert_eq!(position.len(), n); // a permutation (no repeats)
            for (node, node_deps) in deps.iter().enumerate() {
                for &d in node_deps {
                    prop_assert!(position[&d] < position[&node], "dependency must precede");
                }
            }
        }

        /// Arbitrary graphs never panic; a returned order is dependency-first, and a cycle
        /// report is non-empty.
        #[test]
        fn arbitrary_graphs_should_never_panic(
            deps in prop::collection::vec(prop::collection::vec(0usize..12, 0..4), 0..12),
        ) {
            match topological_order(&deps) {
                Ok(order) => {
                    let position: std::collections::HashMap<usize, usize> =
                        order.iter().enumerate().map(|(pos, &node)| (node, pos)).collect();
                    for (node, node_deps) in deps.iter().enumerate() {
                        for &d in node_deps {
                            if d < deps.len() {
                                prop_assert!(position[&d] < position[&node]);
                            }
                        }
                    }
                }
                Err(cyclic) => prop_assert!(!cyclic.is_empty()),
            }
        }
    }
}
