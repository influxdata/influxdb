use std::sync::Arc;

use object_store::ObjectStore;
use observability_deps::tracing::info;
use query::exec::Executor;
use time::TimeProvider;
use trace::TraceCollector;
use write_buffer::config::WriteBufferConfigFactory;

use crate::JobRegistry;

/// A container for application-global resources
/// shared between server and all DatabaseInstances
#[derive(Debug, Clone)]
pub struct ApplicationState {
    object_store: Arc<ObjectStore>,
    write_buffer_factory: Arc<WriteBufferConfigFactory>,
    executor: Arc<Executor>,
    job_registry: Arc<JobRegistry>,
    metric_registry: Arc<metric::Registry>,
    time_provider: Arc<dyn TimeProvider>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl ApplicationState {
    /// Creates a new `ApplicationState`
    ///
    /// Uses number of CPUs in the system if num_worker_threads is not set
    pub fn new(
        object_store: Arc<ObjectStore>,
        num_worker_threads: Option<usize>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Self {
        let num_threads = num_worker_threads.unwrap_or_else(num_cpus::get);
        info!(%num_threads, "using specified number of threads per thread pool");

        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider: Arc<dyn TimeProvider> = Arc::new(time::SystemProvider::new());
        let job_registry = Arc::new(JobRegistry::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));

        let write_buffer_factory =
            Arc::new(WriteBufferConfigFactory::new(Arc::clone(&time_provider)));

        Self {
            object_store,
            write_buffer_factory,
            executor: Arc::new(Executor::new(num_threads)),
            job_registry,
            metric_registry,
            time_provider,
            trace_collector,
        }
    }

    pub fn object_store(&self) -> &Arc<ObjectStore> {
        &self.object_store
    }

    pub fn write_buffer_factory(&self) -> &Arc<WriteBufferConfigFactory> {
        &self.write_buffer_factory
    }

    pub fn job_registry(&self) -> &Arc<JobRegistry> {
        &self.job_registry
    }

    pub fn metric_registry(&self) -> &Arc<metric::Registry> {
        &self.metric_registry
    }

    pub fn time_provider(&self) -> &Arc<dyn TimeProvider> {
        &self.time_provider
    }

    pub fn trace_collector(&self) -> &Option<Arc<dyn TraceCollector>> {
        &self.trace_collector
    }

    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    pub fn join(&self) {
        self.executor.join()
    }
}
