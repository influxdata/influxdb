use crate::config::{object_store::ConfigProviderObjectStorage, ConfigProvider};
use job_registry::JobRegistry;
use object_store::ObjectStore;
use observability_deps::tracing::info;
use query::exec::Executor;
use std::sync::Arc;
use time::TimeProvider;
use trace::TraceCollector;
use write_buffer::config::WriteBufferConfigFactory;

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
    config_provider: Arc<dyn ConfigProvider>,
}

impl ApplicationState {
    /// Creates a new `ApplicationState`
    ///
    /// Uses number of CPUs in the system if num_worker_threads is not set
    pub fn new(
        object_store: Arc<ObjectStore>,
        num_worker_threads: Option<usize>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        config_provider: Option<Arc<dyn ConfigProvider>>,
    ) -> Self {
        let num_threads = num_worker_threads.unwrap_or_else(num_cpus::get);
        info!(%num_threads, "using specified number of threads per thread pool");

        let metric_registry = Arc::new(metric::Registry::new());
        let time_provider: Arc<dyn TimeProvider> = Arc::new(time::SystemProvider::new());
        let job_registry = Arc::new(JobRegistry::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));

        let write_buffer_factory = Arc::new(WriteBufferConfigFactory::new(
            Arc::clone(&time_provider),
            Arc::clone(&metric_registry),
        ));

        let config_provider = config_provider.unwrap_or_else(|| {
            Arc::new(ConfigProviderObjectStorage::new(
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
            ))
        });

        Self {
            object_store,
            write_buffer_factory,
            executor: Arc::new(Executor::new(num_threads)),
            job_registry,
            metric_registry,
            time_provider,
            trace_collector,
            config_provider,
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

    pub fn config_provider(&self) -> &Arc<dyn ConfigProvider> {
        &self.config_provider
    }

    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    pub fn join(&self) {
        self.executor.join()
    }
}
