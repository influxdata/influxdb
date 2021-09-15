use std::sync::Arc;

use object_store::ObjectStore;
use observability_deps::tracing::info;
use query::exec::Executor;
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
}

impl ApplicationState {
    /// Creates a new `ApplicationState`
    ///
    /// Uses number of CPUs in the system if num_worker_threads is not set
    pub fn new(object_store: Arc<ObjectStore>, num_worker_threads: Option<usize>) -> Self {
        Self::with_write_buffer_factory(
            object_store,
            Arc::new(Default::default()),
            num_worker_threads,
        )
    }

    /// Same as [`new`](Self::new) but also specifies the write buffer factory.
    ///
    /// This is mostly useful for testing.
    pub fn with_write_buffer_factory(
        object_store: Arc<ObjectStore>,
        write_buffer_factory: Arc<WriteBufferConfigFactory>,
        num_worker_threads: Option<usize>,
    ) -> Self {
        let num_threads = num_worker_threads.unwrap_or_else(num_cpus::get);
        info!(%num_threads, "using specified number of threads per thread pool");

        let metric_registry = Arc::new(metric::Registry::new());
        let job_registry = Arc::new(JobRegistry::new(Arc::clone(&metric_registry)));

        Self {
            object_store,
            write_buffer_factory,
            executor: Arc::new(Executor::new(num_threads)),
            job_registry,
            metric_registry,
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

    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }

    pub fn join(&self) {
        self.executor.join()
    }
}
