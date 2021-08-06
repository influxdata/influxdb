use std::sync::Arc;

use metrics::MetricRegistry;
use object_store::ObjectStore;
use observability_deps::tracing::info;
use query::exec::Executor;

use crate::JobRegistry;

/// A container for application-global resources
/// shared between server and all DatabaseInstances
#[derive(Debug, Clone)]
pub struct ApplicationState {
    object_store: Arc<ObjectStore>,
    executor: Arc<Executor>,
    job_registry: Arc<JobRegistry>,
    metric_registry: Arc<MetricRegistry>,
}

impl ApplicationState {
    /// Creates a new `ApplicationState`
    ///
    /// Uses number of CPUs in the system if num_worker_threads is not set
    pub fn new(object_store: Arc<ObjectStore>, num_worker_threads: Option<usize>) -> Self {
        let num_threads = num_worker_threads.unwrap_or_else(num_cpus::get);
        info!(%num_threads, "using specified number of threads per thread pool");

        Self {
            object_store,
            executor: Arc::new(Executor::new(num_threads)),
            job_registry: Arc::new(JobRegistry::new()),
            metric_registry: Arc::new(metrics::MetricRegistry::new()),
        }
    }

    pub fn object_store(&self) -> &Arc<ObjectStore> {
        &self.object_store
    }

    pub fn job_registry(&self) -> &Arc<JobRegistry> {
        &self.job_registry
    }

    pub fn metric_registry(&self) -> &Arc<MetricRegistry> {
        &self.metric_registry
    }

    pub fn executor(&self) -> &Arc<Executor> {
        &self.executor
    }
}
