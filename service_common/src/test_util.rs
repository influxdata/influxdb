use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use iox_query::{exec::Executor, test::TestDatabase};
use parking_lot::Mutex;
use trace::span::Span;
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

use crate::QueryNamespaceProvider;

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
    pub metric_registry: Arc<metric::Registry>,
    pub query_semaphore: Arc<InstrumentedAsyncSemaphore>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_semaphore_size(semaphore_size: usize) -> Self {
        let metric_registry = Arc::new(metric::Registry::default());
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metric_registry,
            &[("semaphore", "query_execution")],
        ));
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new_testing()),
            metric_registry,
            query_semaphore: Arc::new(semaphore_metrics.new_semaphore(semaphore_size)),
        }
    }

    pub async fn db_or_create(&self, name: &str) -> Arc<TestDatabase> {
        let mut databases = self.databases.lock();

        if let Some(db) = databases.get(name) {
            Arc::clone(db)
        } else {
            let new_db = Arc::new(TestDatabase::new(Arc::clone(&self.executor)));
            databases.insert(name.to_string(), Arc::clone(&new_db));
            new_db
        }
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self::new_with_semaphore_size(u16::MAX as usize)
    }
}

#[async_trait]
impl QueryNamespaceProvider for TestDatabaseStore {
    type Db = TestDatabase;

    /// Retrieve the database specified name
    async fn db(&self, name: &str, _span: Option<Span>) -> Option<Arc<Self::Db>> {
        let databases = self.databases.lock();

        databases.get(name).cloned()
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_semaphore)
            .acquire_owned(span)
            .await
            .unwrap()
    }
}
