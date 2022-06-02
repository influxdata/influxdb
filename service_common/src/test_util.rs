use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use iox_query::{exec::Executor, test::TestDatabase};
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::QueryDatabaseProvider;

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
    pub metric_registry: Arc<metric::Registry>,
    pub query_semaphore: Arc<Semaphore>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_semaphore_size(semaphore_size: usize) -> Self {
        Self {
            query_semaphore: Arc::new(Semaphore::new(semaphore_size)),
            ..Default::default()
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
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new(1)),
            metric_registry: Default::default(),
            query_semaphore: Arc::new(Semaphore::new(u16::MAX as usize)),
        }
    }
}

#[async_trait]
impl QueryDatabaseProvider for TestDatabaseStore {
    type Db = TestDatabase;

    /// Retrieve the database specified name
    async fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        let databases = self.databases.lock();

        databases.get(name).cloned()
    }

    async fn acquire_semaphore(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.query_semaphore)
            .acquire_owned()
            .await
            .unwrap()
    }
}
