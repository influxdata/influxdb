use async_trait::async_trait;
use query::DatabaseStore;
use snafu::Snafu;
use tokio::sync::RwLock;

use std::sync::Arc;

use std::{collections::BTreeMap, path::PathBuf};

use crate::database::MutableBufferDb;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading from dir {:?}: {}", dir, source))]
    ReadError {
        dir: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct MutableBufferDatabases {
    databases: RwLock<BTreeMap<String, Arc<MutableBufferDb>>>,
}

impl MutableBufferDatabases {
    pub async fn add_db(&self, db: MutableBufferDb) {
        let mut databases = self.databases.write().await;
        databases.insert(db.name.clone(), Arc::new(db));
    }
}

#[async_trait]
impl DatabaseStore for MutableBufferDatabases {
    type Database = MutableBufferDb;
    type Error = Error;

    async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        let databases = self.databases.read().await;

        databases.get(name).cloned()
    }

    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        // get it through a read lock first if we can
        {
            let databases = self.databases.read().await;

            if let Some(db) = databases.get(name) {
                return Ok(db.clone());
            }
        }

        // database doesn't exist yet so acquire the write lock and get or insert
        let mut databases = self.databases.write().await;

        // make sure it didn't get inserted by someone else while we were waiting for
        // the write lock
        if let Some(db) = databases.get(name) {
            return Ok(db.clone());
        }

        let db = MutableBufferDb::new(name);
        let db = Arc::new(db);
        databases.insert(name.to_string(), db.clone());

        Ok(db)
    }
}
