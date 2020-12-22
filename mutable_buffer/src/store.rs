use async_trait::async_trait;
use query::DatabaseStore;
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;

use std::{fs, sync::Arc};

use std::{collections::BTreeMap, path::PathBuf};

use crate::database::MutableBufferDb;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading from dir {:?}: {}", dir, source))]
    ReadError {
        dir: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error from database: {}", source))]
    DatabaseError { source: crate::database::Error },

    #[snafu(display("Error reading metadata: {}", source))]
    ReadMetadataError { source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct MutableBufferDatabases {
    databases: RwLock<BTreeMap<String, Arc<MutableBufferDb>>>,
    base_dir: PathBuf,
}

impl MutableBufferDatabases {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            databases: RwLock::new(BTreeMap::new()),
            base_dir: base_dir.into(),
        }
    }

    /// wal_dirs will traverse the directories from the service base directory
    /// and return the directories that contain WALs for databases, which
    /// can be used to restore those DBs.
    pub fn wal_dirs(&self) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(&self.base_dir).context(ReadError {
            dir: &self.base_dir,
        })?;

        let mut dirs = vec![];

        for entry in entries {
            let entry = entry.context(ReadError {
                dir: &self.base_dir,
            })?;

            let meta = entry.metadata().context(ReadMetadataError {})?;
            if meta.is_dir() {
                if let Some(p) = entry.path().iter().last() {
                    if let Some(s) = p.to_str() {
                        if !s.starts_with('.') {
                            dirs.push(entry.path());
                        };
                    }
                };
            };
        }

        Ok(dirs)
    }

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

        let db = MutableBufferDb::try_with_wal(name, &mut self.base_dir.clone())
            .await
            .context(DatabaseError)?;
        let db = Arc::new(db);
        databases.insert(name.to_string(), db.clone());

        Ok(db)
    }
}
