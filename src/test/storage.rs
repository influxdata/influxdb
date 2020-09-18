/// This module provides a reference implementaton
/// of storage::DatabaseSource and storage::Database for use in testing
///
/// Note: this module is only compiled in  the 'test' cfg,
use arrow::record_batch::RecordBatch;

use crate::storage::{Database, DatabaseStore, TimestampRange};
use delorean_line_parser::{parse_lines, ParsedLine};

use snafu::Snafu;
use std::{collections::BTreeMap, collections::BTreeSet, sync::Arc};
use tonic::async_trait;

use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TestDatabase {
    // lines which have been written to this database, in order
    saved_lines: Mutex<Vec<String>>,
}

#[derive(Snafu, Debug, Clone, Copy)]
pub enum TestError {}

impl Default for TestDatabase {
    fn default() -> Self {
        Self {
            saved_lines: Mutex::new(Vec::new()),
        }
    }
}

impl TestDatabase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all lines written to this database
    pub async fn get_lines(&self) -> Vec<String> {
        self.saved_lines.lock().await.clone()
    }

    /// Parse line protocol and add it as new lines to this
    /// database
    pub async fn add_lp_string(&self, lp_data: &str) {
        let parsed_lines = parse_lines(&lp_data)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| panic!("parsing line protocol: {}", lp_data));

        self.write_lines(&parsed_lines)
            .await
            .expect("writing lines");
    }
}

/// returns true if this line is within the range of the timestamp
fn line_in_range(line: &ParsedLine<'_>, range: &Option<TimestampRange>) -> bool {
    match range {
        Some(range) => {
            let timestamp = line.timestamp.expect("had a timestamp on line");
            range.start <= timestamp && timestamp <= range.end
        }
        None => true,
    }
}

#[async_trait]
impl Database for TestDatabase {
    type Error = TestError;

    /// writes parsed lines into this database
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
        let mut saved_lines = self.saved_lines.lock().await;
        for line in lines {
            saved_lines.push(line.to_string())
        }
        Ok(())
    }

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, _query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
        unimplemented!("query Not yet implemented");
    }

    /// Return all measurement names that are saved in this database
    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error> {
        let saved_lines = self.saved_lines.lock().await;

        Ok(Arc::new(
            parse_lines(&saved_lines.join("\n"))
                .filter_map(|line| {
                    let line = line.expect("Correctly parsed saved line");
                    if line_in_range(&line, &range) {
                        Some(line.series.measurement.to_string())
                    } else {
                        None
                    }
                })
                .collect::<BTreeSet<_>>(),
        ))
    }

    /// Fetch the specified table names and columns as Arrow RecordBatches
    async fn table_to_arrow(
        &self,
        _table_name: &str,
        _columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error> {
        unimplemented!("table_to_arrow Not yet implemented");
    }
}

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse line protocol and add it as new lines to the `db_name` database
    pub async fn add_lp_string(&self, db_name: &str, lp_data: &str) {
        self.db_or_create(db_name)
            .await
            .expect("db_or_create suceeeds")
            .add_lp_string(lp_data)
            .await
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self {
            databases: Mutex::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl DatabaseStore for TestDatabaseStore {
    type Database = TestDatabase;
    type Error = TestError;
    /// Retrieve the database specified name
    async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        let databases = self.databases.lock().await;

        databases.get(name).cloned()
    }

    /// Retrieve the database specified by name, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let mut databases = self.databases.lock().await;

        if let Some(db) = databases.get(name) {
            Ok(db.clone())
        } else {
            let new_db = Arc::new(TestDatabase::new());
            databases.insert(name.to_string(), new_db.clone());
            Ok(new_db)
        }
    }
}
