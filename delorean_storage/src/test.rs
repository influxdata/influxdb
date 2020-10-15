//! This module provides a reference implementaton
//! of storage::DatabaseSource and storage::Database for use in testing
//!
//! Note: this module is only compiled in  the 'test' cfg,
use delorean_arrow::arrow::record_batch::RecordBatch;

use crate::{
    exec::SeriesSetPlans,
    exec::{StringSet, StringSetPlan, StringSetRef},
    Database, DatabaseStore, Predicate, TimestampRange,
};
use delorean_data_types::data::ReplicatedWrite;
use delorean_line_parser::{parse_lines, ParsedLine};

use async_trait::async_trait;
use snafu::{OptionExt, Snafu};
use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TestDatabase {
    /// lines which have been written to this database, in order
    saved_lines: Mutex<Vec<String>>,

    /// replicated writes which have been written to this database, in order
    replicated_writes: Mutex<Vec<ReplicatedWrite>>,

    /// column_names to return upon next request
    column_names: Arc<Mutex<Option<StringSetRef>>>,

    /// the last request for column_names.
    column_names_request: Arc<Mutex<Option<ColumnNamesRequest>>>,

    /// column_values to return upon next request
    column_values: Arc<Mutex<Option<StringSetRef>>>,

    /// the last request for column_values.
    column_values_request: Arc<Mutex<Option<ColumnValuesRequest>>>,

    /// responses to return on the next request to query_series
    query_series_values: Arc<Mutex<Option<SeriesSetPlans>>>,

    /// the last request for query_series
    query_series_request: Arc<Mutex<Option<QuerySeriesRequest>>>,
}

/// Records the parameters passed to a column name request
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnNamesRequest {
    pub table: Option<String>,
    pub range: Option<TimestampRange>,
    /// Stringified '{:?}' version of the predicate
    pub predicate: Option<String>,
}

/// Records the parameters passed to a column values request
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnValuesRequest {
    pub column_name: String,
    pub table: Option<String>,
    pub range: Option<TimestampRange>,
    /// Stringified '{:?}' version of the predicate
    pub predicate: Option<String>,
}

/// Records the parameters passed to a query_series request
#[derive(Debug, PartialEq, Clone)]
pub struct QuerySeriesRequest {
    pub range: Option<TimestampRange>,
    /// Stringified '{:?}' version of the predicate
    pub predicate: Option<String>,
}

#[derive(Snafu, Debug)]
pub enum TestError {
    #[snafu(display("Test database error:  {}", message))]
    General { message: String },

    #[snafu(display("Test database execution:  {:?}", source))]
    Execution { source: crate::exec::Error },
}

impl Default for TestDatabase {
    fn default() -> Self {
        Self {
            saved_lines: Mutex::new(Vec::new()),
            replicated_writes: Mutex::new(Vec::new()),
            column_names: Arc::new(Mutex::new(None)),
            column_names_request: Arc::new(Mutex::new(None)),
            column_values: Arc::new(Mutex::new(None)),
            column_values_request: Arc::new(Mutex::new(None)),
            query_series_values: Arc::new(Mutex::new(None)),
            query_series_request: Arc::new(Mutex::new(None)),
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

    /// Get all replicated writs to this database
    pub async fn get_writes(&self) -> Vec<ReplicatedWrite> {
        self.replicated_writes.lock().await.clone()
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

    /// Set the list of column names that will be returned on a call to column_names
    pub async fn set_column_names(&self, column_names: Vec<String>) {
        let column_names = column_names.into_iter().collect::<StringSet>();
        let column_names = Arc::new(column_names);

        *(self.column_names.clone().lock().await) = Some(column_names)
    }

    /// Get the parameters from the last column name request
    pub async fn get_column_names_request(&self) -> Option<ColumnNamesRequest> {
        self.column_names_request.clone().lock().await.take()
    }

    /// Set the list of column values that will be returned on a call to column_values
    pub async fn set_column_values(&self, column_values: Vec<String>) {
        let column_values = column_values.into_iter().collect::<StringSet>();
        let column_values = Arc::new(column_values);

        *(self.column_values.clone().lock().await) = Some(column_values)
    }

    /// Get the parameters from the last column name request
    pub async fn get_column_values_request(&self) -> Option<ColumnValuesRequest> {
        self.column_values_request.clone().lock().await.take()
    }

    /// Set the series that will be returned on a call to query_series
    /// TODO add in the actual values
    pub async fn set_query_series_values(&self, plan: SeriesSetPlans) {
        *(self.query_series_values.clone().lock().await) = Some(plan);
    }

    /// Get the parameters from the last column name request
    pub async fn get_query_series_request(&self) -> Option<QuerySeriesRequest> {
        self.query_series_request.clone().lock().await.take()
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

    /// adds the replicated write to this database
    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.replicated_writes.lock().await.push(write.clone());
        Ok(())
    }

    /// Execute the specified query and return arrow record batches with the result
    async fn query(&self, _query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
        unimplemented!("query Not yet implemented");
    }

    /// Return all table names that are saved in this database
    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<StringSetPlan, Self::Error> {
        let saved_lines = self.saved_lines.lock().await;

        let names = parse_lines(&saved_lines.join("\n"))
            .filter_map(|line| {
                let line = line.expect("Correctly parsed saved line");
                if line_in_range(&line, &range) {
                    Some(line.series.measurement.to_string())
                } else {
                    None
                }
            })
            .collect::<StringSet>();

        Ok(names.into())
    }

    /// return the mocked out column names, recording the request
    async fn tag_column_names(
        &self,
        table: Option<String>,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<StringSetPlan, Self::Error> {
        // save the request
        let predicate = predicate.map(|p| format!("{:?}", p));

        let new_column_names_request = Some(ColumnNamesRequest {
            table,
            range,
            predicate,
        });

        *self.column_names_request.clone().lock().await = new_column_names_request;

        // pull out the saved columns
        let column_names = self
            .column_names
            .clone()
            .lock()
            .await
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved column_names in TestDatabase",
            });

        Ok(column_names.into())
    }

    /// return the mocked out column values, recording the request
    async fn column_values(
        &self,
        column_name: &str,
        table: Option<String>,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<StringSetPlan, Self::Error> {
        // save the request
        let predicate = predicate.map(|p| format!("{:?}", p));

        let new_column_values_request = Some(ColumnValuesRequest {
            column_name: column_name.to_string(),
            table,
            range,
            predicate,
        });

        *self.column_values_request.clone().lock().await = new_column_values_request;

        // pull out the saved columns
        let column_values = self
            .column_values
            .clone()
            .lock()
            .await
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved column_values in TestDatabase",
            });

        Ok(column_values.into())
    }

    async fn query_series(
        &self,
        range: Option<TimestampRange>,
        predicate: Option<Predicate>,
    ) -> Result<SeriesSetPlans, Self::Error> {
        let predicate = predicate.map(|p| format!("{:?}", p));

        let new_queries_series_request = Some(QuerySeriesRequest { range, predicate });

        *self.query_series_request.clone().lock().await = new_queries_series_request;

        self.query_series_values
            .clone()
            .lock()
            .await
            .take()
            // Turn None into an error
            .context(General {
                message: "No saved query_series in TestDatabase",
            })
    }

    /// Fetch the specified table names and columns as Arrow RecordBatches
    async fn table_to_arrow(
        &self,
        _table_name: &str,
        _columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error> {
        unimplemented!("table_to_arrow Not yet implemented for test database");
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
