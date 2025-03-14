use std::sync::Arc;

use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::ColumnId;
use influxdb3_wal::{Gen1Duration, Row, WriteBatch};
use influxdb3_write::write_buffer::validator::WriteValidator;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

#[derive(Debug)]
pub(crate) struct TestWriter {
    catalog: Arc<Catalog>,
}

impl TestWriter {
    pub(crate) const DB_NAME: &str = "test_db";

    pub(crate) async fn new() -> Self {
        Self {
            catalog: Arc::new(
                Catalog::new(
                    "test-host",
                    Arc::new(InMemory::new()),
                    Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
                )
                .await
                .unwrap(),
            ),
        }
    }

    pub(crate) async fn write_lp_to_rows(&self, lp: impl AsRef<str>, time_ns: i64) -> Vec<Row> {
        WriteValidator::initialize(Self::DB_NAME.try_into().unwrap(), Arc::clone(&self.catalog))
            .expect("initialize write validator")
            .v1_parse_lines_and_catalog_updates(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(time_ns),
                influxdb3_write::Precision::Nanosecond,
            )
            .expect("parse and validate v1 line protocol")
            .commit_catalog_changes()
            .await
            .expect("commit catalog changes on write")
            .unwrap_success()
            .into_inner()
            .to_rows()
    }

    pub(crate) async fn write_lp_to_write_batch(
        &self,
        lp: impl AsRef<str>,
        time_ns: i64,
    ) -> WriteBatch {
        let validated = WriteValidator::initialize(
            Self::DB_NAME.try_into().unwrap(),
            Arc::clone(&self.catalog),
        )
        .expect("initialize write validator")
        .v1_parse_lines_and_catalog_updates(
            lp.as_ref(),
            false,
            Time::from_timestamp_nanos(time_ns),
            influxdb3_write::Precision::Nanosecond,
        )
        .expect("parse and validate v1 line protocol")
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_1m());
        validated.into()
    }

    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    pub(crate) fn db_schema(&self) -> Arc<DatabaseSchema> {
        self.catalog
            .db_schema(Self::DB_NAME)
            .expect("db schema should be initialized")
    }
}

/// Convert a list of column names to their respective [`ColumnId`]s in the given [`TableDefinition`]
pub(crate) fn column_ids_for_names(
    names: impl IntoIterator<Item: AsRef<str>>,
    table_def: &TableDefinition,
) -> Vec<ColumnId> {
    names
        .into_iter()
        .map(|name| table_def.column_name_to_id_unchecked(name.as_ref()))
        .collect()
}
