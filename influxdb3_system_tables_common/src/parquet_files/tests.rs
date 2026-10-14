use std::collections::HashMap;

use datafusion::{
    assert_batches_sorted_eq,
    logical_expr::{col, lit},
};
use influxdb3_catalog::catalog::FieldDataType;

use super::*;

/// [`ParquetFilesSource`] backed by a fixed map of files per `(DbId, TableId)`
#[derive(Debug)]
struct TestParquetFilesSource {
    catalog: Arc<Catalog>,
    files: HashMap<(DbId, TableId), Vec<ParquetFileRow>>,
}

impl ParquetFilesSource for TestParquetFilesSource {
    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFileRow> {
        self.files
            .get(&(db_id, table_id))
            .cloned()
            .unwrap_or_default()
    }
}

fn file(path: &str) -> ParquetFileRow {
    ParquetFileRow {
        path: path.into(),
        size_bytes: 1,
        row_count: 1,
        min_time: 0,
        max_time: 1,
    }
}

/// Two databases whose table ids collide: `db_a.cpu` and `db_b.disk` both have the first
/// table id of their database. Each table has one persisted file.
async fn colliding_table_ids_source() -> (Arc<TestParquetFilesSource>, DbId) {
    let catalog = Catalog::new_in_memory("test").await.unwrap();
    for (db, table) in [("db_a", "cpu"), ("db_a", "mem"), ("db_b", "disk")] {
        catalog
            .create_table(db, table, &["t"], &[("f", FieldDataType::Float)])
            .await
            .unwrap();
    }
    let db_a = catalog.db_schema("db_a").unwrap();
    let db_b = catalog.db_schema("db_b").unwrap();
    let cpu = db_a.table_definition("cpu").unwrap();
    let mem = db_a.table_definition("mem").unwrap();
    let disk = db_b.table_definition("disk").unwrap();
    assert_ne!(db_a.id, db_b.id);
    assert_eq!(
        cpu.table_id, disk.table_id,
        "test requires table ids to collide across databases"
    );

    let files = HashMap::from([
        ((db_a.id, cpu.table_id), vec![file("db_a/cpu/1.parquet")]),
        ((db_a.id, mem.table_id), vec![file("db_a/mem/1.parquet")]),
        ((db_b.id, disk.table_id), vec![file("db_b/disk/1.parquet")]),
    ]);
    (Arc::new(TestParquetFilesSource { catalog, files }), db_a.id)
}

#[tokio::test]
async fn test_parquet_files_no_filter_only_lists_own_database() {
    let (source, db_a_id) = colliding_table_ids_source().await;
    let table = ParquetFilesTable::new(db_a_id, source);

    let batch = table.scan(None, None).await.unwrap();
    assert_batches_sorted_eq!(
        [
            "+------------+--------------------+------------+-----------+----------+----------+",
            "| table_name | path               | size_bytes | row_count | min_time | max_time |",
            "+------------+--------------------+------------+-----------+----------+----------+",
            "| cpu        | db_a/cpu/1.parquet | 1          | 1         | 0        | 1        |",
            "| mem        | db_a/mem/1.parquet | 1          | 1         | 0        | 1        |",
            "+------------+--------------------+------------+-----------+----------+----------+",
        ],
        &[batch]
    );
}

#[tokio::test]
async fn test_parquet_files_table_name_filter() {
    let (source, db_a_id) = colliding_table_ids_source().await;
    let table = ParquetFilesTable::new(db_a_id, source);

    let batch = table
        .scan(Some(vec![col("table_name").eq(lit("cpu"))]), None)
        .await
        .unwrap();
    assert_batches_sorted_eq!(
        [
            "+------------+--------------------+------------+-----------+----------+----------+",
            "| table_name | path               | size_bytes | row_count | min_time | max_time |",
            "+------------+--------------------+------------+-----------+----------+----------+",
            "| cpu        | db_a/cpu/1.parquet | 1          | 1         | 0        | 1        |",
            "+------------+--------------------+------------+-----------+----------+----------+",
        ],
        &[batch]
    );

    // a table that only exists in another database yields no rows
    let batch = table
        .scan(Some(vec![col("table_name").eq(lit("disk"))]), None)
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_parquet_files_limit() {
    let (source, db_a_id) = colliding_table_ids_source().await;
    let table = ParquetFilesTable::new(db_a_id, source);

    let batch = table.scan(None, Some(1)).await.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn test_parquet_files_missing_database() {
    let (source, _) = colliding_table_ids_source().await;
    let table = ParquetFilesTable::new(DbId::new(u32::MAX), source);

    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);

    let batch = table
        .scan(Some(vec![col("table_name").eq(lit("cpu"))]), None)
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 0);
}
