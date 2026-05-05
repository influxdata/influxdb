use arrow_schema::{DataType, TimeUnit};
use datafusion::{
    logical_expr::{BinaryExpr, Operator, expr::ScalarFunction},
    prelude::{Expr, cast, col, date_trunc, lit},
};
use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber};
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_types::http::FieldDataType;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use query_functions::tz::TZ_UDF;
use std::sync::Arc;

use influxdb3_types::DatabaseName;
use influxdb3_types::write::Precision;
use iox_time::Time;

use crate::{
    ChunkFilter, DatabaseTables, ParquetFile, PersistedSnapshot,
    write_buffer::validator::WriteValidator,
};

#[test]
fn test_overall_counts() {
    let host = "host_id";
    // db 1 setup
    let db_id_1 = DbId::from(0);
    let mut dbs_1 = SerdeVecMap::new();
    let table_id_1 = TableId::from(0);
    let mut tables_1 = SerdeVecMap::new();
    let parquet_files_1 = vec![
        ParquetFile {
            id: ParquetFileId::from(1),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
        ParquetFile {
            id: ParquetFileId::from(2),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
    ];
    tables_1.insert(table_id_1, parquet_files_1);
    dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

    // add dbs_1 to snapshot
    let persisted_snapshot_1 = PersistedSnapshot {
        node_id: host.to_string(),
        next_file_id: ParquetFileId::from(0),
        snapshot_sequence_number: SnapshotSequenceNumber::new(124),
        wal_file_sequence_number: WalFileSequenceNumber::new(100),
        catalog_sequence_number: CatalogSequenceNumber::new(100),
        databases: dbs_1,
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        removed_files: SerdeVecMap::new(),
        persisted_at: None,
    };

    // db 2 setup
    let db_id_2 = DbId::from(2);
    let mut dbs_2 = SerdeVecMap::new();
    let table_id_2 = TableId::from(2);
    let mut tables_2 = SerdeVecMap::new();
    let parquet_files_2 = vec![
        ParquetFile {
            id: ParquetFileId::from(4),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
        ParquetFile {
            id: ParquetFileId::from(5),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
    ];
    tables_2.insert(table_id_2, parquet_files_2);
    dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

    // add dbs_2 to snapshot
    let persisted_snapshot_2 = PersistedSnapshot {
        node_id: host.to_string(),
        next_file_id: ParquetFileId::from(5),
        snapshot_sequence_number: SnapshotSequenceNumber::new(124),
        wal_file_sequence_number: WalFileSequenceNumber::new(100),
        catalog_sequence_number: CatalogSequenceNumber::new(100),
        databases: dbs_2,
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        removed_files: SerdeVecMap::new(),
        persisted_at: None,
    };

    let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[
        persisted_snapshot_1,
        persisted_snapshot_2,
    ]);
    assert_eq!((2, 2, 4), overall_counts);
}

#[test]
fn test_overall_counts_zero() {
    // db 1 setup
    let db_id_1 = DbId::from(0);
    let mut dbs_1 = SerdeVecMap::new();
    let table_id_1 = TableId::from(0);
    let mut tables_1 = SerdeVecMap::new();
    let parquet_files_1 = vec![
        ParquetFile {
            id: ParquetFileId::from(1),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
        ParquetFile {
            id: ParquetFileId::from(2),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
    ];
    tables_1.insert(table_id_1, parquet_files_1);
    dbs_1.insert(db_id_1, DatabaseTables { tables: tables_1 });

    // db 2 setup
    let db_id_2 = DbId::from(2);
    let mut dbs_2 = SerdeVecMap::new();
    let table_id_2 = TableId::from(2);
    let mut tables_2 = SerdeVecMap::new();
    let parquet_files_2 = vec![
        ParquetFile {
            id: ParquetFileId::from(4),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
        ParquetFile {
            id: ParquetFileId::from(5),
            path: "some_path".to_string(),
            size_bytes: 100_000,
            row_count: 200,
            chunk_time: 1123456789,
            min_time: 11234567777,
            max_time: 11234567788,
        },
    ];
    tables_2.insert(table_id_2, parquet_files_2);
    dbs_2.insert(db_id_2, DatabaseTables { tables: tables_2 });

    // add dbs_2 to snapshot
    let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&[]);
    assert_eq!((0, 0, 0), overall_counts);
}

#[tokio::test]
async fn test_series_key_columns_with_slashes() {
    let catalog = Catalog::new_in_memory("test-catalog")
        .await
        .map(Arc::new)
        .unwrap();
    let db_name = DatabaseName::new("test-db").unwrap();
    let writer = WriteValidator::initialize(db_name, Arc::clone(&catalog)).unwrap();
    // write and commit the catalog changes:
    let _ = writer
        .v1_parse_lines_and_catalog_updates(
            "foo,a/b=bar value=1",
            false,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success();
    let schema = catalog
        .db_schema("test-db")
        .and_then(|db| db.table_definition("foo"))
        .map(|tbl| tbl.influx_schema().clone())
        .unwrap();
    assert_eq!(["a/b", "time"], schema.primary_key().as_slice());
}

#[test_log::test(tokio::test)]
async fn test_filter_on_time_with_date_trunc() {
    let catalog = Catalog::new_in_memory("test-node").await.unwrap();
    catalog
        .create_table(
            "foo",
            "bar",
            &["t1", "t2"],
            &[("f1", FieldDataType::String)],
        )
        .await
        .unwrap();
    let table_def = catalog
        .db_schema("foo")
        .and_then(|db| db.table_definition("bar"))
        .unwrap();
    let tz_expr = Expr::ScalarFunction(ScalarFunction {
        func: TZ_UDF.clone(),
        args: vec![col("time"), lit("America/Detroit")],
    });
    let date_trunc_expr = date_trunc(lit("day"), tz_expr);
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(date_trunc_expr),
        op: Operator::GtEq,
        right: Box::new(cast(
            lit("2025-03-12T04:00:00Z"),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )),
    });

    // NB(tjh): since the time boundary analysis cannot analyze the above expression, the
    // following filter will not have time boundaries.
    ChunkFilter::new(&table_def, &[expr])
        .inspect(|f| {
            println!("filter: {f:#?}");
            // These assertions will break if we add the ability to handle the above expression in such
            // a way that properly determines the lower time bound, but that is okay; the assertions
            // should be changed in that case.
            assert!(f.time_lower_bound_ns.is_none());
            assert!(f.time_upper_bound_ns.is_none());
        })
        .expect("create ChunkFilter");
}
