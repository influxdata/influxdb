mod common;

use arrow_util::assert_batches_sorted_eq;
use common::*;
use data_types::PartitionKey;
use influxdb_iox_client::flight::generated_types::IngesterQueryRequest;
use iox_catalog::interface::Catalog;
use metric::DurationHistogram;
use std::sync::Arc;

// Write data to an ingester through the RPC interface and query the data, validating the contents.
#[tokio::test]
async fn write_query() {
    let namespace_name = "write_query_test_namespace";
    let mut ctx = test_context().build().await;
    let ns = ctx.ensure_namespace(namespace_name, None).await;

    // Initial write
    let partition_key = PartitionKey::from("1970-01-01");
    ctx.write_lp(
        namespace_name,
        "bananas greatness=\"unbounded\" 10",
        partition_key.clone(),
        0,
    )
    .await;

    // A subsequent write with a non-contiguous sequence number to a different table.
    ctx.write_lp(
        namespace_name,
        "cpu bar=2 20\ncpu bar=3 30",
        partition_key.clone(),
        7,
    )
    .await;

    // And a third write that appends more data to the table in the initial
    // write.
    ctx.write_lp(
        namespace_name,
        "bananas count=42 200",
        partition_key.clone(),
        42,
    )
    .await;

    // Perform a query to validate the actual data buffered.
    let data: Vec<_> = ctx
        .query(IngesterQueryRequest {
            namespace_id: ns.id.get(),
            table_id: ctx.table_id(namespace_name, "bananas").await.get(),
            columns: vec![],
            predicate: None,
        })
        .await
        .expect("query request failed");

    let expected = vec![
        "+-------+-----------+--------------------------------+",
        "| count | greatness | time                           |",
        "+-------+-----------+--------------------------------+",
        "|       | unbounded | 1970-01-01T00:00:00.000000010Z |",
        "| 42    |           | 1970-01-01T00:00:00.000000200Z |",
        "+-------+-----------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &data);

    // Assert various ingest metrics.
    let hist = ctx
        .get_metric::<DurationHistogram, _>(
            "ingester_dml_sink_apply_duration",
            &[("handler", "write_apply"), ("result", "success")],
        )
        .fetch();
    assert_eq!(hist.sample_count(), 3);
}

// Write data to the ingester, which writes it to the WAL, then drop and recreate the WAL and
// validate the data is replayed from the WAL into memory.
#[tokio::test]
async fn wal_replay() {
    let wal_dir = Arc::new(test_helpers::tmp_dir().unwrap());
    let metrics: Arc<metric::Registry> = Default::default();
    let catalog: Arc<dyn Catalog> =
        Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));
    let namespace_name = "wal_replay_test_namespace";

    {
        let mut ctx = test_context()
            .wal_dir(Arc::clone(&wal_dir))
            .catalog(Arc::clone(&catalog))
            .build()
            .await;

        let ns = ctx.ensure_namespace(namespace_name, None).await;

        // Initial write
        let partition_key = PartitionKey::from("1970-01-01");
        ctx.write_lp(
            namespace_name,
            "bananas greatness=\"unbounded\" 10",
            partition_key.clone(),
            0,
        )
        .await;

        // A subsequent write with a non-contiguous sequence number to a different table.
        ctx.write_lp(
            namespace_name,
            "cpu bar=2 20\ncpu bar=3 30",
            partition_key.clone(),
            7,
        )
        .await;

        // And a third write that appends more data to the table in the initial
        // write.
        ctx.write_lp(
            namespace_name,
            "bananas count=42 200",
            partition_key.clone(),
            42,
        )
        .await;

        // Perform a query to validate the actual data buffered.
        let data: Vec<_> = ctx
            .query(IngesterQueryRequest {
                namespace_id: ns.id.get(),
                table_id: ctx.table_id(namespace_name, "bananas").await.get(),
                columns: vec![],
                predicate: None,
            })
            .await
            .expect("query request failed");

        let expected = vec![
            "+-------+-----------+--------------------------------+",
            "| count | greatness | time                           |",
            "+-------+-----------+--------------------------------+",
            "|       | unbounded | 1970-01-01T00:00:00.000000010Z |",
            "| 42    |           | 1970-01-01T00:00:00.000000200Z |",
            "+-------+-----------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &data);
    } // Drop the first ingester instance

    // Restart the ingester and perform replay by creating another ingester using the same WAL
    // directory and catalog
    let ctx = test_context()
        .wal_dir(wal_dir)
        .catalog(catalog)
        .build()
        .await;

    // Validate the data has been replayed and is now in object storage (it won't be in the
    // ingester's memory because replaying the WAL also persists).
    let parquet_files = ctx.catalog_parquet_file_records(namespace_name).await;
    assert_eq!(parquet_files.len(), 2);
    let mut expected_table_ids = vec![
        ctx.table_id(namespace_name, "bananas").await,
        ctx.table_id(namespace_name, "cpu").await,
    ];
    expected_table_ids.sort();
    let mut actual_table_ids: Vec<_> = parquet_files.iter().map(|pf| pf.table_id).collect();
    actual_table_ids.sort();
    assert_eq!(actual_table_ids, expected_table_ids);
}
