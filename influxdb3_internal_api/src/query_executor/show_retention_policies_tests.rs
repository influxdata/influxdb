use std::{sync::Arc, time::Duration};

use datafusion::{
    arrow::array::RecordBatch, assert_batches_eq, execution::SendableRecordBatchStream,
};
use futures::StreamExt;
use influxdb3_catalog::catalog::{Catalog, CreateDatabaseOptions};
use iox_query_influxql::show_retention_policies::InfluxQlShowRetentionPolicies;

use crate::query_executor::ShowRetentionPolicies;

#[tokio::test]
async fn test_show_retention_policies() {
    let catalog = Catalog::new_in_memory("test").await.map(Arc::new).unwrap();
    let days = |n_days: u64| Duration::from_secs(n_days * 24 * 60 * 60);
    catalog
        .create_database_opts(
            "foo",
            CreateDatabaseOptions::default().retention_period(days(7)),
        )
        .await
        .unwrap();
    catalog
        .create_database_opts(
            "bar",
            CreateDatabaseOptions::default().retention_period(days(30)),
        )
        .await
        .unwrap();
    catalog
        .create_database_opts(
            "baz",
            CreateDatabaseOptions::default().retention_period(Duration::from_secs(90)),
        )
        .await
        .unwrap();
    catalog.create_database("mop").await.unwrap();
    let show_retention_policies = ShowRetentionPolicies::new(catalog);
    for (db, expected) in [
        (
            "foo",
            vec![
                "+--------------------+------+----------+---------+",
                "| iox::measurement   | name | duration | default |",
                "+--------------------+------+----------+---------+",
                "| retention_policies | foo  | 168h0m0s | true    |",
                "+--------------------+------+----------+---------+",
            ],
        ),
        (
            "bar",
            vec![
                "+--------------------+------+----------+---------+",
                "| iox::measurement   | name | duration | default |",
                "+--------------------+------+----------+---------+",
                "| retention_policies | bar  | 720h0m0s | true    |",
                "+--------------------+------+----------+---------+",
            ],
        ),
        (
            "baz",
            vec![
                "+--------------------+------+----------+---------+",
                "| iox::measurement   | name | duration | default |",
                "+--------------------+------+----------+---------+",
                "| retention_policies | baz  | 1m30s    | true    |",
                "+--------------------+------+----------+---------+",
            ],
        ),
        (
            "mop",
            vec![
                "+--------------------+------+----------+---------+",
                "| iox::measurement   | name | duration | default |",
                "+--------------------+------+----------+---------+",
                "| retention_policies | mop  | 0s       | true    |",
                "+--------------------+------+----------+---------+",
            ],
        ),
    ] {
        let stream = show_retention_policies
            .show_retention_policies(db.to_string())
            .await
            .unwrap();
        let batches = collect_stream(stream).await;
        assert_batches_eq!(expected, &batches);
    }
}

async fn collect_stream(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.unwrap());
    }
    batches
}
