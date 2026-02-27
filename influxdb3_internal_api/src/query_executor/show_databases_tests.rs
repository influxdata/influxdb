use std::sync::Arc;

use datafusion::{
    arrow::array::RecordBatch, assert_batches_eq, execution::SendableRecordBatchStream,
};
use futures::StreamExt;
use influxdb3_catalog::catalog::Catalog;
use iox_query_influxql::show_databases::InfluxQlShowDatabases;

use crate::query_executor::ShowDatabases;

#[tokio::test]
async fn test_show_databases() {
    let catalog = Catalog::new_in_memory("test").await.map(Arc::new).unwrap();
    catalog.create_database("foo").await.unwrap();
    catalog.create_database("bar").await.unwrap();
    catalog.create_database("mop").await.unwrap();
    let show_databases = ShowDatabases::new(catalog);
    for (dbs, expected) in [
        (
            vec!["foo"],
            vec![
                "+------------------+------+",
                "| iox::measurement | name |",
                "+------------------+------+",
                "| databases        | foo  |",
                "+------------------+------+",
            ],
        ),
        (
            vec!["foo", "bar"],
            vec![
                "+------------------+------+",
                "| iox::measurement | name |",
                "+------------------+------+",
                "| databases        | foo  |",
                "| databases        | bar  |",
                "+------------------+------+",
            ],
        ),
        (
            vec!["foo", "bar", "mop"],
            vec![
                "+------------------+------+",
                "| iox::measurement | name |",
                "+------------------+------+",
                "| databases        | foo  |",
                "| databases        | bar  |",
                "| databases        | mop  |",
                "+------------------+------+",
            ],
        ),
    ] {
        let stream = show_databases
            .show_databases(dbs.into_iter().map(|s| s.to_string()).collect())
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
