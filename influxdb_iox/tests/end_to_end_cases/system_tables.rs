use crate::common::server_fixture::ServerFixture;
use arrow_util::assert_batches_eq;

use super::scenario::{collect_query, create_readable_database, list_chunks, rand_name};

#[tokio::test]
async fn test_operations() {
    let fixture = ServerFixture::create_shared().await;

    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let db_name1 = rand_name();
    let db_name2 = rand_name();
    create_readable_database(&db_name1, fixture.grpc_channel()).await;
    create_readable_database(&db_name2, fixture.grpc_channel()).await;

    // write only into db_name1
    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name1, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = list_chunks(&fixture, &db_name1).await;
    let chunk_id = chunks[0].id;

    // Move the chunk to read buffer
    let iox_operation = management_client
        .close_partition_chunk(&db_name1, table_name, partition_key, chunk_id.into())
        .await
        .expect("new partition chunk");

    let operation_id = iox_operation.operation.id();
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    let mut client = fixture.flight_client();
    let sql_query = "select status, description from system.operations";

    let query_results = client.perform_query(&db_name1, sql_query).await.unwrap();

    let batches = collect_query(query_results).await;

    // parameterize on db_name1

    let expected_read_data = vec![
        "+---------+---------------------------------+",
        "| status  | description                     |",
        "+---------+---------------------------------+",
        "| Success | Compacting chunks to ReadBuffer |",
        "+---------+---------------------------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);

    // Should not see jobs from db1 when querying db2
    let query_results = client.perform_query(&db_name2, sql_query).await.unwrap();

    let batches = collect_query(query_results).await;
    let expected_read_data = vec![
        "+--------+-------------+",
        "| status | description |",
        "+--------+-------------+",
        "+--------+-------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);
}
