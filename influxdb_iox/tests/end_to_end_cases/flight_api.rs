use super::scenario::{create_readable_database, rand_name, Scenario};
use crate::common::server_fixture::{ServerFixture, ServerType};
use arrow_util::assert_batches_sorted_eq;

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;

    let mut write_client = server_fixture.write_client();
    let mut management_client = server_fixture.management_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;

    let expected_read_data = scenario.load_data(&mut write_client).await;
    let sql_query = "select * from cpu_load_short";

    let mut client = server_fixture.flight_client();

    // This does nothing except test the client handshake implementation.
    client.handshake().await.unwrap();

    let batches = client
        .perform_query(scenario.database_name(), sql_query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected_read_data: Vec<_> = expected_read_data.iter().map(|s| s.as_str()).collect();
    assert_batches_sorted_eq!(expected_read_data, &batches);
}

#[tokio::test]
pub async fn test_no_rows() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;

    let db_name = rand_name();
    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    // query returns no results
    let sql_query = "select * from system.chunks limit 0";

    let mut client = server_fixture.flight_client();

    let mut query_results = client.perform_query(&db_name, sql_query).await.unwrap();

    // no record batches are returned
    let batch = query_results.next().await.unwrap();
    assert!(batch.is_none());
}
