use crate::{common::server_fixture::ServerFixture, Scenario};
use arrow_deps::assert_table_eq;
use influxdb_iox_client::flight::Client;

pub async fn test(
    server_fixture: &ServerFixture,
    scenario: &Scenario,
    sql_query: &str,
    expected_read_data: &[String],
) {
    let mut client = Client::new(server_fixture.grpc_channel());

    let mut query_results = client
        .perform_query(scenario.database_name(), sql_query)
        .await
        .unwrap();

    let mut batches = vec![];

    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    assert_table_eq!(expected_read_data, &batches);
}
