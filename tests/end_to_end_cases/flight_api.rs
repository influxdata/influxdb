use super::scenario::Scenario;
use crate::common::server_fixture::ServerFixture;
use arrow_deps::assert_table_eq;

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_shared().await;

    let influxdb2 = server_fixture.influxdb2_client();
    let mut management_client = server_fixture.management_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;

    let expected_read_data = scenario.load_data(&influxdb2).await;
    let sql_query = "select * from cpu_load_short";

    let mut client = server_fixture.flight_client();

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
