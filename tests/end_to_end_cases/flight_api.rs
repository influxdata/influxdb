use crate::{Scenario, GRPC_URL_BASE};
use arrow_deps::assert_table_eq;
use influxdb_iox_client::{connection::Builder, flight::Client};

pub async fn test(scenario: &Scenario, sql_query: &str, expected_read_data: &[String]) {
    let connection = Builder::default().build(GRPC_URL_BASE).await.unwrap();

    let mut client = Client::new(connection);

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
