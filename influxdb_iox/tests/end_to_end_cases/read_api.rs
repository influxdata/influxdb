use super::scenario::Scenario;
use crate::common::server_fixture::{ServerFixture, ServerType};

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = server_fixture.management_client();
    let mut write_client = server_fixture.write_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;

    let mut expected_read_data = scenario.load_data(&mut write_client).await;
    let sql_query = "select * from cpu_load_short";

    let client = reqwest::Client::new();
    let url = format!("{}/api/v3/query", server_fixture.http_base());
    let mut lines: Vec<_> = client
        .get(&url)
        .query(&[("q", sql_query), ("d", scenario.database_name().as_str())])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .trim()
        .split('\n')
        .map(str::to_string)
        .collect();

    expected_read_data.sort();
    lines.sort();

    assert_eq!(
        lines, expected_read_data,
        "Actual:\n\n{:#?}\nExpected:\n\n{:#?}",
        lines, expected_read_data
    );
}
