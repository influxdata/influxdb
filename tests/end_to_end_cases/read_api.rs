use crate::common::server_fixture::ServerFixture;
use crate::Scenario;

pub async fn test(
    server_fixture: &ServerFixture,
    scenario: &Scenario,
    sql_query: &str,
    expected_read_data: &[String],
) {
    let client = reqwest::Client::new();
    let db_name = format!("{}_{}", scenario.org_id_str(), scenario.bucket_id_str());
    let path = format!("/databases/{}/query", db_name);
    let url = format!("{}{}", server_fixture.iox_api_v1_base(), path);
    let lines: Vec<_> = client
        .get(&url)
        .query(&[("q", sql_query)])
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

    assert_eq!(
        lines, expected_read_data,
        "Actual:\n{:#?}\nExpected:\n{:#?}",
        lines, expected_read_data
    );
}
