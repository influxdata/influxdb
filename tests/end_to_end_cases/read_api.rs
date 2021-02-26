use crate::{Scenario, IOX_API_V1_BASE};

pub async fn test(
    client: &reqwest::Client,
    scenario: &Scenario,
    sql_query: &str,
    expected_read_data: &[String],
) {
    let text = read_data_as_sql(&client, scenario, sql_query).await;

    assert_eq!(
        text, expected_read_data,
        "Actual:\n{:#?}\nExpected:\n{:#?}",
        text, expected_read_data
    );
}

async fn read_data_as_sql(
    client: &reqwest::Client,
    scenario: &Scenario,
    sql_query: &str,
) -> Vec<String> {
    let db_name = format!("{}_{}", scenario.org_id_str(), scenario.bucket_id_str());
    let path = format!("/databases/{}/query", db_name);
    let url = format!("{}{}", IOX_API_V1_BASE, path);
    let lines = client
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
    lines
}
