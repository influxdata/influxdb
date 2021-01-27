use crate::{Scenario, API_BASE};

pub async fn test(
    client: &reqwest::Client,
    scenario: &Scenario,
    sql_query: &str,
    expected_read_data: &[String],
) {
    let text = read_data_as_sql(&client, "/read", scenario, sql_query).await;

    assert_eq!(
        text, expected_read_data,
        "Actual:\n{:#?}\nExpected:\n{:#?}",
        text, expected_read_data
    );
}

async fn read_data_as_sql(
    client: &reqwest::Client,
    path: &str,
    scenario: &Scenario,
    sql_query: &str,
) -> Vec<String> {
    let url = format!("{}{}", API_BASE, path);
    let lines = client
        .get(&url)
        .query(&[
            ("bucket", scenario.bucket_id_str()),
            ("org", scenario.org_id_str()),
            ("sql_query", sql_query),
        ])
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
