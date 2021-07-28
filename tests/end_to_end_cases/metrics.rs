use crate::common::server_fixture::ServerFixture;
use crate::end_to_end_cases::scenario::Scenario;

#[tokio::test]
pub async fn test_row_timestamp() {
    let env = vec![(
        "INFLUXDB_IOX_ROW_TIMESTAMP_METRICS".to_string(),
        "system".to_string(),
    )];
    let server_fixture = ServerFixture::create_single_use_with_env(env).await;
    let mut management_client = server_fixture.management_client();

    management_client.update_server_id(1).await.unwrap();
    server_fixture.wait_server_initialized().await;

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;
    scenario.load_data(&server_fixture.influxdb2_client()).await;

    let client = reqwest::Client::new();
    let url = format!("{}/metrics", server_fixture.http_base());

    let payload = client.get(&url).send().await.unwrap().text().await.unwrap();

    let lines: Vec<_> = payload
        .trim()
        .split('\n')
        .filter(|x| x.starts_with("catalog_row_time_seconds_bucket"))
        .collect();

    let db_name = format!("{}_{}", scenario.org_id_str(), scenario.bucket_id_str());
    let db_name_label = format!("db_name=\"{}\"", db_name);

    // Should only be enabled for the system table
    assert_eq!(lines.len(), 60);
    assert!(lines
        .iter()
        .all(|x| x.contains("table=\"system\"") && x.contains(&db_name_label)));
}
