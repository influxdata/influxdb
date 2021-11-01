use crate::common::server_fixture::{ServerFixture, TestConfig};
use crate::end_to_end_cases::scenario::Scenario;
use test_helpers::assert_contains;

#[tokio::test]
pub async fn test_row_timestamp() {
    let test_config = TestConfig::new().with_env("INFLUXDB_IOX_ROW_TIMESTAMP_METRICS", "system");
    let server_fixture = ServerFixture::create_single_use_with_config(test_config).await;
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
    let db_name_attribute = format!("db_name=\"{}\"", db_name);

    // Should only be enabled for the system table
    assert_eq!(lines.len(), 61);
    assert!(lines
        .iter()
        .all(|x| x.contains("table=\"system\"") && x.contains(&db_name_attribute)));
}

#[tokio::test]
pub async fn test_jemalloc_metrics() {
    let server_fixture = ServerFixture::create_shared().await;

    let client = reqwest::Client::new();
    let url = format!("{}/metrics", server_fixture.http_base());

    let payload = client.get(&url).send().await.unwrap().text().await.unwrap();

    let lines: Vec<_> = payload
        .trim()
        .split('\n')
        .filter(|x| x.starts_with("jemalloc_memstats_bytes"))
        .collect();

    assert_eq!(lines.len(), 6);
    assert_contains!(lines[0], "jemalloc_memstats_bytes{stat=\"active\"}");
    assert_contains!(lines[1], "jemalloc_memstats_bytes{stat=\"alloc\"}");
    assert_contains!(lines[2], "jemalloc_memstats_bytes{stat=\"metadata\"}");
    assert_contains!(lines[3], "jemalloc_memstats_bytes{stat=\"mapped\"}");
    assert_contains!(lines[4], "jemalloc_memstats_bytes{stat=\"resident\"}");
    assert_contains!(lines[5], "jemalloc_memstats_bytes{stat=\"retained\"}");
}
