use crate::server::{ConfigProvider, TestConfig};

#[tokio::test]
async fn debug_filter_expanded_at_startup() {
    let server = TestConfig::default()
        .with_log_filter("debug")
        .with_capture_logs()
        .spawn()
        .await;
    server.assert_log_contains("log_filter_active");
    server.assert_log_contains("reqwest=info");
}

#[tokio::test]
async fn debug_filter_not_expanded_when_disabled() {
    let server = TestConfig::default()
        .with_log_filter("debug")
        .with_disable_log_filter_noise_reduction()
        .with_capture_logs()
        .spawn()
        .await;
    server.assert_log_contains("log_filter_active");
    let logs = server.get_logs(None).unwrap();
    assert!(
        !logs.contains("reqwest=info"),
        "filter should NOT be expanded when disabled;\nlogs: {logs}"
    );
}

#[tokio::test]
async fn log_filter_expanded_via_log_filter_env_var() {
    let server = TestConfig::default()
        .with_env_var("LOG_FILTER", "debug")
        .with_capture_logs()
        .spawn()
        .await;
    server.assert_log_contains("log_filter_active");
    server.assert_log_contains("reqwest=info");
}
