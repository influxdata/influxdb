use crate::server::{ConfigProvider, TestConfig};

#[tokio::test]
async fn test_error_logging_with_database() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client
    let client = server.http_client();

    // Send a write request that will fail (invalid line protocol)
    let response = client
        .post(format!(
            "{}/api/v3/write_lp?db=test_db",
            server.client_addr()
        ))
        .header("content-type", "text/plain")
        .body("invalid line protocol !")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check that the error log contains the database name
    server.assert_log_contains("database=test_db");
    server.assert_log_contains("Error while handling request");
}

#[tokio::test]
async fn test_error_logging_with_client_ip() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client with custom headers
    let client = server.http_client();

    // Send a write request with client IP header that will fail
    let response = client
        .post(format!("{}/api/v3/write_lp?db=test", server.client_addr()))
        .header("content-type", "text/plain")
        .header("x-forwarded-for", "192.168.1.100")
        .body("invalid line protocol !")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check that the error log contains the client IP
    server.assert_log_contains("client_ip=192.168.1.100");
    server.assert_log_contains("Error while handling request");
}

#[tokio::test]
async fn test_error_logging_with_both_fields() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client
    let client = server.http_client();

    // Send a write request with both database and client IP that will fail
    let response = client
        .post(format!(
            "{}/api/v3/write_lp?db=my_database",
            server.client_addr()
        ))
        .header("content-type", "text/plain")
        .header("x-real-ip", "10.0.0.50")
        .body("invalid line protocol !")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check that the error log contains both fields
    server.assert_log_contains("database=my_database");
    server.assert_log_contains("client_ip=10.0.0.50");
    server.assert_log_contains("Error while handling request");
}

#[tokio::test]
async fn test_error_logging_without_optional_fields() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client
    let client = server.http_client();

    // Send a write request without database parameter or client IP
    let response = client
        .post(format!("{}/api/v3/write_lp", server.client_addr()))
        .header("content-type", "text/plain")
        .body("some data")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed (missing db parameter)
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let logs = server.get_logs(Some(5)).unwrap();
    // Check that the error log exists and contains no database,
    // because ip has a fallback (to use the remote address) it
    // will never be "unknown" too
    server.assert_log_contains("Error while handling request");
    server.assert_log_contains("client_ip=127.0.0.1");
    assert!(!logs.contains("database="));
}

#[tokio::test]
async fn test_client_ip_extraction_x_forwarded_for_multiple() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client
    let client = server.http_client();

    // Send a write request with multiple IPs in x-forwarded-for
    let response = client
        .post(format!("{}/api/v3/write_lp?db=test", server.client_addr()))
        .header("content-type", "text/plain")
        .header("x-forwarded-for", "192.168.1.100, 10.0.0.1, 172.16.0.1")
        .body("invalid line protocol !")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check that the error log contains only the first IP
    server.assert_log_contains("client_ip=192.168.1.100");

    // Verify it doesn't contain the other IPs
    let logs = server.get_logs(Some(5)).unwrap();
    assert!(!logs.contains("client_ip=10.0.0.1"));
    assert!(!logs.contains("client_ip=172.16.0.1"));
}

#[test_log::test(tokio::test)]
async fn test_error_logging_with_socket_address_fallback() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Create a client without any proxy headers
    let client = server.http_client();

    // Send a write request that will fail (invalid line protocol) without proxy headers
    let response = client
        .post(format!(
            "{}/api/v3/write_lp?db=test_db",
            server.client_addr()
        ))
        .header("content-type", "text/plain")
        .body("invalid line protocol !")
        .send()
        .await
        .expect("send write request");

    // Ensure the request failed
    assert!(!response.status().is_success());

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check that the error log contains a client IP (from socket address)
    // The IP should be either 127.0.0.1 (localhost) or ::1 (IPv6 localhost)
    let logs = server.get_logs(Some(5)).unwrap();

    assert!(logs.contains("Error while handling request"));
    assert!(logs.contains("database=test_db"));
    assert!(logs.contains("client_ip=127.0.0.1") || logs.contains("client_ip=::1"));
}

#[tokio::test]
async fn test_get_logs_with_last_n_lines() {
    // Create a test server with log capture enabled
    let config = TestConfig::default().with_capture_logs();
    let server = config.spawn().await;

    // Generate multiple log entries
    let client = server.http_client();

    // Make several requests to generate log lines
    for i in 0..10 {
        let resp = client
            .post(format!(
                "{}/api/v3/write_lp?db=test_db{i}",
                server.client_addr(),
            ))
            .body("invalid line protocol")
            .send()
            .await
            .expect("send write request");

        // These requests will fail, generating error logs
        assert!(!resp.status().is_success());
    }

    // Wait a bit for logs to be captured
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Get all logs
    let all_logs = server.get_logs(None).unwrap();
    let all_lines_count = all_logs.lines().count();

    // Get only last 3 lines
    let last_3_logs = server.get_logs(Some(3)).unwrap();
    let last_3_lines_count = last_3_logs.lines().count();

    // Verify we got exactly 3 lines (or fewer if logs don't have that many)
    assert!(
        last_3_lines_count <= 3,
        "Expected at most 3 lines, got {last_3_lines_count}",
    );

    // Verify these are actually the last lines from all logs
    let all_lines: Vec<&str> = all_logs.lines().collect();
    if all_lines.len() >= 3 {
        // Check that the last 3 lines appear in the correct order
        let last_3_lines_vec: Vec<&str> = last_3_logs.lines().collect();
        let all_last_3 = &all_lines[all_lines.len() - 3..];
        assert_eq!(last_3_lines_vec.len(), 3);
        for (i, line) in last_3_lines_vec.iter().enumerate() {
            assert_eq!(*line, all_last_3[i], "Line {i} doesn't match");
        }
    }

    // Test edge case: request more lines than available
    let more_than_available = server.get_logs(Some(all_lines_count + 10)).unwrap();
    // Just check they have the same number of lines since trailing newline handling might differ
    assert_eq!(
        more_than_available.lines().count(),
        all_logs.lines().count(),
        "When requesting more lines than available, should return all logs"
    );
}
