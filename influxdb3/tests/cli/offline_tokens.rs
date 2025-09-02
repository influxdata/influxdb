use tempfile::TempDir;
use test_helpers::assert_contains;

use crate::cli::api::run_cmd_with_result;
use crate::server::{ConfigProvider, TestServer};
use reqwest::StatusCode;
use serde_json::Value;

#[test_log::test(tokio::test)]
async fn test_offline_token_creation_flow() {
    // Create temporary directory for token files
    let temp_dir = TempDir::new().unwrap();
    let admin_token_file = temp_dir.path().join("admin_token.json");

    // Step 1: Create offline admin token using CLI
    println!("Creating offline admin token...");
    let result = run_cmd_with_result(
        &[],
        None,
        vec![
            "create",
            "token",
            "--admin",
            "--offline",
            "--output-file",
            admin_token_file.to_str().unwrap(),
        ],
    )
    .unwrap();
    println!("Admin token creation result: {}", result);
    assert_contains!(&result, "Token saved to:");

    // Parse admin token from file
    let admin_token_content = std::fs::read_to_string(&admin_token_file).unwrap();
    let admin_token_json: Value = serde_json::from_str(&admin_token_content).unwrap();
    let admin_token = admin_token_json["token"].as_str().unwrap().to_string();

    // Step 2: Start server with token files
    let mut server = TestServer::configure()
        .with_auth()
        .with_no_admin_token()
        .with_admin_token_file(admin_token_file.to_str().unwrap())
        .spawn()
        .await;

    // Set the admin token for further operations
    server.set_token(Some(admin_token.clone()));

    // Step 4: Verify tokens exist
    let result = server
        .run(
            vec!["show", "tokens"],
            &["--tls-ca", "../testing-certs/rootCA.pem"],
        )
        .unwrap();
    assert_contains!(&result, "_admin");

    // Step 4: Verify database creation
    let result = server
        .run(
            vec!["show", "databases"],
            &["--tls-ca", "../testing-certs/rootCA.pem"],
        )
        .unwrap();
    assert_contains!(&result, "_internal");

    // Step 5: Test admin token privileges
    let client = server.http_client();
    let base = server.client_addr();

    // Admin can query _internal database
    let query_sql_url = format!("{base}/api/v3/query_sql");
    let response = client
        .get(&query_sql_url)
        .query(&[
            ("db", "_internal"),
            ("q", "SELECT * FROM system.tables LIMIT 1"),
        ])
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Admin can create a new database
    let result = server
        .run(
            vec!["create", "database", "admin_test_db"],
            &["--tls-ca", "../testing-certs/rootCA.pem"],
        )
        .unwrap();
    assert_contains!(&result, "Database \"admin_test_db\" created successfully");
}
