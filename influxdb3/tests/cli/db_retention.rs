use crate::server::{ConfigProvider, TestServer};
use serde_json::Value;
use test_helpers::assert_contains;

#[test_log::test(tokio::test)]
async fn test_create_db_with_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_db";

    // Create database with retention period
    let retention_period = "30d";
    let result = server
        .run(
            vec![
                "create",
                "database",
                db_name,
                "--retention-period",
                retention_period,
            ],
            args,
        )
        .expect("create database should succeed");

    assert_contains!(&result, "Database \"test_db\" created successfully");

    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                "SELECT retention_period_ns FROM system.databases WHERE system.databases.database_name='test_db'",
            ],
            args,
        )
        .expect("create database with retention period should succeed");

    assert_eq!(&result, "[{\"retention_period_ns\":2592000000000000}]");
}

#[test_log::test(tokio::test)]
async fn test_create_db_without_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let db_name = "test_db2";
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];

    // Create database without retention period
    let result = server
        .run(vec!["create", "database", db_name], args)
        .expect("create database without retention period should succeed");

    assert_contains!(&result, "Database \"test_db2\" created successfully");

    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                "SELECT retention_period_ns FROM system.databases WHERE system.databases.database_name='test_db2'",
            ],
            args,
        )
        .expect("create database without retention period should succeed");

    assert_eq!(&result, "[{}]");
}

#[test_log::test(tokio::test)]
async fn test_create_db_with_invalid_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let db_name = "test_db3";
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];

    // Try to create database with invalid retention period
    let result = server.run(
        vec![
            "create",
            "database",
            db_name,
            "--retention-period",
            "invalid",
        ],
        args,
    );

    assert!(
        result.is_err(),
        "Creating table with invalid retention period should fail"
    );
}

#[test_log::test(tokio::test)]
async fn test_update_db_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_db_update";

    // Create database with retention period
    let result = server
        .run(
            vec!["create", "database", db_name, "--retention-period", "30d"],
            args,
        )
        .expect("create database should succeed");

    assert_contains!(
        &result,
        format!("Database \"{db_name}\" created successfully")
    );

    // Update database retention period
    let result = server
        .run(
            vec![
                "update",
                "database",
                "--database",
                db_name,
                "--retention-period",
                "60d",
            ],
            args,
        )
        .expect("update database retention period should succeed");

    assert_contains!(
        &result,
        format!("Database \"{db_name}\" updated successfully")
    );

    // Verify the updated retention period
    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!("SELECT retention_period_ns FROM system.databases WHERE system.databases.database_name='{db_name}'"),
            ],
            args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{\"retention_period_ns\":5184000000000000}]"); // 60 days in nanoseconds
}

#[test_log::test(tokio::test)]
async fn test_clear_db_retention_period() {
    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "test_db_clear";

    // Create database with retention period
    let result = server
        .run(
            vec!["create", "database", db_name, "--retention-period", "30d"],
            args,
        )
        .expect("create database should succeed");

    assert_contains!(
        &result,
        format!("Database \"{db_name}\" created successfully")
    );

    // Clear database retention period (set to none)
    let result = server
        .run(
            vec![
                "update",
                "database",
                "--database",
                db_name,
                "--retention-period",
                "none",
            ],
            args,
        )
        .expect("clear database retention period should succeed");

    assert_contains!(
        &result,
        format!("Database \"{db_name}\" updated successfully")
    );

    // Verify the retention period is now none (cleared)
    let args = &[
        "--tls-ca",
        "../testing-certs/rootCA.pem",
        "--format",
        "json",
    ];

    let result = server
        .run(
            vec![
                "query",
                "-d",
                "_internal",
                &format!("SELECT retention_period_ns FROM system.databases WHERE system.databases.database_name='{db_name}'"),
            ],
            args,
        )
        .expect("query should succeed");

    assert_eq!(&result, "[{}]"); // Empty object for none/cleared retention
}

#[test_log::test(tokio::test)]
async fn test_update_db_retention_after_delete() {
    // Testing that updating the retention period of a deleted database should fail.

    let server = TestServer::configure().with_no_admin_token().spawn().await;
    let args = &["--tls-ca", "../testing-certs/rootCA.pem"];
    let db_name = "retention_deleted_db";

    // Create database
    server
        .run(vec!["create", "database", db_name], args)
        .expect("create database should succeed");

    // Delete database
    let delete_output = server
        .delete_database(db_name)
        .run()
        .expect("delete database should succeed");
    assert_contains!(
        &delete_output,
        format!("Database \"{db_name}\" deleted successfully")
    );

    // Get the deleted database name from show databases
    let show_output = server
        .show_databases()
        .with_format("json")
        .show_deleted(true)
        .run()
        .expect("show databases should succeed");
    let databases: Vec<Value> =
        serde_json::from_str(&show_output).expect("show databases output should be valid json");
    let deleted_db_name = databases
        .into_iter()
        .find_map(|entry| {
            let deleted = entry
                .get("deleted")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let name = entry.get("iox::database").and_then(Value::as_str);
            match (deleted, name) {
                (true, Some(name)) if name.starts_with(db_name) => Some(name.to_string()),
                _ => None,
            }
        })
        .expect("deleted database name should be visible via show databases");

    let err = server
        .run(
            vec![
                "update",
                "database",
                "--database",
                &deleted_db_name,
                "--retention-period",
                "3h",
            ],
            args,
        )
        .expect_err("updating retention on a deleted database should fail");
    assert_contains!(
        &err.to_string(),
        "Update command failed: server responded with error [409 Conflict]: attempted to modify resource that was already deleted: "
    );
}
