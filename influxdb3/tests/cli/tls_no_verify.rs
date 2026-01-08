//! Integration tests for the `--tls-no-verify` CLI flag
//!
//! These tests verify that CLI commands work correctly when connecting to a server
//! with invalid/expired TLS certificates when the `--tls-no-verify` flag is used.

use crate::server::{ConfigProvider, TestServer};

// =============================================================================
// Simple commands (no database setup needed)
// =============================================================================

/// Test that `show databases` succeeds with `--tls-no-verify` against bad TLS server
#[test_log::test(tokio::test)]
async fn test_show_databases_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    let result = server
        .run(vec!["show", "databases", "--tls-no-verify"], &[])
        .unwrap();

    assert!(!result.contains("certificate"));
}

/// Test that `show plugins` succeeds with `--tls-no-verify` against bad TLS server
#[test_log::test(tokio::test)]
async fn test_show_plugins_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    let result = server
        .run(vec!["show", "plugins", "--tls-no-verify"], &[])
        .unwrap();

    assert!(!result.contains("certificate"));
}

// =============================================================================
// Commands that need a database
// =============================================================================

/// Test query command with `--tls-no-verify`
#[test_log::test(tokio::test)]
async fn test_query_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    // Create database first
    server
        .run(vec!["create", "database", "--tls-no-verify"], &["testdb"])
        .unwrap();

    let result = server
        .run(
            vec!["query", "--tls-no-verify", "-d", "testdb"],
            &["SELECT 1"],
        )
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

// =============================================================================
// Commands that need a database and table
// =============================================================================

/// Test write command
#[test_log::test(tokio::test)]
async fn test_write_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(vec!["create", "database", "--tls-no-verify"], &["writedb"])
        .unwrap();

    let result = server
        .run(
            vec!["write", "--tls-no-verify", "-d", "writedb"],
            &["cpu,host=server01 value=0.64"],
        )
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

/// Test create table command
#[test_log::test(tokio::test)]
async fn test_create_table_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(vec!["create", "database", "--tls-no-verify"], &["tabledb"])
        .unwrap();

    let result = server
        .run(
            vec![
                "create",
                "table",
                "--tls-no-verify",
                "-d",
                "tabledb",
                "--tags",
                "host",
                "--fields",
                "value:float64",
            ],
            &["cpu"],
        )
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

/// Test delete table command
#[test_log::test(tokio::test)]
async fn test_delete_table_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(
            vec!["create", "database", "--tls-no-verify"],
            &["deltabledb"],
        )
        .unwrap();

    // Create table via write
    server
        .run(
            vec!["write", "--tls-no-verify", "-d", "deltabledb"],
            &["cpu,host=server01 value=0.64"],
        )
        .unwrap();

    let result = server
        .run_with_confirmation(
            vec!["delete", "table", "--tls-no-verify", "-d", "deltabledb"],
            &["cpu"],
        )
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

// =============================================================================
// Cache commands (last_cache and distinct_cache)
// =============================================================================

/// Test create and delete last_cache commands
#[test_log::test(tokio::test)]
async fn test_last_cache_commands_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(vec!["create", "database", "--tls-no-verify"], &["cachedb"])
        .unwrap();

    // Create table via write
    server
        .run(
            vec!["write", "--tls-no-verify", "-d", "cachedb"],
            &["cpu,host=server01 value=0.64"],
        )
        .unwrap();

    // Create last cache
    let result = server
        .run(
            vec![
                "create",
                "last_cache",
                "--tls-no-verify",
                "-d",
                "cachedb",
                "-t",
                "cpu",
            ],
            &[],
        )
        .unwrap();
    assert!(
        !result.contains("certificate"),
        "Create last_cache failed: {result}"
    );

    // Delete last cache
    let result = server
        .run_with_confirmation(
            vec![
                "delete",
                "last_cache",
                "--tls-no-verify",
                "-d",
                "cachedb",
                "-t",
                "cpu",
            ],
            &["cpu_host_last_cache"],
        )
        .unwrap();
    assert!(
        !result.contains("certificate"),
        "Delete last_cache failed: {result}"
    );
}

/// Test create and delete distinct_cache commands
#[test_log::test(tokio::test)]
async fn test_distinct_cache_commands_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(
            vec!["create", "database", "--tls-no-verify"],
            &["dvcachedb"],
        )
        .unwrap();

    // Create table via write
    server
        .run(
            vec!["write", "--tls-no-verify", "-d", "dvcachedb"],
            &["cpu,host=server01 value=0.64"],
        )
        .unwrap();

    // Create distinct cache
    let result = server
        .run(
            vec![
                "create",
                "distinct_cache",
                "--tls-no-verify",
                "-d",
                "dvcachedb",
                "-t",
                "cpu",
                "--columns",
                "host",
            ],
            &[],
        )
        .unwrap();
    assert!(
        !result.contains("certificate"),
        "Create distinct_cache failed: {result}"
    );

    // Delete distinct cache
    let result = server
        .run_with_confirmation(
            vec![
                "delete",
                "distinct_cache",
                "--tls-no-verify",
                "-d",
                "dvcachedb",
                "-t",
                "cpu",
            ],
            &["cpu_host_distinct_cache"],
        )
        .unwrap();
    assert!(
        !result.contains("certificate"),
        "Delete distinct_cache failed: {result}"
    );
}

// =============================================================================
// Database create/delete commands
// =============================================================================

/// Test create database command
#[test_log::test(tokio::test)]
async fn test_create_database_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    let result = server
        .run(vec!["create", "database", "--tls-no-verify"], &["newdb"])
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

/// Test delete database command
#[test_log::test(tokio::test)]
async fn test_delete_database_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(vec!["create", "database", "--tls-no-verify"], &["deletedb"])
        .unwrap();

    let result = server
        .run_with_confirmation(vec!["delete", "database", "--tls-no-verify"], &["deletedb"])
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

// =============================================================================
// Update commands
// =============================================================================

/// Test update database command
#[test_log::test(tokio::test)]
async fn test_update_database_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(vec!["create", "database", "--tls-no-verify"], &["updatedb"])
        .unwrap();

    // Update database with a retention period
    let result = server
        .run(
            vec![
                "update",
                "database",
                "--tls-no-verify",
                "-d",
                "updatedb",
                "--retention-period",
                "30d",
            ],
            &[],
        )
        .unwrap();

    assert!(
        !result.contains("certificate"),
        "Unexpected output: {result}"
    );
}

/// Test update table command
#[test_log::test(tokio::test)]
async fn test_update_table_with_tls_no_verify() {
    let server = TestServer::configure().with_bad_tls(true).spawn().await;

    server
        .run(
            vec!["create", "database", "--tls-no-verify"],
            &["updatetabledb"],
        )
        .unwrap();

    // Create table via write
    server
        .run(
            vec!["write", "--tls-no-verify", "-d", "updatetabledb"],
            &["cpu,host=server01 value=0.64"],
        )
        .unwrap();
}
