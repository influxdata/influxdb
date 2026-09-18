//! End-to-end coverage for explicit schema mode in the oss workspace, which
//! runs the Parquet engine only. The ent suite covers PachaTree as well.

use influxdb3_catalog::catalog::SchemaMode;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use reqwest::StatusCode;
use serde_json::json;

use crate::server::TestServer;

const DB: &str = "enforced";
const TABLE: &str = "cpu";

async fn declared_server(server: &TestServer) {
    server
        .api_v3_create_database_with_schema_mode(DB, None, SchemaMode::Explicit)
        .await
        .expect("create explicit database");
    server
        .api_v3_create_table(
            DB,
            TABLE,
            vec!["host".to_string()],
            vec![(
                "usage".to_string(),
                influxdb3_types::http::FieldType::Float64,
            )],
        )
        .await
        .expect("declare table");
}

#[tokio::test]
async fn explicit_schema_is_enforced() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;

    server
        .write_lp_to_db(
            DB,
            format!("{TABLE},host=a usage=1.0 1000"),
            Precision::Second,
        )
        .await
        .expect("declared write should be accepted");

    let err = server
        .write_lp_to_db(
            DB,
            format!("{TABLE},host=a,region=west usage=1.0 2000"),
            Precision::Second,
        )
        .await
        .expect_err("undeclared tag should be rejected");
    assert!(err.to_string().contains("region"), "got {err}");

    let err = server
        .write_lp_to_db(DB, "mem,host=a used=1.0 3000", Precision::Second)
        .await
        .expect_err("undeclared table should be rejected");
    assert!(err.to_string().contains("mem"), "got {err}");
}

#[tokio::test]
async fn implicit_is_the_default_and_unchanged() {
    let server = TestServer::spawn().await;
    server
        .api_v3_create_database("open", None)
        .await
        .expect("create database without the flag");

    server
        .write_lp_to_db("open", "cpu,host=a usage=1.0 1000", Precision::Second)
        .await
        .expect("first write creates the table");
    server
        .write_lp_to_db(
            "open",
            "cpu,host=a,region=west usage=1.0,free=2i 2000",
            Precision::Second,
        )
        .await
        .expect("later write widens the schema");
}

/// `PATCH /api/v3/configure/table` is new in oss.
#[tokio::test]
async fn patch_table_adds_columns() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;

    let resp = server
        .http_client()
        .patch(format!("{}/api/v3/configure/table", server.client_addr()))
        .json(&json!({
            "db": DB,
            "table": TABLE,
            "tags": ["region"],
            "fields": [{"name": "free", "type": "int64"}]
        }))
        .send()
        .await
        .expect("add columns");
    assert_eq!(resp.status(), StatusCode::OK);

    server
        .write_lp_to_db(
            DB,
            format!("{TABLE},host=a,region=west usage=1.0,free=2i 4000"),
            Precision::Second,
        )
        .await
        .expect("write should be accepted once the columns are declared");
}

#[tokio::test]
async fn patch_table_is_add_only() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;

    // Re-declaring a column at a different type is rejected.
    let resp = server
        .http_client()
        .patch(format!("{}/api/v3/configure/table", server.client_addr()))
        .json(&json!({
            "db": DB,
            "table": TABLE,
            "fields": [{"name": "usage", "type": "utf8"}]
        }))
        .send()
        .await
        .expect("retype request");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// A request naming no columns would commit nothing; it should say so rather
/// than reporting success.
#[tokio::test]
async fn patch_table_rejects_an_empty_request() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;

    let resp = server
        .http_client()
        .patch(format!("{}/api/v3/configure/table", server.client_addr()))
        .json(&json!({ "db": DB, "table": TABLE }))
        .send()
        .await
        .expect("empty request");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn patch_table_requires_an_existing_table() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;

    let resp = server
        .http_client()
        .patch(format!("{}/api/v3/configure/table", server.client_addr()))
        .json(&json!({ "db": DB, "table": "missing", "tags": ["host"] }))
        .send()
        .await
        .expect("unknown table");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// Inspection goes through the query API rather than a dedicated endpoint:
/// `system.databases.schema_mode` says which databases are enforced, and
/// joining `system.tables` on `database_name` says which tables they hold.
#[tokio::test]
async fn system_tables_report_enforced_databases_and_tables() {
    let server = TestServer::spawn().await;
    declared_server(&server).await;
    server
        .api_v3_create_database("open", None)
        .await
        .expect("create implicit database");
    server
        .write_lp_to_db("open", "mem,host=a used=1.0 1000", Precision::Second)
        .await
        .expect("create an implicit table");

    let body = server
        .api_v3_query_sql(&[
            ("db", "_internal"),
            ("format", "json"),
            (
                "q",
                "SELECT t.database_name, t.table_name \
                 FROM system.tables t \
                 JOIN system.databases d ON d.database_name = t.database_name \
                 WHERE d.schema_mode = 'explicit' \
                 ORDER BY t.table_name",
            ),
        ])
        .await
        .text()
        .await
        .expect("read query response");

    assert_eq!(
        body,
        format!("[{{\"database_name\":\"{DB}\",\"table_name\":\"{TABLE}\"}}]"),
        "only the explicit database's tables should be listed"
    );
}
