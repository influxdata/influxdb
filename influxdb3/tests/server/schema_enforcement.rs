//! End-to-end coverage for the schema enforcement surface in the oss
//! workspace: Core refuses to create an explicit database and names
//! Enterprise in the error, and `PATCH /api/v3/configure/table` declares
//! columns on the implicit databases Core does create.

use influxdb3_catalog::catalog::SchemaMode;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use reqwest::StatusCode;
use serde_json::json;

use crate::server::TestServer;

const DB: &str = "sensors";
const TABLE: &str = "cpu";

async fn server_with_table(server: &TestServer) {
    server
        .api_v3_create_database(DB, None)
        .await
        .expect("create database");
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
        .expect("create table");
}

/// Core rejects `schema_mode: explicit` before touching the catalog, and the
/// error says where the feature is available. The same name can be created
/// afterwards, so nothing was left behind.
#[tokio::test]
async fn explicit_schema_mode_is_enterprise_only() {
    let server = TestServer::spawn().await;

    let err = server
        .api_v3_create_database_with_schema_mode(DB, None, SchemaMode::Explicit)
        .await
        .expect_err("explicit schema mode should be refused");
    let influxdb3_client::Error::ApiError { code, message, .. } = err else {
        panic!("expected an API error, got {err}");
    };
    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert_eq!(
        message,
        "explicit schema mode is only available in InfluxDB 3 Enterprise"
    );

    server
        .api_v3_create_database(DB, None)
        .await
        .expect("the refused request should not have created the database");
}

/// `schema_mode: implicit` is accepted and means what the default means.
#[tokio::test]
async fn implicit_schema_mode_is_accepted() {
    let server = TestServer::spawn().await;

    server
        .api_v3_create_database_with_schema_mode(DB, None, SchemaMode::Implicit)
        .await
        .expect("create database with schema_mode: implicit");
    server
        .api_v3_create_database("open", None)
        .await
        .expect("create database without schema_mode");

    for db in [DB, "open"] {
        server
            .write_lp_to_db(db, "cpu,host=a usage=1.0 1000", Precision::Second)
            .await
            .expect("first write creates the table");
        server
            .write_lp_to_db(
                db,
                "cpu,host=a,region=west usage=1.0,free=2i 2000",
                Precision::Second,
            )
            .await
            .expect("later write widens the schema");
    }
}

/// Declared columns bind their type: a write at another type is rejected.
#[tokio::test]
async fn patch_table_adds_columns() {
    let server = TestServer::spawn().await;
    server_with_table(&server).await;

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
        .expect("write at the declared types should be accepted");

    let err = server
        .write_lp_to_db(
            DB,
            format!("{TABLE},host=a,region=west usage=1.0,free=2.0 5000"),
            Precision::Second,
        )
        .await
        .expect_err("write at another type should be rejected");
    assert!(err.to_string().contains("free"), "got {err}");
}

#[tokio::test]
async fn patch_table_is_add_only() {
    let server = TestServer::spawn().await;
    server_with_table(&server).await;

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
    server_with_table(&server).await;

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
    server_with_table(&server).await;

    let resp = server
        .http_client()
        .patch(format!("{}/api/v3/configure/table", server.client_addr()))
        .json(&json!({ "db": DB, "table": "missing", "tags": ["host"] }))
        .send()
        .await
        .expect("unknown table");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
