use hyper::StatusCode;

use crate::TestServer;

#[tokio::test]
async fn api_v3_configure_last_cache_create() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/last_cache",
        base = server.client_addr()
    );

    // Write some LP to the database to initialize the catalog:
    let db_name = "db";
    let tbl_name = "tbl";
    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    #[derive(Default)]
    struct TestCase {
        // These attributes all map to parameters of the request body:
        db: Option<&'static str>,
        table: Option<&'static str>,
        cache_name: Option<&'static str>,
        count: Option<usize>,
        ttl: Option<usize>,
        key_cols: Option<&'static [&'static str]>,
        val_cols: Option<&'static [&'static str]>,
        // This is the status code expected in the response:
        expected: StatusCode,
    }

    let test_cases = [
        // No parameters specified:
        TestCase {
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Missing table name:
        TestCase {
            db: Some(db_name),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Good, will use defaults for everything omitted, and get back a 201:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        // Same as before, will be successful, but with 204:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        // Use a specific cache name, will succeed and create new cache:
        // NOTE: this will only differ from the previous cache in name, should this actually
        // be an error?
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        // Same as previous, but will get 204 because it does nothing:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        // Same as previous, but this time try to use different parameters, this will result in
        // a bad request:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            // The default TTL that would have been used is 4 * 60 * 60 seconds (4 hours)
            ttl: Some(666),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Will create new cache, because key columns are unique, and so will be the name:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["t1", "t2"]),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        // Same as previous, but will get 204 because nothing happens:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["t1", "t2"]),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        // Use an invalid key column (by name) is a bad request:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["not_a_key_column"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Use an invalid key column (by type) is a bad request:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            // f5 is a float, which is not supported as a key column:
            key_cols: Some(&["f5"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Use an invalid value column is a bad request:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            val_cols: Some(&["not_a_value_column"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        // Use an invalid cache size is a bad request:
        TestCase {
            db: Some(db_name),
            table: Some(tbl_name),
            count: Some(11),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
    ];

    for (i, t) in test_cases.into_iter().enumerate() {
        let body = serde_json::json!({
            "db": t.db,
            "table": t.table,
            "name": t.cache_name,
            "key_columns": t.key_cols,
            "value_columns": t.val_cols,
            "count": t.count,
            "ttl": t.ttl,
        });
        let resp = client
            .post(&url)
            .json(&body)
            .send()
            .await
            .expect("send /api/v3/configure/last_cache request");
        let status = resp.status();
        assert_eq!(t.expected, status, "test case ({i}) failed");
    }
}
