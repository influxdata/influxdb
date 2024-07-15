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
        // Missing database name:
        TestCase {
            table: Some(tbl_name),
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

#[tokio::test]
async fn api_v3_configure_last_cache_delete() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/last_cache",
        base = server.client_addr()
    );

    // Write some LP to the database to initialize the catalog:
    let db_name = "db";
    let tbl_name = "tbl";
    let cache_name = "test_cache";
    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    struct TestCase {
        request: Request,
        // This is the status code expected in the response:
        expected: StatusCode,
    }

    enum Request {
        Create(serde_json::Value),
        Delete(DeleteRequest),
    }

    #[derive(Default)]
    struct DeleteRequest {
        db: Option<&'static str>,
        table: Option<&'static str>,
        name: Option<&'static str>,
    }

    use Request::*;
    let mut test_cases = [
        // Create a cache:
        TestCase {
            request: Create(serde_json::json!({
                "db": db_name,
                "table": tbl_name,
                "name": cache_name,
            })),
            expected: StatusCode::CREATED,
        },
        // Missing all params:
        TestCase {
            request: Delete(DeleteRequest {
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        // Partial params:
        TestCase {
            request: Delete(DeleteRequest {
                db: Some(db_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        // Partial params:
        TestCase {
            request: Delete(DeleteRequest {
                table: Some(tbl_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        // Partial params:
        TestCase {
            request: Delete(DeleteRequest {
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        // Partial params:
        TestCase {
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        // All params, good:
        TestCase {
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::OK,
        },
        // Same as previous, with correct parameters provided, but gest 404, as its already deleted:
        TestCase {
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::NOT_FOUND,
        },
    ];

    // Do one pass using the JSON body to delete:
    for (i, t) in test_cases.iter().enumerate() {
        match &t.request {
            Create(body) => assert!(
                client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .expect("create request succeeds")
                    .status()
                    .is_success(),
                "Creation test case ({i}) failed"
            ),
            Delete(req) => {
                let body = serde_json::json!({
                    "db": req.db,
                    "table": req.table,
                    "name": req.name,
                });
                let resp = client
                    .delete(&url)
                    .json(&body)
                    .send()
                    .await
                    .expect("send /api/v3/configure/last_cache request");
                let status = resp.status();
                assert_eq!(
                    t.expected, status,
                    "Deletion test case ({i}) using JSON body failed"
                );
            }
        }
    }

    // Do another pass using the URI query string to delete:
    // Note: this particular test exhibits different status code, because the empty query string
    // as a result of there being no parameters provided makes the request handler attempt to
    // parse the body as JSON - which gives a 415 error, because there is no body or content type
    test_cases[1].expected = StatusCode::UNSUPPORTED_MEDIA_TYPE;
    for (i, t) in test_cases.iter().enumerate() {
        match &t.request {
            Create(body) => assert!(
                client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .expect("create request succeeds")
                    .status()
                    .is_success(),
                "Creation test case ({i}) failed"
            ),
            Delete(req) => {
                let mut params = vec![];
                if let Some(db) = req.db {
                    params.push(("db", db));
                }
                if let Some(table) = req.table {
                    params.push(("table", table));
                }
                if let Some(name) = req.name {
                    params.push(("name", name));
                }
                let resp = client
                    .delete(&url)
                    .query(&params)
                    .send()
                    .await
                    .expect("send /api/v3/configure/last_cache request");
                let status = resp.status();
                assert_eq!(
                    t.expected, status,
                    "Deletion test case ({i}) using URI query string failed"
                );
            }
        }
    }
}
