use hyper::StatusCode;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use test_helpers::assert_contains;

use crate::server::TestServer;

#[tokio::test]
async fn api_v3_configure_distinct_cache_create() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/distinct_cache",
        base = server.client_addr()
    );

    // Write some LP to the database to initialize the catalog
    let db_name = "foo";
    let tbl_name = "bar";
    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=aa,t3=aaa f1=\"potato\",f2=true,f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    #[derive(Default)]
    struct TestCase {
        description: &'static str,
        db: Option<&'static str>,
        table: Option<&'static str>,
        cache_name: Option<&'static str>,
        max_cardinality: Option<usize>,
        max_age: Option<u64>,
        columns: &'static [&'static str],
        expected: StatusCode,
    }

    let test_cases = [
        TestCase {
            description: "no parameters specified",
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "missing database name",
            table: Some("bar"),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "invalid database name",
            db: Some("carrot"),
            table: Some("bar"),
            expected: StatusCode::NOT_FOUND,
            ..Default::default()
        },
        TestCase {
            description: "invalid table name",
            db: Some("foo"),
            table: Some("celery"),
            expected: StatusCode::NOT_FOUND,
            ..Default::default()
        },
        TestCase {
            description: "no columns specified",
            db: Some("foo"),
            table: Some("bar"),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "invalid column specified, that does not exist",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["leek"],
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "invalid column specified, that is not tag or string",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["f2"],
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "valid request, creates cache",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1", "t2"],
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        TestCase {
            description: "identical to previous request, still success, but results in no 204 content",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1", "t2"],
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        TestCase {
            description: "same as previous, but with incompatible configuratin, max cardinality",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1", "t2"],
            max_cardinality: Some(1),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "same as previous, but with incompatible configuratin, max age",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1", "t2"],
            max_age: Some(1),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "valid request, creates cache",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1"],
            cache_name: Some("my_cache"),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        TestCase {
            description: "same as previous, but with incompatible configuratino, column set",
            db: Some("foo"),
            table: Some("bar"),
            columns: &["t1", "t2"],
            cache_name: Some("my_cache"),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
    ];

    for tc in test_cases {
        let body = serde_json::json!({
            "db": tc.db,
            "table": tc.table,
            "name": tc.cache_name,
            "columns": tc.columns,
            "max_cardinality": tc.max_cardinality,
            "max_age": tc.max_age
        });
        let resp = client
            .post(&url)
            .json(&body)
            .send()
            .await
            .expect("send request to create distinct cache");
        let status = resp.status();
        assert_eq!(
            tc.expected,
            status,
            "test case failed: {description}",
            description = tc.description
        );
    }
}

#[tokio::test]
async fn api_v3_configure_distinct_cache_delete() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/distinct_cache",
        base = server.client_addr()
    );

    let db_name = "foo";
    let tbl_name = "bar";
    let cache_name = "test_cache";
    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=aa f1=true 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write lp to db");

    struct TestCase {
        description: &'static str,
        request: Request,
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
        TestCase {
            description: "create a distinct cache",
            request: Create(serde_json::json!({
                "db": db_name,
                "table": tbl_name,
                "name": cache_name,
                "columns": ["t1", "t2"],
            })),
            expected: StatusCode::CREATED,
        },
        TestCase {
            description: "delete with no parameters fails",
            request: Delete(DeleteRequest {
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing table, name params",
            request: Delete(DeleteRequest {
                db: Some(db_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing name param",
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing table param",
            request: Delete(DeleteRequest {
                db: Some(db_name),
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing db param",
            request: Delete(DeleteRequest {
                table: Some(tbl_name),
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing db, table param",
            request: Delete(DeleteRequest {
                name: Some(cache_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "missing db, name param",
            request: Delete(DeleteRequest {
                table: Some(tbl_name),
                ..Default::default()
            }),
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            description: "valid request, will delete",
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                name: Some(cache_name),
            }),
            expected: StatusCode::OK,
        },
        TestCase {
            description: "same as previous, but gets 404 as the cache was already deleted",
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                name: Some(cache_name),
            }),
            expected: StatusCode::NOT_FOUND,
        },
    ];

    // do a pass through the test cases deleting via JSON in the body:
    for tc in &test_cases {
        match &tc.request {
            Create(body) => assert_eq!(
                tc.expected,
                client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .expect("create request sends")
                    .status(),
                "creation test case failed: {description}",
                description = tc.description
            ),
            Delete(DeleteRequest { db, table, name }) => {
                let body = serde_json::json!({
                    "db": db,
                    "table": table,
                    "name": name,
                });
                assert_eq!(
                    tc.expected,
                    client
                        .delete(&url)
                        .json(&body)
                        .send()
                        .await
                        .expect("delete request send")
                        .status(),
                    "deletion test case using JSON body failed: {description}",
                    description = tc.description
                );
            }
        }
    }

    // do another pass through the test cases, this time deleting via parameters in the URI:

    // on this pass, need to switch the status code that does not provide any params to a 415
    // UNSUPPORTED MEDIA TYPE error, as the handler sees no URI parameters, and attempts to parse
    // the body as JSON, thus resulting in the 415.
    test_cases[1].expected = StatusCode::UNSUPPORTED_MEDIA_TYPE;
    for tc in &test_cases {
        match &tc.request {
            Create(body) => assert_eq!(
                tc.expected,
                client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .expect("create request sends")
                    .status(),
                "creation test case failed: {description}",
                description = tc.description
            ),
            Delete(DeleteRequest { db, table, name }) => {
                let mut params = vec![];
                if let Some(db) = db {
                    params.push(("db", db));
                }
                if let Some(table) = table {
                    params.push(("table", table));
                }
                if let Some(name) = name {
                    params.push(("name", name));
                }
                assert_eq!(
                    tc.expected,
                    client
                        .delete(&url)
                        .query(&params)
                        .send()
                        .await
                        .expect("delete request send")
                        .status(),
                    "deletion test case using URI parameters failed: {description}",
                    description = tc.description
                );
            }
        }
    }
}

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
        description: &'static str,
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
        TestCase {
            description: "no parameters specified",
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "missing database name",
            table: Some(tbl_name),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "missing table name",
            db: Some(db_name),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "Good, will use defaults for everything omitted, and get back a 201",
            db: Some(db_name),
            table: Some(tbl_name),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        TestCase {
            description: "Same as before, will be successful, but with 204",
            db: Some(db_name),
            table: Some(tbl_name),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        // NOTE: this will only differ from the previous cache in name, should this actually
        // be an error?
        TestCase {
            description: "Use a specific cache name, will succeed and create new cache",
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        TestCase {
            description: "Same as previous, but will get 204 because it does nothing",
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        TestCase {
            description: "Same as previous, but this time try to use different parameters, this \
            will result in a bad request",
            db: Some(db_name),
            table: Some(tbl_name),
            cache_name: Some("my_cache"),
            // The default TTL that would have been used is 4 * 60 * 60 seconds (4 hours)
            ttl: Some(666),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "Will create new cache, because key columns are unique, and so will be the name",
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["t1", "t2"]),
            expected: StatusCode::CREATED,
            ..Default::default()
        },
        TestCase {
            description: "Same as previous, but will get 204 because nothing happens",
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["t1", "t2"]),
            expected: StatusCode::NO_CONTENT,
            ..Default::default()
        },
        TestCase {
            description: "Use an invalid key column (by name) is a bad request",
            db: Some(db_name),
            table: Some(tbl_name),
            key_cols: Some(&["not_a_key_column"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "Use an invalid key column (by type) is a bad request",
            db: Some(db_name),
            table: Some(tbl_name),
            // f5 is a float, which is not supported as a key column:
            key_cols: Some(&["f5"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "Use an invalid value column is a bad request",
            db: Some(db_name),
            table: Some(tbl_name),
            val_cols: Some(&["not_a_value_column"]),
            expected: StatusCode::BAD_REQUEST,
            ..Default::default()
        },
        TestCase {
            description: "Use an invalid cache size is a bad request",
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
        assert_eq!(
            t.expected,
            status,
            "test case ({i}) failed, {description}",
            description = t.description
        );
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
            }),
            expected: StatusCode::OK,
        },
        // Same as previous, with correct parameters provided, but gest 404, as its already deleted:
        TestCase {
            request: Delete(DeleteRequest {
                db: Some(db_name),
                table: Some(tbl_name),
                name: Some(cache_name),
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

#[test_log::test(tokio::test)]
async fn api_v3_configure_db_delete() {
    let db_name = "foo";
    let tbl_name = "tbl";
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database?db={db_name}",
        base = server.client_addr()
    );

    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // check foo db is present
    let result = server
        .api_v3_query_influxql(&[("q", "SHOW DATABASES"), ("format", "json")])
        .await
        .json::<Value>()
        .await
        .unwrap();
    debug!(result = ?result, ">> RESULT");
    assert_eq!(
        json!([{ "deleted": false, "iox::database": "foo" } ]),
        result
    );

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(200, resp.status());

    // check foo db is now foo-YYYYMMDD..
    let result = server
        .api_v3_query_influxql(&[("q", "SHOW DATABASES"), ("format", "json")])
        .await
        .json::<Value>()
        .await
        .unwrap();
    debug!(result = ?result, ">> RESULT");
    let array_result = result.as_array().unwrap();
    assert_eq!(1, array_result.len());
    let first_db = array_result.first().unwrap();
    assert_contains!(
        first_db
            .as_object()
            .unwrap()
            .get("iox::database")
            .unwrap()
            .as_str()
            .unwrap(),
        "foo-"
    );

    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = server
        .api_v3_query_influxql(&[("q", "SHOW DATABASES"), ("format", "json")])
        .await
        .json::<Value>()
        .await
        .unwrap();
    debug!(result = ?result, ">> RESULT");
    let array_result = result.as_array().unwrap();
    // check there are 2 dbs now, foo and foo-*
    assert_eq!(2, array_result.len());
    let first_db = array_result.first().unwrap();
    let second_db = array_result.get(1).unwrap();
    assert_eq!(
        "foo",
        first_db
            .as_object()
            .unwrap()
            .get("iox::database")
            .unwrap()
            .as_str()
            .unwrap(),
    );
    assert_contains!(
        second_db
            .as_object()
            .unwrap()
            .get("iox::database")
            .unwrap()
            .as_str()
            .unwrap(),
        "foo-"
    );
}

#[tokio::test]
async fn api_v3_configure_db_delete_no_db() {
    let db_name = "db";
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database?db={db_name}",
        base = server.client_addr()
    );

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(StatusCode::NOT_FOUND, resp.status());
}

#[tokio::test]
async fn api_v3_configure_db_delete_missing_query_param() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(StatusCode::BAD_REQUEST, resp.status());
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_db_create() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );

    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(StatusCode::OK, resp.status());
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_db_create_db_with_same_name() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );

    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("create database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());

    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(StatusCode::BAD_REQUEST, resp.status());
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_db_create_db_hit_limit() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );
    for i in 0..100 {
        let resp = client
            .post(&url)
            .json(&json!({ "db": format!("foo{i}") }))
            .send()
            .await
            .expect("create database call did not succeed");
        assert_eq!(StatusCode::OK, resp.status());
    }

    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo100" }))
        .send()
        .await
        .expect("create database succeeded");
    assert_eq!(StatusCode::UNPROCESSABLE_ENTITY, resp.status());
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_db_create_db_reuse_old_name() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );
    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("create database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());
    let resp = client
        .delete(format!("{url}?db=foo"))
        .send()
        .await
        .expect("delete database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());
    let resp = client
        .post(&url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("create database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_table_create_then_write() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let db_url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );
    let table_url = format!("{base}/api/v3/configure/table", base = server.client_addr());

    let resp = client
        .post(&db_url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("create database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());

    let resp = client
        .post(&table_url)
        .json(&json!({
            "db": "foo" ,
            "table": "bar",
            "tags": ["tag1", "tag2"],
            "fields": [
                {
                    "name": "field1",
                    "type": "uint64"
                },
                {
                    "name": "field2",
                    "type": "int64"
                },
                {
                    "name": "field3",
                    "type": "float64"
                },
                {
                    "name": "field4",
                    "type": "utf8"
                },
                {
                    "name": "field5",
                    "type": "bool"
                }
            ]

        }))
        .send()
        .await
        .expect("create table call failed");
    assert_eq!(StatusCode::OK, resp.status());
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(result, json!([]));
    server
        .write_lp_to_db(
            "foo",
            "bar,tag1=1,tag2=2 field1=1u,field2=2i,field3=3,field4=\"4\",field5=true 2998574938",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        result,
        json!([{
            "tag1": "1",
            "tag2": "2",
            "field1": 1,
            "field2": 2,
            "field3": 3.0,
            "field4": "4",
            "field5": true,
            "time": "2065-01-07T17:28:58"
        }])
    );
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_table_create_no_fields() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let db_url = format!(
        "{base}/api/v3/configure/database",
        base = server.client_addr()
    );
    let table_url = format!("{base}/api/v3/configure/table", base = server.client_addr());

    let resp = client
        .post(&db_url)
        .json(&json!({ "db": "foo" }))
        .send()
        .await
        .expect("create database call did not succeed");
    assert_eq!(StatusCode::OK, resp.status());

    let resp = client
        .post(&table_url)
        .json(&json!({
            "db": "foo" ,
            "table": "bar",
            "tags": ["one", "two"],
            "fields": []
        }))
        .send()
        .await
        .expect("create table call failed");
    assert_eq!(StatusCode::OK, resp.status());
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(result, json!([]));
    server
        .write_lp_to_db(
            "foo",
            "bar,one=1,two=2 new_field=0 2998574938",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");
    let result = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM bar"),
            ("format", "json"),
        ])
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        result,
        json!([{
            "one": "1",
            "two": "2",
            "new_field": 0.0,
            "time": "2065-01-07T17:28:58"
        }])
    );
}

#[test_log::test(tokio::test)]
async fn api_v3_configure_table_delete() {
    let db_name = "foo";
    let tbl_name = "tbl";
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/table?db={db_name}&table={tbl_name}",
        base = server.client_addr()
    );

    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete table call succeed");
    assert_eq!(200, resp.status());

    // check foo db has table with name in tbl-YYYYMMDD.. format
    let result = server
        .api_v3_query_influxql(&[("q", "SHOW MEASUREMENTS on foo"), ("format", "json")])
        .await
        .json::<Value>()
        .await
        .unwrap();
    debug!(result = ?result, ">> RESULT");
    let array_result = result.as_array().unwrap();
    assert_eq!(1, array_result.len());
    let first_db = array_result.first().unwrap();
    assert_contains!(
        first_db
            .as_object()
            .unwrap()
            .get("name")
            .unwrap()
            .as_str()
            .unwrap(),
        "tbl-"
    );

    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let result = server
        .api_v3_query_influxql(&[("q", "SHOW MEASUREMENTS on foo"), ("format", "json")])
        .await
        .json::<Value>()
        .await
        .unwrap();
    debug!(result = ?result, ">> RESULT");
    let array_result = result.as_array().unwrap();
    // check there are 2 tables now, tbl and tbl-*
    assert_eq!(2, array_result.len());
    let first_db = array_result.first().unwrap();
    let second_db = array_result.get(1).unwrap();
    assert_eq!(
        "tbl",
        first_db
            .as_object()
            .unwrap()
            .get("name")
            .unwrap()
            .as_str()
            .unwrap(),
    );
    assert_contains!(
        second_db
            .as_object()
            .unwrap()
            .get("name")
            .unwrap()
            .as_str()
            .unwrap(),
        "tbl-"
    );
}

#[tokio::test]
async fn api_v3_configure_table_delete_no_db() {
    let db_name = "db";
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!(
        "{base}/api/v3/configure/table?db={db_name}&table=foo",
        base = server.client_addr()
    );

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete database call succeed");
    assert_eq!(StatusCode::NOT_FOUND, resp.status());
}

#[tokio::test]
async fn api_v3_configure_table_delete_missing_query_param() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/v3/configure/table", base = server.client_addr());

    let resp = client
        .delete(&url)
        .send()
        .await
        .expect("delete table call succeed");
    assert_eq!(StatusCode::BAD_REQUEST, resp.status());
}

#[tokio::test]
async fn try_deleting_table_after_db_is_deleted() {
    let db_name = "db";
    let tbl_name = "tbl";
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let delete_db_url = format!(
        "{base}/api/v3/configure/database?db={db_name}",
        base = server.client_addr()
    );
    let delete_table_url = format!(
        "{base}/api/v3/configure/table?db={db_name}&table={tbl_name}",
        base = server.client_addr()
    );
    server
        .write_lp_to_db(
            db_name,
            format!("{tbl_name},t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000"),
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    // db call should succeed
    let resp = client
        .delete(&delete_db_url)
        .send()
        .await
        .expect("delete database call succeed");

    assert_eq!(StatusCode::OK, resp.status());

    // but table delete call should fail with NOT_FOUND
    let resp = client
        .delete(&delete_table_url)
        .send()
        .await
        .expect("delete table call succeed");
    assert_eq!(StatusCode::NOT_FOUND, resp.status());
}
