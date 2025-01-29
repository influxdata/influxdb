use hyper::StatusCode;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;

use crate::server::TestServer;

#[tokio::test]
async fn api_v1_write_request_parsing() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let write_url = format!("{base}/write", base = server.client_addr());
    let write_body = "cpu,host=a usage=0.5";

    #[derive(Debug)]
    struct TestCase {
        db: Option<&'static str>,
        precision: Option<&'static str>,
        rp: Option<&'static str>,
        expected: StatusCode,
    }

    let test_cases = [
        TestCase {
            db: None,
            precision: None,
            rp: None,
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("s"),
            rp: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("ms"),
            rp: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("u"),
            rp: Some("autogen"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("us"),
            rp: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("n"),
            rp: Some("autogen"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("ns"),
            rp: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("invalid"),
            rp: None,
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: Some("bar"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: Some("default"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: Some("autogen"),
            expected: StatusCode::NO_CONTENT,
        },
    ];

    for t in test_cases {
        println!("Test Case: {t:?}");
        let mut params = vec![];
        if let Some(db) = t.db {
            params.push(("db", db));
        }
        if let Some(rp) = t.rp {
            params.push(("rp", rp));
        }
        if let Some(precision) = t.precision {
            params.push(("precision", precision));
        }
        let resp = client
            .post(&write_url)
            .query(&params)
            .body(write_body)
            .send()
            .await
            .expect("send /write request");
        let status = resp.status();
        let body = resp.text().await.expect("response body as text");
        println!("Response [{status}]:\n{body}");
        assert_eq!(t.expected, status);
    }
}

#[tokio::test]
async fn api_v1_write_round_trip() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let write_url = format!("{base}/write", base = server.client_addr());

    client
        .post(write_url)
        .query(&[("db", "foo"), ("precision", "s")])
        .body(
            "cpu,host=a usage=0.5 2998574931
            cpu,host=a usage=0.6 2998574932
            cpu,host=a usage=0.7 2998574933",
        )
        .send()
        .await
        .expect("send /write request");

    let resp = server
        .api_v3_query_influxql(&[
            ("q", "SELECT time, host, usage FROM foo.autogen.cpu"),
            ("format", "pretty"),
        ])
        .await
        .text()
        .await
        .unwrap();

    assert_eq!(
        resp,
        "+------------------+---------------------+------+-------+\n\
        | iox::measurement | time                | host | usage |\n\
        +------------------+---------------------+------+-------+\n\
        | cpu              | 2065-01-07T17:28:51 | a    | 0.5   |\n\
        | cpu              | 2065-01-07T17:28:52 | a    | 0.6   |\n\
        | cpu              | 2065-01-07T17:28:53 | a    | 0.7   |\n\
        +------------------+---------------------+------+-------+"
    );
}

#[tokio::test]
async fn api_v2_write_request_parsing() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let write_url = format!("{base}/api/v2/write", base = server.client_addr());
    let write_body = "cpu,host=a usage=0.5";

    #[derive(Debug)]
    struct TestCase {
        org: Option<&'static str>,
        bucket: Option<&'static str>,
        precision: Option<&'static str>,
        expected: StatusCode,
    }

    let test_cases = [
        TestCase {
            org: None,
            bucket: None,
            precision: None,
            expected: StatusCode::BAD_REQUEST,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: Some("bar"),
            bucket: Some("foo"),
            precision: None,
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("s"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("ms"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("us"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("u"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("n"),
            expected: StatusCode::NO_CONTENT,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("ns"),
            expected: StatusCode::NO_CONTENT,
        },
    ];

    for t in test_cases {
        println!("Test Case: {t:?}");
        let mut params = vec![];
        if let Some(bucket) = t.bucket {
            params.push(("bucket", bucket));
        }
        if let Some(org) = t.org {
            params.push(("org", org));
        }
        if let Some(precision) = t.precision {
            params.push(("precision", precision));
        }
        let resp = client
            .post(&write_url)
            .query(&params)
            .body(write_body)
            .send()
            .await
            .expect("send /write request");
        let status = resp.status();
        let body = resp.text().await.expect("response body as text");
        println!("Response [{status}]:\n{body}");
        assert_eq!(t.expected, status);
    }
}

#[tokio::test]
async fn api_v2_write_round_trip() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let write_url = format!("{base}/api/v2/write", base = server.client_addr());

    client
        .post(write_url)
        .query(&[("bucket", "foo"), ("precision", "s")])
        .body(
            "cpu,host=a usage=0.5 2998574931
            cpu,host=a usage=0.6 2998574932
            cpu,host=a usage=0.7 2998574933",
        )
        .send()
        .await
        .expect("send /write request");

    let resp = server
        .api_v3_query_influxql(&[
            ("q", "SELECT time, host, usage FROM foo.autogen.cpu"),
            ("format", "pretty"),
        ])
        .await
        .text()
        .await
        .unwrap();

    assert_eq!(
        resp,
        "+------------------+---------------------+------+-------+\n\
        | iox::measurement | time                | host | usage |\n\
        +------------------+---------------------+------+-------+\n\
        | cpu              | 2065-01-07T17:28:51 | a    | 0.5   |\n\
        | cpu              | 2065-01-07T17:28:52 | a    | 0.6   |\n\
        | cpu              | 2065-01-07T17:28:53 | a    | 0.7   |\n\
        +------------------+---------------------+------+-------+"
    );
}

/// Reproducer for [#25006][issue]
///
/// [issue]: https://github.com/influxdata/influxdb/issues/25006
#[tokio::test]
async fn writes_with_different_schema_should_fail() {
    let server = TestServer::spawn().await;
    // send a valid write request with the field t0_f0 as an integer:
    server
        .write_lp_to_db(
            "foo",
            "\
            t0,t0_tag0=initTag t0_f0=0i\n\
            t0,t0_tag0=initTag t0_f0=1i 2998574931",
            Precision::Second,
        )
        .await
        .expect("writes LP with integer field");

    // send another write request, to the same db, but with field t0_f0 as an unsigned integer:
    let error = server
        .write_lp_to_db(
            "foo",
            "\
            t0,t0_tag0=initTag t0_f0=0u 2998574930\n\
            t0,t0_tag0=initTag t0_f0=1u 2998574931",
            Precision::Second,
        )
        .await
        .expect_err("should fail when writing LP with same field as unsigned integer");

    println!("error: {error:#?}");

    // the request should have failed with an API error indicating incorrect schema for the field:
    assert!(
        matches!(
            error,
            influxdb3_client::Error::ApiError {
                code: StatusCode::BAD_REQUEST,
                message: _
            }
        ),
        "the request should hae failed with an API Error"
    );
}
#[tokio::test]
/// Check that the no_sync param can be used on any endpoint. However, this only means that serde
/// will parse it just fine. It is only able to be used in the v3 endpoint and will
/// default to requiring the WAL to synce before returning.
async fn api_no_sync_param() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();
    let v1_write_url = format!("{base}/write", base = server.client_addr());
    let v2_write_url = format!("{base}/api/v2/write", base = server.client_addr());
    let v3_write_url = format!("{base}/api/v3/write_lp", base = server.client_addr());
    let write_body = "cpu,host=a usage=0.5";

    let params = vec![("db", "foo"), ("no_sync", "true")];
    let resp = client
        .post(&v1_write_url)
        .query(&params)
        .body(write_body)
        .send()
        .await
        .expect("send /write request");
    let status = resp.status();
    let body = resp.text().await.expect("response body as text");
    println!("Response [{status}]:\n{body}");
    assert_eq!(status, StatusCode::NO_CONTENT);

    let params = vec![("bucket", "foo"), ("no_sync", "true")];
    let resp = client
        .post(&v2_write_url)
        .query(&params)
        .body(write_body)
        .send()
        .await
        .expect("send api/v2/write request");
    let status = resp.status();
    let body = resp.text().await.expect("response body as text");
    println!("Response [{status}]:\n{body}");
    assert_eq!(status, StatusCode::NO_CONTENT);

    let params = vec![("db", "foo"), ("no_sync", "true")];
    let resp = client
        .post(&v3_write_url)
        .query(&params)
        .body(write_body)
        .send()
        .await
        .expect("send api/v3/write request");
    let status = resp.status();
    let body = resp.text().await.expect("response body as text");
    println!("Response [{status}]:\n{body}");
    assert_eq!(status, StatusCode::NO_CONTENT);
}
