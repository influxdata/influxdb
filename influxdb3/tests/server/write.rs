use hyper::StatusCode;
use pretty_assertions::assert_eq;

use crate::TestServer;

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
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("s"),
            rp: None,
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("ms"),
            rp: None,
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("us"),
            rp: None,
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: Some("ns"),
            rp: None,
            expected: StatusCode::OK,
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
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: Some("default"),
            expected: StatusCode::OK,
        },
        TestCase {
            db: Some("foo"),
            precision: None,
            rp: Some("autogen"),
            expected: StatusCode::OK,
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
        .query(&[("db", "foo")])
        .body(
            "cpu,host=a usage=0.5 1
            cpu,host=a usage=0.6 2
            cpu,host=a usage=0.7 3",
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
        "+------------------+-------------------------------+------+-------+\n\
        | iox::measurement | time                          | host | usage |\n\
        +------------------+-------------------------------+------+-------+\n\
        | cpu              | 1970-01-01T00:00:00.000000001 | a    | 0.5   |\n\
        | cpu              | 1970-01-01T00:00:00.000000002 | a    | 0.6   |\n\
        | cpu              | 1970-01-01T00:00:00.000000003 | a    | 0.7   |\n\
        +------------------+-------------------------------+------+-------+"
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
            expected: StatusCode::OK,
        },
        TestCase {
            org: Some("bar"),
            bucket: Some("foo"),
            precision: None,
            expected: StatusCode::OK,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("s"),
            expected: StatusCode::OK,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("ms"),
            expected: StatusCode::OK,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("us"),
            expected: StatusCode::OK,
        },
        TestCase {
            org: None,
            bucket: Some("foo"),
            precision: Some("ns"),
            expected: StatusCode::OK,
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
        .query(&[("bucket", "foo")])
        .body(
            "cpu,host=a usage=0.5 1
            cpu,host=a usage=0.6 2
            cpu,host=a usage=0.7 3",
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
        "+------------------+-------------------------------+------+-------+\n\
        | iox::measurement | time                          | host | usage |\n\
        +------------------+-------------------------------+------+-------+\n\
        | cpu              | 1970-01-01T00:00:00.000000001 | a    | 0.5   |\n\
        | cpu              | 1970-01-01T00:00:00.000000002 | a    | 0.6   |\n\
        | cpu              | 1970-01-01T00:00:00.000000003 | a    | 0.7   |\n\
        +------------------+-------------------------------+------+-------+"
    );
}
