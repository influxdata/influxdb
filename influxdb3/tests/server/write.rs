use hyper::StatusCode;

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
