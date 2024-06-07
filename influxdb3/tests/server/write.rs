use hyper::StatusCode;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use test_helpers::assert_contains;

use crate::TestServer;

#[tokio::test]
async fn api_v3_write() {
    let server = TestServer::spawn().await;
    let client = reqwest::Client::new();

    let url = format!("{base}/api/v3/write", base = server.client_addr());
    let params = &[("db", "foo")];

    // Make a successful write:
    assert!(client
        .post(&url)
        .query(params)
        .body(
            "\
            cpu region/us-east/host/a1 usage=42.0,temp=10 1234\n\
            cpu region/us-east/host/b1 usage=10.5,temp=18 1234\n\
            cpu region/us-west/host/a2 usage=88.0,temp=15 1234\n\
            cpu region/us-west/host/b2 usage=92.2,temp=14 1234\n\
        "
        )
        .send()
        .await
        .expect("send write request")
        .status()
        .is_success());

    // Query from the table written to:
    let resp = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM cpu"),
            ("format", "pretty"),
        ])
        .await
        .text()
        .await
        .expect("get body");

    assert_eq!(
        "\
         +---------+------+------+-------+---------------------+\n\
         | region  | host | temp | usage | time                |\n\
         +---------+------+------+-------+---------------------+\n\
         | us-east | a1   | 10.0 | 42.0  | 1970-01-01T00:20:34 |\n\
         | us-west | a2   | 15.0 | 88.0  | 1970-01-01T00:20:34 |\n\
         | us-east | b1   | 18.0 | 10.5  | 1970-01-01T00:20:34 |\n\
         | us-west | b2   | 14.0 | 92.2  | 1970-01-01T00:20:34 |\n\
         +---------+------+------+-------+---------------------+",
        resp
    );

    // Test several failure modes:
    struct TestCase {
        body: &'static str,
        response_contains: &'static str,
    }

    let test_cases = [
        // No series key:
        TestCase {
            body: "cpu usage=10.0,temp=5 1235",
            response_contains:
                "write to table cpu was missing a series key, the series key contains [region, host]",
        },
        // Series key out-of-order:
        TestCase {
            body: "cpu host/c1/region/ca-cent usage=22.0,temp=6 1236",
            response_contains: "write to table cpu had the incorrect series key, \
                expected: [region, host], received: [host, region]",
        },
        // Series key with invalid member at end:
        TestCase {
            body: "cpu region/ca-cent/host/c1/container/foo usage=22.0,temp=6 1236",
            response_contains: "write to table cpu had the incorrect series key, \
                expected: [region, host], received: [region, host, container]",
        },
        // Series key with invalid member in middle:
        TestCase {
            body: "cpu region/ca-cent/sub-region/toronto/host/c1 usage=22.0,temp=6 1236",
            response_contains: "write to table cpu had the incorrect series key, \
                expected: [region, host], received: [region, sub-region, host]",
        },
        // Series key with invalid member at start:
        TestCase {
            body: "cpu planet/earth/region/ca-cent/host/c1 usage=22.0,temp=6 1236",
            response_contains: "write to table cpu had the incorrect series key, \
                expected: [region, host], received: [planet, region, host]",
        },
    ];

    for t in test_cases {
        let resp = client
            .post(&url)
            .query(params)
            .body(t.body)
            .send()
            .await
            .expect("get response from server")
            .text()
            .await
            .expect("parse response");

        assert_contains!(resp, t.response_contains);
    }
}

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
            t0,t0_tag0=initTag t0_f0=0i 1715694000\n\
            t0,t0_tag0=initTag t0_f0=1i 1715694001\n\
            t0,t0_tag1=initTag t0_f0=0i 1715694000",
            Precision::Second,
        )
        .await
        .expect("writes LP with integer field");

    // send another write request, to the same db, but with field t0_f0 as an unsigned integer:
    let error = server
        .write_lp_to_db(
            "foo",
            "\
            t0,t0_tag0=initTag t0_f0=0u 1715694000\n\
            t0,t0_tag0=initTag t0_f0=1u 1715694001\n\
            t0,t0_tag1=initTag t0_f0=0u 1715694000",
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
