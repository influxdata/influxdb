use core::str;

use crate::server::TestServer;
use futures::StreamExt;
use hyper::{
    HeaderMap, StatusCode,
    header::{ACCEPT, HeaderValue},
};
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use serde::Serialize;
use serde_json::{Value, json};
use test_helpers::assert_contains;

#[tokio::test]
async fn api_v3_query_sql() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 2998574936\n\
            cpu,host=s1,region=us-east usage=0.89 2998574937\n\
            cpu,host=s1,region=us-east usage=0.85 2998574938",
            Precision::Second,
        )
        .await
        .unwrap();

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT host, region, time, usage FROM cpu",
            expected: "+------+---------+---------------------+-------+\n\
            | host | region  | time                | usage |\n\
            +------+---------+---------------------+-------+\n\
            | s1   | us-east | 2065-01-07T17:28:56 | 0.9   |\n\
            | s1   | us-east | 2065-01-07T17:28:57 | 0.89  |\n\
            | s1   | us-east | 2065-01-07T17:28:58 | 0.85  |\n\
            +------+---------+---------------------+-------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SELECT count(usage) FROM cpu",
            expected: "+------------------+\n\
            | count(cpu.usage) |\n\
            +------------------+\n\
            | 3                |\n\
            +------------------+",
        },
    ];

    for t in test_cases {
        let mut params = vec![("q", t.query), ("format", "pretty")];
        if let Some(db) = t.database {
            params.push(("db", db))
        }
        let resp = server.api_v3_query_sql(&params).await.text().await.unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v3_query_sql_not_found() {
    let server = TestServer::spawn().await;
    let params = vec![
        ("q", "SELECT * FROM foo"),
        ("format", "pretty"),
        ("db", "foo"),
    ];
    let resp = server.api_v3_query_sql(&params).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn api_v3_query_sql_params() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 2998574936
            cpu,host=b,region=us-east usage=0.50 2998574936
            cpu,host=a,region=us-east usage=0.80 2998574937
            cpu,host=b,region=us-east usage=0.60 2998574937
            cpu,host=a,region=us-east usage=0.70 2998574938
            cpu,host=b,region=us-east usage=0.70 2998574938
            cpu,host=a,region=us-east usage=0.50 2998574939
            cpu,host=b,region=us-east usage=0.80 2998574939",
            Precision::Second,
        )
        .await
        .unwrap();

    let client = server.http_client();
    let url = format!("{base}/api/v3/query_sql", base = server.client_addr());

    // Use a POST request
    {
        let resp = client
            .post(&url)
            .json(&json!({
                "db": "foo",
                "q": "SELECT host, region, time, usage FROM cpu WHERE host = $host AND usage > $usage ORDER BY time",
                "params": {
                    "host": "b",
                    "usage": 0.60,
                },
                "format": "pretty",
            }))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            "+------+---------+---------------------+-------+\n\
            | host | region  | time                | usage |\n\
            +------+---------+---------------------+-------+\n\
            | b    | us-east | 2065-01-07T17:28:58 | 0.7   |\n\
            | b    | us-east | 2065-01-07T17:28:59 | 0.8   |\n\
            +------+---------+---------------------+-------+",
            resp
        );
    }

    // Use a GET request
    {
        let params = serde_json::to_string(&json!({
            "host": "b",
            "usage": 0.60,
        }))
        .unwrap();
        let resp = client
            .get(&url)
            .query(&[
                ("db", "foo"),
                (
                    "q",
                    "SELECT host, region, time, usage FROM cpu WHERE host = $host AND usage > $usage ORDER BY time",
                ),
                ("format", "pretty"),
                ("params", params.as_str()),
            ])
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            "+------+---------+---------------------+-------+\n\
            | host | region  | time                | usage |\n\
            +------+---------+---------------------+-------+\n\
            | b    | us-east | 2065-01-07T17:28:58 | 0.7   |\n\
            | b    | us-east | 2065-01-07T17:28:59 | 0.8   |\n\
            +------+---------+---------------------+-------+",
            resp
        );
    }

    // Check for errors
    {
        let resp = client
            .post(&url)
            .json(&json!({
                "db": "foo",
                "q": "SELECT * FROM cpu WHERE host = $host",
                "params": {
                    "not_host": "a"
                },
                "format": "pretty",
            }))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();

        // TODO - it would be nice if this was a 4xx error, because this is really
        //   a user error; however, the underlying error that occurs when Logical
        //   planning is DatafusionError::Internal, and is not so convenient to deal
        //   with. This may get addressed in:
        //
        //   https://github.com/apache/arrow-datafusion/issues/9738
        assert!(status.is_server_error());
        assert_contains!(body, "No value found for placeholder with name $host");
    }
}

#[tokio::test]
async fn api_v3_query_influxql() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 2998574930\n\
            cpu,host=s1,region=us-east usage=0.89 2998574931\n\
            cpu,host=s1,region=us-east usage=0.85 2998574932\n
            mem,host=s1,region=us-east usage=0.5 2998574933\n\
            mem,host=s1,region=us-east usage=0.6 2998574934\n\
            mem,host=s1,region=us-east usage=0.7 2998574935",
            Precision::Second,
        )
        .await
        .unwrap();

    // write to another database for `SHOW DATABASES` and `SHOW RETENTION POLICIES`
    server
        .write_lp_to_db(
            "bar",
            "cpu,host=s1,region=us-east usage=0.9 2998574930\n\
            cpu,host=s1,region=us-east usage=0.89 2998574931\n\
            cpu,host=s1,region=us-east usage=0.85 2998574932\n\
            mem,host=s1,region=us-east usage=0.5 2998574933\n\
            mem,host=s1,region=us-east usage=0.6 2998574934\n\
            mem,host=s1,region=us-east usage=0.7 2998574935",
            Precision::Second,
        )
        .await
        .unwrap();

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT time, host, region, usage FROM cpu",
            expected: "+------------------+---------------------+------+---------+-------+\n\
                | iox::measurement | time                | host | region  | usage |\n\
                +------------------+---------------------+------+---------+-------+\n\
                | cpu              | 2065-01-07T17:28:50 | s1   | us-east | 0.9   |\n\
                | cpu              | 2065-01-07T17:28:51 | s1   | us-east | 0.89  |\n\
                | cpu              | 2065-01-07T17:28:52 | s1   | us-east | 0.85  |\n\
                +------------------+---------------------+------+---------+-------+",
        },
        TestCase {
            database: None,
            query: "SELECT time, host, region, usage FROM foo.autogen.cpu",
            expected: "+------------------+---------------------+------+---------+-------+\n\
                | iox::measurement | time                | host | region  | usage |\n\
                +------------------+---------------------+------+---------+-------+\n\
                | cpu              | 2065-01-07T17:28:50 | s1   | us-east | 0.9   |\n\
                | cpu              | 2065-01-07T17:28:51 | s1   | us-east | 0.89  |\n\
                | cpu              | 2065-01-07T17:28:52 | s1   | us-east | 0.85  |\n\
                +------------------+---------------------+------+---------+-------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SELECT host, region, usage FROM cpu, mem",
            expected: "+------------------+---------------------+------+---------+-------+\n\
                | iox::measurement | time                | host | region  | usage |\n\
                +------------------+---------------------+------+---------+-------+\n\
                | cpu              | 2065-01-07T17:28:50 | s1   | us-east | 0.9   |\n\
                | cpu              | 2065-01-07T17:28:51 | s1   | us-east | 0.89  |\n\
                | cpu              | 2065-01-07T17:28:52 | s1   | us-east | 0.85  |\n\
                | mem              | 2065-01-07T17:28:53 | s1   | us-east | 0.5   |\n\
                | mem              | 2065-01-07T17:28:54 | s1   | us-east | 0.6   |\n\
                | mem              | 2065-01-07T17:28:55 | s1   | us-east | 0.7   |\n\
                +------------------+---------------------+------+---------+-------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW MEASUREMENTS",
            expected: "+------------------+------+\n\
                    | iox::measurement | name |\n\
                    +------------------+------+\n\
                    | measurements     | cpu  |\n\
                    | measurements     | mem  |\n\
                    +------------------+------+",
        },
        TestCase {
            database: None,
            query: "SHOW MEASUREMENTS ON foo",
            expected: "+------------------+------+\n\
                    | iox::measurement | name |\n\
                    +------------------+------+\n\
                    | measurements     | cpu  |\n\
                    | measurements     | mem  |\n\
                    +------------------+------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW FIELD KEYS",
            expected: "+------------------+----------+-----------+\n\
                    | iox::measurement | fieldKey | fieldType |\n\
                    +------------------+----------+-----------+\n\
                    | cpu              | usage    | float     |\n\
                    | mem              | usage    | float     |\n\
                    +------------------+----------+-----------+",
        },
        TestCase {
            database: None,
            query: "SHOW FIELD KEYS ON foo",
            expected: "+------------------+----------+-----------+\n\
                    | iox::measurement | fieldKey | fieldType |\n\
                    +------------------+----------+-----------+\n\
                    | cpu              | usage    | float     |\n\
                    | mem              | usage    | float     |\n\
                    +------------------+----------+-----------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW TAG KEYS",
            expected: "+------------------+--------+\n\
                    | iox::measurement | tagKey |\n\
                    +------------------+--------+\n\
                    | cpu              | host   |\n\
                    | cpu              | region |\n\
                    | mem              | host   |\n\
                    | mem              | region |\n\
                    +------------------+--------+",
        },
        TestCase {
            database: None,
            query: "SHOW TAG KEYS ON foo",
            expected: "+------------------+--------+\n\
                    | iox::measurement | tagKey |\n\
                    +------------------+--------+\n\
                    | cpu              | host   |\n\
                    | cpu              | region |\n\
                    | mem              | host   |\n\
                    | mem              | region |\n\
                    +------------------+--------+",
        },
        TestCase {
            database: Some("foo"),
            // TODO: WHERE time < 2065-01-08 does not work for some reason
            query: "SHOW TAG VALUES WITH KEY = \"host\" WHERE time > 2065-01-07",
            expected: "+------------------+------+-------+\n\
                    | iox::measurement | key  | value |\n\
                    +------------------+------+-------+\n\
                    | cpu              | host | s1    |\n\
                    | mem              | host | s1    |\n\
                    +------------------+------+-------+",
        },
        TestCase {
            database: None,
            // TODO: WHERE time < 2065-01-08 does not work for some reason
            query: "SHOW TAG VALUES ON foo WITH KEY = \"host\" WHERE time > 2065-01-07",
            expected: "+------------------+------+-------+\n\
                    | iox::measurement | key  | value |\n\
                    +------------------+------+-------+\n\
                    | cpu              | host | s1    |\n\
                    | mem              | host | s1    |\n\
                    +------------------+------+-------+",
        },
        TestCase {
            database: None,
            query: "SHOW DATABASES",
            expected: "\
                    +---------------+---------+\n\
                    | iox::database | deleted |\n\
                    +---------------+---------+\n\
                    | _internal     | false   |\n\
                    | bar           | false   |\n\
                    | foo           | false   |\n\
                    +---------------+---------+",
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES",
            expected: "+---------------+---------+----------+\n\
                    | iox::database | name    | duration |\n\
                    +---------------+---------+----------+\n\
                    | _internal     | autogen |          |\n\
                    | bar           | autogen |          |\n\
                    | foo           | autogen |          |\n\
                    +---------------+---------+----------+",
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES ON foo",
            expected: "+---------------+---------+----------+\n\
                    | iox::database | name    | duration |\n\
                    +---------------+---------+----------+\n\
                    | foo           | autogen |          |\n\
                    +---------------+---------+----------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW RETENTION POLICIES",
            expected: "+---------------+---------+----------+\n\
                    | iox::database | name    | duration |\n\
                    +---------------+---------+----------+\n\
                    | foo           | autogen |          |\n\
                    +---------------+---------+----------+",
        },
    ];

    for t in test_cases {
        let mut params = vec![("q", t.query), ("format", "pretty")];
        if let Some(db) = t.database {
            params.push(("db", db))
        }
        let resp = server
            .api_v3_query_influxql(&params)
            .await
            .text()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[cfg(test)]
struct TestCase<'a> {
    database: Option<&'a str>,
    query: &'a str,
    expected: &'a str,
}

#[tokio::test]
async fn api_v3_query_influxql_params() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 2998574931
            cpu,host=b,region=us-east usage=0.50 2998574931
            cpu,host=a,region=us-east usage=0.80 2998574932
            cpu,host=b,region=us-east usage=0.60 2998574932
            cpu,host=a,region=us-east usage=0.70 2998574933
            cpu,host=b,region=us-east usage=0.70 2998574933
            cpu,host=a,region=us-east usage=0.50 2998574934
            cpu,host=b,region=us-east usage=0.80 2998574934",
            Precision::Second,
        )
        .await
        .unwrap();

    let client = server.http_client();
    let url = format!("{base}/api/v3/query_influxql", base = server.client_addr());

    // Use a POST request
    {
        let resp = client
            .post(&url)
            .json(&json!({
                "db": "foo",
                "q": "SELECT time, host, region, usage FROM cpu WHERE host = $host AND usage > $usage",
                "params": {
                    "host": "b",
                    "usage": 0.60,
                },
                "format": "pretty",
            }))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            "+------------------+---------------------+------+---------+-------+\n\
            | iox::measurement | time                | host | region  | usage |\n\
            +------------------+---------------------+------+---------+-------+\n\
            | cpu              | 2065-01-07T17:28:53 | b    | us-east | 0.7   |\n\
            | cpu              | 2065-01-07T17:28:54 | b    | us-east | 0.8   |\n\
            +------------------+---------------------+------+---------+-------+",
            resp
        );
    }

    // Use a GET request
    {
        let params = serde_json::to_string(&json!({
            "host": "b",
            "usage": 0.60,
        }))
        .unwrap();
        let resp = client
            .get(&url)
            .query(&[
                ("db", "foo"),
                (
                    "q",
                    "SELECT time, host, region, usage FROM cpu WHERE host = $host AND usage > $usage",
                ),
                ("format", "pretty"),
                ("params", params.as_str()),
            ])
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert_eq!(
            "+------------------+---------------------+------+---------+-------+\n\
            | iox::measurement | time                | host | region  | usage |\n\
            +------------------+---------------------+------+---------+-------+\n\
            | cpu              | 2065-01-07T17:28:53 | b    | us-east | 0.7   |\n\
            | cpu              | 2065-01-07T17:28:54 | b    | us-east | 0.8   |\n\
            +------------------+---------------------+------+---------+-------+",
            resp
        );
    }

    // Check for errors
    {
        let resp = client
            .post(&url)
            .json(&json!({
                "db": "foo",
                "q": "SELECT * FROM cpu WHERE host = $host",
                "params": {
                    "not_host": "a"
                },
                "format": "pretty",
            }))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();

        // TODO - it would be nice if this was a 4xx error, because this is really
        //   a user error; however, the underlying error that occurs when Logical
        //   planning is DatafusionError::Internal, and is not so convenient to deal
        //   with. This may get addressed in:
        //
        //   https://github.com/apache/arrow-datafusion/issues/9738
        assert!(status.is_server_error());
        assert_contains!(
            body,
            "Bind parameter '$host' was referenced in the InfluxQL \
            statement but its value is undefined"
        );
    }
}

#[tokio::test]
async fn api_v3_query_json_format() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 2998574931
            cpu,host=b,region=us-east usage=0.50 2998574931
            cpu,host=a,region=us-east usage=0.80 2998574932
            cpu,host=b,region=us-east usage=0.60 2998574932
            cpu,host=a,region=us-east usage=0.70 2998574933
            cpu,host=b,region=us-east usage=0.70 2998574933
            cpu,host=a,region=us-east usage=0.50 2998574934
            cpu,host=b,region=us-east usage=0.80 2998574934",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        database: Option<&'a str>,
        query: &'a str,
        expected: Value,
    }

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT time, host, region, usage FROM cpu",
            expected: json!([
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:51",
                    "usage": 0.9
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:51",
                    "usage": 0.5
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:52",
                    "usage": 0.8
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:52",
                    "usage": 0.6
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:53",
                    "usage": 0.7
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:53",
                    "usage": 0.7
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:54",
                    "usage": 0.5
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "2065-01-07T17:28:54",
                    "usage": 0.8
                }
            ]),
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW MEASUREMENTS",
            expected: json!([
                {
                  "iox::measurement": "measurements",
                  "name": "cpu",
                }
            ]),
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW FIELD KEYS",
            expected: json!([
                {
                  "iox::measurement": "cpu",
                  "fieldKey": "usage",
                  "fieldType": "float"
                }
            ]),
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW TAG KEYS",
            expected: json!([
                {
                  "iox::measurement": "cpu",
                  "tagKey": "host",
                },
                {
                  "iox::measurement": "cpu",
                  "tagKey": "region",
                }
            ]),
        },
        TestCase {
            database: None,
            query: "SHOW DATABASES",
            expected: json!([
                {
                  "deleted": false,
                  "iox::database": "_internal",
                },
                {
                  "deleted": false,
                  "iox::database": "foo",
                },
            ]),
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES",
            expected: json!([
                {
                  "iox::database": "_internal",
                  "name": "autogen",
                },
                {
                  "iox::database": "foo",
                  "name": "autogen",
                },
            ]),
        },
    ];
    for t in test_cases {
        let mut params = vec![("q", t.query), ("format", "json")];
        if let Some(db) = t.database {
            params.push(("db", db))
        }
        let resp = server
            .api_v3_query_influxql(&params)
            .await
            .json::<Value>()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_sql_not_found() {
    let server = TestServer::spawn().await;
    let params = vec![
        ("q", "SELECT * FROM foo"),
        ("format", "pretty"),
        ("db", "foo"),
    ];
    let resp = server.api_v1_query(&params, None).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn api_v3_query_jsonl_format() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 2998574931
            cpu,host=b,region=us-east usage=0.50 2998574931
            cpu,host=a,region=us-east usage=0.80 2998574932
            cpu,host=b,region=us-east usage=0.60 2998574932
            cpu,host=a,region=us-east usage=0.70 2998574933
            cpu,host=b,region=us-east usage=0.70 2998574933
            cpu,host=a,region=us-east usage=0.50 2998574934
            cpu,host=b,region=us-east usage=0.80 2998574934",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        database: Option<&'a str>,
        query: &'a str,
        expected: String,
    }

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT time, host, region, usage FROM cpu",
            expected: "{\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:51\",\"host\":\"a\",\"region\":\"us-east\",\"usage\":0.9}\n\
               {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:51\",\"host\":\"b\",\"region\":\"us-east\",\"usage\":0.5}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:52\",\"host\":\"a\",\"region\":\"us-east\",\"usage\":0.8}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:52\",\"host\":\"b\",\"region\":\"us-east\",\"usage\":0.6}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:53\",\"host\":\"a\",\"region\":\"us-east\",\"usage\":0.7}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:53\",\"host\":\"b\",\"region\":\"us-east\",\"usage\":0.7}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:54\",\"host\":\"a\",\"region\":\"us-east\",\"usage\":0.5}\n\
                {\"iox::measurement\":\"cpu\",\"time\":\"2065-01-07T17:28:54\",\"host\":\"b\",\"region\":\"us-east\",\"usage\":0.8}\n"
            .into(),
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW MEASUREMENTS",
            expected: "{\"iox::measurement\":\"measurements\",\"name\":\"cpu\"}\n".into(),
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW FIELD KEYS",
            expected: "{\"iox::measurement\":\"cpu\",\"fieldKey\":\"usage\",\"fieldType\":\"float\"}\n".into()
        },
        TestCase {
            database: Some("foo"),
            query: "SHOW TAG KEYS",
            expected:
                "{\"iox::measurement\":\"cpu\",\"tagKey\":\"host\"}\n\
                {\"iox::measurement\":\"cpu\",\"tagKey\":\"region\"}\n".into()
        },
        TestCase {
            database: None,
            query: "SHOW DATABASES",
            expected:
            "{\"iox::database\":\"_internal\",\"deleted\":false}\n\
            {\"iox::database\":\"foo\",\"deleted\":false}\n".into(),
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES",
            expected:
            "{\"iox::database\":\"_internal\",\"name\":\"autogen\"}\n\
            {\"iox::database\":\"foo\",\"name\":\"autogen\"}\n".into(),
        },
    ];
    for t in test_cases {
        let mut params = vec![("q", t.query), ("format", "jsonl")];
        if let Some(db) = t.database {
            params.push(("db", db))
        }
        let resp = server
            .api_v3_query_influxql(&params)
            .await
            .text()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_json_format() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a usage=0.9 2998574931\n\
            cpu,host=a usage=0.89 2998574932\n\
            cpu,host=a usage=0.85 2998574933\n\
            mem,host=a usage=0.5 2998574934\n\
            mem,host=a usage=0.6 2998574935\n\
            mem,host=a usage=0.7 2998574936",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        database: Option<&'a str>,
        epoch: Option<&'a str>,
        query: &'a str,
        expected: Value,
    }

    let test_cases = [
        // Empty query result:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT * FROM cpu WHERE host='c'",
            expected: json!({"results":[{"statement_id":0}]}),
        },
        // Basic Query:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, host, usage FROM cpu",
            expected: json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "cpu",
                      "values": [
                        ["2998574931000000000", "a", 0.9],
                        ["2998574932000000000", "a", 0.89],
                        ["2998574933000000000", "a", 0.85]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            }),
        },
        // Basic Query with multiple measurements:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, host, usage FROM cpu, mem",
            expected: json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "mem",
                      "values": [
                        ["2998574934000000000", "a", 0.5],
                        ["2998574935000000000", "a", 0.6],
                        ["2998574936000000000", "a", 0.7]
                      ]
                    },
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "cpu",
                      "values": [
                        ["2998574931000000000", "a", 0.9],
                        ["2998574932000000000", "a", 0.89],
                        ["2998574933000000000", "a", 0.85]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            }),
        },
        // Basic Query with db in query string:
        TestCase {
            database: None,
            epoch: None,
            query: "SELECT time, host, usage FROM foo.autogen.cpu",
            expected: json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "cpu",
                      "values": [
                        ["2998574931000000000", "a", 0.9],
                        ["2998574932000000000", "a", 0.89],
                        ["2998574933000000000", "a", 0.85]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            }),
        },
        // Basic Query epoch parameter set:
        TestCase {
            database: Some("foo"),
            epoch: Some("s"),
            query: "SELECT time, host, usage FROM cpu",
            expected: json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "cpu",
                      "values": [
                        [2998574931u32, "a", 0.9],
                        [2998574932u32, "a", 0.89],
                        [2998574933u32, "a", 0.85]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            }),
        },
    ];

    for t in test_cases {
        let mut params = vec![("q", t.query)];
        if let Some(db) = t.database {
            params.push(("db", db));
        }
        if let Some(epoch) = t.epoch {
            params.push(("epoch", epoch));
        }
        let resp = server
            .api_v1_query(&params, None)
            .await
            .json::<Value>()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp:#}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_csv_format() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a usage=0.9 2998574931\n\
          cpu,host=a usage=0.89 2998574932\n\
          cpu,host=a usage=0.85 2998574933\n\
          mem,host=a usage=0.5 2998574934\n\
          mem,host=a usage=0.6 2998574935\n\
          mem,host=a usage=0.7 2998574936",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        database: Option<&'a str>,
        epoch: Option<&'a str>,
        query: &'a str,
        expected: &'a str,
    }

    let test_cases = [
        // Basic Query:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, host, usage FROM cpu",
            expected: "name,tags,time,host,usage\n\
            cpu,,2998574931000000000,a,0.9\n\
            cpu,,2998574932000000000,a,0.89\n\
            cpu,,2998574933000000000,a,0.85\n\r\n",
        },
        // Basic Query with multiple measurements:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, host, usage FROM cpu, mem",
            expected: "name,tags,time,host,usage\n\
            mem,,2998574934000000000,a,0.5\n\
            mem,,2998574935000000000,a,0.6\n\
            mem,,2998574936000000000,a,0.7\n\
            cpu,,2998574931000000000,a,0.9\n\
            cpu,,2998574932000000000,a,0.89\n\
            cpu,,2998574933000000000,a,0.85\n\r\n",
        },
        // Basic Query with db in query string:
        TestCase {
            database: None,
            epoch: None,
            query: "SELECT time, host, usage FROM foo.autogen.cpu",
            expected: "name,tags,time,host,usage\n\
          cpu,,2998574931000000000,a,0.9\n\
          cpu,,2998574932000000000,a,0.89\n\
          cpu,,2998574933000000000,a,0.85\n\r\n",
        },
        // Basic Query epoch parameter set:
        TestCase {
            database: Some("foo"),
            epoch: Some("s"),
            query: "SELECT time, host, usage FROM cpu",
            expected: "name,tags,time,host,usage\n\
        cpu,,2998574931,a,0.9\n\
        cpu,,2998574932,a,0.89\n\
        cpu,,2998574933,a,0.85\n\r\n",
        },
    ];

    for t in test_cases {
        let mut params = vec![("q", t.query)];
        if let Some(db) = t.database {
            params.push(("db", db));
        }
        if let Some(epoch) = t.epoch {
            params.push(("epoch", epoch));
        }
        let headers = vec![("Accept", "application/csv")];

        let resp = server
            .api_v1_query(&params, Some(&headers))
            .await
            .text()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp:#}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_chunked() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a usage=0.9 2998574931\n\
            cpu,host=a usage=0.89 2998574932\n\
            cpu,host=a usage=0.85 2998574933\n\
            mem,host=a usage=0.5 2998574934\n\
            mem,host=a usage=0.6 2998574935\n\
            mem,host=a usage=0.7 2998574936",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        chunk_size: Option<&'a str>,
        query: &'a str,
        expected: Vec<Value>,
    }

    let test_cases = [
        // Basic Query with default chunking:
        TestCase {
            chunk_size: None,
            query: "SELECT time, host, usage FROM cpu",
            expected: vec![json!({
              "results": [
                {
                  "series": [
                    {
                      "name": "cpu",
                      "columns": ["time","host","usage"],
                      "values": [
                        [2998574931u32, "a", 0.9],
                        [2998574932u32, "a", 0.89],
                        [2998574933u32, "a", 0.85]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            })],
        },
        // Basic Query with chunk size:
        TestCase {
            chunk_size: Some("2"),
            query: "SELECT time, host, usage FROM cpu",
            expected: vec![
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574931u32, "a", 0.9],
                            [2998574932u32, "a", 0.89],
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574933u32, "a", 0.85]
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
            ],
        },
        // Query with multiple measurements and default chunking:
        TestCase {
            chunk_size: None,
            query: "SELECT time, host, usage FROM cpu, mem",
            expected: vec![
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574931u32, "a", 0.9],
                            [2998574932u32, "a", 0.89],
                            [2998574933u32, "a", 0.85]
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "mem",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574934u32, "a", 0.5],
                            [2998574935u32, "a", 0.6],
                            [2998574936u32, "a", 0.7]
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
            ],
        },
        // Query with multiple measurements and chunk_size specified:
        TestCase {
            chunk_size: Some("2"),
            query: "SELECT time, host, usage FROM cpu, mem",
            expected: vec![
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574931u32, "a", 0.9],
                            [2998574932u32, "a", 0.89],
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "cpu",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574933u32, "a", 0.85]
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "mem",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574934u32, "a", 0.5],
                            [2998574935u32, "a", 0.6],
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
                json!({
                  "results": [
                    {
                      "series": [
                        {
                          "name": "mem",
                          "columns": ["time","host","usage"],
                          "values": [
                            [2998574936u32, "a", 0.7]
                          ]
                        }
                      ],
                      "statement_id": 0
                    }
                  ]
                }),
            ],
        },
    ];

    for t in test_cases {
        let mut params = vec![
            ("db", "foo"),
            ("q", t.query),
            ("epoch", "s"),
            ("chunked", "true"),
        ];
        if let Some(chunk_size) = t.chunk_size {
            params.push(("chunk_size", chunk_size));
        }
        let stream = server.api_v1_query(&params, None).await.bytes_stream();
        let values = stream
            .filter_map(|chunk| async move {
                let chunk = chunk.unwrap();
                if chunk.is_empty() {
                    None
                } else {
                    println!("{chunk:?}");
                    Some(serde_json::from_slice(&chunk).unwrap())
                }
            })
            .collect::<Vec<Value>>()
            .await;
        println!("\n{q}", q = t.query);
        assert_eq!(t.expected, values, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_data_conversion() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "weather,location=us-midwest temperature_integer=82i 2998574930\n\
          weather,location=us-midwest temperature_float=82 2998574930\n\
          weather,location=us-midwest temperature_str=\"too warm\" 2998574930\n\
          weather,location=us-midwest too_hot=true 2998574930",
            Precision::Second,
        )
        .await
        .unwrap();

    struct TestCase<'a> {
        database: Option<&'a str>,
        epoch: Option<&'a str>,
        query: &'a str,
        expected: Value,
    }

    let test_cases = [
        // Basic Query:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, location, temperature_integer, temperature_float, temperature_str, too_hot FROM weather",
            expected: json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "location",
                        "temperature_integer",
                        "temperature_float",
                        "temperature_str",
                        "too_hot"
                      ],
                      "name": "weather",
                      "values": [
                        ["2998574930000000000", "us-midwest", 82, 82.0, "too warm", true],
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            }),
        },
    ];

    for t in test_cases {
        let mut params = vec![("q", t.query)];
        if let Some(db) = t.database {
            params.push(("db", db));
        }
        if let Some(epoch) = t.epoch {
            params.push(("epoch", epoch));
        }
        let resp = server
            .api_v1_query(&params, None)
            .await
            .json::<Value>()
            .await
            .unwrap();
        println!("\n{q}", q = t.query);
        println!("{resp:#}");
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}

#[tokio::test]
async fn api_v1_query_uri_and_body() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "\
            cpu,host=a usage=0.9 2998674931\n\
            cpu,host=b usage=0.89 2998674931\n\
            cpu,host=c usage=0.85 2998674931\n\
            mem,host=a usage=0.5 2998674932\n\
            mem,host=b usage=0.6 2998674932\n\
            mem,host=c usage=0.7 2998674932\
            ",
            Precision::Second,
        )
        .await
        .unwrap();

    #[derive(Debug, Serialize)]
    struct Params<'a> {
        #[serde(rename = "q")]
        query: Option<&'a str>,
        db: Option<&'a str>,
    }

    struct TestCase<'a> {
        description: &'a str,
        uri: Option<Params<'a>>,
        body: Option<Params<'a>>,
        expected_status: StatusCode,
        expected_body: Option<Value>,
    }

    let test_cases = [
        TestCase {
            description: "query and db in uri",
            uri: Some(Params {
                query: Some("SELECT * FROM cpu"),
                db: Some("foo"),
            }),
            body: None,
            expected_status: StatusCode::OK,
            expected_body: Some(json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "cpu",
                      "values": [
                        [
                          "2998674931000000000",
                          "a",
                          0.9
                        ],
                        [
                          "2998674931000000000",
                          "b",
                          0.89
                        ],
                        [
                          "2998674931000000000",
                          "c",
                          0.85
                        ]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            })),
        },
        TestCase {
            description: "query in uri, db in body",
            uri: Some(Params {
                query: Some("SELECT * FROM cpu"),
                db: None,
            }),
            body: Some(Params {
                query: None,
                db: Some("foo"),
            }),
            expected_status: StatusCode::OK,
            // don't care about the response:
            expected_body: None,
        },
        TestCase {
            description: "query in uri, db in uri overwritten by db in body",
            uri: Some(Params {
                query: Some("SELECT * FROM cpu"),
                db: Some("not_a_valid_db"),
            }),
            body: Some(Params {
                query: None,
                db: Some("foo"),
            }),
            expected_status: StatusCode::OK,
            expected_body: None,
        },
        TestCase {
            description: "query in uri, db in uri overwritten by db in body, db not valid",
            uri: Some(Params {
                query: Some("SELECT * FROM cpu"),
                db: Some("foo"),
            }),
            body: Some(Params {
                query: None,
                db: Some("not_a_valid_db"),
            }),
            // db does not exist:
            expected_status: StatusCode::NOT_FOUND,
            expected_body: None,
        },
        TestCase {
            description: "db in uri, query in body",
            uri: Some(Params {
                query: None,
                db: Some("foo"),
            }),
            body: Some(Params {
                query: Some("SELECT * FROM mem"),
                db: None,
            }),
            expected_status: StatusCode::OK,
            expected_body: Some(json!({
              "results": [
                {
                  "series": [
                    {
                      "columns": [
                        "time",
                        "host",
                        "usage"
                      ],
                      "name": "mem",
                      "values": [
                        [
                          "2998674932000000000",
                          "a",
                          0.5
                        ],
                        [
                          "2998674932000000000",
                          "b",
                          0.6
                        ],
                        [
                          "2998674932000000000",
                          "c",
                          0.7
                        ]
                      ]
                    }
                  ],
                  "statement_id": 0
                }
              ]
            })),
        },
        TestCase {
            description: "no query specified",
            uri: Some(Params {
                query: None,
                db: Some("foo"),
            }),
            body: Some(Params {
                query: None,
                db: None,
            }),
            expected_status: StatusCode::BAD_REQUEST,
            expected_body: None,
        },
    ];

    for t in test_cases {
        let url = format!("{base}/query", base = server.client_addr());
        // test both GET and POST:
        for mut req in [server.http_client.get(&url), server.http_client.post(&url)] {
            println!("test: {desc}", desc = t.description);
            if let Some(ref uri) = t.uri {
                req = req.query(uri);
            }
            if let Some(ref body) = t.body {
                req = req.body(serde_urlencoded::to_string(body).expect("serialize body"));
            }
            let resp = req.send().await.expect("send request");
            let status = resp.status();
            assert_eq!(
                t.expected_status, status,
                "status code did not match expectation"
            );
            if let Some(ref expected_body) = t.expected_body {
                let actual = resp.json::<Value>().await.expect("parse JSON body");
                if expected_body != &actual {
                    // use a panic so we can format the output for copy/paste more easily:
                    panic!(
                        "JSON body did not match expectation,\n\
                        expected:\n{expected_body:#}\n\
                        actual:\n{actual:#}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn api_v1_query_group_by() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "\
            bar,t1=a,t2=aa val=1 2998574931\n\
            bar,t1=b,t2=aa val=2 2998574932\n\
            bar,t1=a,t2=bb val=3 2998574933\n\
            bar,t1=b,t2=bb val=4 2998574934",
            Precision::Second,
        )
        .await
        .unwrap();

    for (chunked, query) in [
        (false, "select * from bar group by t1"),
        (true, "select * from bar group by t1"),
        (false, "select * from bar group by t1, t2"),
        (true, "select * from bar group by t1, t2"),
        (false, "select * from bar group by /t/"),
        (true, "select * from bar group by /t/"),
        (false, "select * from bar group by /t[1]/"),
        (true, "select * from bar group by /t[1]/"),
        (false, "select * from bar group by /t[1,2]/"),
        (true, "select * from bar group by /t[1,2]/"),
        (false, "select * from bar group by t1, t2, t3"),
        (true, "select * from bar group by t1, t2, t3"),
        (false, "select * from bar group by *"),
        (true, "select * from bar group by *"),
        (false, "select * from bar group by /not_a_match/"),
        (true, "select * from bar group by /not_a_match/"),
    ] {
        let params = vec![
            ("db", "foo"),
            ("q", query),
            ("chunked", if chunked { "true" } else { "false" }),
        ];
        let stream = server.api_v1_query(&params, None).await.bytes_stream();
        let values = stream
            .filter_map(|chunk| async move {
                let chunk = chunk.unwrap();
                if chunk.is_empty() {
                    None
                } else {
                    println!("{chunk:?}");
                    Some(serde_json::from_slice(&chunk).unwrap())
                }
            })
            .collect::<Vec<Value>>()
            .await;
        // Use a snapshot to assert on the output structure. This deserializes each emitted line as
        // as JSON first, then combines and collects them into a Vec<Value> to serialize into a JSON
        // array for the snapshot.
        insta::with_settings!({
            description => format!("query: {query}, chunked: {chunked}"),
        }, {
            insta::assert_json_snapshot!(values);
        });
    }
}

#[tokio::test]
async fn test_influxql_group_by_tag_called_name() {
    let server = TestServer::spawn().await;
    server
        .write_lp_to_db(
            "foo",
            "\
        bar,name=a,label=a value=1 1743693748\n\
        bar,name=b,label=a value=2 1743693748\n\
        bar,name=a,label=b value=3 1743693748\n\
        bar,name=b,label=b value=4 1743693748\n\
        ",
            Precision::Second,
        )
        .await
        .unwrap();

    // query using the `name` column without quoting it is an error:
    let unquoted_query_str = "SELECT time, name, label, value FROM bar GROUP BY name LIMIT 1";
    let resp = server
        .api_v1_query(&[("db", "foo"), ("q", unquoted_query_str)], None)
        .await
        .text()
        .await
        .unwrap();
    assert_contains!(resp, "invalid InfluxQL statement");

    // query grouping on the `label` column is fine:
    let quoted_query_str = "SELECT time, \"name\", label, value FROM bar GROUP BY label LIMIT 1";
    let resp = server
        .api_v1_query(&[("db", "foo"), ("q", quoted_query_str)], None)
        .await
        .json::<Value>()
        .await
        .unwrap();
    println!("response when grouping by `label`:\n\n{resp:#}\n");
    assert_eq!(resp.pointer("/results/0/series/0/tags/label").unwrap(), "b");
    assert_eq!(resp.pointer("/results/0/series/1/tags/label").unwrap(), "a");

    // query grouping on `\"name\"`, i.e., quoted, should also be fine:
    let quoted_query_str = "SELECT time, \"name\", label, value FROM bar GROUP BY \"name\" LIMIT 1";
    let resp = server
        .api_v1_query(&[("db", "foo"), ("q", quoted_query_str)], None)
        .await
        .json::<Value>()
        .await
        .unwrap();
    println!("response when grouping by `\"name\"`:\n\n{resp:#}\n");
    assert_eq!(resp.pointer("/results/0/series/0/tags/name").unwrap(), "b");
    assert_eq!(resp.pointer("/results/0/series/1/tags/name").unwrap(), "a");
}

#[tokio::test]
async fn api_v1_query_group_by_with_nulls() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "\
            bar,t1=a val=1 2998574931\n\
            bar val=2 2998574932\n\
            bar,t1=a val=3 2998574933\n\
            ",
            Precision::Second,
        )
        .await
        .unwrap();

    for (chunked, query) in [
        (false, "select * from bar group by t1"),
        (true, "select * from bar group by t1"),
    ] {
        let params = vec![
            ("db", "foo"),
            ("q", query),
            ("chunked", if chunked { "true" } else { "false" }),
        ];
        let stream = server.api_v1_query(&params, None).await.bytes_stream();
        let values = stream
            .filter_map(|chunk| async move {
                let chunk = chunk.unwrap();
                if chunk.is_empty() {
                    None
                } else {
                    println!("{chunk:?}");
                    Some(serde_json::from_slice(&chunk).unwrap())
                }
            })
            .collect::<Vec<Value>>()
            .await;
        // Use a snapshot to assert on the output structure. This deserializes each emitted line as
        // as JSON first, then combines and collects them into a Vec<Value> to serialize into a JSON
        // array for the snapshot.
        insta::with_settings!({
            description => format!("query: {query}, chunked: {chunked}"),
        }, {
            insta::assert_json_snapshot!(values);
        });
    }
}

#[tokio::test]
async fn api_v3_query_sql_distinct_cache() {
    let server = TestServer::spawn().await;
    server
        .write_lp_to_db("foo", "cpu,region=us,host=a usage=99", Precision::Second)
        .await
        .unwrap();

    server
        .http_client
        .post(format!(
            "{base}/api/v3/configure/distinct_cache",
            base = server.client_addr()
        ))
        .json(&serde_json::json!({
            "db": "foo",
            "table": "cpu",
            "columns": ["region", "host"],
        }))
        .send()
        .await
        .unwrap();

    server
        .write_lp_to_db(
            "foo",
            "\
            cpu,region=us,host=a usage=99\n\
            cpu,region=eu,host=b usage=99\n\
            cpu,region=ca,host=c usage=99\n\
            ",
            Precision::Second,
        )
        .await
        .unwrap();

    let resp = server
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("format", "pretty"),
            ("q", "SELECT * FROM distinct_cache('cpu')"),
        ])
        .await
        .text()
        .await
        .unwrap();

    assert_eq!(
        "+--------+------+\n\
        | region | host |\n\
        +--------+------+\n\
        | ca     | c    |\n\
        | eu     | b    |\n\
        | us     | a    |\n\
        +--------+------+\
        ",
        resp
    );

    // do the query using JSON format:
    let resp = server
        .query_sql("foo")
        .with_sql("SELECT * FROM distinct_cache('cpu')")
        .run()
        .unwrap();

    assert_eq!(
        json!([
            {"region": "ca", "host": "c"},
            {"region": "eu", "host": "b"},
            {"region": "us", "host": "a"},
        ]),
        resp
    );
}

/// Test that if we write in a row of LP that is missing a tag value, we're still able to query.
/// Also tests that if the buffer is missing a field, we're still able to query.
#[test_log::test(tokio::test)]
async fn api_v3_query_null_tag_values_null_fields() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9,system=0.1 2998674931
            cpu,host=b usage=0.80,system=0.1 2998674934",
            Precision::Second,
        )
        .await
        .unwrap();

    let client = server.http_client();
    let url = format!("{base}/api/v3/query_sql", base = server.client_addr());

    let resp = client
        .get(&url)
        .query(&[
            ("db", "foo"),
            (
                "q",
                "SELECT host, region, time, usage FROM cpu ORDER BY time",
            ),
            ("format", "pretty"),
        ])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        "+------+---------+---------------------+-------+\n\
            | host | region  | time                | usage |\n\
            +------+---------+---------------------+-------+\n\
            | a    | us-east | 2065-01-08T21:15:31 | 0.9   |\n\
            | b    |         | 2065-01-08T21:15:34 | 0.8   |\n\
            +------+---------+---------------------+-------+",
        resp
    );

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 2998674935",
            Precision::Second,
        )
        .await
        .unwrap();

    let resp = client
        .get(&url)
        .query(&[
            ("db", "foo"),
            ("q", "SELECT * FROM cpu ORDER BY time"),
            ("format", "pretty"),
        ])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        "+------+---------+--------+---------------------+-------+\n\
         | host | region  | system | time                | usage |\n\
         +------+---------+--------+---------------------+-------+\n\
         | a    | us-east | 0.1    | 2065-01-08T21:15:31 | 0.9   |\n\
         | b    |         | 0.1    | 2065-01-08T21:15:34 | 0.8   |\n\
         | a    | us-east |        | 2065-01-08T21:15:35 | 0.9   |\n\
         +------+---------+--------+---------------------+-------+",
        resp
    );
}

#[tokio::test]
async fn api_query_with_default_browser_header() {
    let server = TestServer::spawn().await;
    server
        .write_lp_to_db(
            "foo",
            "cpu,region=us,host=a usage=99 2998674934",
            Precision::Second,
        )
        .await
        .unwrap();

    let mut map = HeaderMap::new();
    map.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    );
    let resp = server
        .api_v3_query_sql_with_header(
            &[
                ("db", "foo"),
                ("format", "pretty"),
                ("q", "SELECT * FROM cpu"),
            ],
            map,
        )
        .await
        .text()
        .await
        .unwrap();

    assert_eq!(
        "+------+--------+---------------------+-------+\n\
        | host | region | time                | usage |\n\
        +------+--------+---------------------+-------+\n\
        | a    | us     | 2065-01-08T21:15:34 | 99.0  |\n\
        +------+--------+---------------------+-------+",
        resp
    );
}

#[tokio::test]
async fn api_v3_query_show_tables_ordering() {
    let server = TestServer::spawn().await;

    let tables = [
        "xxx",
        "table_002",
        "table_009",
        "table_001",
        "table_003",
        "table_006",
        "aaa",
    ];

    for table in tables {
        server.create_table("foo", table).run_api().await.unwrap();
    }

    let output = server
        .api_v3_query_sql(&[("db", "foo"), ("format", "pretty"), ("q", "SHOW TABLES")])
        .await
        .text()
        .await
        .unwrap();

    insta::assert_snapshot!(output);
}
