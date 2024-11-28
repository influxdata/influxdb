use crate::server::TestServer;
use futures::StreamExt;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;
use serde_json::{json, Value};
use test_helpers::assert_contains;

#[tokio::test]
async fn api_v3_query_sql() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 1\n\
            cpu,host=s1,region=us-east usage=0.89 2\n\
            cpu,host=s1,region=us-east usage=0.85 3",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT host, region, time, usage FROM cpu",
            expected: "+------+---------+-------------------------------+-------+\n\
            | host | region  | time                          | usage |\n\
            +------+---------+-------------------------------+-------+\n\
            | s1   | us-east | 1970-01-01T00:00:00.000000001 | 0.9   |\n\
            | s1   | us-east | 1970-01-01T00:00:00.000000002 | 0.89  |\n\
            | s1   | us-east | 1970-01-01T00:00:00.000000003 | 0.85  |\n\
            +------+---------+-------------------------------+-------+",
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
async fn api_v3_query_sql_params() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a,region=us-east usage=0.9 1
            cpu,host=b,region=us-east usage=0.50 1
            cpu,host=a,region=us-east usage=0.80 2
            cpu,host=b,region=us-east usage=0.60 2
            cpu,host=a,region=us-east usage=0.70 3
            cpu,host=b,region=us-east usage=0.70 3
            cpu,host=a,region=us-east usage=0.50 4
            cpu,host=b,region=us-east usage=0.80 4",
            Precision::Second,
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
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
            | b    | us-east | 1970-01-01T00:00:03 | 0.7   |\n\
            | b    | us-east | 1970-01-01T00:00:04 | 0.8   |\n\
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
            | b    | us-east | 1970-01-01T00:00:03 | 0.7   |\n\
            | b    | us-east | 1970-01-01T00:00:04 | 0.8   |\n\
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
            "cpu,host=s1,region=us-east usage=0.9 1\n\
            cpu,host=s1,region=us-east usage=0.89 2\n\
            cpu,host=s1,region=us-east usage=0.85 3\n\
            mem,host=s1,region=us-east usage=0.5 4\n\
            mem,host=s1,region=us-east usage=0.6 5\n\
            mem,host=s1,region=us-east usage=0.7 6",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    // write to another database for `SHOW DATABASES` and `SHOW RETENTION POLICIES`
    server
        .write_lp_to_db(
            "bar",
            "cpu,host=s1,region=us-east usage=0.9 1\n\
            cpu,host=s1,region=us-east usage=0.89 2\n\
            cpu,host=s1,region=us-east usage=0.85 3\n\
            mem,host=s1,region=us-east usage=0.5 4\n\
            mem,host=s1,region=us-east usage=0.6 5\n\
            mem,host=s1,region=us-east usage=0.7 6",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT time, host, region, usage FROM cpu",
            expected:
                "+------------------+-------------------------------+------+---------+-------+\n\
                | iox::measurement | time                          | host | region  | usage |\n\
                +------------------+-------------------------------+------+---------+-------+\n\
                | cpu              | 1970-01-01T00:00:00.000000001 | s1   | us-east | 0.9   |\n\
                | cpu              | 1970-01-01T00:00:00.000000002 | s1   | us-east | 0.89  |\n\
                | cpu              | 1970-01-01T00:00:00.000000003 | s1   | us-east | 0.85  |\n\
                +------------------+-------------------------------+------+---------+-------+",
        },
        TestCase {
            database: None,
            query: "SELECT time, host, region, usage FROM foo.autogen.cpu",
            expected:
                "+------------------+-------------------------------+------+---------+-------+\n\
                | iox::measurement | time                          | host | region  | usage |\n\
                +------------------+-------------------------------+------+---------+-------+\n\
                | cpu              | 1970-01-01T00:00:00.000000001 | s1   | us-east | 0.9   |\n\
                | cpu              | 1970-01-01T00:00:00.000000002 | s1   | us-east | 0.89  |\n\
                | cpu              | 1970-01-01T00:00:00.000000003 | s1   | us-east | 0.85  |\n\
                +------------------+-------------------------------+------+---------+-------+",
        },
        TestCase {
            database: Some("foo"),
            query: "SELECT host, region, usage FROM cpu, mem",
            expected:
                "+------------------+-------------------------------+------+---------+-------+\n\
                | iox::measurement | time                          | host | region  | usage |\n\
                +------------------+-------------------------------+------+---------+-------+\n\
                | cpu              | 1970-01-01T00:00:00.000000001 | s1   | us-east | 0.9   |\n\
                | cpu              | 1970-01-01T00:00:00.000000002 | s1   | us-east | 0.89  |\n\
                | cpu              | 1970-01-01T00:00:00.000000003 | s1   | us-east | 0.85  |\n\
                | mem              | 1970-01-01T00:00:00.000000004 | s1   | us-east | 0.5   |\n\
                | mem              | 1970-01-01T00:00:00.000000005 | s1   | us-east | 0.6   |\n\
                | mem              | 1970-01-01T00:00:00.000000006 | s1   | us-east | 0.7   |\n\
                +------------------+-------------------------------+------+---------+-------+",
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
            query: "SHOW TAG VALUES WITH KEY = \"host\" WHERE time < 1970-01-02",
            expected: "+------------------+------+-------+\n\
                    | iox::measurement | key  | value |\n\
                    +------------------+------+-------+\n\
                    | cpu              | host | s1    |\n\
                    | mem              | host | s1    |\n\
                    +------------------+------+-------+",
        },
        TestCase {
            database: None,
            query: "SHOW TAG VALUES ON foo WITH KEY = \"host\" WHERE time < 1970-01-02",
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
            expected: "+---------------+\n\
                    | iox::database |\n\
                    +---------------+\n\
                    | bar           |\n\
                    | foo           |\n\
                    +---------------+",
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES",
            expected: "+---------------+---------+----------+\n\
                    | iox::database | name    | duration |\n\
                    +---------------+---------+----------+\n\
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
            "cpu,host=a,region=us-east usage=0.9 1
            cpu,host=b,region=us-east usage=0.50 1
            cpu,host=a,region=us-east usage=0.80 2
            cpu,host=b,region=us-east usage=0.60 2
            cpu,host=a,region=us-east usage=0.70 3
            cpu,host=b,region=us-east usage=0.70 3
            cpu,host=a,region=us-east usage=0.50 4
            cpu,host=b,region=us-east usage=0.80 4",
            Precision::Second,
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
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
            | cpu              | 1970-01-01T00:00:03 | b    | us-east | 0.7   |\n\
            | cpu              | 1970-01-01T00:00:04 | b    | us-east | 0.8   |\n\
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
            | cpu              | 1970-01-01T00:00:03 | b    | us-east | 0.7   |\n\
            | cpu              | 1970-01-01T00:00:04 | b    | us-east | 0.8   |\n\
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
            "cpu,host=a,region=us-east usage=0.9 1
            cpu,host=b,region=us-east usage=0.50 1
            cpu,host=a,region=us-east usage=0.80 2
            cpu,host=b,region=us-east usage=0.60 2
            cpu,host=a,region=us-east usage=0.70 3
            cpu,host=b,region=us-east usage=0.70 3
            cpu,host=a,region=us-east usage=0.50 4
            cpu,host=b,region=us-east usage=0.80 4",
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
                    "time": "1970-01-01T00:00:01",
                    "usage": 0.9
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:01",
                    "usage": 0.5
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:02",
                    "usage": 0.8
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:02",
                    "usage": 0.6
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:03",
                    "usage": 0.7
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:03",
                    "usage": 0.7
                },
                {
                    "host": "a",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:04",
                    "usage": 0.5
                },
                {
                    "host": "b",
                    "iox::measurement": "cpu",
                    "region": "us-east",
                    "time": "1970-01-01T00:00:04",
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
                  "iox::database": "foo",
                },
            ]),
        },
        TestCase {
            database: None,
            query: "SHOW RETENTION POLICIES",
            expected: json!([
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
async fn api_v1_query_json_format() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=a usage=0.9 1\n\
            cpu,host=a usage=0.89 2\n\
            cpu,host=a usage=0.85 3\n\
            mem,host=a usage=0.5 4\n\
            mem,host=a usage=0.6 5\n\
            mem,host=a usage=0.7 6",
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
                        ["1970-01-01T00:00:01Z", "a", 0.9],
                        ["1970-01-01T00:00:02Z", "a", 0.89],
                        ["1970-01-01T00:00:03Z", "a", 0.85]
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
                        ["1970-01-01T00:00:04Z", "a", 0.5],
                        ["1970-01-01T00:00:05Z", "a", 0.6],
                        ["1970-01-01T00:00:06Z", "a", 0.7]
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
                        ["1970-01-01T00:00:01Z", "a", 0.9],
                        ["1970-01-01T00:00:02Z", "a", 0.89],
                        ["1970-01-01T00:00:03Z", "a", 0.85]
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
                        ["1970-01-01T00:00:01Z", "a", 0.9],
                        ["1970-01-01T00:00:02Z", "a", 0.89],
                        ["1970-01-01T00:00:03Z", "a", 0.85]
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
                        [1, "a", 0.9],
                        [2, "a", 0.89],
                        [3, "a", 0.85]
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
            "cpu,host=a usage=0.9 1\n\
          cpu,host=a usage=0.89 2\n\
          cpu,host=a usage=0.85 3\n\
          mem,host=a usage=0.5 4\n\
          mem,host=a usage=0.6 5\n\
          mem,host=a usage=0.7 6",
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
            cpu,,1970-01-01T00:00:01Z,a,0.9\n\
            cpu,,1970-01-01T00:00:02Z,a,0.89\n\
            cpu,,1970-01-01T00:00:03Z,a,0.85\n\r\n",
        },
        // Basic Query with multiple measurements:
        TestCase {
            database: Some("foo"),
            epoch: None,
            query: "SELECT time, host, usage FROM cpu, mem",
            expected: "name,tags,time,host,usage\n\
            mem,,1970-01-01T00:00:04Z,a,0.5\n\
            mem,,1970-01-01T00:00:05Z,a,0.6\n\
            mem,,1970-01-01T00:00:06Z,a,0.7\n\
            cpu,,1970-01-01T00:00:01Z,a,0.9\n\
            cpu,,1970-01-01T00:00:02Z,a,0.89\n\
            cpu,,1970-01-01T00:00:03Z,a,0.85\n\r\n",
        },
        // Basic Query with db in query string:
        TestCase {
            database: None,
            epoch: None,
            query: "SELECT time, host, usage FROM foo.autogen.cpu",
            expected: "name,tags,time,host,usage\n\
          cpu,,1970-01-01T00:00:01Z,a,0.9\n\
          cpu,,1970-01-01T00:00:02Z,a,0.89\n\
          cpu,,1970-01-01T00:00:03Z,a,0.85\n\r\n",
        },
        // Basic Query epoch parameter set:
        TestCase {
            database: Some("foo"),
            epoch: Some("s"),
            query: "SELECT time, host, usage FROM cpu",
            expected: "name,tags,time,host,usage\n\
        cpu,,1,a,0.9\n\
        cpu,,2,a,0.89\n\
        cpu,,3,a,0.85\n\r\n",
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
            "cpu,host=a usage=0.9 1\n\
            cpu,host=a usage=0.89 2\n\
            cpu,host=a usage=0.85 3\n\
            mem,host=a usage=0.5 4\n\
            mem,host=a usage=0.6 5\n\
            mem,host=a usage=0.7 6",
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
                        [1, "a", 0.9],
                        [2, "a", 0.89],
                        [3, "a", 0.85]
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
                            [1, "a", 0.9],
                            [2, "a", 0.89],
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
                            [3, "a", 0.85]
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
                            [1, "a", 0.9],
                            [2, "a", 0.89],
                            [3, "a", 0.85]
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
                            [4, "a", 0.5],
                            [5, "a", 0.6],
                            [6, "a", 0.7]
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
                            [1, "a", 0.9],
                            [2, "a", 0.89],
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
                            [3, "a", 0.85]
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
                            [4, "a", 0.5],
                            [5, "a", 0.6],
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
                            [6, "a", 0.7]
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
            .map(|chunk| {
                println!("{chunk:?}");
                serde_json::from_slice(chunk.unwrap().as_ref()).unwrap()
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
            "weather,location=us-midwest temperature_integer=82i 1465839830100400200\n\
          weather,location=us-midwest temperature_float=82 1465839830100400200\n\
          weather,location=us-midwest temperature_str=\"too warm\" 1465839830100400200\n\
          weather,location=us-midwest too_hot=true 1465839830100400200",
            Precision::Nanosecond,
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
                        ["2016-06-13T17:43:50.100400200Z", "us-midwest", 82, 82.0, "too warm", true],
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
async fn api_v3_query_sql_meta_cache() {
    let server = TestServer::spawn().await;
    server
        .write_lp_to_db("foo", "cpu,region=us,host=a usage=99", Precision::Second)
        .await
        .unwrap();

    server
        .http_client
        .post(format!(
            "{base}/api/v3/configure/meta_cache",
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
            ("q", "SELECT * FROM meta_cache('cpu')"),
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
}
