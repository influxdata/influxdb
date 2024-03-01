use crate::TestServer;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;

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
        .await;

    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/v3/query_sql",
            base = server.client_addr()
        ))
        .query(&[
            ("db", "foo"),
            ("q", "SELECT * FROM cpu"),
            ("format", "pretty"),
        ])
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        "+------+---------+-------------------------------+-------+\n\
        | host | region  | time                          | usage |\n\
        +------+---------+-------------------------------+-------+\n\
        | s1   | us-east | 1970-01-01T00:00:00.000000001 | 0.9   |\n\
        | s1   | us-east | 1970-01-01T00:00:00.000000002 | 0.89  |\n\
        | s1   | us-east | 1970-01-01T00:00:00.000000003 | 0.85  |\n\
        +------+---------+-------------------------------+-------+",
        resp,
    );
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
        .await;

    struct TestCase<'a> {
        database: Option<&'a str>,
        query: &'a str,
        expected: &'a str,
    }

    let test_cases = [
        TestCase {
            database: Some("foo"),
            query: "SELECT * FROM cpu",
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
            query: "SELECT * FROM foo.autogen.cpu",
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
            query: "SELECT * FROM cpu, mem",
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
                    | foo           |\n\
                    +---------------+",
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
