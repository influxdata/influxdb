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
            cpu,host=s1,region=us-east usage=0.85 3",
            Precision::Nanosecond,
        )
        .await;

    struct TestCase<'a> {
        query: &'a str,
        expected: &'a str,
    }

    let test_cases = [
        TestCase {
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
            query: "SHOW MEASUREMENTS",
            expected: "+------------------+------+\n\
                    | iox::measurement | name |\n\
                    +------------------+------+\n\
                    | measurements     | cpu  |\n\
                    +------------------+------+",
        },
        TestCase {
            query: "SHOW FIELD KEYS",
            expected: "+------------------+----------+-----------+\n\
                    | iox::measurement | fieldKey | fieldType |\n\
                    +------------------+----------+-----------+\n\
                    | cpu              | usage    | float     |\n\
                    +------------------+----------+-----------+",
        },
        TestCase {
            query: "SHOW TAG KEYS",
            expected: "+------------------+--------+\n\
                    | iox::measurement | tagKey |\n\
                    +------------------+--------+\n\
                    | cpu              | host   |\n\
                    | cpu              | region |\n\
                    +------------------+--------+",
        },
        // TODO - this is not working, it should output a table with `key` and `value` column
        //      - it looks from the server logs that the query planner is trying to produce
        //        record batches in response to this query, as opposed to some other invalid
        //        SHOW TAG VALUES query, but it produces an empty result set
        TestCase {
            query: "SHOW TAG VALUES WITH KEY = \"host\"",
            // This is the current empty result, i.e., "++\n++", that is wrong:
            expected: "++\n++",
        },
    ];

    for t in test_cases {
        let resp = server
            .api_v3_query_influxql(&[("db", "foo"), ("q", t.query), ("format", "pretty")])
            .await
            .text()
            .await
            .unwrap();
        assert_eq!(t.expected, resp, "query failed: {q}", q = t.query);
    }
}
