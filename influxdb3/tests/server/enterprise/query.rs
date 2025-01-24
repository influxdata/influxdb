use crate::server::TestServer;
use influxdb3_client::Precision;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn api_v3_query_sql_before_72_hours() {
    let server = TestServer::spawn().await;

    // Note: the writes below go back unix epoch to prove we can
    //       write/query before 72h, specific to enterprise
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 0\n\
            cpu,host=s1,region=us-east usage=0.89 1\n\
            cpu,host=s1,region=us-east usage=0.85 2",
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
                | s1   | us-east | 1970-01-01T00:00:00           | 0.9   |\n\
                | s1   | us-east | 1970-01-01T00:00:00.000000001 | 0.89  |\n\
                | s1   | us-east | 1970-01-01T00:00:00.000000002 | 0.85  |\n\
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

#[cfg(test)]
struct TestCase<'a> {
    database: Option<&'a str>,
    query: &'a str,
    expected: &'a str,
}
