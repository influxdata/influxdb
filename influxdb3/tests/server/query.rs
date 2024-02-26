use crate::TestServer;

#[tokio::test]
async fn api_v3_query_sql() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 1\n\
            cpu,host=s1,region=us-east usage=0.89 2\n\
            cpu,host=s1,region=us-east usage=0.85 3",
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
        )
        .await;

    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/v3/query_influxql",
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
        "+------------------+-------------------------------+------+---------+-------+\n\
        | iox::measurement | time                          | host | region  | usage |\n\
        +------------------+-------------------------------+------+---------+-------+\n\
        | cpu              | 1970-01-01T00:00:00.000000001 | s1   | us-east | 0.9   |\n\
        | cpu              | 1970-01-01T00:00:00.000000002 | s1   | us-east | 0.89  |\n\
        | cpu              | 1970-01-01T00:00:00.000000003 | s1   | us-east | 0.85  |\n\
        +------------------+-------------------------------+------+---------+-------+",
        resp,
    );
}
