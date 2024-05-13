use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;

use crate::{collect_stream, TestServer};

#[tokio::test]
async fn queries_table() {
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
        .expect("write some lp");

    let mut client = server.flight_sql_client_debug_mode("foo", true).await;

    // Check queries table, there will be only one, for the query we are running here:
    {
        let response = client
            .query("SELECT COUNT(*) FROM system.queries")
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+----------+",
                "| COUNT(*) |",
                "+----------+",
                "| 1        |",
                "+----------+",
            ],
            &batches
        );
    }

    // Do some queries, to produce some query logs:
    {
        let queries = [
            "SELECT * FROM cpu",           // valid
            "SELECT * FROM mem",           // not valid table, will fail
            "SELECT usage, time FROM cpu", // specific columns
        ];
        for q in queries {
            let response = client.query(q).await;
            // collect the stream to make sure the query completes:
            if let Ok(stream) = response {
                let _batches = collect_stream(stream).await;
            }
        }
    }

    // Now check the log:
    {
        // A sub-set of columns is selected in this query, because the queries
        // table contains may columns whose values change in susequent test runs
        let response = client
            .query(
                "SELECT \
                    phase, \
                    query_type, \
                    query_text, \
                    success, \
                    running, \
                    cancelled \
                FROM system.queries \
                WHERE success = true",
            )
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+---------+------------+----------------------------------------------------------+---------+---------+-----------+",
                "| phase   | query_type | query_text                                               | success | running | cancelled |",
                "+---------+------------+----------------------------------------------------------+---------+---------+-----------+",
                "| success | flightsql  | CommandStatementQuerySELECT * FROM cpu                   | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT COUNT(*) FROM system.queries | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT usage, time FROM cpu         | true    | false   | false     |",
                "+---------+------------+----------------------------------------------------------+---------+---------+-----------+",
            ],
            &batches
        );
    }
}
