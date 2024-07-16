use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;
use serde_json::json;

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

    let mut client = server.flight_sql_client("foo").await;

    // Check queries table for completed queries, will be empty:
    {
        let response = client
            .query("SELECT COUNT(*) FROM system.queries WHERE running = false")
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+----------+",
                "| count(*) |",
                "+----------+",
                "| 0        |",
                "+----------+",
            ],
            &batches
        );
    }

    // Do some queries, to produce some query logs:
    {
        let queries = [
            "SELECT * FROM cpu",           // valid
            "SELECT * FROM mem",           // not valid table, will fail, and not be logged
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
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
                "| phase   | query_type | query_text                                                                     | success | running | cancelled |",
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
                "| success | flightsql  | CommandStatementQuerySELECT * FROM cpu                                         | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT COUNT(*) FROM system.queries WHERE running = false | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT usage, time FROM cpu                               | true    | false   | false     |",
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
            ],
            &batches
        );
    }
}

#[tokio::test]
async fn last_caches_table() {
    let server = TestServer::spawn().await;
    let db1_name = "foo";
    let db2_name = "bar";
    // Write some LP to initialize the catalog
    server
        .write_lp_to_db(
            db1_name,
            "\
        cpu,region=us,host=a,cpu=1 usage=90\n\
        mem,region=us,host=a usage=500\n\
        ",
            Precision::Second,
        )
        .await
        .expect("write to db");
    server
        .write_lp_to_db(
            db2_name,
            "\
        cpu,region=us,host=a,cpu=1 usage=90\n\
        mem,region=us,host=a usage=500\n\
        ",
            Precision::Second,
        )
        .await
        .expect("write to db");

    // Check that there are no last caches:
    {
        let resp = server
            .flight_sql_client(&db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }
    {
        let resp = server
            .flight_sql_client(&db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }

    // Create some last caches, two on DB1 and one on DB2:
    assert!(server
        .api_v3_configure_last_cache_create(&json!({
            "db": db1_name,
            "table": "cpu",
            "key_columns": ["host"],
        }))
        .await
        .status()
        .is_success());
    assert!(server
        .api_v3_configure_last_cache_create(&json!({
            "db": db1_name,
            "table": "mem",
            "name": "mem_last_cache",
            "ttl": 60
        }))
        .await
        .status()
        .is_success());
    assert!(server
        .api_v3_configure_last_cache_create(&json!({
            "db": db2_name,
            "table": "cpu",
            "count": 5
        }))
        .await
        .status()
        .is_success());

    // Check the system table for each DB:
    {
        let resp = server
            .flight_sql_client(&db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!([
                "+-------+---------------------+----------------+----------------------------+-------+-------+",
                "| table | name                | key_columns    | value_columns              | count | ttl   |",
                "+-------+---------------------+----------------+----------------------------+-------+-------+",
                "| cpu   | cpu_host_last_cache | [host]         | [cpu, region, time, usage] | 1     | 14400 |",
                "| mem   | mem_last_cache      | [host, region] | [time, usage]              | 1     | 60    |",
                "+-------+---------------------+----------------+----------------------------+-------+-------+",
            ],
            &batches
        );
    }
    {
        let resp = server
            .flight_sql_client(&db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!([
                "+-------+--------------------------------+---------------------+---------------+-------+-------+",
                "| table | name                           | key_columns         | value_columns | count | ttl   |",
                "+-------+--------------------------------+---------------------+---------------+-------+-------+",
                "| cpu   | cpu_cpu_host_region_last_cache | [cpu, host, region] | [time, usage] | 5     | 14400 |",
                "+-------+--------------------------------+---------------------+---------------+-------+-------+",
            ],
            &batches
        );
    }

    // Make some changes to the caches and check the system table

    // Delete one of the caches:
    {
        assert!(server
            .api_v3_configure_last_cache_delete(&json!({
                "db": db1_name,
                "table": "cpu",
                "name": "cpu_host_last_cache",
            }))
            .await
            .status()
            .is_success());

        let resp = server
            .flight_sql_client(&db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(
            [
                "+-------+----------------+----------------+---------------+-------+-----+",
                "| table | name           | key_columns    | value_columns | count | ttl |",
                "+-------+----------------+----------------+---------------+-------+-----+",
                "| mem   | mem_last_cache | [host, region] | [time, usage] | 1     | 60  |",
                "+-------+----------------+----------------+---------------+-------+-----+",
            ],
            &batches
        );
    }

    // Add fields to one of the caches, in this case, the `temp` field will get added to the
    // value columns for the respective cache:
    {
        server
            .write_lp_to_db(
                &db2_name,
                "cpu,region=us,host=a,cpu=2 usage=40,temp=95",
                Precision::Second,
            )
            .await
            .unwrap();

        let resp = server
            .flight_sql_client(&db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!([
                "+-------+--------------------------------+---------------------+---------------------+-------+-------+",
                "| table | name                           | key_columns         | value_columns       | count | ttl   |",
                "+-------+--------------------------------+---------------------+---------------------+-------+-------+",
                "| cpu   | cpu_cpu_host_region_last_cache | [cpu, host, region] | [time, usage, temp] | 5     | 14400 |",
                "+-------+--------------------------------+---------------------+---------------------+-------+-------+",
            ],
            &batches
        );
    }
}
