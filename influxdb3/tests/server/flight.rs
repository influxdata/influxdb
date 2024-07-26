use arrow_flight::sql::SqlInfo;
use arrow_flight::Ticket;
use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;
use test_helpers::assert_contains;

use crate::collect_stream;
use crate::TestServer;

#[tokio::test]
async fn flight() -> Result<(), influxdb3_client::Error> {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 1\n\
        cpu,host=s1,region=us-east usage=0.89 2\n\
        cpu,host=s1,region=us-east usage=0.85 3",
            Precision::Nanosecond,
        )
        .await?;

    let mut client = server.flight_sql_client("foo").await;

    // Ad-hoc Query:
    {
        let response = client
            .query("SELECT host, region, time, usage FROM cpu")
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+------+---------+--------------------------------+-------+",
                "| host | region  | time                           | usage |",
                "+------+---------+--------------------------------+-------+",
                "| s1   | us-east | 1970-01-01T00:00:00.000000001Z | 0.9   |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000002Z | 0.89  |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000003Z | 0.85  |",
                "+------+---------+--------------------------------+-------+",
            ],
            &batches
        );
    }

    // Ad-hoc Query error:
    {
        let error = client
            .query("SELECT * FROM invalid_table")
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("table 'public.iox.invalid_table' not found"));
    }

    // Prepared query:
    {
        let handle = client
            .prepare("SELECT host, region, time, usage FROM cpu".into(), None)
            .await
            .unwrap();
        let stream = client.execute(handle).await.unwrap();

        let batches = collect_stream(stream).await;
        assert_batches_sorted_eq!(
            [
                "+------+---------+--------------------------------+-------+",
                "| host | region  | time                           | usage |",
                "+------+---------+--------------------------------+-------+",
                "| s1   | us-east | 1970-01-01T00:00:00.000000001Z | 0.9   |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000002Z | 0.89  |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000003Z | 0.85  |",
                "+------+---------+--------------------------------+-------+",
            ],
            &batches
        );
    }

    // Get SQL Infos:
    {
        let infos = vec![SqlInfo::FlightSqlServerName as u32];
        let stream = client.get_sql_info(infos).await.unwrap();
        let batches = collect_stream(stream).await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-----------------------------+",
                "| info_name | value                       |",
                "+-----------+-----------------------------+",
                "| 0         | {string_value=InfluxDB IOx} |",
                "+-----------+-----------------------------+",
            ],
            &batches
        );
    }

    // Get Tables
    {
        type OptStr = std::option::Option<&'static str>;
        let stream = client
            .get_tables(OptStr::None, OptStr::None, OptStr::None, vec![], false)
            .await
            .unwrap();
        let batches = collect_stream(stream).await;

        assert_batches_sorted_eq!(
            [
                "+--------------+--------------------+-------------+------------+",
                "| catalog_name | db_schema_name     | table_name  | table_type |",
                "+--------------+--------------------+-------------+------------+",
                "| public       | information_schema | columns     | VIEW       |",
                "| public       | information_schema | df_settings | VIEW       |",
                "| public       | information_schema | schemata    | VIEW       |",
                "| public       | information_schema | tables      | VIEW       |",
                "| public       | information_schema | views       | VIEW       |",
                "| public       | iox                | cpu         | BASE TABLE |",
                "| public       | system             | last_caches | BASE TABLE |",
                "| public       | system             | queries     | BASE TABLE |",
                "+--------------+--------------------+-------------+------------+",
            ],
            &batches
        );
    }

    // Get Catalogs
    {
        let stream = client.get_catalogs().await.unwrap();
        let batches = collect_stream(stream).await;
        assert_batches_sorted_eq!(
            [
                "+--------------+",
                "| catalog_name |",
                "+--------------+",
                "| public       |",
                "+--------------+",
            ],
            &batches
        );
    }

    Ok(())
}

#[tokio::test]
async fn flight_influxql() {
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

    let mut client = server.flight_client().await;

    // Ad-hoc query, using qualified measurement name
    // This is no longer supported in 3.0, see
    // https://github.com/influxdata/influxdb_iox/pull/11254
    {
        let ticket = Ticket::new(
            r#"{
                    "database": "foo",
                    "sql_query": "SELECT time, host, region, usage FROM foo.autogen.cpu",
                    "query_type": "influxql"
                }"#,
        );
        let response = client.do_get(ticket).await.unwrap_err().to_string();

        assert_contains!(response, "database prefix in qualified measurement syntax");
    }

    // InfluxQL-specific query to show measurements:
    {
        let ticket = Ticket::new(
            r#"{
                    "database": "foo",
                    "sql_query": "SHOW MEASUREMENTS",
                    "query_type": "influxql"
                }"#,
        );
        let response = client.do_get(ticket).await.unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+------------------+------+",
                "| iox::measurement | name |",
                "+------------------+------+",
                "| measurements     | cpu  |",
                "+------------------+------+",
            ],
            &batches
        );
    }

    // An InfluxQL query that is not supported over Flight:
    {
        let ticket = Ticket::new(
            r#"{
                    "database": "foo",
                    "sql_query": "SHOW DATABASES",
                    "query_type": "influxql"
                }"#,
        );
        let response = client.do_get(ticket).await.unwrap_err();

        assert_contains!(
            response.to_string(),
            "This feature is not implemented: SHOW DATABASES"
        );
    }
}
