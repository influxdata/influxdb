use arrow::record_batch::RecordBatch;
use arrow_flight::{decode::FlightRecordBatchStream, sql::SqlInfo};
use arrow_util::assert_batches_sorted_eq;
use futures::TryStreamExt;
use influxdb3_client::Precision;

use crate::common::TestServer;

mod common;

#[tokio::test]
async fn flight() {
    let server = TestServer::spawn().await;

    // use the influxdb3_client to write in some data
    write_lp_to_db(
        &server,
        "foo",
        "cpu,host=s1,region=us-east usage=0.9 1\n\
        cpu,host=s1,region=us-east usage=0.89 2\n\
        cpu,host=s1,region=us-east usage=0.85 3",
        Precision::Nanosecond,
    )
    .await;

    let mut client = server.flight_client("foo").await;

    // Ad-hoc Query:
    {
        let response = client.query("SELECT * FROM cpu").await.unwrap();

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
        let handle = client.prepare("SELECT * FROM cpu".into()).await.unwrap();
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
                "| public       | information_schema | tables      | VIEW       |",
                "| public       | information_schema | views       | VIEW       |",
                "| public       | iox                | cpu         | BASE TABLE |",
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
}

async fn write_lp_to_db(
    server: &TestServer,
    database: &str,
    lp: &'static str,
    precision: Precision,
) {
    let client = influxdb3_client::Client::new(server.client_addr()).unwrap();
    client
        .api_v3_write_lp(database)
        .body(lp)
        .precision(precision)
        .send()
        .await
        .unwrap();
}

async fn collect_stream(stream: FlightRecordBatchStream) -> Vec<RecordBatch> {
    stream
        .try_collect()
        .await
        .expect("gather record batch stream")
}
