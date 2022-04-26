use crate::{
    common::{
        server_fixture::{ServerFixture, ServerType, TestConfig},
        udp_listener::UdpCapture,
    },
    end_to_end_cases::scenario::{rand_name, wildcard_router_config, DatabaseBuilder},
};
use arrow_util::assert_batches_sorted_eq;
use generated_types::influxdata::iox::write_buffer::v1::WriteBufferConnection;
use influxdb_iox_client::{
    delete::generated_types::{Predicate, TimestampRange},
    error::Error,
    flight::generated_types::ReadInfo,
    management::generated_types::WriteBufferCreationConfig,
};
use iox_time::SystemProvider;
use std::{num::NonZeroU32, sync::Arc};
use tempfile::TempDir;
use test_helpers::{assert_contains, assert_error};
use write_buffer::{core::WriteBufferWriting, file::FileBufferProducer};

#[tokio::test]
async fn reads_come_from_write_buffer() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database to read from write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 2,
            ..Default::default()
        }),
        ..Default::default()
    };

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .build(server.grpc_channel())
        .await;

    // put some points in write buffer
    let time_provider = Arc::new(SystemProvider::new());
    let producer = FileBufferProducer::new(
        write_buffer_dir.path(),
        &db_name,
        Default::default(),
        time_provider,
    )
    .await
    .unwrap();
    let mut sequencer_ids = producer.sequencer_ids().into_iter();
    let sequencer_id_1 = sequencer_ids.next().unwrap();
    let sequencer_id_2 = sequencer_ids.next().unwrap();

    // Put some data for `upc,region=west` in sequencer 1
    let lp_lines = [
        "upc,region=west user=23.2 100",
        "upc,region=west user=21.0 150",
    ];
    producer
        .store_lp(sequencer_id_1, &lp_lines.join("\n"), 0)
        .await
        .unwrap();

    // Put some data for `upc,region=east` in sequencer 2
    let lp_lines = [
        "upc,region=east user=76.2 300",
        "upc,region=east user=88.7 350",
    ];
    producer
        .store_lp(sequencer_id_2, &lp_lines.join("\n"), 0)
        .await
        .unwrap();

    let check = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            // query for the data
            let query_results = server
                .flight_client()
                .perform_query(ReadInfo {
                    namespace_name: db_name.clone(),
                    sql_query: "select * from upc".to_string(),
                })
                .await;

            if let Ok(mut results) = query_results {
                let mut batches = Vec::new();
                let mut num_rows = 0;
                while let Some(data) = results.next().await.unwrap() {
                    num_rows += data.num_rows();
                    batches.push(data);
                }

                // Since data is streamed using two partitions, only a subset of the data might be present. If that's
                // the case, ignore that record batch and try again.
                if num_rows < 4 {
                    continue;
                }

                let expected = vec![
                    "+--------+--------------------------------+------+",
                    "| region | time                           | user |",
                    "+--------+--------------------------------+------+",
                    "| east   | 1970-01-01T00:00:00.000000300Z | 76.2 |",
                    "| east   | 1970-01-01T00:00:00.000000350Z | 88.7 |",
                    "| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |",
                    "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
                    "+--------+--------------------------------+------+",
                ];
                assert_batches_sorted_eq!(&expected, &batches);
                break;
            }

            interval.tick().await;
        }
    };
    let check = tokio::time::timeout(std::time::Duration::from_secs(10), check);
    check.await.unwrap();
}

#[tokio::test]
async fn cant_write_to_db_reading_from_write_buffer() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database to read from write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            ..Default::default()
        }),
        ..Default::default()
    };

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .build(server.grpc_channel())
        .await;

    // Writing to this database is an error; all data comes from write buffer
    let mut write_client = server.write_client();
    let err = write_client
        .write_lp(&db_name, "temp,region=south color=1", 0)
        .await
        .expect_err("expected write to fail");

    assert_contains!(err.to_string(), "only allowed through write buffer");
    assert!(matches!(err, Error::InvalidArgument(_)));

    // Deleting from this database is an error; all data comes from write buffer
    let mut delete_client = server.delete_client();
    let err = delete_client
        .delete(
            &db_name,
            "temp",
            Predicate {
                range: Some(TimestampRange { start: 1, end: 2 }),
                exprs: vec![],
            },
        )
        .await
        .expect_err("expected delete to fail");

    assert_contains!(err.to_string(), "only allowed through write buffer");
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[tokio::test]
async fn test_create_database_missing_write_buffer_sequencers() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database to read from write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        ..Default::default()
    };

    let r = DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .try_build(server.grpc_channel())
        .await;

    assert_error!(r, Error::InvalidArgument(_));
}

#[tokio::test]
pub async fn test_cross_write_buffer_tracing() {
    let write_buffer_dir = TempDir::new().unwrap();

    // setup tracing
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new(ServerType::Router)
        .with_env("TRACES_EXPORTER", "jaeger")
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
        .with_client_header("jaeger-debug-id", "some-debug-id");

    // we need to use two servers but the same DB name here because the write buffer topic is named after the DB name
    let db_name = rand_name();

    // create producer server
    let server_write = ServerFixture::create_single_use_with_config(test_config.clone()).await;
    server_write
        .deployment_client()
        .update_server_id(NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    let router_cfg = wildcard_router_config(&db_name, write_buffer_dir.path());
    server_write
        .router_client()
        .update_router(router_cfg)
        .await
        .unwrap();

    // create consumer DB
    let server_read = ServerFixture::create_single_use_with_config(
        test_config.with_server_type(ServerType::Database),
    )
    .await;
    server_read
        .deployment_client()
        .update_server_id(NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    server_read.wait_server_initialized().await;
    let conn_read = WriteBufferConnection {
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    DatabaseBuilder::new(db_name.clone())
        .write_buffer(conn_read)
        .build(server_read.grpc_channel())
        .await;

    // write some points
    let mut write_client = server_write.write_client();
    let lp_lines = [
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];
    let num_lines_written = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("cannot write");
    assert_eq!(num_lines_written, 3);

    //  "shallow" packet inspection and verify the UDP server got
    //  something that had some expected results (maybe we could
    //  eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("IOx write buffer"))
        .await;
    udp_capture
        .wait_for(|m| m.to_string().contains("stored write"))
        .await;

    // // debugging assistance
    // tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await
}
