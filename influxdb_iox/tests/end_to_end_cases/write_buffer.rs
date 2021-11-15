use crate::{
    common::{
        server_fixture::{ServerFixture, ServerType, TestConfig},
        udp_listener::UdpCapture,
    },
    end_to_end_cases::scenario::{rand_name, DatabaseBuilder},
};
use arrow_util::assert_batches_sorted_eq;
use dml::DmlOperation;
use futures::StreamExt;
use generated_types::influxdata::iox::write_buffer::v1::{
    write_buffer_connection::Direction as WriteBufferDirection, WriteBufferConnection,
};
use influxdb_iox_client::{
    management::{generated_types::WriteBufferCreationConfig, CreateDatabaseError},
    write::WriteError,
};
use std::sync::Arc;
use tempfile::TempDir;
use test_helpers::assert_contains;
use time::SystemProvider;
use write_buffer::{
    core::{WriteBufferReading, WriteBufferWriting},
    file::{FileBufferConsumer, FileBufferProducer},
};

#[tokio::test]
async fn writes_go_to_write_buffer() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database with a write buffer pointing at write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Write.into(),
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            ..Default::default()
        }),
        ..Default::default()
    };

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection.clone())
        .build(server.grpc_channel())
        .await;

    // write some points
    let mut write_client = server.write_client();

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

    // check the data is in write buffer
    let mut consumer =
        FileBufferConsumer::new(write_buffer_dir.path(), &db_name, Default::default(), None)
            .await
            .unwrap();
    let (_, mut stream) = consumer.streams().into_iter().next().unwrap();
    match stream.stream.next().await.unwrap().unwrap() {
        DmlOperation::Write(write) => assert_eq!(write.table_count(), 2),
    }
}

#[tokio::test]
async fn writes_go_to_write_buffer_whitelist() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database with a write buffer pointing at write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Write.into(),
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
        .write_buffer_table_whitelist(vec!["cpu".to_string()])
        .build(server.grpc_channel())
        .await;

    // write some points
    let mut write_client = server.write_client();

    let lp_lines = [
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
        "mem,region=east bytes=123 250",
    ];

    let num_lines_written = write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("cannot write");
    assert_eq!(num_lines_written, 4);

    // check the data is in write buffer
    let mut consumer =
        FileBufferConsumer::new(write_buffer_dir.path(), &db_name, Default::default(), None)
            .await
            .unwrap();
    let (_, mut stream) = consumer.streams().into_iter().next().unwrap();
    match stream.stream.next().await.unwrap().unwrap() {
        DmlOperation::Write(write) => assert_eq!(write.table_count(), 1),
    }
}

#[tokio::test]
async fn reads_come_from_write_buffer() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database to read from write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
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
                .perform_query(&db_name, "select * from upc")
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
        direction: WriteBufferDirection::Read.into(),
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

    assert_contains!(
        err.to_string(),
        format!(
            r#"Cannot write to database {}, it's configured to only read from the write buffer"#,
            db_name
        )
    );
    assert!(matches!(dbg!(err), WriteError::ServerError(_)));
}

#[tokio::test]
async fn test_create_database_missing_write_buffer_sequencers() {
    let write_buffer_dir = TempDir::new().unwrap();

    // set up a database to read from write buffer
    let server = ServerFixture::create_shared(ServerType::Database).await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        ..Default::default()
    };

    let err = DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .try_build(server.grpc_channel())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CreateDatabaseError::InvalidArgument { .. }),
        "{}",
        &err
    );
}

#[tokio::test]
pub async fn test_cross_write_buffer_tracing() {
    let write_buffer_dir = TempDir::new().unwrap();

    // setup tracing
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new(ServerType::Database)
        .with_env("TRACES_EXPORTER", "jaeger")
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
        .with_client_header("jaeger-debug-id", "some-debug-id");

    // we need to use two servers but the same DB name here because the write buffer topic is named after the DB name
    let db_name = rand_name();

    // create producer server
    let server_write = ServerFixture::create_single_use_with_config(test_config.clone()).await;
    server_write
        .management_client()
        .update_server_id(1)
        .await
        .unwrap();
    server_write.wait_server_initialized().await;
    let conn_write = WriteBufferConnection {
        direction: WriteBufferDirection::Write.into(),
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    DatabaseBuilder::new(db_name.clone())
        .write_buffer(conn_write.clone())
        .build(server_write.grpc_channel())
        .await;

    // create consumer DB
    let server_read = ServerFixture::create_single_use_with_config(test_config).await;
    server_read
        .management_client()
        .update_server_id(2)
        .await
        .unwrap();
    server_read.wait_server_initialized().await;
    let conn_read = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
        ..conn_write
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
