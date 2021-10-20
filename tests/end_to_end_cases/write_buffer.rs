use crate::{
    common::{
        server_fixture::{ServerFixture, TestConfig},
        udp_listener::UdpCapture,
    },
    end_to_end_cases::scenario::{rand_name, DatabaseBuilder},
};
use arrow_util::assert_batches_sorted_eq;
use entry::{test_helpers::lp_to_entry, Entry};
use generated_types::influxdata::iox::management::v1::{
    write_buffer_connection::Direction as WriteBufferDirection, WriteBufferConnection,
};
use influxdb_iox_client::{
    management::{generated_types::WriteBufferCreationConfig, CreateDatabaseError},
    write::WriteError,
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::convert::TryFrom;
use tempfile::TempDir;
use test_helpers::assert_contains;
use write_buffer::{kafka::test_utils::kafka_sequencer_options, maybe_skip_kafka_integration};

#[tokio::test]
async fn writes_go_to_kafka() {
    let kafka_connection = maybe_skip_kafka_integration!();

    // set up a database with a write buffer pointing at kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Write.into(),
        r#type: "kafka".to_string(),
        connection: kafka_connection.to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            options: kafka_sequencer_options(),
        }),
        ..Default::default()
    };

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
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
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("cannot write");
    assert_eq!(num_lines_written, 3);

    // check the data is in kafka
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);
    cfg.set("session.timeout.ms", "6000");
    cfg.set("enable.auto.commit", "false");
    cfg.set("group.id", "placeholder");

    let consumer: StreamConsumer = cfg.create().unwrap();
    let mut topics = TopicPartitionList::new();
    topics.add_partition(&db_name, 0);
    topics
        .set_partition_offset(&db_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&topics).unwrap();

    let message = consumer.recv().await.unwrap();
    assert_eq!(message.topic(), db_name);

    let entry = Entry::try_from(message.payload().unwrap().to_vec()).unwrap();
    let partition_writes = entry.partition_writes().unwrap();
    assert_eq!(partition_writes.len(), 2);
}

#[tokio::test]
async fn writes_go_to_kafka_whitelist() {
    let kafka_connection = maybe_skip_kafka_integration!();

    // set up a database with a write buffer pointing at kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Write.into(),
        r#type: "kafka".to_string(),
        connection: kafka_connection.to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            options: kafka_sequencer_options(),
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
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("cannot write");
    assert_eq!(num_lines_written, 4);

    // check the data is in kafka
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);
    cfg.set("session.timeout.ms", "6000");
    cfg.set("enable.auto.commit", "false");
    cfg.set("group.id", "placeholder");

    let consumer: StreamConsumer = cfg.create().unwrap();
    let mut topics = TopicPartitionList::new();
    topics.add_partition(&db_name, 0);
    topics
        .set_partition_offset(&db_name, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&topics).unwrap();

    let message = consumer.recv().await.unwrap();
    assert_eq!(message.topic(), db_name);

    let entry = Entry::try_from(message.payload().unwrap().to_vec()).unwrap();
    let partition_writes = entry.partition_writes().unwrap();
    assert_eq!(partition_writes.len(), 1);
}

async fn produce_to_kafka_directly(
    producer: &FutureProducer,
    lp: &str,
    topic: &str,
    partition: Option<i32>,
) {
    let entry = lp_to_entry(lp);
    let mut record: FutureRecord<'_, String, _> = FutureRecord::to(topic).payload(entry.data());

    if let Some(pid) = partition {
        record = record.partition(pid);
    }

    producer
        .send_result(record)
        .unwrap()
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn reads_come_from_kafka() {
    let kafka_connection = maybe_skip_kafka_integration!();

    // set up a database to read from Kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
        r#type: "kafka".to_string(),
        connection: kafka_connection.to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 2,
            options: kafka_sequencer_options(),
        }),
        ..Default::default()
    };

    // Common Kafka config
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &kafka_connection);
    cfg.set("message.timeout.ms", "5000");

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .build(server.grpc_channel())
        .await;

    // put some points in Kafka
    let producer: FutureProducer = cfg.create().unwrap();

    // Kafka partitions must be configured based on the primary key because ordering across Kafka
    // partitions is undefined, so the upsert semantics would be undefined. Entries that can
    // potentially be merged must end up in the same Kafka partition. This test follows that
    // constraint, but doesn't actually encode it.

    // Put some data for `upc,region=west` in partition 0
    let lp_lines = [
        "upc,region=west user=23.2 100",
        "upc,region=west user=21.0 150",
    ];
    produce_to_kafka_directly(&producer, &lp_lines.join("\n"), &db_name, Some(0)).await;

    // Put some data for `upc,region=east` in partition 1
    let lp_lines = [
        "upc,region=east user=76.2 300",
        "upc,region=east user=88.7 350",
    ];
    produce_to_kafka_directly(&producer, &lp_lines.join("\n"), &db_name, Some(1)).await;

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
async fn cant_write_to_db_reading_from_kafka() {
    let kafka_connection = maybe_skip_kafka_integration!();

    // set up a database to read from Kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
        r#type: "kafka".to_string(),
        connection: kafka_connection.to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            options: kafka_sequencer_options(),
        }),
        ..Default::default()
    };

    DatabaseBuilder::new(db_name.clone())
        .write_buffer(write_buffer_connection)
        .build(server.grpc_channel())
        .await;

    // Writing to this database is an error; all data comes from Kafka
    let mut write_client = server.write_client();
    let err = write_client
        .write(&db_name, "temp,region=south color=1")
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
    let kafka_connection = maybe_skip_kafka_integration!();

    // set up a database to read from Kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection {
        direction: WriteBufferDirection::Read.into(),
        r#type: "kafka".to_string(),
        connection: kafka_connection.to_string(),
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
    let test_config = TestConfig::new()
        .with_env("TRACES_EXPORTER", "jaeger")
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
        .with_client_header("jaeger-debug-id", "some-debug-id");

    // we need to use two servers but the same DB name here because the Kafka topic is named after the DB name
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
            options: kafka_sequencer_options(),
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
        .write(&db_name, lp_lines.join("\n"))
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
        .wait_for(|m| m.to_string().contains("stored entry"))
        .await;

    // // debugging assistance
    // tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await
}
