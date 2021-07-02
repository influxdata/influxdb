use crate::{
    common::server_fixture::ServerFixture,
    end_to_end_cases::scenario::{create_readable_database_plus, rand_name},
};
use arrow_util::assert_batches_sorted_eq;
use entry::{test_helpers::lp_to_entry, Entry};
use generated_types::influxdata::iox::management::v1::database_rules::WriteBufferConnection;
use influxdb_iox_client::write::WriteError;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::convert::TryFrom;
use test_helpers::assert_contains;

/// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
/// caller.
///
/// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
/// guidance for setting `KAFKA_CONNECTION`.
///
/// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
macro_rules! maybe_skip_integration {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("KAFKA_CONNECT").ok(),
        ) {
            (true, Some(kafka_connection)) => kafka_connection,
            (true, None) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but \
                     KAFKA_CONNECT is not set. Please run Kafka, perhaps by using the command \
                     `docker-compose -f docker/ci-kafka-docker-compose.yml up kafka`, then \
                     set KAFKA_CONNECT to the host and port where Kafka is accessible. If \
                     running the `docker-compose` command and the Rust tests on the host, the \
                     value for `KAFKA_CONNECT` should be `localhost:9093`. If running the Rust \
                     tests in another container in the `docker-compose` network as on CI, \
                     `KAFKA_CONNECT` should be `kafka:9092`."
                )
            }
            (false, Some(_)) => {
                eprintln!("skipping Kafka integration tests - set TEST_INTEGRATION to run");
                return;
            }
            (false, None) => {
                eprintln!(
                    "skipping Kafka integration tests - set TEST_INTEGRATION and KAFKA_CONNECT to \
                    run"
                );
                return;
            }
        }
    }};
}

#[tokio::test]
async fn writes_go_to_kafka() {
    let kafka_connection = maybe_skip_integration!();

    // set up a database with a write buffer pointing at kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection::Writing(kafka_connection.to_string());
    create_readable_database_plus(&db_name, server.grpc_channel(), |mut rules| {
        rules.write_buffer_connection = Some(write_buffer_connection);
        rules
    })
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
    let kafka_connection = maybe_skip_integration!();

    // set up a database to read from Kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection::Reading(kafka_connection.to_string());
    create_readable_database_plus(&db_name, server.grpc_channel(), |mut rules| {
        rules.write_buffer_connection = Some(write_buffer_connection);
        rules
    })
    .await;

    // Common Kafka config
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);
    cfg.set("message.timeout.ms", "5000");

    // Create a partition with 2 topics in Kafka
    let num_partitions = 2;
    let admin: AdminClient<DefaultClientContext> = cfg.clone().create().unwrap();
    let topic = NewTopic::new(&db_name, num_partitions, TopicReplication::Fixed(1));
    let opts = AdminOptions::default();
    admin.create_topics(&[topic], &opts).await.unwrap();

    // put some points in Kafka
    let producer: FutureProducer = cfg.create().unwrap();

    // Put some data in partition 0
    let lp_lines = [
        "upc,region=west user=23.2 100",
        "upc,region=east user=21.0 150",
        "diskio,region=east bytes=99i 200",
    ];
    produce_to_kafka_directly(&producer, &lp_lines.join("\n"), &db_name, Some(0)).await;

    // Put some data in partition 1
    let lp_lines = [
        "upc,region=north user=76.2 300",
        "upc,region=east user=88.7 350",
        "diskio,region=east bytes=106i 400",
    ];
    produce_to_kafka_directly(&producer, &lp_lines.join("\n"), &db_name, Some(1)).await;

    // Put some data in no partition
    let lp_lines = [
        "upc,region=south user=0.2 500",
        "upc,region=east user=37.5 550",
        "diskio,region=east bytes=9i 600",
    ];
    produce_to_kafka_directly(&producer, &lp_lines.join("\n"), &db_name, None).await;

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
                while let Some(data) = results.next().await.unwrap() {
                    batches.push(data);
                }

                let expected = vec![
                    "+--------+-------------------------------+------+",
                    "| region | time                          | user |",
                    "+--------+-------------------------------+------+",
                    "| east   | 1970-01-01 00:00:00.000000150 | 21   |",
                    "| east   | 1970-01-01 00:00:00.000000350 | 88.7 |",
                    "| east   | 1970-01-01 00:00:00.000000550 | 37.5 |",
                    "| north  | 1970-01-01 00:00:00.000000300 | 76.2 |",
                    "| south  | 1970-01-01 00:00:00.000000500 | 0.2  |",
                    "| west   | 1970-01-01 00:00:00.000000100 | 23.2 |",
                    "+--------+-------------------------------+------+",
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
    let kafka_connection = maybe_skip_integration!();

    // set up a database to read from Kafka
    let server = ServerFixture::create_shared().await;
    let db_name = rand_name();
    let write_buffer_connection = WriteBufferConnection::Reading(kafka_connection.to_string());
    create_readable_database_plus(&db_name, server.grpc_channel(), |mut rules| {
        rules.write_buffer_connection = Some(write_buffer_connection);
        rules
    })
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
