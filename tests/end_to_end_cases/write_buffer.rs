use crate::{
    common::server_fixture::ServerFixture,
    end_to_end_cases::scenario::{create_readable_database_plus, rand_name},
};
use entry::Entry;
use generated_types::influxdata::iox::management::v1::database_rules::WriteBufferConnection;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::convert::TryFrom;

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
