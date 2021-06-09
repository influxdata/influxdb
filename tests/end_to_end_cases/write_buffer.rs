use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{
    array,
    convert::TryInto,
    time::{SystemTime, UNIX_EPOCH},
};

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
                     `docker compose -f docker/ci-kafka-docker-compose.yml up kafka`, then \
                     set KAFKA_CONNECT to the host and port where Kafka is accessible. If \
                     running the `docker compose` command and the Rust tests on the host, the \
                     value for `KAFKA_CONNECT` should be `localhost:9093`. If running the Rust \
                     tests in another container in the `docker compose` network as on CI, \
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

// This is the test I actually want to write but can't yet because the code doesn't exist
// #[tokio::test]
// async fn writes_go_to_kafka() {
//     // start up kafka
//
//     // set up a database with a write buffer pointing at kafka
//
//     // write some points
//
//     // check the data is in kafka
//
//     // stop kafka
// }

// This test validates the Kafka/Docker Compose setup and that the Rust tests can use it

#[tokio::test]
async fn can_connect_to_kafka() {
    // TODO: this should be the database name and managed by IOx
    const TOPIC: &str = "my-topic22227";
    // TODO: this should go away
    const NUM_MSGS: usize = 10;

    let kafka_connection = maybe_skip_integration!();

    // connect to kafka, produce, and consume
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);

    let admin_cfg = cfg.clone();

    let mut producer_cfg = cfg.clone();
    producer_cfg.set("message.timeout.ms", "5000");

    let mut consumer_cfg = cfg.clone();
    consumer_cfg.set("session.timeout.ms", "6000");
    consumer_cfg.set("enable.auto.commit", "false");
    consumer_cfg.set("group.id", "placeholder");

    let admin: AdminClient<DefaultClientContext> = admin_cfg.create().unwrap();
    let producer: FutureProducer = producer_cfg.create().unwrap();
    let consumer: StreamConsumer = consumer_cfg.create().unwrap();

    let topic = NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::default();
    admin.create_topics(&[topic], &opts).await.unwrap();

    let mut topics = TopicPartitionList::new();
    topics.add_partition(TOPIC, 0);
    topics
        .set_partition_offset(TOPIC, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&topics).unwrap();

    let consumer_task = tokio::spawn(async move {
        eprintln!("Consumer task starting");

        let mut counter = NUM_MSGS;

        loop {
            let p = consumer.recv().await.unwrap();
            eprintln!("Received a {:?}", p.payload().map(String::from_utf8_lossy));
            counter -= 1;
            if counter == 0 {
                break;
            }
        }
        assert_eq!(counter, 0);
        eprintln!("Exiting Consumer");
    });

    // TODO all the producing should move to server/src/write_buffer.rs
    let producer_task = tokio::spawn(async move {
        eprintln!("Producer task starting");
        for i in 0..NUM_MSGS {
            let s = format!("hello! {}", i);
            let record = FutureRecord::to(TOPIC).key(&s).payload(&s).timestamp(now());
            match producer.send_result(record) {
                Ok(x) => match x.await.unwrap() {
                    Ok((partition, offset)) => {
                        // TODO remove all the dbg
                        dbg!(&s, partition, offset);
                    }
                    Err((e, msg)) => panic!("oh no {}, {:?}", e, msg),
                },
                Err((e, msg)) => panic!("oh no {}, {:?}", e, msg),
            }
            eprintln!("Sent {}", i);
        }
        eprintln!("exiting producer");
    });

    let mut tasks: FuturesUnordered<_> =
        array::IntoIter::new([consumer_task, producer_task]).collect();

    while let Some(t) = tasks.next().await {
        t.unwrap();
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
