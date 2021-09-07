//! CLI to create databases.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use clap::{App, Arg};
use generated_types::influxdata::iox::management::v1::{
    self as management, database_rules::*, lifecycle_rules::*, *,
};

#[tokio::main]
async fn main() {
    let help = r#"IOx database creator

Examples:
    # Create a database named `foo_bar` with the IOx server listening at the default gRPC address:
    create_database foo_bar

    # Create a database named `myorg_mybucket` with the IOx server listening at
    # 127.0.0.1:9000:
    create_database --grpc-bind 127.0.0.1:9000 myorg_mybucket
"#;

    let matches = App::new(help)
        .about("IOx Database creation script")
        .arg(
            Arg::with_name("DATABASE_NAME")
                .help("Name of the database to create")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("WRITER")
                .long("writer")
                .help("The gRPC host and port of the IOx server that should write to Kafka")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("READER")
                .long("reader")
                .help("The gRPC host and port of the IOx server that should read from Kafka")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("KAFKA")
                .long("kafka")
                .help("The connection address of the Kafka instance")
                .takes_value(true)
                .default_value("127.0.0.1:9093"),
        )
        .get_matches();

    let db_name = matches
        .value_of("DATABASE_NAME")
        .expect("DATABASE_NAME is required")
        .to_string();
    let writer = matches.value_of("WRITER").expect("WRITER is required");
    let reader = matches.value_of("READER").expect("READER is required");
    let kafka = matches
        .value_of("KAFKA")
        .expect("KAFKA has a default value");

    // Edit these to whatever DatabaseRules you want to use
    let writer_database_rules = DatabaseRules {
        name: db_name.clone(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Time(
                    "%Y-%m-%d %H:00:00".into(),
                )),
            }],
        }),
        lifecycle_rules: Some(LifecycleRules {
            immutable: true,
            ..Default::default()
        }),
        worker_cleanup_avg_sleep: None,
        routing_rules: Some(RoutingRules::RoutingConfig(RoutingConfig {
            sink: Some(management::Sink {
                sink: Some(management::sink::Sink::Kafka(KafkaProducer {})),
            }),
        })),
        write_buffer_connection: Some(WriteBufferConnection {
            direction: write_buffer_connection::Direction::Write.into(),
            r#type: "kafka".to_string(),
            connection: kafka.to_string(),
            ..Default::default()
        }),
    };
    let reader_database_rules = DatabaseRules {
        name: db_name.clone(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Time(
                    "%Y-%m-%d %H:00:00".into(),
                )),
            }],
        }),
        lifecycle_rules: Some(LifecycleRules {
            buffer_size_soft: 1024 * 1024 * 1024,
            buffer_size_hard: 1024 * 1024 * 1024 * 2,
            worker_backoff_millis: 100,
            max_active_compactions_cfg: Some(MaxActiveCompactionsCfg::MaxActiveCompactions(1)),
            persist: true,
            persist_row_threshold: 10 * 1000 * 1000,
            ..Default::default()
        }),
        worker_cleanup_avg_sleep: None,
        routing_rules: Some(RoutingRules::RoutingConfig(RoutingConfig {
            sink: Some(management::Sink {
                sink: Some(management::sink::Sink::Kafka(KafkaProducer {})),
            }),
        })),
        write_buffer_connection: Some(WriteBufferConnection {
            direction: write_buffer_connection::Direction::Read.into(),
            r#type: "kafka".to_string(),
            connection: kafka.to_string(),
            ..Default::default()
        }),
    };

    // Create the writer db
    let writer_grpc_bind_addr = format!("http://{}", writer);
    let writer_grpc_channel = influxdb_iox_client::connection::Builder::default()
        .build(writer_grpc_bind_addr)
        .await
        .unwrap();
    let mut writer_management_client =
        influxdb_iox_client::management::Client::new(writer_grpc_channel.clone());
    writer_management_client
        .create_database(writer_database_rules)
        .await
        .expect("create writer database failed");

    // Write a few points
    let mut write_client = influxdb_iox_client::write::Client::new(writer_grpc_channel);
    let lp_lines = [
        "write_test,region=west user=23.2 100",
        "write_test,region=west user=21.0 150",
        "write_test,region=east bytes=99i 200",
    ];
    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("cannot write");
    assert_eq!(num_lines_written, 3);

    // Create the reader db
    let reader_grpc_bind_addr = format!("http://{}", reader);
    let reader_grpc_channel = influxdb_iox_client::connection::Builder::default()
        .build(reader_grpc_bind_addr)
        .await
        .unwrap();
    let mut reader_management_client =
        influxdb_iox_client::management::Client::new(reader_grpc_channel.clone());
    reader_management_client
        .create_database(reader_database_rules)
        .await
        .expect("create reader database failed");

    println!("Created database {}", db_name);
}
