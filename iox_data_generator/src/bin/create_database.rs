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

use std::collections::HashMap;

use clap::{App, Arg};
use influxdb_iox_client::{
    management::generated_types::{
        lifecycle_rules, partition_template, DatabaseRules, LifecycleRules, PartitionTemplate,
    },
    router::generated_types::{
        write_sink, Matcher, MatcherToShard, Router, ShardConfig, WriteBufferConnection, WriteSink,
        WriteSinkSet,
    },
    write::generated_types::{column, Column, DatabaseBatch, TableBatch, WriteRequest},
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
            Arg::new("DATABASE_NAME")
                .help("Name of the database to create")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::new("WRITER")
                .long("writer")
                .help("The gRPC host and port of the IOx server that should write to Kafka")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::new("READER")
                .long("reader")
                .help("The gRPC host and port of the IOx server that should read from Kafka")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::new("KAFKA")
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
    let router_config = Router {
        name: db_name.clone(),
        write_sharder: Some(ShardConfig {
            specific_targets: vec![MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: String::from(".*"),
                }),
                shard: 1,
            }],
            hash_ring: None,
        }),
        write_sinks: HashMap::from([(
            1,
            WriteSinkSet {
                sinks: vec![WriteSink {
                    sink: Some(write_sink::Sink::WriteBuffer(WriteBufferConnection {
                        r#type: "kafka".to_string(),
                        connection: kafka.to_string(),
                        ..Default::default()
                    })),
                    ignore_errors: false,
                }],
            },
        )]),
        query_sinks: None,
    };
    let database_rules = DatabaseRules {
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
            max_active_compactions_cfg: Some(
                lifecycle_rules::MaxActiveCompactionsCfg::MaxActiveCompactions(1),
            ),
            persist: true,
            persist_row_threshold: 10 * 1000 * 1000,
            ..Default::default()
        }),
        worker_cleanup_avg_sleep: None,
        write_buffer_connection: Some(WriteBufferConnection {
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
    let mut writer_router_client =
        influxdb_iox_client::router::Client::new(writer_grpc_channel.clone());
    writer_router_client
        .update_router(router_config)
        .await
        .expect("create router failed");

    // Write a few points
    let mut write_client = influxdb_iox_client::write::Client::new(writer_grpc_channel);
    write_client
        .write_pb(test_write(&db_name))
        .await
        .expect("cannot write");

    // Create the reader db
    let reader_grpc_bind_addr = format!("http://{}", reader);
    let reader_grpc_channel = influxdb_iox_client::connection::Builder::default()
        .build(reader_grpc_bind_addr)
        .await
        .unwrap();
    let mut reader_management_client =
        influxdb_iox_client::management::Client::new(reader_grpc_channel.clone());
    reader_management_client
        .create_database(database_rules)
        .await
        .expect("create reader database failed");

    println!("Created database {}", db_name);
}

/// 3 rows of test data
///
/// "write_test,region=west user=23.2 100"
//  "write_test,region=west user=21.0 150"
//  "write_test,region=east bytes=99i 200"
fn test_write(db_name: &str) -> WriteRequest {
    WriteRequest {
        database_batch: Some(DatabaseBatch {
            database_name: db_name.to_string(),
            table_batches: vec![TableBatch {
                table_name: "write_test".to_string(),
                columns: vec![
                    Column {
                        column_name: "time".to_string(),
                        semantic_type: column::SemanticType::Time as _,
                        values: Some(column::Values {
                            i64_values: vec![100, 150, 200],
                            ..Default::default()
                        }),
                        null_mask: vec![],
                    },
                    Column {
                        column_name: "region".to_string(),
                        semantic_type: column::SemanticType::Tag as _,
                        values: Some(column::Values {
                            string_values: vec![
                                "west".to_string(),
                                "west".to_string(),
                                "east".to_string(),
                            ],
                            ..Default::default()
                        }),
                        null_mask: vec![],
                    },
                    Column {
                        column_name: "user".to_string(),
                        semantic_type: column::SemanticType::Field as _,
                        values: Some(column::Values {
                            f64_values: vec![23.2, 21.0],
                            ..Default::default()
                        }),
                        null_mask: vec![0b00000100],
                    },
                    Column {
                        column_name: "bytes".to_string(),
                        semantic_type: column::SemanticType::Field as _,
                        values: Some(column::Values {
                            i64_values: vec![99],
                            ..Default::default()
                        }),
                        null_mask: vec![0b00000011],
                    },
                ],
                row_count: 3,
            }],
        }),
    }
}
