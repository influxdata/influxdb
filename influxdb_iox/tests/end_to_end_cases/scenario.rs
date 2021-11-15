use std::collections::HashMap;
use std::iter::once;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::Duration;
use std::{convert::TryInto, str, u32};
use std::{sync::Arc, time::SystemTime};

use arrow::{
    array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use data_types::chunk_metadata::{ChunkStorage, ChunkSummary};
use futures::prelude::*;
use influxdb_iox_client::management::generated_types::partition_template;
use influxdb_iox_client::management::generated_types::write_buffer_connection;
use influxdb_iox_client::management::generated_types::WriteBufferConnection;
use influxdb_iox_client::management::CreateDatabaseError;
use prost::Message;
use rand::{
    distributions::{Alphanumeric, Standard},
    thread_rng, Rng,
};
use tempfile::TempDir;
use test_helpers::assert_contains;

use data_types::{names::org_and_bucket_to_database, DatabaseName};
use database_rules::RoutingRules;
use generated_types::google::protobuf::Empty;
use generated_types::{
    influxdata::iox::{
        management::v1::{self as management, *},
        write_buffer::v1::WriteBufferCreationConfig,
    },
    ReadSource, TimestampRange,
};
use influxdb_iox_client::{connection::Connection, flight::PerformQuery};
use time::SystemProvider;
use write_buffer::core::{WriteBufferReading, WriteBufferWriting};
use write_buffer::file::{FileBufferConsumer, FileBufferProducer};

use crate::common::server_fixture::{ServerFixture, ServerType, TestConfig, DEFAULT_SERVER_ID};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

/// A test fixture used for working with the influxdb v2 data model
/// (storage gRPC api and v2 write api).
///
/// Each scenario is assigned a a random org and bucket id to ensure
/// tests do not interfere with one another
#[derive(Debug)]
pub struct Scenario {
    org_id: String,
    bucket_id: String,
    ns_since_epoch: i64,
}

impl Scenario {
    /// Create a new `Scenario` with a random org_id and bucket_id
    pub fn new() -> Self {
        let ns_since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should have been after the epoch")
            .as_nanos()
            .try_into()
            .expect("Unable to represent system time");

        Self {
            ns_since_epoch,
            org_id: rand_id(),
            bucket_id: rand_id(),
        }
    }

    pub fn org_id_str(&self) -> &str {
        &self.org_id
    }

    pub fn bucket_id_str(&self) -> &str {
        &self.bucket_id
    }

    pub fn org_id(&self) -> u64 {
        u64::from_str_radix(&self.org_id, 16).unwrap()
    }

    pub fn bucket_id(&self) -> u64 {
        u64::from_str_radix(&self.bucket_id, 16).unwrap()
    }

    pub fn database_name(&self) -> DatabaseName<'_> {
        org_and_bucket_to_database(&self.org_id, &self.bucket_id).unwrap()
    }

    pub fn ns_since_epoch(&self) -> i64 {
        self.ns_since_epoch
    }

    pub fn read_source(&self) -> Option<generated_types::google::protobuf::Any> {
        let partition_id = u64::from(u32::MAX);
        let read_source = ReadSource {
            org_id: self.org_id(),
            bucket_id: self.bucket_id(),
            partition_id,
        };

        let mut d = bytes::BytesMut::new();
        read_source.encode(&mut d).unwrap();
        let read_source = generated_types::google::protobuf::Any {
            type_url: "/TODO".to_string(),
            value: d.freeze(),
        };

        Some(read_source)
    }

    pub fn timestamp_range(&self) -> Option<TimestampRange> {
        Some(TimestampRange {
            start: self.ns_since_epoch(),
            end: self.ns_since_epoch() + 10,
        })
    }

    /// Creates the database on the server for this scenario
    pub async fn create_database(&self, client: &mut influxdb_iox_client::management::Client) {
        client
            .create_database(DatabaseRules {
                name: self.database_name().to_string(),
                lifecycle_rules: Some(Default::default()),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    pub async fn load_data(&self, influxdb2: &influxdb2_client::Client) -> Vec<String> {
        // TODO: make a more extensible way to manage data for tests, such as in
        // external fixture files or with factories.
        let points = vec![
            influxdb2_client::models::DataPoint::builder("cpu_load_short")
                .tag("host", "server01")
                .tag("region", "us-west")
                .field("value", 0.64)
                .timestamp(self.ns_since_epoch())
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("cpu_load_short")
                .tag("host", "server01")
                .field("value", 27.99)
                .timestamp(self.ns_since_epoch() + 1)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("cpu_load_short")
                .tag("host", "server02")
                .tag("region", "us-west")
                .field("value", 3.89)
                .timestamp(self.ns_since_epoch() + 2)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("cpu_load_short")
                .tag("host", "server01")
                .tag("region", "us-east")
                .field("value", 1234567.891011)
                .timestamp(self.ns_since_epoch() + 3)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("cpu_load_short")
                .tag("host", "server01")
                .tag("region", "us-west")
                .field("value", 0.000003)
                .timestamp(self.ns_since_epoch() + 4)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("system")
                .tag("host", "server03")
                .field("uptime", 1303385)
                .timestamp(self.ns_since_epoch() + 5)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("swap")
                .tag("host", "server01")
                .tag("name", "disk0")
                .field("in", 3)
                .field("out", 4)
                .timestamp(self.ns_since_epoch() + 6)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("status")
                .field("active", true)
                .timestamp(self.ns_since_epoch() + 7)
                .build()
                .unwrap(),
            influxdb2_client::models::DataPoint::builder("attributes")
                .field("color", "blue")
                .timestamp(self.ns_since_epoch() + 8)
                .build()
                .unwrap(),
        ];
        self.write_data(influxdb2, points).await.unwrap();

        let host_array = StringArray::from(vec![
            Some("server01"),
            Some("server01"),
            Some("server02"),
            Some("server01"),
            Some("server01"),
        ]);
        let region_array = StringArray::from(vec![
            Some("us-west"),
            None,
            Some("us-west"),
            Some("us-east"),
            Some("us-west"),
        ]);
        let time_array = TimestampNanosecondArray::from_vec(
            vec![
                self.ns_since_epoch,
                self.ns_since_epoch + 1,
                self.ns_since_epoch + 2,
                self.ns_since_epoch + 3,
                self.ns_since_epoch + 4,
            ],
            None,
        );
        let value_array = Float64Array::from(vec![0.64, 27.99, 3.89, 1234567.891011, 0.000003]);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("host", Arc::new(host_array) as ArrayRef, true),
            ("region", Arc::new(region_array), true),
            ("time", Arc::new(time_array), true),
            ("value", Arc::new(value_array), true),
        ])
        .unwrap();

        arrow_util::display::pretty_format_batches(&[batch])
            .unwrap()
            .trim()
            .split('\n')
            .map(|s| s.to_string())
            .collect()
    }

    async fn write_data(
        &self,
        client: &influxdb2_client::Client,
        points: Vec<influxdb2_client::models::DataPoint>,
    ) -> Result<()> {
        client
            .write(
                self.org_id_str(),
                self.bucket_id_str(),
                stream::iter(points),
            )
            .await?;
        Ok(())
    }
}

/// substitutes "ns" --> ns_since_epoch, ns1-->ns_since_epoch+1, etc
pub fn substitute_nanos(ns_since_epoch: i64, lines: &[&str]) -> Vec<String> {
    let substitutions = vec![
        ("ns0", format!("{}", ns_since_epoch)),
        ("ns1", format!("{}", ns_since_epoch + 1)),
        ("ns2", format!("{}", ns_since_epoch + 2)),
        ("ns3", format!("{}", ns_since_epoch + 3)),
        ("ns4", format!("{}", ns_since_epoch + 4)),
        ("ns5", format!("{}", ns_since_epoch + 5)),
        ("ns6", format!("{}", ns_since_epoch + 6)),
    ];

    lines
        .iter()
        .map(|line| {
            let mut line = line.to_string();
            for (from, to) in &substitutions {
                line = line.replace(from, to);
            }
            line
        })
        .collect()
}

/// Return a random string suitable for use as a database name
pub fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

// return a random 16 digit string comprised of numbers suitable for
// use as a influxdb2 org_id or bucket_id
pub fn rand_id() -> String {
    thread_rng()
        .sample_iter(&Standard)
        .filter_map(|c: u8| {
            if c.is_ascii_digit() {
                Some(char::from(c))
            } else {
                // discard if out of range
                None
            }
        })
        .take(16)
        .collect()
}

pub struct DatabaseBuilder {
    name: String,
    partition_template: PartitionTemplate,
    lifecycle_rules: LifecycleRules,
    write_buffer: Option<WriteBufferConnection>,
    table_whitelist: Option<Vec<String>>,
}

impl DatabaseBuilder {
    pub fn new(name: String) -> Self {
        Self {
            name,
            partition_template: PartitionTemplate {
                parts: vec![partition_template::Part {
                    part: Some(partition_template::part::Part::Table(Empty {})),
                }],
            },
            lifecycle_rules: LifecycleRules {
                buffer_size_soft: 512 * 1024,       // 512K
                buffer_size_hard: 10 * 1024 * 1024, // 10MB
                worker_backoff_millis: 100,
                ..Default::default()
            },
            write_buffer: None,
            table_whitelist: None,
        }
    }

    pub fn buffer_size_hard(mut self, buffer_size_hard: u64) -> Self {
        self.lifecycle_rules.buffer_size_hard = buffer_size_hard;
        self
    }

    pub fn buffer_size_soft(mut self, buffer_size_soft: u64) -> Self {
        self.lifecycle_rules.buffer_size_soft = buffer_size_soft;
        self
    }

    pub fn persist(mut self, persist: bool) -> Self {
        self.lifecycle_rules.persist = persist;
        self
    }

    pub fn mub_row_threshold(mut self, threshold: u64) -> Self {
        self.lifecycle_rules.mub_row_threshold = threshold;
        self
    }

    pub fn persist_age_threshold_seconds(mut self, threshold: u32) -> Self {
        self.lifecycle_rules.persist_age_threshold_seconds = threshold;
        self
    }

    pub fn persist_row_threshold(mut self, threshold: u64) -> Self {
        self.lifecycle_rules.persist_row_threshold = threshold;
        self
    }

    pub fn late_arrive_window_seconds(mut self, late_arrive_window_seconds: u32) -> Self {
        self.lifecycle_rules.late_arrive_window_seconds = late_arrive_window_seconds;
        self
    }

    pub fn write_buffer(mut self, write_buffer: WriteBufferConnection) -> Self {
        self.write_buffer = Some(write_buffer);
        self
    }

    pub fn write_buffer_table_whitelist(mut self, whitelist: Vec<String>) -> Self {
        self.table_whitelist = Some(whitelist);
        self
    }

    pub fn worker_backoff_millis(mut self, millis: u64) -> Self {
        self.lifecycle_rules.worker_backoff_millis = millis;
        self
    }

    // Build a database
    pub async fn try_build(self, channel: Connection) -> Result<(), CreateDatabaseError> {
        let mut management_client = influxdb_iox_client::management::Client::new(channel);

        let routing_rules = if self.write_buffer.is_some() {
            const KAFKA_PRODUCER_SINK_ID: u32 = 0;
            let kafka_producer_sink = management::Sink {
                sink: Some(management::sink::Sink::Kafka(KafkaProducer {})),
            };
            const DEV_NULL_SINK_ID: u32 = 1;
            let dev_null_sink = management::Sink {
                sink: Some(management::sink::Sink::DevNull(DevNull {})),
            };

            let to_shard = |shard: u32| {
                Box::new(move |i: String| MatcherToShard {
                    matcher: Some(Matcher {
                        table_name_regex: format!("^{}$", i),
                    }),
                    shard,
                })
            };

            if let Some(table_whitelist) = self.table_whitelist {
                Some(RoutingRules::ShardConfig(ShardConfig {
                    specific_targets: table_whitelist
                        .into_iter()
                        .map(to_shard(KAFKA_PRODUCER_SINK_ID))
                        .chain(once(to_shard(DEV_NULL_SINK_ID)(".*".to_string())))
                        .collect(),
                    shards: vec![
                        (KAFKA_PRODUCER_SINK_ID, kafka_producer_sink),
                        (DEV_NULL_SINK_ID, dev_null_sink),
                    ]
                    .into_iter()
                    .collect(),
                    ..Default::default()
                }))
            } else {
                Some(RoutingRules::RoutingConfig(RoutingConfig {
                    sink: Some(management::Sink {
                        sink: Some(management::sink::Sink::Kafka(KafkaProducer {})),
                    }),
                }))
            }
        } else {
            None
        };

        management_client
            .create_database(DatabaseRules {
                name: self.name,
                partition_template: Some(self.partition_template),
                lifecycle_rules: Some(self.lifecycle_rules),
                worker_cleanup_avg_sleep: None,
                routing_rules,
                write_buffer_connection: self.write_buffer,
            })
            .await?;
        Ok(())
    }

    // Build a database
    pub async fn build(self, channel: Connection) {
        self.try_build(channel)
            .await
            .expect("create database failed");
    }
}

/// given a channel to talk with the management api, create a new
/// database with the specified name configured with a 10MB mutable
/// buffer, partitioned on table
pub async fn create_readable_database(db_name: impl Into<String>, channel: Connection) {
    DatabaseBuilder::new(db_name.into()).build(channel).await
}

/// given a channel to talk with the management api, create a new
/// database with no mutable buffer configured, no partitioning rules
pub async fn create_unreadable_database(db_name: impl Into<String>, channel: Connection) {
    let mut management_client = influxdb_iox_client::management::Client::new(channel);

    let rules = DatabaseRules {
        name: db_name.into(),
        ..Default::default()
    };

    management_client
        .create_database(rules.clone())
        .await
        .expect("create database failed");
}

/// given a channel to talk with the management api, create a new
/// database with the specified name configured with a 10MB mutable
/// buffer, partitioned on table, with some data written into two partitions
pub async fn create_two_partition_database(db_name: impl Into<String>, channel: Connection) {
    let mut write_client = influxdb_iox_client::write::Client::new(channel.clone());

    let db_name = db_name.into();
    create_readable_database(&db_name, channel).await;

    let lp_lines = vec![
        "mem,host=foo free=27875999744i,cached=0i,available_percent=62.2 1591894320000000000",
        "cpu,host=foo running=4i,sleeping=514i,total=519i 1592894310000000000",
    ];

    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("write succeded");
}

/// Collect the results of a query into a vector of record batches
pub async fn collect_query(mut query_results: PerformQuery) -> Vec<RecordBatch> {
    let mut batches = vec![];
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }
    batches
}

/// Wait for the chunks to be in exactly `desired_storages` states
pub async fn wait_for_exact_chunk_states(
    fixture: &ServerFixture,
    db_name: &str,
    mut desired_storages: Vec<ChunkStorage>,
    wait_time: std::time::Duration,
) -> Vec<ChunkSummary> {
    // ensure consistent order
    desired_storages.sort();

    let fail_message = format!("persisted chunks in exactly {:?}", desired_storages);
    let pred = |chunks: &[ChunkSummary]| {
        let actual_storages = chunks.iter().map(|chunk| chunk.storage).collect::<Vec<_>>();

        desired_storages == actual_storages
    };
    wait_for_state(fixture, db_name, pred, fail_message, wait_time).await
}

/// Wait for the predicate to pass
async fn wait_for_state<P>(
    fixture: &ServerFixture,
    db_name: &str,
    mut pred: P,
    fail_message: String,
    wait_time: std::time::Duration,
) -> Vec<ChunkSummary>
where
    P: FnMut(&[ChunkSummary]) -> bool,
{
    let t_start = std::time::Instant::now();

    loop {
        let chunks = list_chunks(fixture, db_name).await;

        if pred(&chunks) {
            return chunks;
        }

        // Log the current status of the chunks
        for chunk in &chunks {
            println!(
                "{:?}: chunk {} partition {} storage: {:?} row_count: {} time_of_last_write: {:?}",
                (t_start.elapsed()),
                chunk.id,
                chunk.partition_key,
                chunk.storage,
                chunk.row_count,
                chunk.time_of_last_write
            );
        }

        if t_start.elapsed() >= wait_time {
            let mut operations = fixture.operations_client().list_operations().await.unwrap();
            operations.sort_by(|a, b| a.operation.name.cmp(&b.operation.name));

            panic!(
                "Could not find {} within {:?}.\nChunks were: {:#?}\nOperations were: {:#?}",
                fail_message, wait_time, chunks, operations
            )
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Gets the list of ChunkSummaries from the server
pub async fn list_chunks(fixture: &ServerFixture, db_name: &str) -> Vec<ChunkSummary> {
    let mut management_client = fixture.management_client();
    let chunks = management_client.list_chunks(db_name).await.unwrap();
    let mut chunks: Vec<ChunkSummary> = chunks.into_iter().map(|c| c.try_into().unwrap()).collect();
    chunks.sort_by_key(|summary| {
        (
            Arc::clone(&summary.table_name),
            Arc::clone(&summary.partition_key),
            summary.id,
        )
    });
    chunks
}

/// Creates a database with a broken catalog
pub async fn fixture_broken_catalog(db_name: &str) -> ServerFixture {
    let server_id = DEFAULT_SERVER_ID;

    let test_config =
        TestConfig::new(ServerType::Database).with_env("INFLUXDB_IOX_WIPE_CATALOG_ON_ERROR", "no");

    let fixture = ServerFixture::create_single_use_with_config(test_config).await;
    fixture
        .management_client()
        .update_server_id(server_id)
        .await
        .unwrap();
    fixture.wait_server_initialized().await;

    //
    // Create database with corrupted catalog
    //

    let uuid = fixture
        .management_client()
        .create_database(DatabaseRules {
            name: db_name.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut path = fixture.dir().to_path_buf();
    path.push("dbs");
    path.push(uuid.to_string());

    path.push("transactions");
    path.push("00000000000000000001");
    std::fs::create_dir(path.clone()).unwrap();

    path.push("48eb9059-ca73-45e1-b6b4-e1f47d6159fb.txn");
    std::fs::write(path, "INVALID").unwrap();

    //
    // Try to load broken catalog and error
    //

    let fixture = fixture.restart_server().await;

    let status = fixture.wait_server_initialized().await;
    assert_eq!(status.database_statuses.len(), 1);

    let load_error = &status.database_statuses[0].error.as_ref().unwrap().message;
    assert_contains!(
        load_error,
        "error loading catalog: Cannot load preserved catalog"
    );

    fixture
}

/// Creates a database that cannot be replayed
pub async fn fixture_replay_broken(db_name: &str, write_buffer_path: &Path) -> ServerFixture {
    let server_id = DEFAULT_SERVER_ID;

    let test_config =
        TestConfig::new(ServerType::Database).with_env("INFLUXDB_IOX_SKIP_REPLAY", "no");

    let fixture = ServerFixture::create_single_use_with_config(test_config).await;
    fixture
        .management_client()
        .update_server_id(server_id)
        .await
        .unwrap();
    fixture.wait_server_initialized().await;

    // Create database
    fixture
        .management_client()
        .create_database(DatabaseRules {
            name: db_name.to_string(),
            write_buffer_connection: Some(WriteBufferConnection {
                direction: write_buffer_connection::Direction::Read.into(),
                r#type: "file".to_string(),
                connection: write_buffer_path.display().to_string(),
                creation_config: Some(WriteBufferCreationConfig {
                    n_sequencers: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            partition_template: Some(PartitionTemplate {
                parts: vec![partition_template::Part {
                    part: Some(partition_template::part::Part::Column(
                        "partition_by".to_string(),
                    )),
                }],
            }),
            lifecycle_rules: Some(LifecycleRules {
                persist: true,
                late_arrive_window_seconds: 1,
                persist_age_threshold_seconds: 3600,
                persist_row_threshold: 2,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    // ingest data as mixed throughput
    let time_provider = Arc::new(SystemProvider::new());
    let producer = FileBufferProducer::new(
        write_buffer_path,
        db_name,
        Default::default(),
        time_provider,
    )
    .await
    .unwrap();
    let sequencer_id = producer.sequencer_ids().into_iter().next().unwrap();
    let meta1 = producer
        .store_lp(sequencer_id, "table_1,partition_by=a foo=1 10", 0)
        .await
        .unwrap();
    let meta2 = producer
        .store_lp(sequencer_id, "table_1,partition_by=b foo=2 20", 0)
        .await
        .unwrap();
    let meta3 = producer
        .store_lp(sequencer_id, "table_1,partition_by=b foo=3 30", 0)
        .await
        .unwrap();

    // wait for ingest, compaction and persistence
    wait_for_exact_chunk_states(
        &fixture,
        db_name,
        vec![
            // that's the single entry from partition a
            ChunkStorage::ReadBuffer,
            // these are the two entries from partition b that got persisted due to the row limit
            ChunkStorage::ReadBufferAndObjectStore,
        ],
        Duration::from_secs(10),
    )
    .await;

    // add new entry to the end
    producer
        .store_lp(sequencer_id, "table_1,partition_by=c foo=4 40", 0)
        .await
        .unwrap();

    // purge data from write buffer
    write_buffer::file::test_utils::remove_entry(
        write_buffer_path,
        db_name,
        sequencer_id,
        meta1.sequence().unwrap().number,
    )
    .await;
    write_buffer::file::test_utils::remove_entry(
        write_buffer_path,
        db_name,
        sequencer_id,
        meta2.sequence().unwrap().number,
    )
    .await;
    write_buffer::file::test_utils::remove_entry(
        write_buffer_path,
        db_name,
        sequencer_id,
        meta3.sequence().unwrap().number,
    )
    .await;

    // Try to replay and error
    let fixture = fixture.restart_server().await;

    let status = fixture.wait_server_initialized().await;
    assert_eq!(status.database_statuses.len(), 1);

    let load_error = &status.database_statuses[0].error.as_ref().unwrap().message;
    assert_contains!(load_error, "error during replay: Cannot replay");

    fixture
}

pub async fn create_router_to_write_buffer(
    fixture: &ServerFixture,
    db_name: &str,
) -> (TempDir, Box<dyn WriteBufferReading>) {
    use influxdb_iox_client::router::generated_types::{
        write_sink::Sink, Matcher, MatcherToShard, Router, ShardConfig, WriteSink, WriteSinkSet,
    };

    let write_buffer_dir = TempDir::new().unwrap();

    let write_buffer_connection = WriteBufferConnection {
        direction: write_buffer_connection::Direction::Write.into(),
        r#type: "file".to_string(),
        connection: write_buffer_dir.path().display().to_string(),
        creation_config: Some(WriteBufferCreationConfig {
            n_sequencers: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let router_cfg = Router {
        name: db_name.to_string(),
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
                    ignore_errors: false,
                    sink: Some(Sink::WriteBuffer(write_buffer_connection)),
                }],
            },
        )]),
        query_sinks: Default::default(),
    };
    fixture
        .router_client()
        .update_router(router_cfg)
        .await
        .unwrap();

    let write_buffer: Box<dyn WriteBufferReading> = Box::new(
        FileBufferConsumer::new(
            write_buffer_dir.path(),
            db_name,
            Some(&data_types::write_buffer::WriteBufferCreationConfig {
                n_sequencers: NonZeroU32::new(1).unwrap(),
                ..Default::default()
            }),
            None,
        )
        .await
        .unwrap(),
    );

    (write_buffer_dir, write_buffer)
}
