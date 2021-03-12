use rand::{distributions::Alphanumeric, thread_rng, Rng};

use generated_types::google::protobuf::Empty;
use generated_types::influxdata::iox::management::v1::*;

/// Return a random string suitable for use as a database name
pub fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

/// given a channel to talk with the managment api, create a new
/// database with the specified name configured with a 10MB mutable
/// buffer, partitioned on table
pub async fn create_readable_database(
    db_name: impl Into<String>,
    channel: tonic::transport::Channel,
) {
    let mut management_client = influxdb_iox_client::management::Client::new(channel);

    let rules = DatabaseRules {
        name: db_name.into(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Table(Empty {})),
            }],
        }),
        mutable_buffer_config: Some(MutableBufferConfig {
            buffer_size: 10 * 1024 * 1024,
            ..Default::default()
        }),
        ..Default::default()
    };

    management_client
        .create_database(rules.clone())
        .await
        .expect("create database failed");
}

/// given a channel to talk with the managment api, create a new
/// database with no mutable buffer configured, no partitioning rules
pub async fn create_unreadable_database(
    db_name: impl Into<String>,
    channel: tonic::transport::Channel,
) {
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
