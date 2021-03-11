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

/// given a channel to talk with the managment api, create a new
/// database with the specified name configured with a 10MB mutable
/// buffer, partitioned on table, with some data written into two partitions
pub async fn create_two_partition_database(
    db_name: impl Into<String>,
    channel: tonic::transport::Channel,
) {
    let mut write_client = influxdb_iox_client::write::Client::new(channel.clone());

    let db_name = db_name.into();
    create_readable_database(&db_name, channel).await;

    let lp_lines = vec![
        "mem,host=foo free=27875999744i,cached=0i,available_percent=62.2 1591894320000000000",
        "cpu,host=foo running=4i,sleeping=514i,total=519i 1592894310000000000",
    ];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");
}
