use std::num::NonZeroU32;

use generated_types::google::protobuf::Empty;
use generated_types::{google::protobuf::Duration, influxdata::iox::management::v1::*};
use influxdb_iox_client::management::{Client, CreateDatabaseError};

use super::util::rand_name;

pub async fn test(client: &mut Client) {
    test_set_get_writer_id(client).await;
    test_create_database_duplicate_name(client).await;
    test_create_database_invalid_name(client).await;
    test_list_databases(client).await;
    test_create_get_database(client).await;
}

async fn test_set_get_writer_id(client: &mut Client) {
    const TEST_ID: u32 = 42;

    client
        .update_writer_id(NonZeroU32::new(TEST_ID).unwrap())
        .await
        .expect("set ID failed");

    let got = client.get_writer_id().await.expect("get ID failed");

    assert_eq!(got.get(), TEST_ID);
}

async fn test_create_database_duplicate_name(client: &mut Client) {
    let db_name = rand_name();

    client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let err = client
        .create_database(DatabaseRules {
            name: db_name,
            ..Default::default()
        })
        .await
        .expect_err("create database failed");

    assert!(matches!(
        dbg!(err),
        CreateDatabaseError::DatabaseAlreadyExists
    ))
}

async fn test_create_database_invalid_name(client: &mut Client) {
    let err = client
        .create_database(DatabaseRules {
            name: "my_example\ndb".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("expected request to fail");

    assert!(matches!(dbg!(err), CreateDatabaseError::InvalidArgument(_)));
}

async fn test_list_databases(client: &mut Client) {
    let name = rand_name();
    client
        .create_database(DatabaseRules {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let names = client
        .list_databases()
        .await
        .expect("list databases failed");
    assert!(names.contains(&name));
}

async fn test_create_get_database(client: &mut Client) {
    let db_name = rand_name();

    // Specify everything to allow direct comparison between request and response
    // Otherwise would expect difference due to server-side defaulting
    let rules = DatabaseRules {
        name: db_name.clone(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Table(Empty {})),
            }],
        }),
        wal_buffer_config: Some(WalBufferConfig {
            buffer_size: 24,
            segment_size: 2,
            buffer_rollover: wal_buffer_config::Rollover::DropIncoming as _,
            persist_segments: true,
            close_segment_after: Some(Duration {
                seconds: 324,
                nanos: 2,
            }),
        }),
        mutable_buffer_config: Some(MutableBufferConfig {
            buffer_size: 553,
            reject_if_not_persisted: true,
            partition_drop_order: Some(mutable_buffer_config::PartitionDropOrder {
                order: Order::Asc as _,
                sort: Some(
                    mutable_buffer_config::partition_drop_order::Sort::CreatedAtTime(Empty {}),
                ),
            }),
            persist_after_cold_seconds: 34,
        }),
    };

    client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    let response = client
        .get_database(db_name)
        .await
        .expect("get database failed");

    assert_eq!(response, rules);
}
