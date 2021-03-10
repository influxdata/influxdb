use std::num::NonZeroU32;

use generated_types::google::protobuf::Empty;
use generated_types::{google::protobuf::Duration, influxdata::iox::management::v1::*};
use influxdb_iox_client::management::{Client, CreateDatabaseError};

use crate::common::server_fixture::ServerFixture;

use super::util::rand_name;

#[tokio::test]
pub async fn test() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = Client::new(server_fixture.grpc_channel());

    test_list_update_remotes(&mut client).await;
    test_set_get_writer_id(&mut client).await;
    test_create_database_duplicate_name(&mut client).await;
    test_create_database_invalid_name(&mut client).await;
    test_list_databases(&mut client).await;
    test_create_get_database(&mut client).await;
}

async fn test_list_update_remotes(client: &mut Client) {
    const TEST_REMOTE_ID_1: u32 = 42;
    const TEST_REMOTE_ADDR_1: &str = "1.2.3.4:1234";
    const TEST_REMOTE_ID_2: u32 = 84;
    const TEST_REMOTE_ADDR_2: &str = "4.3.2.1:4321";
    const TEST_REMOTE_ADDR_2_UPDATED: &str = "40.30.20.10:4321";

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 0);

    client
        .update_remote(TEST_REMOTE_ID_1, TEST_REMOTE_ADDR_1)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].id, TEST_REMOTE_ID_1);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_1);
    assert_eq!(res[1].id, TEST_REMOTE_ID_2);
    assert_eq!(res[1].connection_string, TEST_REMOTE_ADDR_2);

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect("delete failed");

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect_err("expected delete to fail");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2_UPDATED)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2_UPDATED);
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
