use std::num::NonZeroU32;

use influxdb_iox_client::error::Error;
use test_helpers::assert_error;

use crate::common::server_fixture::{ServerFixture, ServerType, TestConfig};

#[tokio::test]
async fn test_set_get_server_id_database() {
    assert_set_get_server_id(ServerFixture::create_single_use(ServerType::Database).await).await;
}

#[tokio::test]
async fn test_set_get_server_id_router() {
    assert_set_get_server_id(ServerFixture::create_single_use(ServerType::Router).await).await;
}

#[tokio::test]
async fn test_set_get_server_id_already_set_database() {
    let server_id = NonZeroU32::new(42).unwrap();
    let test_config = TestConfig::new(ServerType::Database).with_server_id(server_id);
    assert_set_get_server_id_already_set(
        ServerFixture::create_single_use_with_config(test_config).await,
        server_id,
    )
    .await;
}

#[tokio::test]
async fn test_set_get_server_id_already_set_router() {
    let server_id = NonZeroU32::new(42).unwrap();
    let test_config = TestConfig::new(ServerType::Router).with_server_id(server_id);
    assert_set_get_server_id_already_set(
        ServerFixture::create_single_use_with_config(test_config).await,
        server_id,
    )
    .await;
}

async fn assert_set_get_server_id(server_fixture: ServerFixture) {
    let mut client = server_fixture.deployment_client();

    let test_id = NonZeroU32::try_from(42).unwrap();

    client
        .update_server_id(test_id)
        .await
        .expect("set ID failed");

    let got = client.get_server_id().await.expect("get ID failed");
    assert_eq!(got, Some(test_id));

    // setting server ID to same ID should be OK
    client
        .update_server_id(test_id)
        .await
        .expect("set ID again failed");

    let got = client.get_server_id().await.expect("get ID failed");
    assert_eq!(got, Some(test_id));

    // setting server ID to a different ID should fail
    let result = client
        .update_server_id(NonZeroU32::try_from(13).unwrap())
        .await;
    assert_error!(result, Error::InvalidArgument(_));
}

async fn assert_set_get_server_id_already_set(
    server_fixture: ServerFixture,
    server_id: NonZeroU32,
) {
    let mut client = server_fixture.deployment_client();

    let got = client.get_server_id().await.expect("get ID failed");
    assert_eq!(got, Some(server_id));

    // setting server ID a second time should fail
    let result = client
        .update_server_id(NonZeroU32::try_from(13).unwrap())
        .await;
    assert_error!(result, Error::InvalidArgument(_));
}
