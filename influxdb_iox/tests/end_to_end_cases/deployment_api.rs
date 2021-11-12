use influxdb_iox_client::{
    management::generated_types::DatabaseRules, router::generated_types::Router, write::WriteError,
};
use tonic::Code;

use crate::common::server_fixture::{ServerFixture, ServerType};

#[tokio::test]
async fn test_serving_readiness_database() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let mut deployment_client = server_fixture.deployment_client();
    let mut mgmt_client = server_fixture.management_client();
    let mut write_client = server_fixture.write_client();

    let name = "foo";
    let lp_data = "bar baz=1 10";

    deployment_client
        .update_server_id(42)
        .await
        .expect("set ID failed");
    server_fixture.wait_server_initialized().await;
    mgmt_client
        .create_database(DatabaseRules {
            name: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    assert!(deployment_client.get_serving_readiness().await.unwrap());

    deployment_client
        .set_serving_readiness(false)
        .await
        .unwrap();
    let err = write_client.write_lp(name, lp_data, 0).await.unwrap_err();
    assert!(
        matches!(&err, WriteError::ServerError(status) if status.code() == Code::Unavailable),
        "{}",
        &err
    );

    assert!(!deployment_client.get_serving_readiness().await.unwrap());

    deployment_client.set_serving_readiness(true).await.unwrap();
    assert!(deployment_client.get_serving_readiness().await.unwrap());
    write_client.write_lp(name, lp_data, 0).await.unwrap();
}

#[tokio::test]
async fn test_serving_readiness_router() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Router).await;
    let mut deployment_client = server_fixture.deployment_client();
    let mut router_client = server_fixture.router_client();
    let mut write_client = server_fixture.write_client();

    let name = "foo";
    let lp_data = "bar baz=1 10";

    deployment_client
        .update_server_id(42)
        .await
        .expect("set ID failed");
    router_client
        .update_router(Router {
            name: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("create router failed");

    assert!(deployment_client.get_serving_readiness().await.unwrap());

    deployment_client
        .set_serving_readiness(false)
        .await
        .unwrap();
    let err = write_client.write_lp(name, lp_data, 0).await.unwrap_err();
    assert!(
        matches!(&err, WriteError::ServerError(status) if status.code() == Code::Unavailable),
        "{}",
        &err
    );

    assert!(!deployment_client.get_serving_readiness().await.unwrap());

    deployment_client.set_serving_readiness(true).await.unwrap();
    assert!(deployment_client.get_serving_readiness().await.unwrap());
    write_client.write_lp(name, lp_data, 0).await.unwrap();
}

#[tokio::test]
async fn test_set_get_writer_id_database() {
    assert_set_get_writer_id(ServerFixture::create_single_use(ServerType::Database).await).await;
}

#[tokio::test]
async fn test_set_get_writer_id_router() {
    assert_set_get_writer_id(ServerFixture::create_single_use(ServerType::Router).await).await;
}

async fn assert_set_get_writer_id(server_fixture: ServerFixture) {
    let mut client = server_fixture.deployment_client();

    const TEST_ID: u32 = 42;

    client
        .update_server_id(TEST_ID)
        .await
        .expect("set ID failed");

    let got = client.get_server_id().await.expect("get ID failed");

    assert_eq!(got.get(), TEST_ID);
}
