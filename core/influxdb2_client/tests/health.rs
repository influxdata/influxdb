// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

pub mod common;
use common::server_fixture::ServerFixture;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[tokio::test]
async fn get_health() -> Result {
    // Using a server that has been set up
    let server_fixture = maybe_skip_integration!(ServerFixture::create_shared()).await;
    let client = server_fixture.client();

    let res = client.health().await?;

    assert_eq!(res.status, influxdb2_client::models::Status::Pass);

    Ok(())
}
