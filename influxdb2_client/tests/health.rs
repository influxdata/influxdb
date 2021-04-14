pub mod common;
use common::server_fixture::ServerFixture;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[tokio::test]
async fn get_health() -> Result {
    // Using a server that has been set up
    let server_fixture = maybe_skip_integration!(ServerFixture::create_shared()).await;
    let client = server_fixture.client();

    client.health().await?;

    Ok(())
}
