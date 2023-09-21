pub mod common;
use common::server_fixture::ServerFixture;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

#[tokio::test]
async fn new_server_needs_onboarded() -> Result {
    let server_fixture = maybe_skip_integration!(ServerFixture::create_single_use()).await;
    let client = server_fixture.client();

    let res = client.is_onboarding_allowed().await?;
    assert!(res);

    // Creating a new setup user without first onboarding is an error
    let username = "some-user";
    let org = "some-org";
    let bucket = "some-bucket";
    let password = "some-password";
    let retention_period_hrs = 0;

    let err = client
        .post_setup_user(
            username,
            org,
            bucket,
            Some(password.to_string()),
            Some(retention_period_hrs),
            None,
        )
        .await
        .expect_err("Expected error, got success");

    assert!(matches!(
        err,
        influxdb2_client::RequestError::Http {
            status: reqwest::StatusCode::UNAUTHORIZED,
            ..
        }
    ));

    Ok(())
}

#[tokio::test]
async fn onboarding() -> Result {
    let server_fixture = maybe_skip_integration!(ServerFixture::create_single_use()).await;
    let client = server_fixture.client();

    let username = "some-user";
    let org = "some-org";
    let bucket = "some-bucket";
    let password = "some-password";
    let retention_period_hrs = 0;

    client
        .onboarding(
            username,
            org,
            bucket,
            Some(password.to_string()),
            Some(retention_period_hrs),
            None,
        )
        .await?;

    let res = client.is_onboarding_allowed().await?;
    assert!(!res);

    // Onboarding twice is an error
    let err = client
        .onboarding(
            username,
            org,
            bucket,
            Some(password.to_string()),
            Some(retention_period_hrs),
            None,
        )
        .await
        .expect_err("Expected error, got success");

    assert!(matches!(
        err,
        influxdb2_client::RequestError::Http {
            status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            ..
        }
    ));

    Ok(())
}

#[tokio::test]
async fn create_users() -> Result {
    // Using a server that has been set up
    let server_fixture = maybe_skip_integration!(ServerFixture::create_shared()).await;
    let client = server_fixture.client();

    let username = "another-user";
    let org = "another-org";
    let bucket = "another-bucket";
    let password = "another-password";
    let retention_period_hrs = 0;

    // Creating a user should work
    client
        .post_setup_user(
            username,
            org,
            bucket,
            Some(password.to_string()),
            Some(retention_period_hrs),
            None,
        )
        .await?;

    Ok(())
}
