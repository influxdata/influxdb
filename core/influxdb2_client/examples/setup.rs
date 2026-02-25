// This example doesn't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "http://localhost:8888";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    if client.is_onboarding_allowed().await? {
        println!(
            "{:?}",
            client
                .onboarding("some-user", "some-org", "some-bucket", None, None, None,)
                .await?
        );
    }

    println!(
        "{:?}",
        client
            .post_setup_user(
                "some-new-user",
                "some-new-org",
                "some-new-bucket",
                None,
                None,
                None,
            )
            .await?
    );

    Ok(())
}
