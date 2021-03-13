#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "http://localhost:8888";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    println!("{:?}", client.setup().await?);
    println!(
        "{:?}",
        client
            .setup_init(
                "some-user",
                "some-org",
                "some-bucket",
                Some("some-password".to_string()),
                Some(1),
                None
            )
            .await?
    );
    println!(
        "{:?}",
        client
            .setup_new(
                "some-new-user",
                "some-new-org",
                "some-new-bucket",
                Some("some-new-password".to_string()),
                Some(1),
                None,
            )
            .await?
    );
    Ok(())
}
