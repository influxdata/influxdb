#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "some-url";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    println!("{:?}", client.ready().await?);

    Ok(())
}
