// This example doesn't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "some-url";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    println!("{:?}", client.health().await?);

    Ok(())
}
