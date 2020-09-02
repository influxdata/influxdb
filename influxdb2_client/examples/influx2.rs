use futures::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let org = "0000000000000000";
    let bucket = "1111111111111111";
    let influx_url = "http://localhost:9999";
    let token = "my-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    let points = vec![
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .tag("region", "us-west")
            .field("value", 0.64)
            .build()?,
        influxdb2_client::DataPoint::builder("cpu_load_short")
            .tag("host", "server01")
            .field("value", 27.99)
            .build()?,
    ];

    client.write(org, bucket, stream::iter(points)).await?;

    Ok(())
}
