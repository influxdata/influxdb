use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "http://localhost:8888";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    println!("{:?}", client.labels().await?);
    println!("{:?}", client.labels_by_org("some-org_id").await?);
    println!("{:?}", client.find_label("some-label_id").await?);
    let mut properties = HashMap::new();
    properties.insert("some-key".to_string(), "some-value".to_string());
    println!(
        "{:?}",
        client
            .create_label("some-org_id", "some-name", Some(properties))
            .await?
    );
    println!(
        "{:?}",
        client
            .update_label(Some("some-name".to_string()), None, "some-label_id")
            .await?
    );
    println!("{:?}", client.delete_label("some-label_id").await?);
    Ok(())
}
