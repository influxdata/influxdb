use influxdb2_client::models::{LanguageRequest, Query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let influx_url = "http://localhost:8086";
    let token = "some-token";

    let client = influxdb2_client::Client::new(influx_url, token);

    client.query_suggestions().await?;
    client.query_suggestions_name("some-name").await?;

    client
        .query_raw("some-org", Some(Query::new("some-query".to_string())))
        .await?;

    client
        .query_analyze(Some(Query::new("some-query".to_string())))
        .await?;

    client
        .query_ast(Some(LanguageRequest::new("some-query".to_string())))
        .await?;

    Ok(())
}
