//! Health
//!
//! Get health of an InfluxDB instance

use crate::models::HealthCheck;
use crate::{Client, HttpSnafu, RequestError, ReqwestProcessingSnafu};
use reqwest::{Method, StatusCode};
use snafu::ResultExt;

impl Client {
    /// Get health of an instance
    pub async fn health(&self) -> Result<HealthCheck, RequestError> {
        let health_url = format!("{}/health", self.url);
        let response = self
            .request(Method::GET, &health_url)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<HealthCheck>()
                .await
                .context(ReqwestProcessingSnafu)?),
            StatusCode::SERVICE_UNAVAILABLE => Ok(response
                .json::<HealthCheck>()
                .await
                .context(ReqwestProcessingSnafu)?),
            status => {
                let text = response.text().await.context(ReqwestProcessingSnafu)?;
                HttpSnafu { status, text }.fail()?
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;

    #[tokio::test]
    async fn health() {
        let mock_server = mock("GET", "/health").create();

        let client = Client::new(mockito::server_url(), "");

        let _result = client.health().await;

        mock_server.assert();
    }
}
