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
    use mockito::Server;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn health() {
        let mut mock_server = Server::new_async().await;
        let mock = mock_server.mock("GET", "/health").create_async().await;

        let client = Client::new(mock_server.url(), "");

        let _result = client.health().await;

        mock.assert_async().await;
    }
}
