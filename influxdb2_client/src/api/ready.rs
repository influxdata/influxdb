//! Ready
//!
//! Check readiness of an InfluxDB instance at startup

use reqwest::{Method, StatusCode};
use snafu::ResultExt;

use crate::{Client, HttpSnafu, RequestError, ReqwestProcessingSnafu};

impl Client {
    /// Get the readiness of an instance at startup
    pub async fn ready(&self) -> Result<bool, RequestError> {
        let ready_url = format!("{}/ready", self.url);
        let response = self
            .request(Method::GET, &ready_url)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(true),
            _ => {
                let status = response.status();
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
    async fn ready() {
        let mock_server = mock("GET", "/ready").create();

        let client = Client::new(mockito::server_url(), "");

        let _result = client.ready().await;

        mock_server.assert();
    }
}
