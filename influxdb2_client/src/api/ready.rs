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
    use mockito::Server;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn ready() {
        let mut mock_server = Server::new_async().await;
        let mock = mock_server.mock("GET", "/ready").create_async().await;

        let client = Client::new(mock_server.url(), "");

        let _result = client.ready().await;

        mock.assert_async().await;
    }
}
