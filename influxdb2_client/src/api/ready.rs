//! Ready
//!
//! Check readiness of an InfluxDB instance at startup

use reqwest::{Method, StatusCode};
use snafu::ResultExt;

use crate::{Client, Http, RequestError, ReqwestProcessing};

impl Client {
    /// Get the readiness of an instance at startup
    pub async fn ready(&self) -> Result<bool, RequestError> {
        let ready_url = format!("{}/ready", self.url);
        let response = self
            .request(Method::GET, &ready_url)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(true),
            _ => {
                let status = response.status();
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn ready() -> Result {
        let token = "some-token";

        let mock_server = mock("GET", "/ready")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.ready().await;

        mock_server.assert();

        Ok(())
    }
}
