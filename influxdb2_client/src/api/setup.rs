//! Onboarding/Setup
//!
//! Initate and start onboarding process of InfluxDB server.

use crate::{Client, Http, RequestError, ReqwestProcessing, Serializing};
use reqwest::{Method, StatusCode};
use snafu::ResultExt;

use crate::models::{IsOnboarding, OnboardingRequest, OnboardingResponse};

impl Client {
    /// Check if database has default user, org, bucket
    pub async fn is_onboarding_allowed(&self) -> Result<bool, RequestError> {
        let setup_url = format!("{}/api/v2/setup", self.url);
        let response = self
            .request(Method::GET, &setup_url)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<IsOnboarding>()
                .await
                .context(ReqwestProcessing)?
                .allowed
                .unwrap()),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
            }
        }
    }

    /// Set up initial user, org and bucket
    pub async fn onboarding(
        &self,
        username: &str,
        org: &str,
        bucket: &str,
        password: Option<String>,
        retention_period_hrs: Option<i32>,
        retention_period_seconds: Option<i32>,
    ) -> Result<OnboardingResponse, RequestError> {
        let setup_init_url = format!("{}/api/v2/setup", self.url);

        let body = OnboardingRequest {
            username: username.into(),
            org: org.into(),
            bucket: bucket.into(),
            password,
            retention_period_hrs,
            retention_period_seconds,
        };

        let response = self
            .request(Method::POST, &setup_init_url)
            .body(serde_json::to_string(&body).context(Serializing)?)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<OnboardingResponse>()
                .await
                .context(ReqwestProcessing)?),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
            }
        }
    }

    /// Set up a new user, org and bucket
    pub async fn post_setup_user(
        &self,
        username: &str,
        org: &str,
        bucket: &str,
        password: Option<String>,
        retention_period_hrs: Option<i32>,
        retention_period_seconds: Option<i32>,
    ) -> Result<OnboardingResponse, RequestError> {
        let setup_new_url = format!("{}/api/v2/setup/user", self.url);

        let body = OnboardingRequest {
            username: username.into(),
            org: org.into(),
            bucket: bucket.into(),
            password,
            retention_period_hrs,
            retention_period_seconds,
        };

        let response = self
            .request(Method::POST, &setup_new_url)
            .body(serde_json::to_string(&body).context(Serializing)?)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<OnboardingResponse>()
                .await
                .context(ReqwestProcessing)?),
            status => {
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
    async fn is_onboarding_allowed() -> Result {
        let token = "some-token";

        let mock_server = mock("GET", "/api/v2/setup")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.is_onboarding_allowed().await;

        mock_server.assert();
        Ok(())
    }

    #[tokio::test]
    async fn onboarding() -> Result {
        let token = "some-token";
        let username = "some-user";
        let org = "some-org";
        let bucket = "some-bucket";
        let password = "some-password";
        let retention_period_hrs = 1;

        let mock_server = mock("POST", "/api/v2/setup")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"username":"{}","org":"{}","bucket":"{}","password":"{}","retentionPeriodHrs":{}}}"#,
                    username, org, bucket, password, retention_period_hrs
                ).as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client
            .onboarding(
                username,
                org,
                bucket,
                Some(password.to_string()),
                Some(retention_period_hrs),
                None,
            )
            .await;

        mock_server.assert();
        Ok(())
    }

    #[tokio::test]
    async fn post_setup_user() -> Result {
        let token = "some-token";
        let username = "some-user";
        let org = "some-org";
        let bucket = "some-bucket";
        let password = "some-password";
        let retention_period_hrs = 1;

        let mock_server = mock("POST", "/api/v2/setup/user")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"username":"{}","org":"{}","bucket":"{}","password":"{}","retentionPeriodHrs":{}}}"#,
                    username, org, bucket, password, retention_period_hrs
                ).as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client
            .post_setup_user(
                username,
                org,
                bucket,
                Some(password.to_string()),
                Some(retention_period_hrs),
                None,
            )
            .await;

        mock_server.assert();
        Ok(())
    }

    #[tokio::test]
    async fn onboarding_opt() -> Result {
        let token = "some-token";
        let username = "some-user";
        let org = "some-org";
        let bucket = "some-bucket";

        let mock_server = mock("POST", "/api/v2/setup")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"username":"{}","org":"{}","bucket":"{}"}}"#,
                    username, org, bucket,
                )
                .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client
            .onboarding(username, org, bucket, None, None, None)
            .await;

        mock_server.assert();
        Ok(())
    }

    #[tokio::test]
    async fn post_setup_user_opt() -> Result {
        let token = "some-token";
        let username = "some-user";
        let org = "some-org";
        let bucket = "some-bucket";

        let mock_server = mock("POST", "/api/v2/setup/user")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"username":"{}","org":"{}","bucket":"{}"}}"#,
                    username, org, bucket,
                )
                .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client
            .post_setup_user(username, org, bucket, None, None, None)
            .await;

        mock_server.assert();
        Ok(())
    }
}
