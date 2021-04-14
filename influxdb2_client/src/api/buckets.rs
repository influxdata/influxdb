//! Buckets API

use crate::models::PostBucketRequest;
use crate::{Client, Http, RequestError, ReqwestProcessing, Serializing};
use reqwest::Method;
use snafu::ResultExt;

impl Client {
    /// Create a new bucket in the organization specified by the 16-digit
    /// hexadecimal `org_id` and with the bucket name `bucket`.
    pub async fn create_bucket(
        &self,
        post_bucket_request: Option<PostBucketRequest>,
    ) -> Result<(), RequestError> {
        let create_bucket_url = format!("{}/api/v2/buckets", self.url);

        let response = self
            .request(Method::POST, &create_bucket_url)
            .body(
                serde_json::to_string(&post_bucket_request.unwrap_or_default())
                    .context(Serializing)?,
            )
            .send()
            .await
            .context(ReqwestProcessing)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.context(ReqwestProcessing)?;
            Http { status, text }.fail()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;

    #[tokio::test]
    async fn create_bucket() {
        let org_id = "0000111100001111".to_string();
        let bucket = "some-bucket".to_string();
        let token = "some-token";

        let mock_server = mock("POST", "/api/v2/buckets")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_body(
                format!(
                    r#"{{"orgID":"{}","name":"{}","retentionRules":[]}}"#,
                    org_id, bucket
                )
                .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client
            .create_bucket(Some(PostBucketRequest::new(org_id, bucket)))
            .await;

        mock_server.assert();
    }
}
