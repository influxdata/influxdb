//! Buckets API

use crate::models::PostBucketRequest;
use crate::{Client, HttpSnafu, RequestError, ReqwestProcessingSnafu, SerializingSnafu};
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
            .header("Content-Type", "application/json")
            .body(
                serde_json::to_string(&post_bucket_request.unwrap_or_default())
                    .context(SerializingSnafu)?,
            )
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.context(ReqwestProcessingSnafu)?;
            HttpSnafu { status, text }.fail()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn create_bucket() {
        let org_id = "0000111100001111".to_string();
        let bucket = "some-bucket".to_string();
        let token = "some-token";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/buckets")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                format!(r#"{{"orgID":"{org_id}","name":"{bucket}","retentionRules":[]}}"#).as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client
            .create_bucket(Some(PostBucketRequest::new(org_id, bucket)))
            .await;

        mock.assert_async().await;
    }
}
