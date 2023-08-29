//! Query
//!
//! Query InfluxDB using InfluxQL or Flux Query

use crate::{
    Client, HttpSnafu, RequestError, ReqwestProcessingSnafu, ResponseBytesSnafu,
    ResponseStringSnafu, SerializingSnafu,
};
use reqwest::{Method, StatusCode};
use snafu::ResultExt;

use crate::models::{
    AnalyzeQueryResponse, AstResponse, FluxSuggestion, FluxSuggestions, LanguageRequest, Query,
};

impl Client {
    /// Get Query Suggestions
    pub async fn query_suggestions(&self) -> Result<FluxSuggestions, RequestError> {
        let req_url = format!("{}/api/v2/query/suggestions", self.url);
        let response = self
            .request(Method::GET, &req_url)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<FluxSuggestions>()
                .await
                .context(ReqwestProcessingSnafu)?),
            status => {
                let text = response.text().await.context(ReqwestProcessingSnafu)?;
                HttpSnafu { status, text }.fail()?
            }
        }
    }

    /// Query Suggestions with name
    pub async fn query_suggestions_name(&self, name: &str) -> Result<FluxSuggestion, RequestError> {
        let req_url = format!(
            "{}/api/v2/query/suggestions/{name}",
            self.url,
            name = crate::common::urlencode(name),
        );

        let response = self
            .request(Method::GET, &req_url)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<FluxSuggestion>()
                .await
                .context(ReqwestProcessingSnafu)?),
            status => {
                let text = response.text().await.context(ReqwestProcessingSnafu)?;
                HttpSnafu { status, text }.fail()?
            }
        }
    }

    /// Query and return the raw string data from the server
    pub async fn query_raw(&self, org: &str, query: Option<Query>) -> Result<String, RequestError> {
        let req_url = format!("{}/api/v2/query", self.url);

        let response = self
            .request(Method::POST, &req_url)
            .header("Accepting-Encoding", "identity")
            .header("Content-Type", "application/json")
            .query(&[("org", &org)])
            .body(serde_json::to_string(&query.unwrap_or_default()).context(SerializingSnafu)?)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => {
                let bytes = response.bytes().await.context(ResponseBytesSnafu)?;
                String::from_utf8(bytes.to_vec()).context(ResponseStringSnafu)
            }
            status => {
                let text = response.text().await.context(ReqwestProcessingSnafu)?;
                HttpSnafu { status, text }.fail()?
            }
        }
    }

    /// Analyze Query
    pub async fn query_analyze(
        &self,
        query: Option<Query>,
    ) -> Result<AnalyzeQueryResponse, RequestError> {
        let req_url = format!("{}/api/v2/query/analyze", self.url);

        let response = self
            .request(Method::POST, &req_url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&query.unwrap_or_default()).context(SerializingSnafu)?)
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<AnalyzeQueryResponse>()
                .await
                .context(ReqwestProcessingSnafu)?),
            status => {
                let text = response.text().await.context(ReqwestProcessingSnafu)?;
                HttpSnafu { status, text }.fail()?
            }
        }
    }

    /// Get Query AST Repsonse
    pub async fn query_ast(
        &self,
        language_request: Option<LanguageRequest>,
    ) -> Result<AstResponse, RequestError> {
        let req_url = format!("{}/api/v2/query/ast", self.url);

        let response = self
            .request(Method::POST, &req_url)
            .header("Content-Type", "application/json")
            .body(
                serde_json::to_string(&language_request.unwrap_or_default())
                    .context(SerializingSnafu)?,
            )
            .send()
            .await
            .context(ReqwestProcessingSnafu)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<AstResponse>()
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
    use mockito::{Matcher, Server};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_suggestions() {
        let token = "some-token";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("GET", "/api/v2/query/suggestions")
            .match_header("Authorization", format!("Token {token}").as_str())
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_suggestions().await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_suggestions_name() {
        let token = "some-token";
        let suggestion_name = "some-name";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock(
                "GET",
                format!(
                    "/api/v2/query/suggestions/{name}",
                    name = crate::common::urlencode(suggestion_name)
                )
                .as_str(),
            )
            .match_header("Authorization", format!("Token {token}").as_str())
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_suggestions_name(suggestion_name).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_raw() {
        let token = "some-token";
        let org = "some-org";
        let query: Option<Query> = Some(Query::new("some-influx-query-string".to_string()));

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Accepting-Encoding", "identity")
            .match_header("Content-Type", "application/json")
            .match_query(Matcher::UrlEncoded("org".into(), org.into()))
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_raw(org, query).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_raw_opt() {
        let token = "some-token";
        let org = "some-org";
        let query: Option<Query> = None;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Accepting-Encoding", "identity")
            .match_header("Content-Type", "application/json")
            .match_query(Matcher::UrlEncoded("org".into(), org.into()))
            .match_body(
                #[allow(clippy::unnecessary_literal_unwrap)]
                serde_json::to_string(&query.unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_raw(org, None).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_analyze() {
        let token = "some-token";
        let query: Option<Query> = Some(Query::new("some-influx-query-string".to_string()));

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query/analyze")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_analyze(query).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_analyze_opt() {
        let token = "some-token";
        let query: Option<Query> = None;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query/analyze")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_analyze(query).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_ast() {
        let token = "some-token";
        let language_request: Option<LanguageRequest> =
            Some(LanguageRequest::new("some-influx-query-string".to_string()));

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query/ast")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&language_request.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_ast(language_request).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_ast_opt() {
        let token = "some-token";
        let language_request: Option<LanguageRequest> = None;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query/ast")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&language_request.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let _result = client.query_ast(language_request).await;

        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_raw_no_results() {
        let token = "some-token";
        let org = "some-org";
        let query: Option<Query> = Some(Query::new("some-influx-query-string".to_string()));

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v2/query")
            .match_header("Authorization", format!("Token {token}").as_str())
            .match_header("Accepting-Encoding", "identity")
            .match_header("Content-Type", "application/json")
            .match_query(Matcher::UrlEncoded("org".into(), org.into()))
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .with_body("")
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), token);

        let result = client.query_raw(org, query).await.expect("request success");
        assert_eq!(result, "");

        mock.assert_async().await;
    }
}
