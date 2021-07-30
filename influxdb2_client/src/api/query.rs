//! Query
//!
//! Query InfluxDB using InfluxQL or Flux Query

use crate::{Client, Http, RequestError, ReqwestProcessing, Serializing};
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
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<FluxSuggestions>()
                .await
                .context(ReqwestProcessing)?),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
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
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<FluxSuggestion>()
                .await
                .context(ReqwestProcessing)?),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
            }
        }
    }

    /// Query
    pub async fn query(&self, org: &str, query: Option<Query>) -> Result<String, RequestError> {
        let req_url = format!("{}/api/v2/query", self.url);

        let response = self
            .request(Method::POST, &req_url)
            .header("Accepting-Encoding", "identity")
            .header("Content-Type", "application/json")
            .query(&[("org", &org)])
            .body(serde_json::to_string(&query.unwrap_or_default()).context(Serializing)?)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response.json::<String>().await.context(ReqwestProcessing)?),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
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
            .body(serde_json::to_string(&query.unwrap_or_default()).context(Serializing)?)
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<AnalyzeQueryResponse>()
                .await
                .context(ReqwestProcessing)?),
            status => {
                let text = response.text().await.context(ReqwestProcessing)?;
                Http { status, text }.fail()?
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
                    .context(Serializing)?,
            )
            .send()
            .await
            .context(ReqwestProcessing)?;

        match response.status() {
            StatusCode::OK => Ok(response
                .json::<AstResponse>()
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
    use mockito::{mock, Matcher};

    #[tokio::test]
    async fn query_suggestions() {
        let token = "some-token";

        let mock_server = mock("GET", "/api/v2/query/suggestions")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_suggestions().await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_suggestions_name() {
        let token = "some-token";
        let suggestion_name = "some-name";

        let mock_server = mock(
            "GET",
            format!(
                "/api/v2/query/suggestions/{name}",
                name = crate::common::urlencode(suggestion_name)
            )
            .as_str(),
        )
        .match_header("Authorization", format!("Token {}", token).as_str())
        .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_suggestions_name(suggestion_name).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query() {
        let token = "some-token";
        let org = "some-org";
        let query: Option<Query> = Some(Query::new("some-influx-query-string".to_string()));
        let mock_server = mock("POST", "/api/v2/query")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Accepting-Encoding", "identity")
            .match_header("Content-Type", "application/json")
            .match_query(Matcher::UrlEncoded("org".into(), org.into()))
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query(org, query).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_opt() {
        let token = "some-token";
        let org = "some-org";
        let query: Option<Query> = None;

        let mock_server = mock("POST", "/api/v2/query")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Accepting-Encoding", "identity")
            .match_header("Content-Type", "application/json")
            .match_query(Matcher::UrlEncoded("org".into(), org.into()))
            .match_body(
                serde_json::to_string(&query.unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query(org, None).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_analyze() {
        let token = "some-token";
        let query: Option<Query> = Some(Query::new("some-influx-query-string".to_string()));
        let mock_server = mock("POST", "/api/v2/query/analyze")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_analyze(query).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_analyze_opt() {
        let token = "some-token";
        let query: Option<Query> = None;
        let mock_server = mock("POST", "/api/v2/query/analyze")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&query.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_analyze(query).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_ast() {
        let token = "some-token";
        let language_request: Option<LanguageRequest> =
            Some(LanguageRequest::new("some-influx-query-string".to_string()));
        let mock_server = mock("POST", "/api/v2/query/ast")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&language_request.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_ast(language_request).await;

        mock_server.assert();
    }

    #[tokio::test]
    async fn query_ast_opt() {
        let token = "some-token";
        let language_request: Option<LanguageRequest> = None;
        let mock_server = mock("POST", "/api/v2/query/ast")
            .match_header("Authorization", format!("Token {}", token).as_str())
            .match_header("Content-Type", "application/json")
            .match_body(
                serde_json::to_string(&language_request.clone().unwrap_or_default())
                    .unwrap()
                    .as_str(),
            )
            .create();

        let client = Client::new(&mockito::server_url(), token);

        let _result = client.query_ast(language_request).await;

        mock_server.assert();
    }
}
