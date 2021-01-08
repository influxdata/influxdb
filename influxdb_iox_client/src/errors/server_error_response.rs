use reqwest::Response;

use super::RequestError;

/// A generic error message from the IOx server.
#[derive(Debug, serde::Deserialize)]
pub struct ServerErrorResponse {
    error: Option<String>,
}

impl std::fmt::Display for ServerErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            &self
                .error
                .as_deref()
                .unwrap_or("no error message provided by server"),
        )
    }
}

impl std::error::Error for ServerErrorResponse {}

impl ServerErrorResponse {
    /// Try and parse a JSON "error" field from the response body.
    pub(crate) async fn try_from_body(r: Response) -> Result<Self, RequestError> {
        r.json::<ServerErrorResponse>()
            .await
            .map_err(|e| RequestError::HttpRequestError { source: e.into() })
    }
}
