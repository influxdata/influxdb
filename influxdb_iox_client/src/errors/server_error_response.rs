use reqwest::Response;

/// A generic error message from the IOx server.
///
/// All IOx error responses are first deserialised into this type. API methods
/// that have specific error types pick out known error codes (using
/// [`ServerErrorResponse::error_code()`]) and map them to their respective
/// error instances to provide more context to the caller.
///
/// API methods without specific error types freely return this type as a
/// generic "server error" response.
#[derive(Debug, serde::Deserialize)]
pub struct ServerErrorResponse {
    error: String,
    error_code: Option<u32>,
    http_status: Option<u16>,
}

impl std::fmt::Display for ServerErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            format!(
                "{} (HTTP status {:?}, IOx error code {:?})",
                &self.error, &self.http_status, &self.error_code,
            )
            .as_str(),
        )
    }
}

impl std::error::Error for ServerErrorResponse {}

impl ServerErrorResponse {
    /// Try and parse a JSON "error" field from the [reqwest::Response].
    pub(crate) async fn from_response(r: Response) -> Self {
        let status = r.status().as_u16();
        match r.json::<ServerErrorResponse>().await {
            Ok(e) => e,
            Err(e) => Self {
                error: format!("error decoding JSON body: {}", e),
                error_code: None,
                http_status: Some(status),
            },
        }
    }

    /// Return the IOx error code in the response, if any.
    pub fn error_code(&self) -> Option<u32> {
        self.error_code
    }

    /// Return the HTTP status code sent by the server.
    pub fn http_status_code(&self) -> Option<u16> {
        self.http_status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_generic_error_response() {
        let body = serde_json::json!({"error": "something terrible", "error_code": 42}).to_string();

        let response: ServerErrorResponse = serde_json::from_str(&body).unwrap();

        assert_eq!(response.error, "something terrible");
        assert_eq!(response.error_code, Some(42));
        assert_eq!(response.http_status, None);
    }

    #[test]
    fn test_parse_generic_error_no_code() {
        let body = serde_json::json!({"error": "something terrible"}).to_string();

        let response: ServerErrorResponse = serde_json::from_str(&body).unwrap();

        assert_eq!(response.error, "something terrible");
        assert_eq!(response.error_code, None);
        assert_eq!(response.http_status, None);
    }
}
