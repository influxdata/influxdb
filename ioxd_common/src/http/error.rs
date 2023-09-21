use hyper::{Body, Response, StatusCode};
use observability_deps::tracing::warn;

/// Constants used in API error codes.
///
/// See <https://docs.influxdata.com/influxdb/v2.1/api/#operation/PostWrite>.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(dead_code)]
pub enum HttpApiErrorCode {
    InternalError,
    NotFound,
    Conflict,
    Invalid,
    UnprocessableEntity,
    EmptyValue,
    Unavailable,
    Forbidden,
    TooManyRequests,
    Unauthorized,
    MethodNotAllowed,
    RequestTooLarge,
    UnsupportedMediaType,
}

impl HttpApiErrorCode {
    /// Get machine-readable text representation.
    fn as_text(&self) -> &'static str {
        match self {
            Self::InternalError => "internal error",
            Self::NotFound => "not found",
            Self::Conflict => "conflict",
            Self::Invalid => "invalid",
            Self::UnprocessableEntity => "unprocessable entity",
            Self::EmptyValue => "empty value",
            Self::Unavailable => "unavailable",
            Self::Forbidden => "forbidden",
            Self::TooManyRequests => "too many requests",
            Self::Unauthorized => "unauthorized",
            Self::MethodNotAllowed => "method not allowed",
            Self::RequestTooLarge => "request too large",
            Self::UnsupportedMediaType => "unsupported media type",
        }
    }

    /// Get tonic HTTP status code.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::UnprocessableEntity => StatusCode::UNPROCESSABLE_ENTITY,
            Self::EmptyValue => StatusCode::NO_CONTENT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        }
    }

    /// Check if the code is an internal server error.
    fn is_internal(&self) -> bool {
        matches!(self, Self::InternalError)
    }
}

impl From<StatusCode> for HttpApiErrorCode {
    fn from(s: StatusCode) -> Self {
        match s {
            StatusCode::INTERNAL_SERVER_ERROR => Self::InternalError,
            StatusCode::NOT_FOUND => Self::NotFound,
            StatusCode::CONFLICT => Self::Conflict,
            StatusCode::BAD_REQUEST => Self::Invalid,
            StatusCode::UNPROCESSABLE_ENTITY => Self::UnprocessableEntity,
            StatusCode::NO_CONTENT => Self::EmptyValue,
            StatusCode::SERVICE_UNAVAILABLE => Self::Unavailable,
            StatusCode::FORBIDDEN => Self::Forbidden,
            StatusCode::TOO_MANY_REQUESTS => Self::TooManyRequests,
            StatusCode::UNAUTHORIZED => Self::Unauthorized,
            StatusCode::METHOD_NOT_ALLOWED => Self::MethodNotAllowed,
            StatusCode::PAYLOAD_TOO_LARGE => Self::RequestTooLarge,
            StatusCode::UNSUPPORTED_MEDIA_TYPE => Self::UnsupportedMediaType,
            v => {
                warn!(code=%v, "returning unexpected status code as internal error");
                Self::InternalError
            }
        }
    }
}

/// Error that is compatible with the Influxdata Cloud 2 HTTP API.
///
/// See <https://docs.influxdata.com/influxdb/v2.1/api/#operation/PostWrite>.
#[derive(Debug)]
pub struct HttpApiError {
    /// Machine-readable error code.
    code: HttpApiErrorCode,

    /// Human-readable message.
    msg: String,
}

impl HttpApiError {
    /// Create new error from code and message.
    pub fn new(code: impl Into<HttpApiErrorCode>, msg: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            msg: msg.into(),
        }
    }

    /// Generate response body for this error.
    fn body(&self) -> Body {
        let json = serde_json::json!({
            "code": self.code.as_text().to_string(),
            "message": self.msg.clone(),
        })
        .to_string();

        Body::from(json)
    }

    /// Generate response for this error.
    pub fn response(&self) -> Response<Body> {
        Response::builder()
            .status(self.code.status_code())
            .header("content-type", "application/json")
            .body(self.body())
            .unwrap()
    }

    /// Check if the error is an internal server error.
    pub fn is_internal(&self) -> bool {
        self.code.is_internal()
    }
}

impl std::fmt::Display for HttpApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_text(), self.msg)
    }
}

impl std::error::Error for HttpApiError {}

/// Mixin-trait to simplify creation of [`HttpApiError`].
pub trait HttpApiErrorExt {
    /// No data can be returned, but the server was asked to do so.
    fn empty_value(&self) -> HttpApiError;

    /// Internal server error. This is a bug / misconfiguration.
    fn internal_error(&self) -> HttpApiError;

    /// Invalid/bad request.
    fn invalid(&self) -> HttpApiError;

    /// Resource was not found.
    fn not_found(&self) -> HttpApiError;
}

impl<E> HttpApiErrorExt for E
where
    E: std::error::Error,
{
    fn empty_value(&self) -> HttpApiError {
        HttpApiError::new(HttpApiErrorCode::EmptyValue, self.to_string())
    }

    fn internal_error(&self) -> HttpApiError {
        HttpApiError::new(HttpApiErrorCode::InternalError, self.to_string())
    }

    fn invalid(&self) -> HttpApiError {
        HttpApiError::new(HttpApiErrorCode::Invalid, self.to_string())
    }

    fn not_found(&self) -> HttpApiError {
        HttpApiError::new(HttpApiErrorCode::NotFound, self.to_string())
    }
}

/// An error that can be transformed into a [`HttpApiError`].
pub trait HttpApiErrorSource: std::error::Error {
    /// Create [`HttpApiError`].
    fn to_http_api_error(&self) -> HttpApiError;
}
