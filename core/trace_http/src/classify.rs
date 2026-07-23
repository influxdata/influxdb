use http::StatusCode;
use std::borrow::Cow;

/// A classification of if a given request was successful
///
/// Note: the variant order defines the override order for classification
/// e.g. a request that encounters both a ClientErr and a ServerErr will
/// be recorded as a ServerErr
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Classification {
    /// Successful request
    Ok,

    /// The request was to an unrecognized path
    ///
    /// This is used by the metrics collection to avoid generating a new set of metrics
    /// for a request path that doesn't correspond to a valid route
    PathNotFound,

    /// Method was not allowed.
    MethodNotAllowed,

    /// The request was unsuccessful (4XX) but it was not the fault of the service
    ClientErr,

    /// The request was rejected because the service lacked the resources to serve
    /// it (e.g. a query exceeding the memory budget).
    ///
    /// This is neither a client nor a server error: the service is working as
    /// designed by protecting itself against an over-large request. It is tracked
    /// separately so it does not drain the server-error SLO while remaining
    /// alertable for genuine cluster-wide capacity exhaustion.
    ResourceExhausted,

    /// The request was unsuccessful (5XX) and it was the fault of the service
    ServerErr,

    /// The request produced a response that is not 2XX Ok, 4XX ClientErr or 5XX
    /// ServerErr. This is unexpected and likely shouldn't happen
    UnexpectedResponse,
}

pub(crate) fn classify_response<B>(
    response: &http::Response<B>,
) -> (Cow<'static, str>, Classification) {
    let status = response.status();

    // The HTTP specification describes NOT_MODIFIED as a redirection to the locally cached resource
    // Whilst pedantically true, for the purposes of request classification we categorise this as success
    if status.is_success() || status == StatusCode::NOT_MODIFIED {
        classify_headers(Some(response.headers()))
    } else if status.is_client_error() {
        match status {
            http::StatusCode::NOT_FOUND => ("not found".into(), Classification::PathNotFound),
            http::StatusCode::METHOD_NOT_ALLOWED => (
                "method not allowed".into(),
                Classification::MethodNotAllowed,
            ),
            _ => (
                format!("unexpected 4XX status code: {status}").into(),
                Classification::ClientErr,
            ),
        }
    } else if status.is_server_error() {
        match status {
            // While Not Implemented is technically a server error, it generally indicates that the
            // client is trying to use an unsupported feature, and we do not want these errors to
            // contribute to server error alerting.
            StatusCode::NOT_IMPLEMENTED => ("unimplemented".into(), Classification::ClientErr),
            _ => (
                format!("unexpected 5XX status code: {status}").into(),
                Classification::ServerErr,
            ),
        }
    } else {
        (
            format!("unexpected non-error status code: {status}").into(),
            Classification::UnexpectedResponse,
        )
    }
}

/// gRPC indicates failure via a [special][1] header allowing it to signal an error
/// at the end of an HTTP chunked stream as part of the [response trailer][2]
///
/// [1]: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
/// [2]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Trailer
pub(crate) fn classify_headers(
    headers: Option<&http::header::HeaderMap>,
) -> (Cow<'static, str>, Classification) {
    match headers.and_then(|headers| headers.get("grpc-status")) {
        Some(header) => {
            let value = match header.to_str() {
                Ok(value) => value,
                Err(_) => return ("grpc status not string".into(), Classification::ServerErr),
            };
            let value: i32 = match value.parse() {
                Ok(value) => value,
                Err(_) => return ("grpc status not integer".into(), Classification::ServerErr),
            };

            match value {
                0 => ("ok".into(), Classification::Ok),
                1 => ("cancelled".into(), Classification::ClientErr),
                2 => ("unknown".into(), Classification::ServerErr),
                3 => ("invalid argument".into(), Classification::ClientErr),
                4 => ("deadline exceeded".into(), Classification::ServerErr),
                5 => ("not found".into(), Classification::ClientErr),
                6 => ("already exists".into(), Classification::ClientErr),
                7 => ("permission denied".into(), Classification::ClientErr),
                8 => (
                    "resource exhausted".into(),
                    Classification::ResourceExhausted,
                ),
                9 => ("failed precondition".into(), Classification::ClientErr),
                10 => ("aborted".into(), Classification::ClientErr),
                11 => ("out of range".into(), Classification::ClientErr),
                // Treated as PathNotFound to align with HTTP 404 Not Found so that the path is not
                // included in the metrics to prevent metric cardinality explosion.
                12 => ("unimplemented".into(), Classification::PathNotFound),
                13 => ("internal".into(), Classification::ServerErr),
                14 => ("unavailable".into(), Classification::ServerErr),
                15 => ("data loss".into(), Classification::ServerErr),
                16 => ("unauthenticated".into(), Classification::ClientErr),
                _ => (
                    format!("unrecognised status code: {value}").into(),
                    Classification::ServerErr,
                ),
            }
        }
        None => ("ok".into(), Classification::Ok),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify_grpc_status(code: i32) -> Classification {
        let mut headers = http::header::HeaderMap::new();
        headers.insert("grpc-status", code.to_string().parse().unwrap());
        classify_headers(Some(&headers)).1
    }

    #[test]
    fn resource_exhausted_is_its_own_classification() {
        // A resource-exhausted query (gRPC 8) is the service protecting itself
        // against an over-large request. It must not count as a server error and
        // drain the server-error SLO, but it is also not a client error.
        assert_eq!(classify_grpc_status(8), Classification::ResourceExhausted);
    }

    #[test]
    fn grpc_status_classification() {
        assert_eq!(classify_grpc_status(0), Classification::Ok);
        assert_eq!(classify_grpc_status(3), Classification::ClientErr);
        assert_eq!(classify_grpc_status(8), Classification::ResourceExhausted);
        assert_eq!(classify_grpc_status(13), Classification::ServerErr);
        assert_eq!(classify_grpc_status(12), Classification::PathNotFound);
        assert_eq!(classify_grpc_status(16), Classification::ClientErr);
    }

    #[test]
    fn resource_exhausted_does_not_override_server_error() {
        // The variant ordering decides which classification wins when a request
        // hits more than one: a genuine server error must still dominate so it
        // remains alertable.
        assert!(Classification::ServerErr > Classification::ResourceExhausted);
        assert!(Classification::ResourceExhausted > Classification::ClientErr);
    }
}
