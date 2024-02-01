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

    if status.is_success() {
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
        (
            format!("unexpected 5XX status code: {status}").into(),
            Classification::ServerErr,
        )
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
                8 => ("resource exhausted".into(), Classification::ServerErr),
                9 => ("failed precondition".into(), Classification::ClientErr),
                10 => ("aborted".into(), Classification::ClientErr),
                11 => ("out of range".into(), Classification::ClientErr),
                12 => ("unimplemented".into(), Classification::ServerErr),
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
