//! Defines type aliases and helper functions involving `hyper`, `http`, `http_body`, and
//! `http_body_utils` for use in any other crate. Goals:
//!
//! - Reduce duplication
//! - Make upgrades of these http-related crates easier by having one place where definitions may
//!   need to be updated
//! - Make it easier to pass http-related types between crates
//!
//! This crate is lower-level than `iox_http`; this crate is meant to be more general-purpose and
//! `iox_http` is meant for services providing HTTP APIs.

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use futures::{Stream, StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::{BodyExt, Empty, Full, StreamBody};

pub use http_body::Body;

mod uri;
pub use uri::TryIntoUri;

/// Error trait object with constraints for use in async code
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The type of all request bodies. Boxed to allow us to create instances (say, in tests) because
/// `hyper::Incoming` can only be created by `hyper` and using `B: Body` instead would be annoying.
pub type RequestBody = http_body_util::combinators::BoxBody<hyper::body::Bytes, BoxError>;

/// The type of all requests.
pub type Request = hyper::Request<RequestBody>;

/// Turn any request into a boxed trait object request with boxed errors.
pub fn box_request<B>(request: hyper::Request<B>) -> Request
where
    B: Body<Data = hyper::body::Bytes> + Send + Sync + 'static,
    <B as Body>::Error: std::error::Error + Send + Sync + 'static,
{
    request.map(|body| body.map_err(|e| Box::new(e) as _).boxed())
}

/// Builder for requests. Mostly useful in tests.
pub type RequestBuilder = http::request::Builder;

/// Empty request body, properly typed.
///
/// Mostly useful when constructing test requests.
pub fn empty_request_body() -> RequestBody {
    Empty::new().map_err(|err| match err {}).boxed()
}

/// Convert something that can be converted into a [`hyper::body::Bytes`] into a [`RequestBody`]
/// that sends one chunk of bytes in full.
///
/// Mostly useful when constructing test requests.
pub fn bytes_to_request_body(bytes: impl Into<hyper::body::Bytes>) -> RequestBody {
    Full::new(bytes.into()).map_err(|err| match err {}).boxed()
}

/// The type of all response bodies. Boxed to allow for empty, full, or streaming responses with
/// any error type. Uses [`http_body_util::combinators::UnsyncBoxBody`] because this type is used
/// in some futures that need the types involved to not implement or require `Sync`.
pub type ResponseBody = http_body_util::combinators::UnsyncBoxBody<hyper::body::Bytes, BoxError>;

/// The type of all responses.
pub type Response = hyper::Response<ResponseBody>;

/// Turn any response into a boxed trait object request with boxed errors.
pub fn box_response<B>(response: hyper::Response<B>) -> Response
where
    B: Body<Data = hyper::body::Bytes> + Send + Sync + 'static,
    <B as Body>::Error: std::error::Error + Send + Sync + 'static,
{
    response.map(|b| b.map_err(|e| Box::new(e) as _).boxed_unsync())
}

/// Builder for responses.
pub type ResponseBuilder = http::response::Builder;

/// Empty response body when there's no content to return.
pub fn empty_response_body() -> ResponseBody {
    Empty::new().map_err(|err| match err {}).boxed_unsync()
}

/// Responding with one chunk of bytes. For streaming, see [`stream_bytes_to_response_body`].
pub fn bytes_to_response_body(bytes: impl Into<hyper::body::Bytes>) -> ResponseBody {
    Full::new(bytes.into())
        .map_err(|err| match err {})
        .boxed_unsync()
}

/// Responding with a stream of bytes, wrapping each frame in `Ok`.
///
/// If you have a stream of `Result`s of `Bytes`, see [`stream_results_to_response_body`].
///
/// If you don't want to stream, see [`bytes_to_response_body`].
pub fn stream_bytes_to_response_body<S, B>(stream: S) -> ResponseBody
where
    S: Stream<Item = B> + Send + 'static,
    B: Into<hyper::body::Bytes> + 'static,
{
    let stream = stream.map(Ok::<B, std::convert::Infallible>);
    stream_results_to_response_body(stream)
}

/// Responding with a stream of bytes, wrapping each frame in `Ok`.
///
/// If you have a stream of `Result`s of `Bytes`, see [`stream_results_to_response_body`].
///
/// If you don't want to stream, see [`bytes_to_response_body`].
pub fn stream_results_to_response_body<S, B, E>(stream: S) -> ResponseBody
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: Into<hyper::body::Bytes> + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let stream = stream
        .map_ok(Into::into)
        .map_ok(Frame::data)
        .map_err(|e| Box::new(e) as _);
    BodyExt::boxed_unsync(StreamBody::new(stream))
}

/// FOR TESTS ONLY: Read the full response as bytes.
///
/// # Panics
///
/// Panics if reading any frame fails! Non-test code should be processing the stream and
/// propagating errors correctly!
pub async fn read_body_bytes_for_tests(body: ResponseBody) -> hyper::body::Bytes {
    body.collect()
        .await
        .expect("failed to read response body")
        .to_bytes()
}
