use std::num::{NonZeroU128, NonZeroU64, ParseIntError};
use std::str::FromStr;
use std::sync::Arc;

use http::HeaderMap;
use rand::Rng;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{
    span::{Span, SpanStatus},
    TraceCollector,
};

const B3_FLAGS: &str = "X-B3-Flags";
const B3_SAMPLED_HEADER: &str = "X-B3-Sampled";
const B3_TRACE_ID_HEADER: &str = "X-B3-TraceId";
const B3_PARENT_SPAN_ID_HEADER: &str = "X-B3-ParentSpanId";
const B3_SPAN_ID_HEADER: &str = "X-B3-SpanId";

/// Error decoding SpanContext from transport representation
#[derive(Debug, Snafu)]
pub enum ContextError {
    #[snafu(display("header '{}' not found", header))]
    Missing { header: &'static str },

    #[snafu(display("header '{}' has non-UTF8 content: {}", header, source))]
    InvalidUtf8 {
        header: &'static str,
        source: http::header::ToStrError,
    },

    #[snafu(display("error decoding header '{}': {}", header, source))]
    HeaderDecodeError {
        header: &'static str,
        source: DecodeError,
    },
}

/// Error decoding a specific header value
#[derive(Debug, Snafu)]
pub enum DecodeError {
    #[snafu(display("value decode error: {}", source))]
    ValueDecodeError { source: ParseIntError },

    #[snafu(display("value cannot be 0"))]
    ZeroError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TraceId(pub NonZeroU128);

impl<'a> FromStr for TraceId {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            NonZeroU128::new(
                s.parse()
                    .map_err(|source| DecodeError::ValueDecodeError { source })?,
            )
            .ok_or(DecodeError::ZeroError)?,
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SpanId(pub NonZeroU64);

impl SpanId {
    pub fn gen() -> Self {
        // Should this be a UUID?
        Self(rand::thread_rng().gen())
    }
}

impl<'a> FromStr for SpanId {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            NonZeroU64::new(
                s.parse()
                    .map_err(|source| DecodeError::ValueDecodeError { source })?,
            )
            .ok_or(DecodeError::ZeroError)?,
        ))
    }
}

/// The immutable context of a `Span`
///
/// Importantly this contains all the information necessary to create a child `Span`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    pub trace_id: TraceId,

    pub parent_span_id: Option<SpanId>,

    pub span_id: SpanId,

    #[serde(skip)]
    pub collector: Option<Arc<dyn TraceCollector>>,
}

impl SpanContext {
    /// Creates a new child of the Span described by this TraceContext
    pub fn child<'a>(&self, name: &'a str) -> Span<'a> {
        Span {
            name,
            ctx: Self {
                trace_id: self.trace_id,
                span_id: SpanId::gen(),
                collector: self.collector.clone(),
                parent_span_id: Some(self.span_id),
            },
            start: None,
            end: None,
            status: SpanStatus::Unknown,
            metadata: Default::default(),
            events: Default::default(),
        }
    }

    /// Create a SpanContext for the trace described in the request's headers
    ///
    /// Follows the B3 multiple header encoding defined here
    /// - <https://github.com/openzipkin/b3-propagation#multiple-headers>
    pub fn from_headers(
        collector: &Arc<dyn TraceCollector>,
        headers: &HeaderMap,
    ) -> Result<Option<Self>, ContextError> {
        let debug = decoded_header(headers, B3_FLAGS)?
            .map(|header| header == "1")
            .unwrap_or(false);

        let sampled = match debug {
            // Debug implies an accept decision
            true => true,
            false => decoded_header(headers, B3_SAMPLED_HEADER)?
                .map(|value| value == "1" || value == "true")
                .unwrap_or(false),
        };

        if !sampled {
            return Ok(None);
        }

        Ok(Some(Self {
            trace_id: required_header(headers, B3_TRACE_ID_HEADER)?,
            parent_span_id: parsed_header(headers, B3_PARENT_SPAN_ID_HEADER)?,
            span_id: required_header(headers, B3_SPAN_ID_HEADER)?,
            collector: Some(Arc::clone(collector)),
        }))
    }
}

/// Decodes a given header from the provided HeaderMap to a string
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string
/// - Returns Ok(Some(_)) otherwise
fn decoded_header<'a>(
    headers: &'a HeaderMap,
    header: &'static str,
) -> Result<Option<&'a str>, ContextError> {
    headers
        .get(header)
        .map(|value| {
            value
                .to_str()
                .map_err(|source| ContextError::InvalidUtf8 { header, source })
        })
        .transpose()
}

/// Decodes and parses a given header from the provided HeaderMap
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string or fails to parse
/// - Returns Ok(Some(_)) otherwise
fn parsed_header<T: FromStr<Err = DecodeError>>(
    headers: &HeaderMap,
    header: &'static str,
) -> Result<Option<T>, ContextError> {
    decoded_header(headers, header)?
        .map(FromStr::from_str)
        .transpose()
        .map_err(|source| ContextError::HeaderDecodeError { source, header })
}

/// Decodes and parses a given required header from the provided HeaderMap
///
/// - Returns Err if the header fails to decode to a string, fails to parse, or doesn't exist
/// - Returns Ok(str) otherwise
fn required_header<T: FromStr<Err = DecodeError>>(
    headers: &HeaderMap,
    header: &'static str,
) -> Result<T, ContextError> {
    parsed_header(headers, header)?.ok_or(ContextError::Missing { header })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_decode() {
        let collector: Arc<dyn TraceCollector> = Arc::new(crate::LogTraceCollector::new());
        let mut headers = HeaderMap::new();

        // No headers should be None
        assert!(SpanContext::from_headers(&collector, &headers)
            .unwrap()
            .is_none());

        headers.insert(B3_SAMPLED_HEADER, HeaderValue::from_static("0"));

        // Not sampled
        assert!(SpanContext::from_headers(&collector, &headers)
            .unwrap()
            .is_none());

        headers.insert(B3_SAMPLED_HEADER, HeaderValue::from_static("1"));

        // Missing required headers
        assert_eq!(
            SpanContext::from_headers(&collector, &headers)
                .unwrap_err()
                .to_string(),
            "header 'X-B3-TraceId' not found"
        );

        headers.insert(B3_TRACE_ID_HEADER, HeaderValue::from_static("99999999"));
        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("69559"));

        let span = SpanContext::from_headers(&collector, &headers)
            .unwrap()
            .unwrap();

        assert_eq!(span.span_id.0.get(), 69559);
        assert_eq!(span.trace_id.0.get(), 99999999);
        assert!(span.parent_span_id.is_none());

        headers.insert(
            B3_PARENT_SPAN_ID_HEADER,
            HeaderValue::from_static("4595945"),
        );

        let span = SpanContext::from_headers(&collector, &headers)
            .unwrap()
            .unwrap();

        assert_eq!(span.span_id.0.get(), 69559);
        assert_eq!(span.trace_id.0.get(), 99999999);
        assert_eq!(span.parent_span_id.unwrap().0.get(), 4595945);

        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("not a number"));

        assert_eq!(
            SpanContext::from_headers(&collector, &headers)
                .unwrap_err()
                .to_string(),
            "error decoding header 'X-B3-SpanId': value decode error: invalid digit found in string"
        );

        headers.insert(B3_SPAN_ID_HEADER, HeaderValue::from_static("0"));

        assert_eq!(
            SpanContext::from_headers(&collector, &headers)
                .unwrap_err()
                .to_string(),
            "error decoding header 'X-B3-SpanId': value cannot be 0"
        );
    }
}
