use std::sync::Arc;

use data_types::NamespaceName;
use hyper::{Body, Request};
use serde::Deserialize;

use crate::server::http::Error;

#[derive(Clone, Debug, Deserialize)]
pub(crate) enum Precision {
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "us")]
    Microseconds,
    #[serde(rename = "ns")]
    Nanoseconds,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Nanoseconds
    }
}

impl Precision {
    /// Returns the multiplier to convert to nanosecond timestamps
    pub(crate) fn timestamp_base(&self) -> i64 {
        match self {
            Precision::Seconds => 1_000_000_000,
            Precision::Milliseconds => 1_000_000,
            Precision::Microseconds => 1_000,
            Precision::Nanoseconds => 1,
        }
    }
}

#[derive(Debug)]
/// Standardized DML operation parameters
pub struct WriteParams {
    pub(crate) namespace: NamespaceName<'static>,
    pub(crate) precision: Precision,
}

/// A [`WriteParamExtractor`] abstraction returns [`WriteParams`] from
/// [`Request`] that conform to the [V1 Write API] or [V2 Write API].
///
/// Differing request parsing semantics are abstracted through this trait
/// (single tenant, vs multi tenant).
///
/// [V1 Write API]:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]:
///     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
pub trait WriteParamExtractor: std::fmt::Debug + Send + Sync {
    /// Parse a [`WriteParams`] from a HTTP [`Request]`, according to the V1
    /// Write API.
    fn parse_v1(&self, req: &Request<Body>) -> Result<WriteParams, Error>;

    /// Parse a [`WriteParams`] from a HTTP [`Request]`, according to the V2
    /// Write API.
    fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error>;
}

impl<T> WriteParamExtractor for Arc<T>
where
    T: WriteParamExtractor,
{
    fn parse_v1(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        (**self).parse_v1(req)
    }

    fn parse_v2(&self, req: &Request<Body>) -> Result<WriteParams, Error> {
        (**self).parse_v2(req)
    }
}

#[cfg(test)]
pub mod mock {
    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub enum MockExtractorCall {
        V1,
        V2,
    }

    struct State {
        calls: Vec<MockExtractorCall>,
        ret: Box<dyn Iterator<Item = Result<WriteParams, Error>> + Send + Sync>,
    }

    impl Default for State {
        fn default() -> Self {
            Self {
                calls: Default::default(),
                ret: Box::new(std::iter::empty()),
            }
        }
    }

    impl std::fmt::Debug for State {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("State").field("calls", &self.calls).finish()
        }
    }

    #[derive(Debug, Default)]
    pub struct MockWriteParamsExtractor {
        state: Mutex<State>,
    }

    impl MockWriteParamsExtractor {
        /// Read values off of the provided iterator and return them for calls
        /// to [`Self::write()`].
        pub(crate) fn with_ret<T, U>(self, ret: T) -> Self
        where
            T: IntoIterator<IntoIter = U>,
            U: Iterator<Item = Result<WriteParams, Error>> + Send + Sync + 'static,
        {
            self.state.lock().ret = Box::new(ret.into_iter());
            self
        }

        pub(crate) fn calls(&self) -> Vec<MockExtractorCall> {
            self.state.lock().calls.clone()
        }
    }

    impl WriteParamExtractor for MockWriteParamsExtractor {
        fn parse_v1(&self, _req: &Request<Body>) -> Result<WriteParams, Error> {
            let mut guard = self.state.lock();
            guard.calls.push(MockExtractorCall::V1);
            guard.ret.next().unwrap()
        }

        fn parse_v2(&self, _req: &Request<Body>) -> Result<WriteParams, Error> {
            let mut guard = self.state.lock();
            guard.calls.push(MockExtractorCall::V2);
            guard.ret.next().unwrap()
        }
    }
}
