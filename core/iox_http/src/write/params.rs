use std::sync::Arc;

use async_trait::async_trait;
use data_types::NamespaceName;
use iox_http_util::Request;
use serde::Deserialize;

use super::{multi_tenant::MultiTenantExtractError, single_tenant::SingleTenantExtractError};

#[derive(Clone, Copy, Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    #[serde(alias = "s")]
    Second,
    #[serde(alias = "ms")]
    Millisecond,
    #[serde(alias = "us", alias = "u")]
    Microsecond,
    #[serde(alias = "ns", alias = "n")]
    #[default]
    Nanosecond,
}

impl Precision {
    /// Returns the multiplier to convert to nanosecond timestamps
    pub fn timestamp_base(&self) -> i64 {
        match self {
            Self::Second => 1_000_000_000,
            Self::Millisecond => 1_000_000,
            Self::Microsecond => 1_000,
            Self::Nanosecond => 1,
        }
    }
}

#[derive(Debug)]
/// Standardized DML operation parameters
pub struct WriteParams {
    pub namespace: NamespaceName<'static>,
    pub precision: Precision,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteParseError {
    #[error("not implemented")]
    NotImplemented,

    /// An error parsing a single-tenant HTTP request.
    #[error(transparent)]
    SingleTenantError(#[from] SingleTenantExtractError),

    /// An error parsing a multi-tenant HTTP request.
    #[error(transparent)]
    MultiTenantError(#[from] MultiTenantExtractError),
}

/// A [`WriteRequestUnifier`] abstraction returns a unified [`WriteParams`]
/// from [`Request`] that conform to the [V1 Write API] or [V2 Write API].
///
/// Differing request parsing semantics and authorization are abstracted
/// through this trait (single tenant, vs multi tenant).
///
/// [V1 Write API]:
///     https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint
/// [V2 Write API]:
///     https://docs.influxdata.com/influxdb/v2.6/api/#operation/PostWrite
#[async_trait]
pub trait WriteRequestUnifier: std::fmt::Debug + Send + Sync {
    /// Perform a unifying parse to produce a [`WriteParams`] from a HTTP [`Request]`,
    /// according to the V1 Write API.
    async fn parse_v1(&self, req: &Request) -> Result<WriteParams, WriteParseError>;

    /// Perform a unifying parse to produce a [`WriteParams`] from a HTTP [`Request]`,
    /// according to the V2 Write API.
    async fn parse_v2(&self, req: &Request) -> Result<WriteParams, WriteParseError>;
}

#[async_trait]
impl<T> WriteRequestUnifier for Arc<T>
where
    T: WriteRequestUnifier,
{
    async fn parse_v1(&self, req: &Request) -> Result<WriteParams, WriteParseError> {
        (**self).parse_v1(req).await
    }

    async fn parse_v2(&self, req: &Request) -> Result<WriteParams, WriteParseError> {
        (**self).parse_v2(req).await
    }
}
