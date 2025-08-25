pub(crate) mod versions;
pub use versions::v2::*;

#[derive(Debug, thiserror::Error)]
#[error("error in catalog update subscribers: {0:#}")]
pub struct SubscriptionError(#[from] anyhow::Error);

const CATALOG_SUBSCRIPTION_BUFFER_SIZE: usize = 10_000;
