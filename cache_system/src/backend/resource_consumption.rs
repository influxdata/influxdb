//! Reasoning about resource consumption of cached data.
use std::{
    fmt::Debug,
    ops::{Add, Sub},
};

/// Strongly-typed resource consumption.
///
/// Can be used to represent in-RAM memory as well as on-disc memory.
pub trait Resource:
    Add<Output = Self> + Copy + Debug + PartialOrd + Send + Sub<Output = Self> + 'static
{
    /// Create resource consumption of zero.
    fn zero() -> Self;
}

/// An estimator of [`Resource`] consumption for a given key-value pair.
pub trait ResourceEstimator: Debug + Send + Sync + 'static {
    /// Cache key.
    type K;

    /// Cached value.
    type V;

    /// Size that can be estimated.
    type S: Resource;

    /// Estimate size of given key-value pair.
    fn consumption(&self, k: &Self::K, v: &Self::V) -> Self::S;
}
