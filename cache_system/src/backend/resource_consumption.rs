//! Reasoning about resource consumption of cached data.
use std::{
    fmt::Debug,
    ops::{Add, Sub},
};

/// Strongly-typed resource consumption.
///
/// Can be used to represent in-RAM memory as well as on-disc memory.
pub trait Resource:
    Add<Output = Self> + Copy + Debug + Into<u64> + PartialOrd + Send + Sub<Output = Self> + 'static
{
    /// Create resource consumption of zero.
    fn zero() -> Self;

    /// Unit name.
    ///
    /// This must be a single lowercase word.
    fn unit() -> &'static str;
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

type BoxedEstimatorFn<K, V, S> = Box<dyn (Fn(&K, &V) -> S) + Send + Sync>;

/// A simple function-based [`ResourceEstimator].
pub struct FunctionEstimator<K, V, S>
where
    K: 'static,
    V: 'static,
    S: Resource,
{
    estimator: BoxedEstimatorFn<K, V, S>,
}

impl<K, V, S> FunctionEstimator<K, V, S>
where
    K: 'static,
    V: 'static,
    S: Resource,
{
    /// Create new resource estimator from given function.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&K, &V) -> S + Send + Sync + 'static,
    {
        Self {
            estimator: Box::new(f),
        }
    }
}

impl<K, V, S> std::fmt::Debug for FunctionEstimator<K, V, S>
where
    K: 'static,
    V: 'static,
    S: Resource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionEstimator").finish_non_exhaustive()
    }
}

impl<K, V, S> ResourceEstimator for FunctionEstimator<K, V, S>
where
    K: 'static,
    V: 'static,
    S: Resource,
{
    type K = K;
    type V = V;
    type S = S;

    fn consumption(&self, k: &Self::K, v: &Self::V) -> Self::S {
        (self.estimator)(k, v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_estimator() {
        let estimator =
            FunctionEstimator::new(|k: &u8, v: &u16| TestSize((*k as usize) * 10 + (*v as usize)));
        assert_eq!(estimator.consumption(&3, &2), TestSize(32));
    }

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    struct TestSize(usize);

    impl Resource for TestSize {
        fn zero() -> Self {
            Self(0)
        }

        fn unit() -> &'static str {
            "bytes"
        }
    }

    impl From<TestSize> for u64 {
        fn from(s: TestSize) -> Self {
            s.0 as Self
        }
    }

    impl Add for TestSize {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_add(rhs.0).expect("overflow"))
        }
    }

    impl Sub for TestSize {
        type Output = Self;

        fn sub(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_sub(rhs.0).expect("underflow"))
        }
    }
}
