//! Reasoning about resource consumption of cached data.
use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Add, Sub},
};

/// Strongly-typed resource consumption.
///
/// Can be used to represent in-RAM memory as well as on-disc memory.
pub trait Resource:
    Add<Output = Self>
    + Copy
    + Debug
    + Into<u64>
    + Ord
    + PartialOrd
    + Send
    + Sync
    + Sub<Output = Self>
    + 'static
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

/// A simple function-based [`ResourceEstimator].
///
/// # Typing
/// Semantically this wrapper has only one degree of freedom: `F`, which is the estimator function. However until
/// [`fn_traits`] are stable, there is no way to extract the parameters and return value from a function via associated
/// types. So we need to add additional type parametes for the special `Fn(...) -> ...` handling.
///
/// It is likely that `F` will be a closure, e.g.:
///
/// ```
/// use cache_system::resource_consumption::{
///     FunctionEstimator,
///     test_util::TestSize,
/// };
///
/// let my_estimator = FunctionEstimator::new(|_k: &u8, v: &String| -> TestSize {
///     TestSize(std::mem::size_of::<(u8, String)>() + v.capacity())
/// });
/// ```
///
/// There is no way to spell out the exact type of `my_estimator` in the above example, because  the closure has an
/// anonymous type. If you need the type signature of [`FunctionEstimator`], you have to
/// [erase the type](https://en.wikipedia.org/wiki/Type_erasure) by putting the [`FunctionEstimator`] it into a [`Box`],
/// e.g.:
///
/// ```
/// use cache_system::resource_consumption::{
///     FunctionEstimator,
///     ResourceEstimator,
///     test_util::TestSize,
/// };
///
/// let my_estimator = FunctionEstimator::new(|_k: &u8, v: &String| -> TestSize {
///     TestSize(std::mem::size_of::<(u8, String)>() + v.capacity())
/// });
/// let my_estimator: Box<dyn ResourceEstimator<K = u8, V = String, S = TestSize>> = Box::new(my_estimator);
/// ```
///
///
/// [`fn_traits`]: https://doc.rust-lang.org/beta/unstable-book/library-features/fn-traits.html
pub struct FunctionEstimator<F, K, V, S>
where
    F: Fn(&K, &V) -> S + Send + Sync + 'static,
    K: 'static,
    V: 'static,
    S: Resource,
{
    estimator: F,
    _phantom: PhantomData<dyn Fn() -> (K, V, S) + Send + Sync + 'static>,
}

impl<F, K, V, S> FunctionEstimator<F, K, V, S>
where
    F: Fn(&K, &V) -> S + Send + Sync + 'static,
    K: 'static,
    V: 'static,
    S: Resource,
{
    /// Create new resource estimator from given function.
    pub fn new(f: F) -> Self {
        Self {
            estimator: f,
            _phantom: PhantomData,
        }
    }
}

impl<F, K, V, S> std::fmt::Debug for FunctionEstimator<F, K, V, S>
where
    F: Fn(&K, &V) -> S + Send + Sync + 'static,
    K: 'static,
    V: 'static,
    S: Resource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionEstimator").finish_non_exhaustive()
    }
}

impl<F, K, V, S> ResourceEstimator for FunctionEstimator<F, K, V, S>
where
    F: Fn(&K, &V) -> S + Send + Sync + 'static,
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

pub mod test_util {
    //! Helpers to test resource consumption-based algorithms.
    use super::*;

    /// Simple resource type for testing.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct TestSize(pub usize);

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

#[cfg(test)]
mod tests {
    use crate::resource_consumption::test_util::TestSize;

    use super::*;

    #[test]
    fn test_function_estimator() {
        let estimator =
            FunctionEstimator::new(|k: &u8, v: &u16| TestSize((*k as usize) * 10 + (*v as usize)));
        assert_eq!(estimator.consumption(&3, &2), TestSize(32));
    }
}
