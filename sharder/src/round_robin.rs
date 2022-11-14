use std::{cell::RefCell, fmt::Debug, sync::Arc};

use crate::Sharder;

thread_local! {
    /// A per-thread counter incremented once per call to
    /// [`RoundRobin::next()`].
    static COUNTER: RefCell<usize>  = RefCell::new(0);
}

/// A round-robin sharder (with no data locality) that arbitrarily maps to `T`
/// with an approximately uniform distribution.
///
/// # Distribution
///
/// Requests are distributed uniformly across all shards **per thread**. Given
/// enough requests (where `N` is significantly larger than the number of
/// threads) an approximately uniform distribution is achieved.
#[derive(Debug)]
pub struct RoundRobin<T> {
    shards: Vec<T>,
}

impl<T> RoundRobin<T> {
    /// Construct a new [`RoundRobin`] sharder that maps requests to each of
    /// `shards`.
    pub fn new(shards: impl IntoIterator<Item = T>) -> Self {
        Self {
            shards: shards.into_iter().collect(),
        }
    }

    /// Return the next `T` to be used.
    pub fn next(&self) -> &T {
        // Grab and increment the current counter.
        let counter = COUNTER.with(|cell| {
            let mut cell = cell.borrow_mut();
            let new_value = cell.wrapping_add(1);
            *cell = new_value;
            new_value
        });

        // Reduce it to the range of [0, N) where N is the number of shards in
        // this sharder.
        let idx = counter % self.shards.len();

        self.shards.get(idx).expect("mapped to out-of-bounds shard")
    }
}

impl<T, U> Sharder<U> for RoundRobin<Arc<T>>
where
    T: Send + Sync + Debug,
    U: Send + Sync + Debug,
{
    type Item = Arc<T>;

    fn shard(
        &self,
        _table: &str,
        _namespace: &data_types::NamespaceName<'_>,
        _payload: &U,
    ) -> Self::Item {
        Arc::clone(self.next())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    // Note this is a property test that asserts the round-robin nature of the
    // returned results, not the values themselves.
    #[test]
    fn test_round_robin() {
        // Initialise sharder with a set of 5 shards
        let shards = ["s1", "s2", "s3", "s4", "s5"];
        let sharder = RoundRobin::new(shards.iter().map(Arc::new));

        // Request the first N mappings.
        #[allow(clippy::needless_collect)] // Incorrect lint
        let mappings = (0..shards.len())
            .map(|_| sharder.next())
            .collect::<Vec<_>>();

        // Request another 100 shard mappings, and ensure the shards are
        // yielded in round-robin fashion (matching the initial shard
        // mappings)
        for want in mappings.into_iter().cycle().take(100) {
            assert_eq!(want, sharder.next());
        }
    }
}
