//! A optimised resolver of a partition [`SortKey`].

use std::{sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use parking_lot::Mutex;
use rand::Rng;
use schema::sort::SortKey;
use tokio::task::JoinHandle;

/// The states of a [`DeferredSortKey`] instance.
#[derive(Debug)]
enum State {
    /// The value has not yet been fetched by the background task.
    Unresolved,
    /// The value was fetched by the background task and is read to be consumed.
    Resolved(Option<SortKey>),
}

/// A resolver of [`SortKey`] from the catalog for a given partition.
///
/// This implementation combines lazy / deferred loading of the [`SortKey`] from
/// the [`Catalog`], and a background timer that pre-fetches the [`SortKey`]
/// after some random duration of time. Combined, these behaviours smear the
/// [`SortKey`] queries across the allowable time range, avoiding a large number
/// of queries from executing when multiple [`SortKey`] are needed in the system
/// at one point in time.
///
/// If the [`DeferredSortKey`] is dropped and the background task is still
/// incomplete (sleeping / actively fetching the [`SortKey`]) it is aborted
/// immediately. The background task exists once it has successfully fetched the
/// [`SortKey`].
///
/// # Stale Cached Values
///
/// This is effectively a cache that is pre-warmed in the background - this
/// necessitates that the caller can tolerate, or determine, stale values.
#[derive(Debug)]
pub(crate) struct DeferredSortKey {
    value: Arc<Mutex<State>>,
    partition_id: PartitionId,

    handle: JoinHandle<()>,

    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl DeferredSortKey {
    /// Construct a [`DeferredSortKey`] instance that fetches the [`SortKey`]
    /// for the specified `partition_id`.
    ///
    /// The background task will wait a uniformly random duration of time
    /// between `[0, max_smear)` before attempting to pre-fetch the [`SortKey`]
    /// from `catalog`.
    pub(crate) fn new(
        partition_id: PartitionId,
        max_smear: Duration,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> Self {
        // Init the value container the background thread populates.
        let value = Arc::new(Mutex::new(State::Unresolved));

        // Select random duration from a uniform distribution, up to the
        // configured maximum.
        let wait_for = rand::thread_rng().gen_range(Duration::ZERO..max_smear);

        // Spawn the background task, sleeping for the random duration of time
        // before fetching the sort key.
        let handle = tokio::spawn({
            let value = Arc::clone(&value);
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();
            async move {
                // Sleep for the random duration
                tokio::time::sleep(wait_for).await;
                // Fetch the sort key from the catalog
                let v = fetch(partition_id, &*catalog, &backoff_config).await;
                // And attempt to update the value container, if it hasn't
                // already resolved
                let mut state = value.lock();
                *state = match *state {
                    State::Unresolved => State::Resolved(v),
                    State::Resolved(_) => return,
                };
            }
        });

        Self {
            value,
            partition_id,
            handle,
            backoff_config,
            catalog,
        }
    }

    /// Read the [`SortKey`] for the partition.
    ///
    /// If the [`SortKey`] was pre-fetched in the background, it is returned
    /// immediately. If the [`SortKey`] has not yet been resolved, this call
    /// blocks while it is read from the [`Catalog`].
    ///
    /// # Concurrency
    ///
    /// If this method requires resolving the [`SortKey`], N concurrent callers
    /// will cause N queries against the catalog.
    ///
    /// # Await Safety
    ///
    /// Cancelling the future returned by calling [`Self::get()`] before
    /// completion will leave [`Self`] without a background task. The next call
    /// to [`Self::get()`] will incur a catalog query (see concurrency above).
    pub(crate) async fn get(&self) -> Option<SortKey> {
        {
            let state = self.value.lock();

            // If there is a resolved value, return it.
            if let State::Resolved(v) = &*state {
                return v.clone();
            }
        }

        // Otherwise resolve the value immediately, aborting the background
        // task.
        self.handle.abort();
        let sort_key = fetch(self.partition_id, &*self.catalog, &self.backoff_config).await;

        {
            let mut state = self.value.lock();
            *state = State::Resolved(sort_key.clone());
        }

        sort_key
    }
}

impl Drop for DeferredSortKey {
    fn drop(&mut self) {
        // Attempt to abort the background task, regardless of it having
        // completed or not.
        self.handle.abort()
    }
}

/// Fetch the [`SortKey`] from the [`Catalog`] for `partition_id`, retrying
/// endlessly when errors occur.
async fn fetch(
    partition_id: PartitionId,
    catalog: &dyn Catalog,
    backoff_config: &BackoffConfig,
) -> Option<SortKey> {
    Backoff::new(backoff_config)
        .retry_all_errors("fetch partition sort key", || async {
            let s = catalog
                .repositories()
                .await
                .partitions()
                .get_by_id(partition_id)
                .await?
                .expect("resolving sort key for non-existent partition")
                .sort_key();

            Result::<_, iox_catalog::interface::Error>::Ok(s)
        })
        .await
        .expect("retry forever")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::ShardIndex;
    use test_helpers::timeout::FutureTimeout;

    use crate::test_util::populate_catalog;

    use super::*;

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const PARTITION_KEY: &str = "platanos";

    // A test that (most likely) exercises the "read on demand" code path.
    //
    // The background task is configured to run some time between now, and
    // 10,000,000 seconds in the future - it most likely doesn't get to complete
    // before the get() call is issued.
    //
    // If this test flakes, it is POSSIBLE but UNLIKELY that the background task
    // has completed and the get() call reads a pre-fetched value.
    #[tokio::test]
    async fn test_read_demand() {
        const LONG_LONG_TIME: Duration = Duration::from_secs(10_000_000);

        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, _ns_id, table_id) =
            populate_catalog(&*catalog, SHARD_INDEX, NAMESPACE_NAME, TABLE_NAME).await;

        let partition_id = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(PARTITION_KEY.into(), shard_id, table_id)
            .await
            .expect("should create")
            .id;

        // Read the just-created sort key (None)
        let fetched = DeferredSortKey::new(
            partition_id,
            Duration::from_secs(36_000_000),
            Arc::clone(&catalog),
            backoff_config.clone(),
        )
        .get()
        .await;
        assert!(fetched.is_none());

        // Set the sort key
        let catalog_state = catalog
            .repositories()
            .await
            .partitions()
            .update_sort_key(partition_id, &["uno", "dos", "bananas"])
            .await
            .expect("should update existing partition key");

        // Read the updated sort key
        let fetched = DeferredSortKey::new(
            partition_id,
            LONG_LONG_TIME,
            Arc::clone(&catalog),
            backoff_config,
        )
        .get()
        .await;

        assert!(fetched.is_some());
        assert_eq!(fetched, catalog_state.sort_key());
    }

    // A test that deterministically exercises the "background pre-fetch" code path.
    #[tokio::test]
    async fn test_read_pre_fetched() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, _ns_id, table_id) =
            populate_catalog(&*catalog, SHARD_INDEX, NAMESPACE_NAME, TABLE_NAME).await;

        let partition_id = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(PARTITION_KEY.into(), shard_id, table_id)
            .await
            .expect("should create")
            .id;

        // Read the just-created sort key (None)
        let fetcher = DeferredSortKey::new(
            partition_id,
            Duration::from_nanos(1),
            Arc::clone(&catalog),
            backoff_config.clone(),
        );

        // Spin, waiting for the background task to show as complete.
        async {
            loop {
                if fetcher.handle.is_finished() {
                    return;
                }

                tokio::task::yield_now().await
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        assert!(fetcher.get().await.is_none());

        // Set the sort key
        let catalog_state = catalog
            .repositories()
            .await
            .partitions()
            .update_sort_key(partition_id, &["uno", "dos", "bananas"])
            .await
            .expect("should update existing partition key");

        // Read the updated sort key
        let fetcher = DeferredSortKey::new(
            partition_id,
            Duration::from_nanos(1),
            Arc::clone(&catalog),
            backoff_config.clone(),
        );

        // Spin, waiting for the background task to show as complete.
        async {
            loop {
                if fetcher.handle.is_finished() {
                    return;
                }

                tokio::task::yield_now().await
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        let fetched = fetcher.get().await;
        assert!(fetched.is_some());
        assert_eq!(fetched, catalog_state.sort_key());
    }
}
