use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_channel::Sender;
use moka::future::{Cache, FutureExt};
use moka::notification::ListenerFuture;
use moka::Expiry;
use object_store::ObjectMeta;
use observability_deps::tracing::error;
use parking_lot::Mutex;
use tokio::task::JoinSet;

use crate::data_types::{ObjectParams, PolicyConfig};
use crate::server::error::Error;

type ExternalRequestKey = String;
type CacheManagerKey = Arc<String>;

#[derive(Debug, Clone)]
pub struct CacheManagerValue {
    /// Required for eviction policy.
    pub params: ObjectParams,
    /// Returned on `GET /metadata` head requests.
    pub metadata: ObjectMeta,
}

type InMemoryCache = Cache<CacheManagerKey, CacheManagerValue>;

/// Manages the cache eviction policy.
///
/// Cache manager built upon a fast, concurrent in-memory cache.
/// In-memory will be the keys, as well as minimum metadata for managing cache eviction.
#[derive(Debug)]
pub struct CacheManager {
    /// High-concurrency in-memory cache, used for the eviction policy.
    manager: Arc<InMemoryCache>,
    /// Current size of the cache.
    current_size: Arc<AtomicU64>,
}

impl CacheManager {
    pub fn new(config: PolicyConfig, evict_tx: Sender<String>) -> Self {
        let current_size = Arc::new(AtomicU64::new(0));

        // listener => then evict from local store
        let current_size_ = Arc::clone(&current_size);
        let listener =
            move |k: Arc<CacheManagerKey>, v: CacheManagerValue, _cause| -> ListenerFuture {
                let evict_tx = evict_tx.clone();
                let current_size = Arc::clone(&current_size_);
                async move {
                    // use async_channel to ensure evicted, before removing from current_size
                    match evict_tx.send((**k).clone()).await {
                        Ok(_) => {
                            current_size
                                .fetch_sub(v.params.file_size_bytes as u64, AtomicOrdering::SeqCst);
                        }
                        Err(e) => {
                            error!("CacheManager eviction listener failed to send: {:?}", e);
                        }
                    }
                }
                .boxed()
            };

        // event-recency
        let evicter = Arc::new(Evictor::new_with_placeholder_cache_ref());
        let expiry = Arc::new(EventRecency::new(
            Arc::clone(&current_size),
            config.max_capacity,
            Arc::clone(&evicter),
        ));

        // cache manager
        let manager = Arc::new(
            Cache::builder()
                .max_capacity(config.max_capacity)
                .weigher(Self::size_weigher) // triggers eviction
                .expire_after(EntryExpiry::new(config, Arc::clone(&expiry))) // triggered on insert & read
                .async_eviction_listener(listener)
                .build(),
        );

        // set cache on evicter
        evicter.set_cache(Arc::clone(&manager));

        Self {
            manager,
            current_size,
        }
    }

    /// Maps the max_capacity to the disk bytes.
    fn size_weigher(_k: &CacheManagerKey, v: &CacheManagerValue) -> u32 {
        v.params.file_size_bytes as u32
    }

    /// Inserts the key-value pair into the cache.
    pub async fn insert(&self, k: ExternalRequestKey, v: CacheManagerValue) {
        let size = v.params.file_size_bytes;
        self.manager.entry(Arc::new(k)).or_insert(v).await;
        self.current_size
            .fetch_add(size as u64, AtomicOrdering::SeqCst);
    }

    /// Returns Ok if the key is in the cache.
    pub async fn in_cache(&self, k: &ExternalRequestKey) -> Result<(), Error> {
        self.manager
            .get(k)
            .await
            .map(|_| ())
            .ok_or(Error::CacheMiss)
    }

    /// Returns the metadata for the object.
    pub async fn fetch_metadata(&self, k: &ExternalRequestKey) -> Result<ObjectMeta, Error> {
        Ok(self.manager.get(k).await.ok_or(Error::CacheMiss)?.metadata)
    }

    /// Explicitly evict key from cache.
    #[cfg(test)]
    async fn invalidate(&self, k: ExternalRequestKey) {
        self.manager.invalidate(&k).await;
    }

    /// Trigger moka to flush all pending tasks. Use for testing ONLY.
    #[cfg(test)]
    pub(crate) async fn flush_pending(&self) {
        self.manager.run_pending_tasks().await;
    }
}

#[derive(Clone)]
pub struct EntryExpiry {
    /// Outer bound on how long to hold.
    max_recency_duration: Duration,
    /// Handles event recency.
    event_recency: Arc<EventRecency>,
}

impl EntryExpiry {
    fn new(config: PolicyConfig, evicter: Arc<EventRecency>) -> Self {
        Self {
            max_recency_duration: Duration::from_nanos(
                config.event_recency_max_duration_nanoseconds,
            ),
            event_recency: evicter,
        }
    }
}

/// Moka helps achieve high concurrency with buffered inserts.
///
/// When pending tasks are applied, if more space is needed then existing keys are flushed
/// based upon expiration.
impl Expiry<CacheManagerKey, CacheManagerValue> for EntryExpiry {
    /// Sets the expiry duration for every insertion.
    /// If incoming should not be inserted, then set expiry to 0.
    fn expire_after_create(
        &self,
        k: &CacheManagerKey,
        v: &CacheManagerValue,
        _inserted_at: Instant,
    ) -> Option<Duration> {
        if !self.event_recency.should_insert(k, v) {
            return Some(Duration::from_secs(0));
        }

        if let Some(now) = chrono::Utc::now().timestamp_nanos_opt() {
            let event_timestamp_nanos = v.params.max_time;

            let age_out_nanoseconds =
                event_timestamp_nanos.saturating_add(self.max_recency_duration.as_nanos() as i64);
            let duration_until_event_ages_out = age_out_nanoseconds.saturating_sub(now);

            Some(Duration::from_nanos(duration_until_event_ages_out as u64))
        } else {
            None
        }
    }
}

/// Tracks the event time recency, and evicts based upon the event time.
struct EventRecency {
    /// Current size of the cache.
    ///
    /// Used to determine when to evict.
    /// Does not rely upon the moka-buffered inserts (unlike [`Cache`].weighted_size()).
    current_size: Arc<AtomicU64>,
    /// Upper bound on cache size.
    max_capacity: u64,

    /// Min-heap, used to track event time recency.
    min_heap: Arc<Mutex<BinaryHeap<Slot>>>,
    /// Tracks the current min, which will be updated with store() to minimize lock contention.
    current_min: Arc<AtomicU64>,
    /// Handles updates to min-heap on separate threads, to avoid locking on the hot path.
    background_tasks: JoinSet<()>,
    insert_tx: tokio::sync::mpsc::UnboundedSender<Slot>,
    remove_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl EventRecency {
    /// Creates a new [`EventRecency`].
    fn new(current_size: Arc<AtomicU64>, max_capacity: u64, evictor: Arc<Evictor>) -> Self {
        let min_heap: Arc<Mutex<BinaryHeap<Slot>>> = Default::default();
        let current_min: Arc<AtomicU64> = Default::default();

        // TODO: replace with bounded channels.
        let (insert_tx, mut insert_rx) = tokio::sync::mpsc::unbounded_channel();
        let (remove_tx, mut remove_rx) = tokio::sync::mpsc::unbounded_channel();

        // insert into min-heap, off the hot path
        let mut background_tasks = JoinSet::new();
        let min_heap_ = Arc::clone(&min_heap);
        background_tasks.spawn(async move {
            loop {
                if let Some(slot) = insert_rx.recv().await {
                    let mut guard = min_heap_.lock();
                    guard.push(slot);
                    drop(guard);
                }
            }
        });

        // remove from min-heap, off the hot path
        let min_heap_ = Arc::clone(&min_heap);
        let current_min_ = Arc::clone(&current_min);
        background_tasks.spawn(async move {
            loop {
                if remove_rx.recv().await.is_some() {
                    let mut guard = min_heap_.lock();
                    let to_evict = guard.pop().expect("should have entry via peek");
                    let new_min = guard.peek().map(|slot| slot.max_time);
                    drop(guard);

                    if let Some(new_min) = new_min {
                        current_min_.store(new_min as u64, AtomicOrdering::Release);
                    }

                    evictor.evict_from_cache(to_evict.key);
                }
            }
        });

        Self {
            current_size,
            max_capacity,
            min_heap,
            current_min,
            background_tasks,
            insert_tx,
            remove_tx,
        }
    }

    /// Returns true if the incoming entry should be inserted.
    fn should_insert(&self, incoming_k: &CacheManagerKey, incoming_v: &CacheManagerValue) -> bool {
        let incoming_size = incoming_v.params.file_size_bytes as u64;
        let should_insert =
            if self.current_size.load(AtomicOrdering::SeqCst) + incoming_size > self.max_capacity {
                self.max_capacity_should_insert(incoming_v)
            } else {
                true
            };

        if should_insert {
            self.insert_tx
                .send(Slot {
                    max_time: incoming_v.params.max_time,
                    key: Arc::clone(incoming_k),
                })
                .expect("should send min heap insert");
        }
        should_insert
    }

    /// Returns true if incoming entry should be inserted.
    ///
    /// Handles the case where the cache is at max_capacity,
    /// by either evicting based upon event time recency,
    /// or rejecting the incoming entry.
    fn max_capacity_should_insert(&self, incoming_v: &CacheManagerValue) -> bool {
        match self
            .current_min
            .load(AtomicOrdering::Relaxed)
            .partial_cmp(&(incoming_v.params.max_time as u64))
        {
            Some(Ordering::Less) | Some(Ordering::Equal) => {
                // incoming event time is more recent than current min
                // therefore, evict current min
                let _ = self.remove_tx.send(());
                true
            }
            Some(Ordering::Greater) => false, // incoming event time is older than current min
            None => true,                     // no entries in min-heap
        }
    }
}

/// Slot in the min-heap, used to evict based upon event timestamp recency.
///
/// [`BinaryHeap`] is a max-heap, therefore the Ord implementation is reversed.
#[derive(Debug, Eq, PartialEq)]
struct Slot {
    max_time: i64,
    key: CacheManagerKey,
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.max_time.partial_cmp(&self.max_time)
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Does the eviction.
#[derive(Debug)]
struct Evictor {
    /// Ref to cache, in order to evict.
    cache_manager: ArcSwap<InMemoryCache>,
}

impl Evictor {
    /// Creates a new [`Evictor`], with a placeholder cache ref.
    fn new_with_placeholder_cache_ref() -> Self {
        Self {
            cache_manager: ArcSwap::new(Arc::new(Cache::new(0))),
        }
    }

    /// Sets the cache ref.
    fn set_cache(&self, cache: Arc<InMemoryCache>) {
        self.cache_manager.store(cache);
    }

    /// Evicts the key from the cache.
    ///
    /// Must be a non-blocking downstream action from [`EntryExpiry`].
    ///
    /// [`Cache`].invalidate() provides immediate invalidation of the entry,
    /// outside of any pending moka insert tasks.
    ///
    /// When pending moka insert tasks are applied, if max_capacity is reached
    /// then existing keys are flushed based upon expiration.
    /// As we are spawning a non-blocking thread, we are not guaranteed
    /// to have this eviction occur prior to the flushing of the task queue.
    ///
    /// Worst case scenario is that an incoming key is rejected (not accepted into cache) due to space.
    fn evict_from_cache(&self, key: CacheManagerKey) {
        let guard = self.cache_manager.load();
        let cache = guard.as_ref().clone();
        tokio::spawn(async move {
            cache.invalidate(&key).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use async_channel::unbounded;

    use crate::data_types::PolicyConfig;

    use super::*;

    fn now_nanos() -> i64 {
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    }

    fn cache_manager_value(size: usize, max_time: Option<i64>) -> CacheManagerValue {
        let max_time = max_time.unwrap_or(now_nanos());

        CacheManagerValue {
            params: ObjectParams {
                file_size_bytes: size as i64,
                max_time,
                min_time: max_time - 1_000_000_000,
                ..Default::default()
            },
            metadata: ObjectMeta {
                last_modified: chrono::Utc::now(),
                location: object_store::path::Path::from("not_used"),
                size,
                e_tag: None,
                version: None,
            },
        }
    }

    fn policy_config(max_capacity: u64) -> PolicyConfig {
        PolicyConfig {
            max_capacity,
            event_recency_max_duration_nanoseconds: 1_000_000_000 * 60 * 60,
        }
    }

    #[tokio::test]
    async fn test_eviction_listener() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert entry
        let value = cache_manager_value(
            1_000_000, None, // all will have same event timestamp
        );
        let to_evict = "k_a".to_string();
        cache_manager.insert(to_evict.clone(), value.clone()).await;

        // check current_size
        assert_eq!(
            cache_manager.current_size.load(AtomicOrdering::SeqCst),
            1_000_000
        );

        // explicitly evict
        cache_manager.invalidate(to_evict.clone()).await;

        // eviction listener should receive notification
        assert_matches!(
            evict_rx.recv().await,
            Ok(_),
            "should have received eviction notice",
        );

        assert_eq!(
            cache_manager.current_size.load(AtomicOrdering::SeqCst),
            0,
            "should have zero current_size after eviction"
        );
    }

    #[tokio::test]
    async fn test_evicts_at_max_capacity() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert 2 entries
        let value = cache_manager_value(
            max_capacity / 2,
            None, // all will have same event timestamp
        );
        let oldest = "k_a".to_string();
        cache_manager.insert(oldest.clone(), value.clone()).await;
        cache_manager.insert("k_b".into(), value.clone()).await;

        // To Discuss: this flush is needed, in order to apply ordering in k_a+k_b, as before k_c.
        // otherwise, the k_c is evicted instead
        cache_manager.manager.run_pending_tasks().await;

        // insert 1 more entry, which should force an eviction (over capacity)
        cache_manager.insert("k_c".into(), value).await;
        cache_manager.manager.run_pending_tasks().await;

        // should evict oldest inserted entry
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if *v == oldest,
            "should have evicted oldest inserted key, instead found {:?}", res
        );

        // should still have other 2 entries
        assert!(
            cache_manager.in_cache(&"k_b".to_string()).await.is_ok(),
            "should still have k_b"
        );
        assert!(
            cache_manager.in_cache(&"k_c".to_string()).await.is_ok(),
            "should still have k_c"
        );
    }

    #[tokio::test]
    async fn test_lfu_eviction() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert 2 entries
        let value = cache_manager_value(
            max_capacity / 2,
            None, // all will have same event timestamp
        );
        let read = "k_a".to_string();
        cache_manager.insert(read.clone(), value.clone()).await;
        let not_read = "k_b".to_string();
        cache_manager.insert(not_read.clone(), value.clone()).await;

        // read one entry, many times, to pass the probability threshold
        // To Discuss: is this sufficient for LFU?
        //      * the write-back will be triggered on a single cache miss
        //      * the LFU eviction would be using moka's probabilistic algorithm
        for _ in 0..63 {
            assert!(
                cache_manager.in_cache(&read).await.is_ok(),
                "should have read key"
            );
        }

        // insert 1 more entry
        cache_manager.insert("k_c".into(), value).await;
        cache_manager.manager.run_pending_tasks().await;

        // should evict unread entry
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if *v == not_read,
            "should have evicted unread key, instead found {:?}", res
        );

        // should have other 2 entries
        assert!(
            cache_manager.in_cache(&read).await.is_ok(),
            "should still have the read key"
        );
        assert!(
            cache_manager.in_cache(&"k_c".to_string()).await.is_ok(),
            "should have newly inserted k_c"
        );
    }

    #[tokio::test]
    async fn test_event_time_recency_eviction() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert 2 entries, where the older entry has a more recent event time
        let older_event_time = cache_manager_value(
            max_capacity / 2,
            Some(now_nanos() - 5_000_000_000), // 5 seconds ago
        );
        let newer_event_time = cache_manager_value(
            max_capacity / 2,
            Some(now_nanos() - 1_000_000_000), // 1 second ago
        );
        let should_keep = "younger_event_time_but_older_insert".to_string();
        cache_manager
            .insert(should_keep.clone(), newer_event_time.clone())
            .await;

        let should_evict = "older_event_time_but_younger_insert".to_string();
        cache_manager
            .insert(should_evict.clone(), older_event_time)
            .await;

        // insert 1 more entry, with same event time as should_keep
        cache_manager
            .insert("k_c".into(), newer_event_time.clone())
            .await;

        // To Discuss: this is waiting for event time recency eviction to occur
        // before the moka task queue is flushed.
        // This is the race condition explained in doc comments for
        // Evicter::evict_from_cache().
        tokio::time::sleep(Duration::from_micros(1)).await;
        cache_manager.manager.run_pending_tasks().await;

        // should evict based on event time, not insertion order
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if *v == should_evict,
            "should have evicted older_event_time, instead found {:?}", res
        );

        // LFU as a tie-breaker with same event time
        assert!(
            cache_manager.in_cache(&should_keep).await.is_ok(),
            "should have read key"
        );
        cache_manager.insert("k_d".into(), newer_event_time).await; // now have 3 with newer_event_time
        cache_manager.manager.run_pending_tasks().await;
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if v == "k_c",
            "should have evicted least recently queried key, instead found {:?}", res
        );
    }

    #[tokio::test]
    async fn test_event_time_trumps_lfu() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert 2 entries, where the older entry has a more recent event time
        let older_event_time = cache_manager_value(
            max_capacity / 2,
            Some(now_nanos() - 5_000_000_000), // 5 seconds ago
        );
        let newer_event_time = cache_manager_value(
            max_capacity / 2,
            Some(now_nanos() - 1_000_000_000), // 1 second ago
        );
        let should_keep = "younger_event_time_but_older_insert".to_string();
        cache_manager
            .insert(should_keep.clone(), newer_event_time.clone())
            .await;
        let should_evict = "older_event_time_but_younger_insert".to_string();
        cache_manager
            .insert(should_evict.clone(), older_event_time)
            .await;

        // query the older timestamp, many times, to pass the probability threshold
        for _ in 0..63 {
            assert!(
                cache_manager.in_cache(&should_evict).await.is_ok(),
                "should have read key"
            );
        }

        // insert 1 more entry, with same event time as should_keep
        cache_manager
            .insert("k_c".into(), newer_event_time.clone())
            .await;

        // To Discuss: this is waiting for event time recency eviction to occur
        // before the moka task queue is flushed.
        // This is the race condition explained in doc comments for
        // Evicter::evict_from_cache().
        tokio::time::sleep(Duration::from_micros(1)).await;
        cache_manager.manager.run_pending_tasks().await;

        // should evict based on event time, not LFU
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if *v == should_evict,
            "should have evicted older_event_time, instead found {:?}", res
        );
    }

    #[tokio::test]
    async fn test_event_time_recency_age_out() {
        let (evict_tx, _) = unbounded();

        // build cache manager, with 2 second ageout
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            PolicyConfig {
                max_capacity: max_capacity as u64,
                event_recency_max_duration_nanoseconds: 1_000_000_000 * 2,
            },
            evict_tx,
        ));

        // insert
        let value = cache_manager_value(
            max_capacity / 2,
            None, // will have current event timestamp
        );
        let now = "now_event_time".to_string();
        cache_manager.insert(now.clone(), value.clone()).await;
        assert!(
            cache_manager.in_cache(&now).await.is_ok(),
            "should have now"
        );

        // age out
        tokio::time::sleep(Duration::from_secs(3)).await;
        cache_manager.manager.run_pending_tasks().await;
        assert!(
            cache_manager.in_cache(&now).await.is_err(),
            "should no longer have now"
        );
    }

    #[tokio::test]
    async fn test_event_time_recency_age_out_with_future_time() {
        let (evict_tx, _) = unbounded();

        // build cache manager, with 2 second ageout
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            PolicyConfig {
                max_capacity: max_capacity as u64,
                event_recency_max_duration_nanoseconds: 1_000_000_000 * 2,
            },
            evict_tx,
        ));

        // insert
        let value = cache_manager_value(
            max_capacity / 2,
            Some(now_nanos() + 2_000_000_000), // 2 seconds into future
        );
        let future_event = "future_event_time".to_string();
        cache_manager
            .insert(future_event.clone(), value.clone())
            .await;
        assert!(
            cache_manager.in_cache(&future_event).await.is_ok(),
            "should have future_event"
        );

        // age out
        tokio::time::sleep(Duration::from_secs(3 + 2)).await;
        cache_manager.manager.run_pending_tasks().await;
        assert!(
            cache_manager.in_cache(&future_event).await.is_err(),
            "should no longer have future_event"
        );
    }

    #[tokio::test]
    async fn test_fetch_metadata() {
        let (evict_tx, evict_rx) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // insert 2 entries
        let value = cache_manager_value(
            max_capacity / 2,
            None, // all will have same event timestamp
        );
        let read = "k_a".to_string();
        cache_manager.insert(read.clone(), value.clone()).await;
        let not_read = "k_b".to_string();
        cache_manager.insert(not_read.clone(), value.clone()).await;

        // assert can find metadata
        let expected_metadata = value.clone().metadata;
        assert_matches!(
            cache_manager.fetch_metadata(&read).await,
            Ok(metadata) if metadata == expected_metadata,
            "should have found metadata"
        );

        // assert metadata access applies to LFU eviction policy
        for _ in 0..63 {
            assert!(
                cache_manager.fetch_metadata(&read).await.is_ok(),
                "should have read key"
            );
        }
        cache_manager.manager.run_pending_tasks().await;

        // insert 1 more entry
        cache_manager.insert("k_c".into(), value).await;
        cache_manager.manager.run_pending_tasks().await;

        // should evict unread entry
        let res = evict_rx.recv().await;
        assert_matches!(
            res,
            Ok(v) if *v == not_read,
            "should have evicted unread key, instead found {:?}", res
        );

        // should have other 2 entries
        assert!(
            cache_manager.in_cache(&read).await.is_ok(),
            "should still have the read key"
        );
        assert!(
            cache_manager.in_cache(&"k_c".to_string()).await.is_ok(),
            "should have newly inserted k_c"
        );
    }

    #[tokio::test]
    async fn test_cache_misses() {
        let (evict_tx, _) = unbounded();

        // build cache manager
        let max_capacity: usize = 3_200_000;
        let cache_manager = Arc::new(CacheManager::new(
            policy_config(max_capacity as u64),
            evict_tx,
        ));

        // cache misses
        assert_matches!(
            cache_manager
                .fetch_metadata(&"not_in_cache".to_string())
                .await,
            Err(Error::CacheMiss),
            "should have returned cache miss for metadata",
        );
        assert_matches!(
            cache_manager.in_cache(&"not_in_cache".to_string()).await,
            Err(Error::CacheMiss),
            "should have returned cache miss for object",
        );
        // when cache miss:
        // 1. return error
        // 2. upper layer (DataService) will handle any write back
    }
}
