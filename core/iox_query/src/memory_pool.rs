use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use jemalloc_stats::{AllocationMonitor, AllocationMonitorError};
use metric::U64Counter;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self
            .value
            .fetch_add(amount, std::sync::atomic::Ordering::Relaxed);
        self.max
            .fetch_max(old + amount, std::sync::atomic::Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value
            .fetch_sub(amount, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> crate::exec::context::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }
}

/// [`PerQueryMemoryPool`] is a memory pool that is pre-reserved for each query.
///
/// If `INFLUXDB_IOX_EXEC_PER_QUERY_MEM_POOL_BYTES` is configured, each query will be
/// pre-reserved a fixed memory size (e.g., 25 MB). Based on the maximum number
/// of concurrent queries, configured by `INFLUXDB_IOX_MAX_CONCURRENT_QUERIES`
/// (e.g., 80), the total pre-reserved memory for all queries is calculated as:
///
/// `25 MB/query x 80 queries = 2 GB`
///
/// This memory pool is separate from the central memory pool. This memory pool is
/// pre-allocated for each query, whereas the central memory pool is shared among
/// all the queries and is first come, first served.
///
/// Most queries will run successfully within the 25 MB allocation. However, queries
/// requiring additional memory will allocate the excess from the central memory pool.
///
/// ```text
///    │<------- INFLUXDB_IOX_EXEC_MEM_POOL_BYTES -------->│
///    |<-- 2 GB --->|
///    ┌─────────────┐┌────────────────────────────────────┐
///    │             ││                                    │
///    │  Per Query  ││           Central                  │
///    │   Memory    ││           Memory                   │
///    │    Pool     ││            Pool                    │
///    │             ││                                    │
///    └──────▲──────┘└──────────────▲─────────────────────┘
///           │                      │
///         1 │                    2 │
///           │                      │
///      Query A  ───────────────────┘
/// ```
///
/// 1. Query A uses memory from the "PerQueryMemoryPool" which has a
///    fixed memory per query, e.g. 25 MB.
///
/// 2. Query A uses memory from the "Central Memory Pool" if the 25 MB
///    fixed allocation is insufficient.
#[derive(Debug)]
pub struct PerQueryMemoryPool {
    /// The [`MemoryReservation`] for the central memory pool.
    /// This is used when the allocated memory exceeds the pre-reserved limit.
    central_reservation: Arc<std::sync::Mutex<MemoryReservation>>,

    /// The current allocated size for this query. If this value exceeds
    /// the `pre_reserved` value, the excess amount is allocated from the
    /// central memory pool.
    allocated: AtomicUsize,

    /// The pre-reserved size for this query in the [`PerQueryMemoryPool`].
    /// This is a fixed value and represents the initial memory allocation
    /// for each query.
    pre_reserved: NonZeroUsize,
}

impl PerQueryMemoryPool {
    pub fn new(reservation: MemoryReservation, pre_reserved: NonZeroUsize) -> Self {
        Self {
            central_reservation: Arc::new(std::sync::Mutex::new(reservation)),
            allocated: AtomicUsize::new(0),
            pre_reserved,
        }
    }

    /// Resize the central memory reservation to the currently allocated amount.
    ///
    /// It does not matter whether a thread attempts to grow or shrink the allocation;
    /// as long as the central memory reservation reflects the current memory requirement
    /// at a given point in time, the behavior is correct. This function can be a no-op
    /// if another thread has already updated the central memory reservation.
    fn sync_central_reservation(&self) {
        let mut reservation = self
            .central_reservation
            .lock()
            .expect("Acquired central reservation");

        // Load the allocated value again to avoid race condition
        let allocated_load = self.allocated.load(std::sync::atomic::Ordering::SeqCst);

        // Use `saturating_sub` to avoid underflow
        reservation.resize(allocated_load.saturating_sub(self.pre_reserved.into()));
    }

    /// Try to resize the central memory reservation to the currently allocated amount.
    ///
    /// It does not matter whether a thread attempts to grow or shrink the allocation;
    /// as long as the central memory reservation reflects the current memory requirement
    /// at a given point in time, the behavior is correct. This function can be a no-op
    /// if another thread has already updated the central memory reservation.
    ///
    /// # Errors
    /// An error may occur if the lock fails to be acquired, or if resizing the central memory pool
    /// fails. In either case, the central memory reservation is not updated. The caller of this
    /// function may need to revert the allocation in the per-query memory pool.
    fn try_sync_central_reservation(&self) -> crate::exec::context::Result<()> {
        if let Ok(mut reservation) = self.central_reservation.lock() {
            // Successfully acquire the lock for the "central memory pool"

            // Load the allocated value again to avoid race condition.
            let allocated_load = self.allocated.load(std::sync::atomic::Ordering::SeqCst);

            // Use `saturating_sub` to prevent underflow
            reservation.try_resize(allocated_load.saturating_sub(self.pre_reserved.into()))
        } else {
            // Lock acquisition may fail if another thread panicked while holding the mutex.
            // Since we are unable to allocate additional memory for this query, return an error.
            Err(datafusion::error::DataFusionError::ResourcesExhausted(
                String::from("Cannot allocate more memory for this query in PerQueryMemoryPool"),
            ))
        }
    }
}

impl MemoryPool for PerQueryMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let old = self
            .allocated
            .fetch_add(additional, std::sync::atomic::Ordering::SeqCst);
        let allocated = old + additional;

        if allocated > self.pre_reserved.into() {
            self.sync_central_reservation();
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        if shrink > self.allocated.load(std::sync::atomic::Ordering::SeqCst) {
            // Do not shrink to avoid underflowing
            return;
        }

        let old = self
            .allocated
            .fetch_sub(shrink, std::sync::atomic::Ordering::SeqCst);

        if old > self.pre_reserved.into() {
            self.sync_central_reservation();
        }
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> crate::exec::context::Result<()> {
        let old = self
            .allocated
            .fetch_add(additional, std::sync::atomic::Ordering::SeqCst);
        let allocated = old + additional;

        let pre_reserved = self.pre_reserved.into();

        if allocated <= pre_reserved {
            return Ok(());
        }

        self.try_sync_central_reservation().inspect_err(|_| {
            // Failed to sync the central memory pool reservation, so revert the
            // allocation in the per-query memory pool
            self.allocated
                .fetch_sub(additional, std::sync::atomic::Ordering::SeqCst);
        })
    }

    fn reserved(&self) -> usize {
        self.allocated.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct AllocationMonitoringMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<AllocationMonitor>,
}

impl AllocationMonitoringMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<AllocationMonitor>) -> Self {
        Self { inner, monitor }
    }
}

impl MemoryPool for AllocationMonitoringMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.monitor.reserve(additional);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Shrinking doesn't update the monitor. This means that the
        // monitor will always see the memory reservation is increasing
        // causing the statistics to be polled from jemalloc
        // occasionally, keeping the statistics more accurate.
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> crate::exec::context::Result<()> {
        self.monitor.try_reserve(additional).map_err(|e| match e {
            AllocationMonitorError::Jemalloc { source } => {
                DataFusionError::External(Box::new(source))
            }
            AllocationMonitorError::HeapExhausted => {
                DataFusionError::ResourcesExhausted(e.to_string())
            }
        })?;
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }
}

#[derive(Debug)]
pub struct OomTrackingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    ooms: U64Counter,
}

impl OomTrackingMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, metrics: Arc<metric::Registry>) -> Self {
        let ooms = metrics
            .register_metric::<U64Counter>(
                "query_datafusion_query_execution_ooms",
                "Number of OOMs encountered by the query engine",
            )
            .recorder(&[]);
        Self { inner, ooms }
    }
}

impl MemoryPool for OomTrackingMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> crate::exec::context::Result<()> {
        match self.inner.try_grow(reservation, additional) {
            Ok(()) => Ok(()),
            Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                self.ooms.inc(1);
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, TrackConsumersPool,
    };
    use std::{num::NonZeroUsize, sync::Arc};
    use test_helpers::assert_error;

    use super::{OomTrackingMemoryPool, PerQueryMemoryPool};

    const TEST_PRE_RESERVE_MEMORY: NonZeroUsize = std::num::NonZero::new(10usize).unwrap();

    #[test]
    fn test_per_query_memory_pool() {
        // Create a central memory pool that allocate up to 100 bytes
        let central_memory_pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(2).unwrap(),
        ));

        // ==== Query 1 ====
        let central_reservation_1 =
            MemoryConsumer::new("central_reservation_1").register(&central_memory_pool);
        let dummy_reservation = central_reservation_1.new_empty();

        // Create a PerQueryMemoryPool with a pre-reserved memory of 10 bytes.
        let q1_pool = PerQueryMemoryPool::new(central_reservation_1, TEST_PRE_RESERVE_MEMORY);

        // Grow query 1's PerQueryMemoryPool by 9 bytes, within the pre-reserved limit
        q1_pool.grow(&dummy_reservation, 9);

        // Verify that the reserved memory for query 1 is 9 bytes.
        assert_eq!(q1_pool.reserved(), 9);

        // Verify that the central memory pool has not been used.
        assert_eq!(central_memory_pool.reserved(), 0);

        // ==== Query 2 ====
        let central_reservation_2 =
            MemoryConsumer::new("central_reservation_2").register(&central_memory_pool);
        let q2_pool = PerQueryMemoryPool::new(central_reservation_2, TEST_PRE_RESERVE_MEMORY);

        // Grow query 2's PerQueryMemoryPool by 15 bytes, exceeding the pre-reserved limit by 5 bytes.
        q2_pool.grow(&dummy_reservation, 15);
        assert_eq!(q2_pool.reserved(), 15);

        // Verify that the central memory pool has been used for the extra 5 bytes.
        assert_eq!(central_memory_pool.reserved(), 5);

        // Try to grow query 2's PerQueryMemoryPool by an additional 20 bytes.
        q2_pool
            .try_grow(&dummy_reservation, 20)
            .expect("should grow successfully for query 2");

        // Verify the reserved memory for query 2 is 35 bytes (15 + 20).
        assert_eq!(q2_pool.reserved(), 35);

        // Verify that the central memory pool has been used for the extra 25 bytes (35 - 10).
        assert_eq!(central_memory_pool.reserved(), 25);

        // Shrink query 2's PerQueryMemoryPool by 5 bytes
        q2_pool.shrink(&dummy_reservation, 5);
        assert_eq!(q2_pool.reserved(), 30);
        assert_eq!(central_memory_pool.reserved(), 20);

        // Shrink query 2's PerQueryMemoryPool by 50 bytes, which is more than what it was allocated
        q2_pool.shrink(&dummy_reservation, 50);
        // The reserved memory for query 2 should remain at 30 bytes without shrinking further
        assert_eq!(q2_pool.reserved(), 30);
        assert_eq!(central_memory_pool.reserved(), 20);

        // Grow query 1's PerQueryMemoryPool by an additional 16 bytes
        q1_pool
            .try_grow(&dummy_reservation, 16)
            .expect("should grow successfully for query 1");

        // Verify the reserved memory for query 1 is 25 bytes (9 + 16)
        assert_eq!(q1_pool.reserved(), 25);

        // Verify the central memory pool's reserved memory is 35 bytes,
        // 20 (query 2) + 15 (query 1)
        assert_eq!(central_memory_pool.reserved(), 35);

        // ==== Query 3 ====
        let central_reservation_3 =
            MemoryConsumer::new("central_reservation_3").register(&central_memory_pool);
        let q3_pool = PerQueryMemoryPool::new(central_reservation_3, TEST_PRE_RESERVE_MEMORY);

        // Try to grow query 3's PerQueryMemoryPool by 100 bytes, which should fail
        let result = q3_pool.try_grow(&dummy_reservation, 100);
        assert_error!(result.as_ref(), DataFusionError::ResourcesExhausted(_));
        insta::assert_snapshot!(
            // reservations are numbered non-deterministically, so we need to normalize that
            regex::Regex::new("#[0-9]+").unwrap().replace_all(result.unwrap_err().to_string().as_str(), ""),
            @r"
        Resources exhausted: Additional allocation failed with top memory consumers (across reservations) as:
          central_reservation_2(can spill: false) consumed 20.0 B, peak 25.0 B,
          central_reservation_1(can spill: false) consumed 15.0 B, peak 15.0 B.
        Error: Failed to allocate additional 90.0 B for central_reservation_3 with 0.0 B already allocated for this reservation - 65.0 B remain available for the total pool
        "
        );

        // Verify that the reserved memory for query 3 is 0 bytes since the allocation failed.
        assert_eq!(q3_pool.reserved(), 0);

        // Verify that the central memory pool's reserved memory remains unchanged at 35 bytes.
        assert_eq!(central_memory_pool.reserved(), 35);
    }

    #[test]
    fn test_oom_tracking_memory_pool() {
        // Create a central memory pool that allocate up to 100 bytes
        let central_memory_pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(2).unwrap(),
        ));

        // Create an OomTrackingMemoryPool with the central memory pool
        let metric_registry = metric::Registry::default();
        let oom_tracking_pool =
            OomTrackingMemoryPool::new(Arc::clone(&central_memory_pool), Arc::new(metric_registry));

        let central_reservation_1 =
            MemoryConsumer::new("central_reservation_1").register(&central_memory_pool);
        let dummy_reservation = central_reservation_1.new_empty();

        // Try to grow the OomTrackingMemoryPool by 200 bytes, which should fail
        let result = oom_tracking_pool.try_grow(&dummy_reservation, 200);
        assert_error!(result.as_ref(), DataFusionError::ResourcesExhausted(_));

        // The OOM counter should be incremented
        assert_eq!(oom_tracking_pool.ooms.fetch(), 1);
    }
}
