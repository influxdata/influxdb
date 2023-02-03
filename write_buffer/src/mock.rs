use crate::{
    config::WriteBufferCreationConfig,
    core::{
        test_utils::lp_to_batches, WriteBufferError, WriteBufferReading, WriteBufferStreamHandler,
        WriteBufferWriting,
    },
};
use async_trait::async_trait;
use data_types::{NamespaceId, Sequence, SequenceNumber, ShardIndex};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use futures::{stream::BoxStream, StreamExt};
use iox_time::TimeProvider;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
    task::{Poll, Waker},
};

#[derive(Debug, Default)]
struct WriteResVec {
    /// The maximum sequence number in the entries
    max_seqno: Option<i64>,

    /// The writes
    writes: Vec<Result<DmlOperation, WriteBufferError>>,

    /// A list of Waker waiting for a new entry to be pushed
    ///
    /// Note: this is a list because it is possible to create
    /// two streams consuming from the same shared state
    wait_list: Vec<Waker>,
}

impl WriteResVec {
    pub fn push(&mut self, val: Result<DmlOperation, WriteBufferError>) {
        if let Ok(entry) = &val {
            if let Some(seqno) = entry.meta().sequence() {
                self.max_seqno = Some(match self.max_seqno {
                    Some(current) => current.max(seqno.sequence_number.get()),
                    None => seqno.sequence_number.get(),
                });
            }
        }

        self.writes.push(val);
        for waker in self.wait_list.drain(..) {
            waker.wake()
        }
    }

    /// Register a waker to be notified when a new entry is pushed
    pub fn register_waker(&mut self, waker: &Waker) {
        for wait_waker in &self.wait_list {
            if wait_waker.will_wake(waker) {
                return;
            }
        }
        self.wait_list.push(waker.clone())
    }
}

/// Mocked entries for [`MockBufferForWriting`] and [`MockBufferForReading`].
#[derive(Debug, Clone)]
pub struct MockBufferSharedState {
    /// Lock-protected entries.
    ///
    /// The inner `Option` is `None` if the shards are not created yet.
    writes: Arc<Mutex<Option<BTreeMap<ShardIndex, WriteResVec>>>>,
}

impl MockBufferSharedState {
    /// Create new shared state w/ N shards.
    ///
    /// This is equivalent to [`uninitialized`](Self::uninitialized) followed by
    /// [`init`](Self::init).
    pub fn empty_with_n_shards(n_shards: NonZeroU32) -> Self {
        let state = Self::uninitialized();
        state.init(n_shards);
        state
    }

    /// Create new shared state w/o any shards.
    pub fn uninitialized() -> Self {
        Self {
            writes: Arc::new(Mutex::new(None)),
        }
    }

    pub fn high_watermark(&self, shard: ShardIndex) -> Option<i64> {
        self.writes.lock().as_ref()?.get(&shard)?.max_seqno
    }

    /// Initialize shared state w/ N shards.
    ///
    /// # Panics
    ///
    /// - when state is already initialized
    pub fn init(&self, n_shards: NonZeroU32) {
        let mut guard = self.writes.lock();

        if guard.is_some() {
            panic!("already initialized");
        }

        *guard = Some(Self::init_inner(n_shards));
    }

    fn init_inner(n_shards: NonZeroU32) -> BTreeMap<ShardIndex, WriteResVec> {
        (0..n_shards.get())
            .map(|shard_index| (ShardIndex::new(shard_index as i32), Default::default()))
            .collect()
    }

    /// Push a new delete to the specified shard
    ///
    /// # Panics
    ///
    /// - when delete is not sequenced
    /// - when no shard was initialized
    /// - when specified shard does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_delete(&self, delete: DmlDelete) {
        self.push_operation(DmlOperation::Delete(delete))
    }

    /// Push a new entry to the specified shard.
    ///
    /// # Panics
    ///
    /// - when write is not sequenced
    /// - when no shard was initialized
    /// - when specified shard does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_write(&self, write: DmlWrite) {
        self.push_operation(DmlOperation::Write(write))
    }

    /// Push a new operation to the specified shard
    ///
    /// # Panics
    ///
    /// - when operation is not sequenced
    /// - when no shard was initialized
    /// - when specified shard does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_operation(&self, write: DmlOperation) {
        let sequence = write.meta().sequence().expect("write must be sequenced");
        assert!(
            write.meta().producer_ts().is_some(),
            "write must have timestamp"
        );

        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no shards initialized");
        let writes_vec = writes
            .get_mut(&sequence.shard_index)
            .expect("invalid shard index");

        if let Some(max_sequence_number) = writes_vec.max_seqno {
            assert!(
                max_sequence_number < sequence.sequence_number.get(),
                "sequence number {} is less/equal than current max sequence number {}",
                sequence.sequence_number.get(),
                max_sequence_number
            );
        }

        writes_vec.push(Ok(write));
    }

    /// Push line protocol data with placeholder values used for write metadata
    pub fn push_lp(&self, sequence: Sequence, lp: &str) {
        let tables = lp_to_batches(lp);
        let meta = DmlMeta::sequenced(sequence, iox_time::Time::from_timestamp_nanos(0), None, 0);
        self.push_write(DmlWrite::new(
            NamespaceId::new(42),
            tables,
            "test-partition".into(),
            meta,
        ))
    }

    /// Push error to specified shard.
    ///
    /// # Panics
    ///
    /// - when no shard was initialized
    /// - when shard does not exist
    pub fn push_error(&self, error: WriteBufferError, shard_index: ShardIndex) {
        let mut guard = self.writes.lock();
        let entries = guard.as_mut().expect("no shards initialized");
        let entry_vec = entries.get_mut(&shard_index).expect("invalid shard index");

        entry_vec.push(Err(error));
    }

    /// Get messages (entries and errors) for specified shard.
    ///
    /// # Panics
    ///
    /// - when no shard was initialized
    /// - when shard does not exist
    pub fn get_messages(
        &self,
        shard_index: ShardIndex,
    ) -> Vec<Result<DmlOperation, WriteBufferError>> {
        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no shards initialized");
        let writes_vec = writes.get_mut(&shard_index).expect("invalid shard index");

        writes_vec
            .writes
            .iter()
            .map(|write_res| match write_res {
                Ok(write) => Ok(write.clone()),
                Err(e) => Err(e.to_string().into()),
            })
            .collect()
    }

    /// Provides a way to wipe messages (e.g. to simulate retention periods in Kafka)
    ///
    /// # Panics
    ///
    /// - when no shard was initialized
    /// - when shard does not exist
    pub fn clear_messages(&self, shard_index: ShardIndex) {
        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no shards initialized");
        let writes_vec = writes.get_mut(&shard_index).expect("invalid shard index");

        std::mem::take(writes_vec);
    }

    fn maybe_auto_init(&self, creation_config: Option<&WriteBufferCreationConfig>) {
        if let Some(cfg) = creation_config {
            let mut guard = self.writes.lock();
            if guard.is_none() {
                *guard = Some(Self::init_inner(cfg.n_shards));
            }
        }
    }
}

#[derive(Debug)]
pub struct MockBufferForWriting {
    state: Arc<MockBufferSharedState>,
    time_provider: Arc<dyn TimeProvider>,
}

impl MockBufferForWriting {
    pub fn new(
        state: MockBufferSharedState,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self, WriteBufferError> {
        state.maybe_auto_init(creation_config);

        {
            let guard = state.writes.lock();
            if guard.is_none() {
                return Err("no shards initialized".to_string().into());
            }
        }

        Ok(Self {
            state: state.into(),
            time_provider,
        })
    }

    pub fn state(&self) -> Arc<MockBufferSharedState> {
        Arc::clone(&self.state)
    }
}

#[async_trait]
impl WriteBufferWriting for MockBufferForWriting {
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        let mut guard = self.state.writes.lock();
        let entries = guard.as_mut().unwrap();
        entries.keys().copied().collect()
    }

    async fn store_operation(
        &self,
        shard_index: ShardIndex,
        mut operation: DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let mut guard = self.state.writes.lock();
        let writes = guard.as_mut().unwrap();
        let writes_vec = writes
            .get_mut(&shard_index)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown shard index: {shard_index}").into()
            })?;

        let sequence_number = writes_vec.max_seqno.map(|n| n + 1).unwrap_or(0);

        let sequence = Sequence {
            shard_index,
            sequence_number: SequenceNumber::new(sequence_number),
        };

        let timestamp = operation
            .meta()
            .producer_ts()
            .unwrap_or_else(|| self.time_provider.now());

        let meta = DmlMeta::sequenced(
            sequence,
            timestamp,
            operation.meta().span_context().cloned(),
            0,
        );

        operation.set_meta(meta.clone());

        writes_vec.push(Ok(operation));

        Ok(meta)
    }

    async fn flush(&self) -> Result<(), WriteBufferError> {
        // no buffer
        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "mock"
    }
}

/// A [`WriteBufferWriting`] that will error for every action.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockBufferForWritingThatAlwaysErrors;

#[async_trait]
impl WriteBufferWriting for MockBufferForWritingThatAlwaysErrors {
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        IntoIterator::into_iter([ShardIndex::new(0)]).collect()
    }

    async fn store_operation(
        &self,
        _shard_index: ShardIndex,
        _operation: DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        Err(String::from(
            "Something bad happened on the way to writing an entry in the write buffer",
        )
        .into())
    }

    async fn flush(&self) -> Result<(), WriteBufferError> {
        // no buffer
        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "mock_failing"
    }
}

#[derive(Debug)]
pub struct MockBufferForReading {
    shared_state: Arc<MockBufferSharedState>,
    n_shards: u32,
}

impl MockBufferForReading {
    pub fn new(
        state: MockBufferSharedState,
        creation_config: Option<&WriteBufferCreationConfig>,
    ) -> Result<Self, WriteBufferError> {
        state.maybe_auto_init(creation_config);

        let n_shards = {
            let guard = state.writes.lock();
            let entries = match guard.as_ref() {
                Some(entries) => entries,
                None => {
                    return Err("no shards initialized".to_string().into());
                }
            };
            entries.len() as u32
        };

        Ok(Self {
            shared_state: Arc::new(state),
            n_shards,
        })
    }
}

/// Shard-specific playback state
#[derive(Debug)]
pub struct MockBufferStreamHandler {
    /// Shared state.
    shared_state: Arc<MockBufferSharedState>,

    /// Own shard index.
    shard_index: ShardIndex,

    /// Index within the entry vector.
    vector_index: Arc<AtomicUsize>,

    /// Offset within the sequence numbers or "earliest" if not set.
    offset: Option<i64>,

    /// Flags if the stream is terminated, e.g. due to "offset out of range"
    terminated: Arc<AtomicBool>,
}

#[async_trait]
impl WriteBufferStreamHandler for MockBufferStreamHandler {
    async fn stream(&mut self) -> BoxStream<'static, Result<DmlOperation, WriteBufferError>> {
        // Don't reference `self` in the closure, move these instead
        let terminated = Arc::clone(&self.terminated);
        let shared_state = Arc::clone(&self.shared_state);
        let shard_index = self.shard_index;
        let vector_index = Arc::clone(&self.vector_index);
        let offset = self.offset;

        let mut found_something = false;

        futures::stream::poll_fn(move |cx| {
            if terminated.load(SeqCst) {
                return Poll::Ready(None);
            }

            let mut guard = shared_state.writes.lock();
            let writes = guard.as_mut().unwrap();
            let writes_vec = writes.get_mut(&shard_index).unwrap();

            let entries = &writes_vec.writes;
            let mut vi = vector_index.load(SeqCst);
            while entries.len() > vi {
                let write_result = &entries[vi];

                // consume entry
                vi = vector_index
                    .fetch_update(SeqCst, SeqCst, |n| Some(n + 1))
                    .unwrap();

                match write_result {
                    Ok(write) => {
                        // found an entry => need to check if it is within the offset
                        if let Some(offset) = offset {
                            let sequence_number = write.meta().sequence().unwrap().sequence_number.get();

                            match sequence_number.cmp(&offset) {
                                std::cmp::Ordering::Greater if found_something => {
                                    // this is not the first entry, so we're within range
                                    return Poll::Ready(Some(Ok(write.clone())));
                                }
                                std::cmp::Ordering::Greater => {
                                    // no other entry was found beforehand, this is bad
                                    terminated.store(true, SeqCst);
                                    return Poll::Ready(Some(Err(
                                        WriteBufferError::sequence_number_no_longer_exists(format!(
                                            "sequence number no longer exists, low watermark is {sequence_number}"
                                        )),
                                    )));
                                }
                                std::cmp::Ordering::Equal => {
                                    // found exactly the seek point
                                    found_something = true;
                                    return Poll::Ready(Some(Ok(write.clone())));
                                }
                                std::cmp::Ordering::Less => {
                                    // offset is larger then the current entry => ignore entry and try next
                                    found_something = true;
                                    continue;
                                }
                            }
                        } else {
                            // start at earlist, i.e. no need to check the sequence number of the entry
                            return Poll::Ready(Some(Ok(write.clone())));
                        }
                    }
                    Err(e) => {
                        // found an error => return entry to caller
                        return Poll::Ready(Some(Err(e.to_string().into())));
                    }
                }
            }

            // check if we have seeked too far
            if let Some(offset) = offset {
                let next_offset = entries
                    .iter()
                    .filter_map(|write_result| {
                        if let Ok(write) = write_result {
                            let sequence = write.meta().sequence().unwrap();
                            Some(sequence.sequence_number.get())
                        } else {
                            None
                        }
                    })
                    .max()
                    .map(|x| x + 1)
                    .unwrap_or_default();
                if offset > next_offset {
                    terminated.store(true, SeqCst);
                    return Poll::Ready(Some(Err(
                        WriteBufferError::sequence_number_after_watermark(format!(
                            "unknown sequence number, high watermark is {next_offset}"
                        )),
                    )));
                }
            }

            // we are at the end of the recorded entries => report pending
            writes_vec.register_waker(cx.waker());
            Poll::Pending
        })
        .boxed()
    }

    async fn seek(&mut self, sequence_number: SequenceNumber) -> Result<(), WriteBufferError> {
        let offset = sequence_number.get();
        let current = self
            .shared_state
            .high_watermark(self.shard_index)
            .unwrap_or_default();
        if offset > current {
            return Err(WriteBufferError::sequence_number_after_watermark(format!(
                "attempted to seek to offset {offset}, but current high \
                watermark for partition {p} is {current}",
                p = self.shard_index
            )));
        }

        self.offset = Some(offset);

        // reset position to start since seeking might go backwards
        self.vector_index.store(0, SeqCst);

        // reset termination state
        self.terminated.store(false, SeqCst);

        Ok(())
    }

    fn reset_to_earliest(&mut self) {
        self.offset = None;
        self.vector_index.store(0, SeqCst);
        self.terminated.store(false, SeqCst);
    }
}

#[async_trait]
impl WriteBufferReading for MockBufferForReading {
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        (0..self.n_shards)
            .into_iter()
            .map(|i| ShardIndex::new(i as i32))
            .collect()
    }

    async fn stream_handler(
        &self,
        shard_index: ShardIndex,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        if shard_index.get() as u32 >= self.n_shards {
            return Err(format!("Unknown shard index: {shard_index}").into());
        }

        Ok(Box::new(MockBufferStreamHandler {
            shared_state: Arc::clone(&self.shared_state),
            shard_index,
            vector_index: Arc::new(AtomicUsize::new(0)),
            offset: None,
            terminated: Arc::new(AtomicBool::new(false)),
        }))
    }

    async fn fetch_high_watermark(
        &self,
        shard_index: ShardIndex,
    ) -> Result<SequenceNumber, WriteBufferError> {
        let guard = self.shared_state.writes.lock();
        let entries = guard.as_ref().unwrap();
        let entry_vec = entries
            .get(&shard_index)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown shard index: {shard_index}").into()
            })?;
        let watermark = entry_vec.max_seqno.map(|n| n + 1).unwrap_or(0);

        Ok(SequenceNumber::new(watermark))
    }

    fn type_name(&self) -> &'static str {
        "mock"
    }
}
/// A [`WriteBufferReading`] that will error for every action.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockBufferForReadingThatAlwaysErrors;

#[derive(Debug, Default, Clone, Copy)]
pub struct MockStreamHandlerThatAlwaysErrors;

#[async_trait]
impl WriteBufferStreamHandler for MockStreamHandlerThatAlwaysErrors {
    async fn stream(&mut self) -> BoxStream<'static, Result<DmlOperation, WriteBufferError>> {
        futures::stream::poll_fn(|_cx| {
            Poll::Ready(Some(Err(String::from(
                "Something bad happened while reading from stream",
            )
            .into())))
        })
        .boxed()
    }

    async fn seek(&mut self, _sequence_number: SequenceNumber) -> Result<(), WriteBufferError> {
        Err(String::from("Something bad happened while seeking the stream").into())
    }

    fn reset_to_earliest(&mut self) {
        // Intentionally left blank
    }
}

#[async_trait]
impl WriteBufferReading for MockBufferForReadingThatAlwaysErrors {
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        BTreeSet::from([ShardIndex::new(0)])
    }

    async fn stream_handler(
        &self,
        _shard_index: ShardIndex,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        Ok(Box::new(MockStreamHandlerThatAlwaysErrors {}))
    }

    async fn fetch_high_watermark(
        &self,
        _shard_index: ShardIndex,
    ) -> Result<SequenceNumber, WriteBufferError> {
        Err(String::from("Something bad happened while fetching the high watermark").into())
    }

    fn type_name(&self) -> &'static str {
        "mock_failing"
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::time::Duration;

    use data_types::NamespaceId;
    use futures::StreamExt;
    use iox_time::TimeProvider;
    use test_helpers::assert_contains;
    use trace::RingBufferTraceCollector;

    use crate::core::{
        test_utils::{lp_to_batches, perform_generic_tests, TestAdapter, TestContext},
        WriteBufferErrorKind,
    };

    use super::*;

    struct MockTestAdapter {}

    #[async_trait]
    impl TestAdapter for MockTestAdapter {
        type Context = MockTestContext;

        async fn new_context_with_time(
            &self,
            n_shards: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            MockTestContext {
                state: MockBufferSharedState::uninitialized(),
                n_shards,
                time_provider,
                trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
            }
        }
    }

    struct MockTestContext {
        state: MockBufferSharedState,
        n_shards: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
        trace_collector: Arc<RingBufferTraceCollector>,
    }

    impl MockTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_shards: self.n_shards,
                ..Default::default()
            })
        }
    }

    #[async_trait]
    impl TestContext for MockTestContext {
        type Writing = MockBufferForWriting;

        type Reading = MockBufferForReading;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            MockBufferForWriting::new(
                self.state.clone(),
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
            )
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            MockBufferForReading::new(
                self.state.clone(),
                self.creation_config(creation_config).as_ref(),
            )
        }

        fn trace_collector(&self) -> Arc<RingBufferTraceCollector> {
            Arc::clone(&self.trace_collector)
        }
    }

    #[tokio::test]
    async fn test_generic() {
        perform_generic_tests(MockTestAdapter {}).await;
    }

    #[test]
    #[should_panic(expected = "invalid shard index")]
    fn test_state_push_write_panic_wrong_shard() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.push_lp(
            Sequence::new(ShardIndex::new(2), SequenceNumber::new(0)),
            "upc,region=east user=1 100",
        );
    }

    #[test]
    #[should_panic(expected = "no shards initialized")]
    fn test_state_push_write_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.push_lp(
            Sequence::new(ShardIndex::new(0), SequenceNumber::new(0)),
            "upc,region=east user=1 100",
        );
    }

    #[test]
    #[should_panic(
        expected = "sequence number 13 is less/equal than current max sequence number 13"
    )]
    fn test_state_push_write_panic_wrong_sequence_number_equal() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.push_lp(
            Sequence::new(ShardIndex::new(0), SequenceNumber::new(13)),
            "upc,region=east user=1 100",
        );
        state.push_lp(
            Sequence::new(ShardIndex::new(0), SequenceNumber::new(13)),
            "upc,region=east user=1 100",
        );
    }

    #[test]
    #[should_panic(
        expected = "sequence number 12 is less/equal than current max sequence number 13"
    )]
    fn test_state_push_write_panic_wrong_sequence_number_less() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.push_lp(
            Sequence::new(ShardIndex::new(0), SequenceNumber::new(13)),
            "upc,region=east user=1 100",
        );
        state.push_lp(
            Sequence::new(ShardIndex::new(0), SequenceNumber::new(12)),
            "upc,region=east user=1 100",
        );
    }

    #[test]
    #[should_panic(expected = "invalid shard index")]
    fn test_state_push_error_panic_wrong_shard() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        let error = "foo".to_string().into();
        state.push_error(error, ShardIndex::new(2));
    }

    #[test]
    #[should_panic(expected = "no shards initialized")]
    fn test_state_push_error_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        let error = "foo".to_string().into();
        state.push_error(error, ShardIndex::new(0));
    }

    #[test]
    #[should_panic(expected = "invalid shard index")]
    fn test_state_get_messages_panic_wrong_shard() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.get_messages(ShardIndex::new(2));
    }

    #[test]
    #[should_panic(expected = "no shards initialized")]
    fn test_state_get_messages_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.get_messages(ShardIndex::new(0));
    }

    #[test]
    #[should_panic(expected = "invalid shard index")]
    fn test_state_clear_messages_panic_wrong_shard() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.clear_messages(ShardIndex::new(2));
    }

    #[test]
    #[should_panic(expected = "no shards initialized")]
    fn test_state_clear_messages_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.clear_messages(ShardIndex::new(0));
    }

    #[test]
    fn test_clear_messages() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        let shard_0 = ShardIndex::new(0);
        let shard_1 = ShardIndex::new(1);

        state.push_lp(
            Sequence::new(shard_0, SequenceNumber::new(11)),
            "upc user=1 100",
        );
        state.push_lp(
            Sequence::new(shard_1, SequenceNumber::new(12)),
            "upc user=1 100",
        );

        assert_eq!(state.get_messages(shard_0).len(), 1);
        assert_eq!(state.get_messages(shard_1).len(), 1);

        state.clear_messages(shard_0);

        assert_eq!(state.get_messages(shard_0).len(), 0);
        assert_eq!(state.get_messages(shard_1).len(), 1);
    }

    #[tokio::test]
    async fn test_always_error_read() {
        let reader = MockBufferForReadingThatAlwaysErrors {};
        let shard_0 = ShardIndex::new(0);

        assert_contains!(
            reader
                .fetch_high_watermark(shard_0)
                .await
                .unwrap_err()
                .to_string(),
            "Something bad happened while fetching the high watermark"
        );

        let mut stream_handler = reader.stream_handler(shard_0).await.unwrap();

        assert_contains!(
            stream_handler
                .seek(SequenceNumber::new(0))
                .await
                .unwrap_err()
                .to_string(),
            "Something bad happened while seeking the stream"
        );

        assert_contains!(
            stream_handler
                .stream()
                .await
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string(),
            "Something bad happened while reading from stream"
        );
    }

    #[tokio::test]
    async fn test_always_error_write() {
        let writer = MockBufferForWritingThatAlwaysErrors {};

        let tables = lp_to_batches("upc user=1 100");
        let operation = DmlOperation::Write(DmlWrite::new(
            NamespaceId::new(42),
            tables,
            "bananas".into(),
            Default::default(),
        ));

        assert_contains!(
            writer
                .store_operation(ShardIndex::new(0), operation)
                .await
                .unwrap_err()
                .to_string(),
            "Something bad happened on the way to writing an entry in the write buffer"
        );
    }

    #[test]
    fn test_delayed_init() {
        let state = MockBufferSharedState::uninitialized();
        state.init(NonZeroU32::try_from(2).unwrap());
        let shard_0 = ShardIndex::new(0);
        let shard_1 = ShardIndex::new(1);

        state.push_lp(
            Sequence::new(shard_0, SequenceNumber::new(11)),
            "upc user=1 100",
        );

        assert_eq!(state.get_messages(shard_0).len(), 1);
        assert_eq!(state.get_messages(shard_1).len(), 0);

        let error = "foo".to_string().into();
        state.push_error(error, shard_1);

        assert_eq!(state.get_messages(shard_0).len(), 1);
        assert_eq!(state.get_messages(shard_1).len(), 1);

        state.clear_messages(shard_0);

        assert_eq!(state.get_messages(shard_0).len(), 0);
        assert_eq!(state.get_messages(shard_1).len(), 1);
    }

    #[test]
    #[should_panic(expected = "already initialized")]
    fn test_double_init_panics() {
        let state = MockBufferSharedState::uninitialized();
        state.init(NonZeroU32::try_from(2).unwrap());
        state.init(NonZeroU32::try_from(2).unwrap());
    }

    #[test]
    #[should_panic(expected = "already initialized")]
    fn test_init_after_constructor_panics() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(2).unwrap());
        state.init(NonZeroU32::try_from(2).unwrap());
    }

    #[tokio::test]
    async fn test_delayed_insert() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let shard_0 = ShardIndex::new(0);

        state.push_lp(
            Sequence::new(shard_0, SequenceNumber::new(0)),
            "mem foo=1 10",
        );

        let read = MockBufferForReading::new(state.clone(), None).unwrap();

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let consumer = tokio::spawn(async move {
            let mut stream_handler = read.stream_handler(shard_0).await.unwrap();
            let mut stream = stream_handler.stream().await;
            stream.next().await.unwrap().unwrap();
            barrier_captured.wait().await;
            stream.next().await.unwrap().unwrap();
        });

        // Wait for consumer to read first entry
        barrier.wait().await;

        state.push_lp(
            Sequence::new(shard_0, SequenceNumber::new(1)),
            "mem foo=2 20",
        );

        tokio::time::timeout(Duration::from_millis(100), consumer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_sequence_number_no_longer_exists() {
        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let shard_0 = ShardIndex::new(0);

        state.push_lp(
            Sequence::new(shard_0, SequenceNumber::new(11)),
            "upc user=1 100",
        );

        let read = MockBufferForReading::new(state.clone(), None).unwrap();
        let mut stream_handler = read.stream_handler(shard_0).await.unwrap();
        stream_handler.seek(SequenceNumber::new(1)).await.unwrap();
        let mut stream = stream_handler.stream().await;

        let err = stream.next().await.unwrap().unwrap_err();
        assert_eq!(
            err.kind(),
            WriteBufferErrorKind::SequenceNumberNoLongerExists
        );

        // terminated
        assert!(stream.next().await.is_none());
    }
}
