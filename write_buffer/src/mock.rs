use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::Arc,
    task::{Poll, Waker},
};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use parking_lot::Mutex;

use data_types::sequence::Sequence;
use data_types::write_buffer::WriteBufferCreationConfig;
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use time::TimeProvider;

use crate::core::{
    WriteBufferError, WriteBufferReading, WriteBufferStreamHandler, WriteBufferWriting,
};

#[derive(Debug, Default)]
struct WriteResVec {
    /// The maximum sequence number in the entries
    max_seqno: Option<u64>,

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
                    Some(current) => current.max(seqno.sequence_number),
                    None => seqno.sequence_number,
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
    /// The inner `Option` is `None` if the sequencers are not created yet.
    writes: Arc<Mutex<Option<BTreeMap<u32, WriteResVec>>>>,
}

impl MockBufferSharedState {
    /// Create new shared state w/ N sequencers.
    ///
    /// This is equivalent to [`uninitialized`](Self::uninitialized) followed by [`init`](Self::init).
    pub fn empty_with_n_sequencers(n_sequencers: NonZeroU32) -> Self {
        let state = Self::uninitialized();
        state.init(n_sequencers);
        state
    }

    /// Create new shared state w/o any sequencers.
    pub fn uninitialized() -> Self {
        Self {
            writes: Arc::new(Mutex::new(None)),
        }
    }

    /// Initialize shared state w/ N sequencers.
    ///
    /// # Panics
    /// - when state is already initialized
    pub fn init(&self, n_sequencers: NonZeroU32) {
        let mut guard = self.writes.lock();

        if guard.is_some() {
            panic!("already initialized");
        }

        *guard = Some(Self::init_inner(n_sequencers));
    }

    fn init_inner(n_sequencers: NonZeroU32) -> BTreeMap<u32, WriteResVec> {
        (0..n_sequencers.get())
            .map(|sequencer_id| (sequencer_id, Default::default()))
            .collect()
    }

    /// Push a new delete to the specified sequencer
    ///
    /// # Panics
    /// - when delete is not sequenced
    /// - when no sequencer was initialized
    /// - when specified sequencer does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_delete(&self, delete: DmlDelete) {
        self.push_operation(DmlOperation::Delete(delete))
    }

    /// Push a new entry to the specified sequencer.
    ///
    /// # Panics
    /// - when write is not sequenced
    /// - when no sequencer was initialized
    /// - when specified sequencer does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_write(&self, write: DmlWrite) {
        self.push_operation(DmlOperation::Write(write))
    }

    /// Push a new operation to the specified sequencer
    ///
    /// # Panics
    /// - when operation is not sequenced
    /// - when no sequencer was initialized
    /// - when specified sequencer does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_operation(&self, write: DmlOperation) {
        let sequence = write.meta().sequence().expect("write must be sequenced");
        assert!(
            write.meta().producer_ts().is_some(),
            "write must have timestamp"
        );

        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no sequencers initialized");
        let writes_vec = writes
            .get_mut(&sequence.sequencer_id)
            .expect("invalid sequencer ID");

        if let Some(max_sequence_number) = writes_vec.max_seqno {
            assert!(
                max_sequence_number < sequence.sequence_number,
                "sequence number {} is less/equal than current max sequencer number {}",
                sequence.sequence_number,
                max_sequence_number
            );
        }

        writes_vec.push(Ok(write));
    }

    /// Push line protocol data with placeholder values used for write metadata
    pub fn push_lp(&self, sequence: Sequence, lp: &str) {
        let tables = mutable_batch_lp::lines_to_batches(lp, 0).unwrap();
        let meta = DmlMeta::sequenced(sequence, time::Time::from_timestamp_nanos(0), None, 0);
        self.push_write(DmlWrite::new("foo", tables, meta))
    }

    /// Push error to specified sequencer.
    ///
    /// # Panics
    /// - when no sequencer was initialized
    /// - when sequencer does not exist
    pub fn push_error(&self, error: WriteBufferError, sequencer_id: u32) {
        let mut guard = self.writes.lock();
        let entries = guard.as_mut().expect("no sequencers initialized");
        let entry_vec = entries
            .get_mut(&sequencer_id)
            .expect("invalid sequencer ID");

        entry_vec.push(Err(error));
    }

    /// Get messages (entries and errors) for specified sequencer.
    ///
    /// # Panics
    /// - when no sequencer was initialized
    /// - when sequencer does not exist
    pub fn get_messages(&self, sequencer_id: u32) -> Vec<Result<DmlOperation, WriteBufferError>> {
        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no sequencers initialized");
        let writes_vec = writes.get_mut(&sequencer_id).expect("invalid sequencer ID");

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
    /// - when no sequencer was initialized
    /// - when sequencer does not exist
    pub fn clear_messages(&self, sequencer_id: u32) {
        let mut guard = self.writes.lock();
        let writes = guard.as_mut().expect("no sequencers initialized");
        let writes_vec = writes.get_mut(&sequencer_id).expect("invalid sequencer ID");

        std::mem::take(writes_vec);
    }

    fn maybe_auto_init(&self, creation_config: Option<&WriteBufferCreationConfig>) {
        if let Some(cfg) = creation_config {
            let mut guard = self.writes.lock();
            if guard.is_none() {
                *guard = Some(Self::init_inner(cfg.n_sequencers));
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
                return Err("no sequencers initialized".to_string().into());
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
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        let mut guard = self.state.writes.lock();
        let entries = guard.as_mut().unwrap();
        entries.keys().copied().collect()
    }

    async fn store_operation(
        &self,
        sequencer_id: u32,
        operation: &DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let mut guard = self.state.writes.lock();
        let writes = guard.as_mut().unwrap();
        let writes_vec = writes
            .get_mut(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown sequencer: {}", sequencer_id).into()
            })?;

        let sequence_number = writes_vec.max_seqno.map(|n| n + 1).unwrap_or(0);

        let sequence = Sequence {
            sequencer_id,
            sequence_number,
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

        let mut operation = operation.clone();
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
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        IntoIterator::into_iter([0]).collect()
    }

    async fn store_operation(
        &self,
        _sequencer_id: u32,
        _operation: &DmlOperation,
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
    shared_state: MockBufferSharedState,
    n_sequencers: u32,
}

impl MockBufferForReading {
    pub fn new(
        state: MockBufferSharedState,
        creation_config: Option<&WriteBufferCreationConfig>,
    ) -> Result<Self, WriteBufferError> {
        state.maybe_auto_init(creation_config);

        let n_sequencers = {
            let guard = state.writes.lock();
            let entries = match guard.as_ref() {
                Some(entries) => entries,
                None => {
                    return Err("no sequencers initialized".to_string().into());
                }
            };
            entries.len() as u32
        };

        Ok(Self {
            shared_state: state,
            n_sequencers,
        })
    }
}

/// Sequencer-specific playback state
#[derive(Debug)]
pub struct MockBufferStreamHandler {
    /// Shared state.
    shared_state: MockBufferSharedState,

    /// Own sequencer ID.
    sequencer_id: u32,

    /// Index within the entry vector.
    vector_index: usize,

    /// Offset within the sequencer IDs.
    offset: u64,

    /// Flags if the stream is terminated, e.g. due to "offset out of range"
    terminated: bool,
}

#[async_trait]
impl WriteBufferStreamHandler for MockBufferStreamHandler {
    async fn stream(&mut self) -> BoxStream<'_, Result<DmlOperation, WriteBufferError>> {
        futures::stream::poll_fn(|cx| {
            if self.terminated {
                return Poll::Ready(None);
            }

            let mut guard = self.shared_state.writes.lock();
            let writes = guard.as_mut().unwrap();
            let writes_vec = writes.get_mut(&self.sequencer_id).unwrap();

            let entries = &writes_vec.writes;
            while entries.len() > self.vector_index {
                let write_result = &entries[self.vector_index];

                // consume entry
                self.vector_index += 1;

                match write_result {
                    Ok(write) => {
                        // found an entry => need to check if it is within the offset
                        let sequence = write.meta().sequence().unwrap();
                        if sequence.sequence_number >= self.offset {
                            // within offset => return entry to caller
                            return Poll::Ready(Some(Ok(write.clone())));
                        } else {
                            // offset is larger then the current entry => ignore entry and try next
                            continue;
                        }
                    }
                    Err(e) => {
                        // found an error => return entry to caller
                        return Poll::Ready(Some(Err(e.to_string().into())));
                    }
                }
            }

            // check if we have seeked to far
            let next_offset = entries
                .iter()
                .filter_map(|write_result| {
                    if let Ok(write) = write_result {
                        let sequence = write.meta().sequence().unwrap();
                        Some(sequence.sequence_number)
                    } else {
                        None
                    }
                })
                .max()
                .map(|x| x + 1)
                .unwrap_or_default();
            if self.offset > next_offset {
                self.terminated = true;
                return Poll::Ready(Some(Err(WriteBufferError::unknown_sequence_number(
                    format!("unknown sequence number, high watermark is {next_offset}"),
                ))));
            }

            // we are at the end of the recorded entries => report pending
            writes_vec.register_waker(cx.waker());
            Poll::Pending
        })
        .boxed()
    }

    async fn seek(&mut self, sequence_number: u64) -> Result<(), WriteBufferError> {
        self.offset = sequence_number;

        // reset position to start since seeking might go backwards
        self.vector_index = 0;

        // reset termination state
        self.terminated = false;

        Ok(())
    }
}

#[async_trait]
impl WriteBufferReading for MockBufferForReading {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        (0..self.n_sequencers).into_iter().collect()
    }
    async fn stream_handler(
        &self,
        sequencer_id: u32,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        if sequencer_id >= self.n_sequencers {
            return Err(format!("Unknown sequencer: {}", sequencer_id).into());
        }

        Ok(Box::new(MockBufferStreamHandler {
            shared_state: self.shared_state.clone(),
            sequencer_id,
            vector_index: 0,
            offset: 0,
            terminated: false,
        }))
    }

    async fn fetch_high_watermark(&self, sequencer_id: u32) -> Result<u64, WriteBufferError> {
        let guard = self.shared_state.writes.lock();
        let entries = guard.as_ref().unwrap();
        let entry_vec = entries
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown sequencer: {}", sequencer_id).into()
            })?;
        let watermark = entry_vec.max_seqno.map(|n| n + 1).unwrap_or(0);

        Ok(watermark)
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
    async fn stream(&mut self) -> BoxStream<'_, Result<DmlOperation, WriteBufferError>> {
        futures::stream::poll_fn(|_cx| {
            Poll::Ready(Some(Err(String::from(
                "Something bad happened while reading from stream",
            )
            .into())))
        })
        .boxed()
    }

    async fn seek(&mut self, _sequence_number: u64) -> Result<(), WriteBufferError> {
        Err(String::from("Something bad happened while seeking the stream").into())
    }
}

#[async_trait]
impl WriteBufferReading for MockBufferForReadingThatAlwaysErrors {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        BTreeSet::from([0])
    }

    async fn stream_handler(
        &self,
        _sequencer_id: u32,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        Ok(Box::new(MockStreamHandlerThatAlwaysErrors {}))
    }

    async fn fetch_high_watermark(&self, _sequencer_id: u32) -> Result<u64, WriteBufferError> {
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

    use futures::StreamExt;
    use mutable_batch_lp::lines_to_batches;
    use test_helpers::assert_contains;
    use time::TimeProvider;
    use trace::RingBufferTraceCollector;

    use crate::core::test_utils::{perform_generic_tests, TestAdapter, TestContext};

    use super::*;

    struct MockTestAdapter {}

    #[async_trait]
    impl TestAdapter for MockTestAdapter {
        type Context = MockTestContext;

        async fn new_context_with_time(
            &self,
            n_sequencers: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            MockTestContext {
                state: MockBufferSharedState::uninitialized(),
                n_sequencers,
                time_provider,
                trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
            }
        }
    }

    struct MockTestContext {
        state: MockBufferSharedState,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
        trace_collector: Arc<RingBufferTraceCollector>,
    }

    impl MockTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_sequencers: self.n_sequencers,
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
    #[should_panic(expected = "write must be sequenced")]
    fn test_state_push_write_panic_unsequenced() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let tables = lines_to_batches("upc user=1 100", 0).unwrap();
        state.push_write(DmlWrite::new("test_db", tables, DmlMeta::unsequenced(None)));
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_push_write_panic_wrong_sequencer() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.push_lp(Sequence::new(2, 0), "upc,region=east user=1 100");
    }

    #[test]
    #[should_panic(expected = "no sequencers initialized")]
    fn test_state_push_write_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.push_lp(Sequence::new(0, 0), "upc,region=east user=1 100");
    }

    #[test]
    #[should_panic(
        expected = "sequence number 13 is less/equal than current max sequencer number 13"
    )]
    fn test_state_push_write_panic_wrong_sequence_number_equal() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.push_lp(Sequence::new(0, 13), "upc,region=east user=1 100");
        state.push_lp(Sequence::new(0, 13), "upc,region=east user=1 100");
    }

    #[test]
    #[should_panic(
        expected = "sequence number 12 is less/equal than current max sequencer number 13"
    )]
    fn test_state_push_write_panic_wrong_sequence_number_less() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.push_lp(Sequence::new(0, 13), "upc,region=east user=1 100");
        state.push_lp(Sequence::new(0, 12), "upc,region=east user=1 100");
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_push_error_panic_wrong_sequencer() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let error = "foo".to_string().into();
        state.push_error(error, 2);
    }

    #[test]
    #[should_panic(expected = "no sequencers initialized")]
    fn test_state_push_error_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        let error = "foo".to_string().into();
        state.push_error(error, 0);
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_get_messages_panic_wrong_sequencer() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.get_messages(2);
    }

    #[test]
    #[should_panic(expected = "no sequencers initialized")]
    fn test_state_get_messages_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.get_messages(0);
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_clear_messages_panic_wrong_sequencer() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.clear_messages(2);
    }

    #[test]
    #[should_panic(expected = "no sequencers initialized")]
    fn test_state_clear_messages_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        state.clear_messages(0);
    }

    #[test]
    fn test_clear_messages() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());

        state.push_lp(Sequence::new(0, 11), "upc user=1 100");
        state.push_lp(Sequence::new(1, 12), "upc user=1 100");

        assert_eq!(state.get_messages(0).len(), 1);
        assert_eq!(state.get_messages(1).len(), 1);

        state.clear_messages(0);

        assert_eq!(state.get_messages(0).len(), 0);
        assert_eq!(state.get_messages(1).len(), 1);
    }

    #[tokio::test]
    async fn test_always_error_read() {
        let reader = MockBufferForReadingThatAlwaysErrors {};

        assert_contains!(
            reader
                .fetch_high_watermark(0)
                .await
                .unwrap_err()
                .to_string(),
            "Something bad happened while fetching the high watermark"
        );

        let mut stream_handler = reader.stream_handler(0).await.unwrap();

        assert_contains!(
            stream_handler.seek(0).await.unwrap_err().to_string(),
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

        let tables = lines_to_batches("upc user=1 100", 0).unwrap();
        let operation = DmlOperation::Write(DmlWrite::new("test_db", tables, Default::default()));

        assert_contains!(
            writer
                .store_operation(0, &operation)
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

        state.push_lp(Sequence::new(0, 11), "upc user=1 100");

        assert_eq!(state.get_messages(0).len(), 1);
        assert_eq!(state.get_messages(1).len(), 0);

        let error = "foo".to_string().into();
        state.push_error(error, 1);

        assert_eq!(state.get_messages(0).len(), 1);
        assert_eq!(state.get_messages(1).len(), 1);

        state.clear_messages(0);

        assert_eq!(state.get_messages(0).len(), 0);
        assert_eq!(state.get_messages(1).len(), 1);
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
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        state.init(NonZeroU32::try_from(2).unwrap());
    }

    #[tokio::test]
    async fn test_delayed_insert() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());

        state.push_lp(Sequence::new(0, 0), "mem foo=1 10");

        let read = MockBufferForReading::new(state.clone(), None).unwrap();

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let consumer = tokio::spawn(async move {
            let mut stream_handler = read.stream_handler(0).await.unwrap();
            let mut stream = stream_handler.stream().await;
            stream.next().await.unwrap().unwrap();
            barrier_captured.wait().await;
            stream.next().await.unwrap().unwrap();
        });

        // Wait for consumer to read first entry
        barrier.wait().await;

        state.push_lp(Sequence::new(0, 1), "mem foo=2 20");

        tokio::time::timeout(Duration::from_millis(100), consumer)
            .await
            .unwrap()
            .unwrap();
    }
}
