use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::Arc,
    task::{Poll, Waker},
};

use async_trait::async_trait;
use futures::{stream, FutureExt, StreamExt};
use parking_lot::Mutex;

use data_types::sequence::Sequence;
use data_types::write_buffer::WriteBufferCreationConfig;
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use time::TimeProvider;

use crate::core::{
    FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting, WriteStream,
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
                    Some(current) => current.max(seqno.number),
                    None => seqno.number,
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
        let writes_vec = writes.get_mut(&sequence.id).expect("invalid sequencer ID");

        if let Some(max_sequence_number) = writes_vec.max_seqno {
            assert!(
                max_sequence_number < sequence.number,
                "sequence number {} is less/equal than current max sequencer number {}",
                sequence.number,
                max_sequence_number
            );
        }

        writes_vec.push(Ok(write));
    }

    /// Push line protocol data with placeholder values used for write metadata
    pub fn push_lp(&self, sequence: Sequence, lp: &str) {
        let tables = mutable_batch_lp::lines_to_batches(lp, 0).unwrap();
        let meta = DmlMeta::sequenced(sequence, time::Time::from_timestamp_nanos(0), None, 0);
        self.push_write(DmlWrite::new(tables, meta))
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
            id: sequencer_id,
            number: sequence_number,
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

    fn type_name(&self) -> &'static str {
        "mock_failing"
    }
}

/// Sequencer-specific playback state
struct PlaybackState {
    /// Index within the entry vector.
    vector_index: usize,

    /// Offset within the sequencer IDs.
    offset: u64,
}

pub struct MockBufferForReading {
    shared_state: MockBufferSharedState,
    playback_states: Arc<Mutex<BTreeMap<u32, PlaybackState>>>,
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
        let playback_states: BTreeMap<_, _> = (0..n_sequencers)
            .map(|sequencer_id| {
                (
                    sequencer_id,
                    PlaybackState {
                        vector_index: 0,
                        offset: 0,
                    },
                )
            })
            .collect();

        Ok(Self {
            shared_state: state,
            playback_states: Arc::new(Mutex::new(playback_states)),
        })
    }
}

impl std::fmt::Debug for MockBufferForReading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBufferForReading").finish()
    }
}

#[async_trait]
impl WriteBufferReading for MockBufferForReading {
    fn streams(&mut self) -> BTreeMap<u32, WriteStream<'_>> {
        let sequencer_ids: Vec<_> = {
            let playback_states = self.playback_states.lock();
            playback_states.keys().copied().collect()
        };

        let mut streams = BTreeMap::new();
        for sequencer_id in sequencer_ids {
            let shared_state = self.shared_state.clone();
            let playback_states = Arc::clone(&self.playback_states);

            let stream = stream::poll_fn(move |cx| {
                let mut guard = shared_state.writes.lock();
                let writes = guard.as_mut().unwrap();
                let writes_vec = writes.get_mut(&sequencer_id).unwrap();

                let mut playback_states = playback_states.lock();
                let playback_state = playback_states.get_mut(&sequencer_id).unwrap();

                let entries = &writes_vec.writes;
                while entries.len() > playback_state.vector_index {
                    let write_result = &entries[playback_state.vector_index];

                    // consume entry
                    playback_state.vector_index += 1;

                    match write_result {
                        Ok(write) => {
                            // found an entry => need to check if it is within the offset
                            let sequence = write.meta().sequence().unwrap();
                            if sequence.number >= playback_state.offset {
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

                // we are at the end of the recorded entries => report pending
                writes_vec.register_waker(cx.waker());
                Poll::Pending
            })
            .boxed();

            let shared_state = self.shared_state.clone();

            let fetch_high_watermark = move || {
                let shared_state = shared_state.clone();

                let fut = async move {
                    let guard = shared_state.writes.lock();
                    let entries = guard.as_ref().unwrap();
                    let entry_vec = entries.get(&sequencer_id).unwrap();
                    let watermark = entry_vec.max_seqno.map(|n| n + 1).unwrap_or(0);

                    Ok(watermark)
                };
                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.insert(
                sequencer_id,
                WriteStream {
                    stream,
                    fetch_high_watermark,
                },
            );
        }

        streams
    }

    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        let mut playback_states = self.playback_states.lock();

        if let Some(playback_state) = playback_states.get_mut(&sequencer_id) {
            playback_state.offset = sequence_number;

            // reset position to start since seeking might go backwards
            playback_state.vector_index = 0;
        }

        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "mock"
    }
}
/// A [`WriteBufferReading`] that will error for every action.
#[derive(Debug, Default, Clone, Copy)]
pub struct MockBufferForReadingThatAlwaysErrors;

#[async_trait]
impl WriteBufferReading for MockBufferForReadingThatAlwaysErrors {
    fn streams(&mut self) -> BTreeMap<u32, WriteStream<'_>> {
        let stream = stream::poll_fn(|_ctx| {
            Poll::Ready(Some(Err(String::from(
                "Something bad happened while reading from stream",
            )
            .into())))
        })
        .boxed();
        let fetch_high_watermark = move || {
            let fut = async move {
                Err(String::from("Something bad happened while fetching the high watermark").into())
            };
            fut.boxed() as FetchHighWatermarkFut<'_>
        };
        let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;
        IntoIterator::into_iter([(
            0,
            WriteStream {
                stream,
                fetch_high_watermark,
            },
        )])
        .collect()
    }

    async fn seek(
        &mut self,
        _sequencer_id: u32,
        _sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        Err(String::from("Something bad happened while seeking the stream").into())
    }

    fn type_name(&self) -> &'static str {
        "mock_failing"
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::time::Duration;

    use mutable_batch_lp::lines_to_batches;
    use time::TimeProvider;

    use crate::core::test_utils::{map_pop_first, perform_generic_tests, TestAdapter, TestContext};

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
            }
        }
    }

    struct MockTestContext {
        state: MockBufferSharedState,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
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
        state.push_write(DmlWrite::new(tables, DmlMeta::unsequenced(None)));
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
        let mut reader = MockBufferForReadingThatAlwaysErrors {};

        assert_eq!(
            reader.seek(0, 0).await.unwrap_err().to_string(),
            "Something bad happened while seeking the stream"
        );

        let mut streams = reader.streams();
        let (_id, mut stream) = map_pop_first(&mut streams).unwrap();
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap_err().to_string(),
            "Something bad happened while reading from stream"
        );
        assert_eq!(
            (stream.fetch_high_watermark)()
                .await
                .unwrap_err()
                .to_string(),
            "Something bad happened while fetching the high watermark"
        );
    }

    #[tokio::test]
    async fn test_always_error_write() {
        let writer = MockBufferForWritingThatAlwaysErrors {};

        let tables = lines_to_batches("upc user=1 100", 0).unwrap();
        let operation = DmlOperation::Write(DmlWrite::new(tables, Default::default()));

        assert_eq!(
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

        let mut read = MockBufferForReading::new(state.clone(), None).unwrap();
        let playback_state = Arc::clone(&read.playback_states);

        let consumer = tokio::spawn(async move {
            let mut stream = map_pop_first(&mut read.streams()).unwrap().1.stream;
            stream.next().await.unwrap().unwrap();
            stream.next().await.unwrap().unwrap();
        });

        // Wait for consumer to read first entry
        while playback_state.lock().get(&0).unwrap().vector_index < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        state.push_lp(Sequence::new(0, 1), "mem foo=2 20");

        tokio::time::timeout(Duration::from_millis(100), consumer)
            .await
            .unwrap()
            .unwrap();
    }
}
