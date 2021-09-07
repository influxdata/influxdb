use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc, task::Poll};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use data_types::database_rules::WriteBufferSequencerCreation;
use entry::{Entry, Sequence, SequencedEntry};
use futures::{stream, FutureExt, StreamExt};
use parking_lot::Mutex;

use crate::core::{
    EntryStream, FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting,
};

type EntryResVec = Vec<Result<SequencedEntry, WriteBufferError>>;

/// Mocked entries for [`MockBufferForWriting`] and [`MockBufferForReading`].
#[derive(Debug, Clone)]
pub struct MockBufferSharedState {
    /// Lock-protected entries.
    ///
    /// The inner `Option` is `None` if the sequencers are not created yet.
    entries: Arc<Mutex<Option<BTreeMap<u32, EntryResVec>>>>,
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
            entries: Arc::new(Mutex::new(None)),
        }
    }

    /// Initialize shared state w/ N sequencers.
    ///
    /// # Panics
    /// - when state is already initialized
    pub fn init(&self, n_sequencers: NonZeroU32) {
        let mut guard = self.entries.lock();

        if guard.is_some() {
            panic!("already initialized");
        }

        *guard = Some(Self::init_inner(n_sequencers));
    }

    fn init_inner(n_sequencers: NonZeroU32) -> BTreeMap<u32, EntryResVec> {
        (0..n_sequencers.get())
            .map(|sequencer_id| (sequencer_id, vec![]))
            .collect()
    }

    /// Push a new entry to the specified sequencer.
    ///
    /// # Panics
    /// - when given entry is not sequenced
    /// - when no sequencer was initialized
    /// - when specified sequencer does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_entry(&self, entry: SequencedEntry) {
        let sequence = entry.sequence().expect("entry must be sequenced");

        let mut guard = self.entries.lock();
        let entries = guard.as_mut().expect("no sequencers initialized");
        let entry_vec = entries.get_mut(&sequence.id).expect("invalid sequencer ID");

        let max_sequence_number = entry_vec
            .iter()
            .filter_map(|entry_res| {
                entry_res
                    .as_ref()
                    .ok()
                    .map(|entry| entry.sequence().unwrap().number)
            })
            .max();
        if let Some(max_sequence_number) = max_sequence_number {
            assert!(
                max_sequence_number < sequence.number,
                "sequence number {} is less/equal than current max sequencer number {}",
                sequence.number,
                max_sequence_number
            );
        }

        entry_vec.push(Ok(entry));
    }

    /// Push error to specified sequencer.
    ///
    /// # Panics
    /// - when no sequencer was initialized
    /// - when sequencer does not exist
    pub fn push_error(&self, error: WriteBufferError, sequencer_id: u32) {
        let mut guard = self.entries.lock();
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
    pub fn get_messages(&self, sequencer_id: u32) -> Vec<Result<SequencedEntry, WriteBufferError>> {
        let mut guard = self.entries.lock();
        let entries = guard.as_mut().expect("no sequencers initialized");
        let entry_vec = entries
            .get_mut(&sequencer_id)
            .expect("invalid sequencer ID");

        entry_vec
            .iter()
            .map(|entry_res| match entry_res {
                Ok(entry) => Ok(entry.clone()),
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
        let mut guard = self.entries.lock();
        let entries = guard.as_mut().expect("no sequencers initialized");
        let entry_vec = entries
            .get_mut(&sequencer_id)
            .expect("invalid sequencer ID");

        entry_vec.clear();
    }

    fn maybe_auto_init(&self, auto_create_sequencers: Option<&WriteBufferSequencerCreation>) {
        if let Some(cfg) = auto_create_sequencers {
            let mut guard = self.entries.lock();
            if guard.is_none() {
                *guard = Some(Self::init_inner(cfg.n_sequencers));
            }
        }
    }
}

#[derive(Debug)]
pub struct MockBufferForWriting {
    state: MockBufferSharedState,
}

impl MockBufferForWriting {
    pub fn new(
        state: MockBufferSharedState,
        auto_create_sequencers: Option<&WriteBufferSequencerCreation>,
    ) -> Result<Self, WriteBufferError> {
        state.maybe_auto_init(auto_create_sequencers);

        {
            let guard = state.entries.lock();
            if guard.is_none() {
                return Err("no sequencers initialized".to_string().into());
            }
        }

        Ok(Self { state })
    }
}

#[async_trait]
impl WriteBufferWriting for MockBufferForWriting {
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
    ) -> Result<(Sequence, DateTime<Utc>), WriteBufferError> {
        let mut guard = self.state.entries.lock();
        let entries = guard.as_mut().unwrap();
        let sequencer_entries = entries.get_mut(&sequencer_id).unwrap();

        let sequence_number = sequencer_entries
            .iter()
            .filter_map(|entry_res| {
                entry_res
                    .as_ref()
                    .ok()
                    .map(|entry| entry.sequence().unwrap().number)
            })
            .max()
            .map(|n| n + 1)
            .unwrap_or(0);

        let sequence = Sequence {
            id: sequencer_id,
            number: sequence_number,
        };
        let timestamp = Utc::now();
        sequencer_entries.push(Ok(SequencedEntry::new_from_sequence(
            sequence,
            timestamp,
            entry.clone(),
        )));

        Ok((sequence, timestamp))
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
    async fn store_entry(
        &self,
        _entry: &Entry,
        _sequencer_id: u32,
    ) -> Result<(Sequence, DateTime<Utc>), WriteBufferError> {
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
        auto_create_sequencers: Option<&WriteBufferSequencerCreation>,
    ) -> Result<Self, WriteBufferError> {
        state.maybe_auto_init(auto_create_sequencers);

        let n_sequencers = {
            let guard = state.entries.lock();
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
    fn streams(&mut self) -> Vec<(u32, EntryStream<'_>)> {
        let sequencer_ids: Vec<_> = {
            let playback_states = self.playback_states.lock();
            playback_states.keys().copied().collect()
        };

        let mut streams = vec![];
        for sequencer_id in sequencer_ids {
            let shared_state = self.shared_state.clone();
            let playback_states = Arc::clone(&self.playback_states);

            let stream = stream::poll_fn(move |_ctx| {
                let guard = shared_state.entries.lock();
                let entries = guard.as_ref().unwrap();
                let entry_vec = entries.get(&sequencer_id).unwrap();

                let mut playback_states = playback_states.lock();
                let playback_state = playback_states.get_mut(&sequencer_id).unwrap();

                while entry_vec.len() > playback_state.vector_index {
                    let entry_result = &entry_vec[playback_state.vector_index];

                    // consume entry
                    playback_state.vector_index += 1;

                    match entry_result {
                        Ok(entry) => {
                            // found an entry => need to check if it is within the offset
                            let sequence = entry.sequence().unwrap();
                            if sequence.number >= playback_state.offset {
                                // within offset => return entry to caller
                                return Poll::Ready(Some(Ok(entry.clone())));
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
                Poll::Pending
            })
            .boxed();

            let shared_state = self.shared_state.clone();

            let fetch_high_watermark = move || {
                let shared_state = shared_state.clone();

                let fut = async move {
                    let guard = shared_state.entries.lock();
                    let entries = guard.as_ref().unwrap();
                    let entry_vec = entries.get(&sequencer_id).unwrap();
                    let watermark = entry_vec
                        .iter()
                        .filter_map(|entry_res| {
                            entry_res
                                .as_ref()
                                .ok()
                                .map(|entry| entry.sequence().unwrap().number)
                        })
                        .max()
                        .map(|n| n + 1)
                        .unwrap_or(0);

                    Ok(watermark)
                };
                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.push((
                sequencer_id,
                EntryStream {
                    stream,
                    fetch_high_watermark,
                },
            ));
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
    fn streams(&mut self) -> Vec<(u32, EntryStream<'_>)> {
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
        vec![(
            0,
            EntryStream {
                stream,
                fetch_high_watermark,
            },
        )]
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

    use entry::test_helpers::lp_to_entry;

    use crate::core::test_utils::{perform_generic_tests, TestAdapter, TestContext};

    use super::*;

    struct MockTestAdapter {}

    #[async_trait]
    impl TestAdapter for MockTestAdapter {
        type Context = MockTestContext;

        async fn new_context(&self, n_sequencers: NonZeroU32) -> Self::Context {
            MockTestContext {
                state: MockBufferSharedState::uninitialized(),
                n_sequencers,
            }
        }
    }

    struct MockTestContext {
        state: MockBufferSharedState,
        n_sequencers: NonZeroU32,
    }

    impl MockTestContext {
        fn auto_create_sequencers(&self, value: bool) -> Option<WriteBufferSequencerCreation> {
            value.then(|| WriteBufferSequencerCreation {
                n_sequencers: self.n_sequencers,
                ..Default::default()
            })
        }
    }

    #[async_trait]
    impl TestContext for MockTestContext {
        type Writing = MockBufferForWriting;

        type Reading = MockBufferForReading;

        async fn writing(
            &self,
            auto_create_sequencers: bool,
        ) -> Result<Self::Writing, WriteBufferError> {
            MockBufferForWriting::new(
                self.state.clone(),
                self.auto_create_sequencers(auto_create_sequencers).as_ref(),
            )
        }

        async fn reading(
            &self,
            auto_create_sequencers: bool,
        ) -> Result<Self::Reading, WriteBufferError> {
            MockBufferForReading::new(
                self.state.clone(),
                self.auto_create_sequencers(auto_create_sequencers).as_ref(),
            )
        }
    }

    #[tokio::test]
    async fn test_generic() {
        perform_generic_tests(MockTestAdapter {}).await;
    }

    #[test]
    #[should_panic(expected = "entry must be sequenced")]
    fn test_state_push_entry_panic_unsequenced() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let entry = lp_to_entry("upc,region=east user=1 100");
        state.push_entry(SequencedEntry::new_unsequenced(entry));
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_push_entry_panic_wrong_sequencer() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence = Sequence::new(2, 0);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence,
            Utc::now(),
            entry,
        ));
    }

    #[test]
    #[should_panic(expected = "no sequencers initialized")]
    fn test_state_push_entry_panic_uninitialized() {
        let state = MockBufferSharedState::uninitialized();
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence = Sequence::new(0, 0);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence,
            Utc::now(),
            entry,
        ));
    }

    #[test]
    #[should_panic(
        expected = "sequence number 13 is less/equal than current max sequencer number 13"
    )]
    fn test_state_push_entry_panic_wrong_sequence_number_equal() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence = Sequence::new(1, 13);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence,
            Utc::now(),
            entry.clone(),
        ));
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence,
            Utc::now(),
            entry,
        ));
    }

    #[test]
    #[should_panic(
        expected = "sequence number 12 is less/equal than current max sequencer number 13"
    )]
    fn test_state_push_entry_panic_wrong_sequence_number_less() {
        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence_1 = Sequence::new(1, 13);
        let sequence_2 = Sequence::new(1, 12);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence_1,
            Utc::now(),
            entry.clone(),
        ));
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence_2,
            Utc::now(),
            entry,
        ));
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

        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence_1 = Sequence::new(0, 11);
        let sequence_2 = Sequence::new(1, 12);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence_1,
            Utc::now(),
            entry.clone(),
        ));
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence_2,
            Utc::now(),
            entry,
        ));

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
        let (_id, mut stream) = streams.pop().unwrap();
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

        let entry = lp_to_entry("upc user=1 100");
        assert_eq!(
            writer.store_entry(&entry, 0).await.unwrap_err().to_string(),
            "Something bad happened on the way to writing an entry in the write buffer"
        );
    }

    #[test]
    fn test_delayed_init() {
        let state = MockBufferSharedState::uninitialized();
        state.init(NonZeroU32::try_from(2).unwrap());

        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence_1 = Sequence::new(0, 11);
        state.push_entry(SequencedEntry::new_from_sequence(
            sequence_1,
            Utc::now(),
            entry,
        ));

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
}
