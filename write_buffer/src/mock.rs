use std::{collections::BTreeMap, sync::Arc, task::Poll};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
    entries: Arc<Mutex<BTreeMap<u32, EntryResVec>>>,
}

impl MockBufferSharedState {
    /// Create new shared state w/ N sequencers.
    pub fn empty_with_n_sequencers(n_sequencers: u32) -> Self {
        let entries: BTreeMap<_, _> = (0..n_sequencers)
            .map(|sequencer_id| (sequencer_id, vec![]))
            .collect();

        Self {
            entries: Arc::new(Mutex::new(entries)),
        }
    }

    /// Push a new entry to the specified sequencer.
    ///
    /// # Panics
    /// - when given entry is not sequenced
    /// - when specified sequencer does not exist
    /// - when sequence number in entry is not larger the current maximum
    pub fn push_entry(&self, entry: SequencedEntry) {
        let sequence = entry.sequence().expect("entry must be sequenced");
        let mut entries = self.entries.lock();
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
    /// - when sequencer does not exist
    pub fn push_error(&self, error: WriteBufferError, sequencer_id: u32) {
        let mut entries = self.entries.lock();
        let entry_vec = entries
            .get_mut(&sequencer_id)
            .expect("invalid sequencer ID");
        entry_vec.push(Err(error));
    }

    /// Get messages (entries and errors) for specified sequencer.
    ///
    /// # Panics
    /// - when sequencer does not exist
    pub fn get_messages(&self, sequencer_id: u32) -> Vec<Result<SequencedEntry, WriteBufferError>> {
        let mut entries = self.entries.lock();
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
}

#[derive(Debug)]
pub struct MockBufferForWriting {
    state: MockBufferSharedState,
}

impl MockBufferForWriting {
    pub fn new(state: MockBufferSharedState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl WriteBufferWriting for MockBufferForWriting {
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
    ) -> Result<(Sequence, DateTime<Utc>), WriteBufferError> {
        let mut entries = self.state.entries.lock();
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
}

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
    pub fn new(state: MockBufferSharedState) -> Self {
        let n_sequencers = state.entries.lock().len() as u32;
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

        Self {
            shared_state: state,
            playback_states: Arc::new(Mutex::new(playback_states)),
        }
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
                let entries = shared_state.entries.lock();
                let mut playback_states = playback_states.lock();

                let entry_vec = entries.get(&sequencer_id).unwrap();
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
                    let entries = shared_state.entries.lock();
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
}

#[cfg(test)]
mod tests {
    use entry::test_helpers::lp_to_entry;

    use crate::core::test_utils::{perform_generic_tests, TestAdapter, TestContext};

    use super::*;

    struct MockTestAdapter {}

    #[async_trait]
    impl TestAdapter for MockTestAdapter {
        type Context = MockTestContext;

        async fn new_context(&self, n_sequencers: u32) -> Self::Context {
            MockTestContext {
                state: MockBufferSharedState::empty_with_n_sequencers(n_sequencers),
            }
        }
    }

    struct MockTestContext {
        state: MockBufferSharedState,
    }

    #[async_trait]
    impl TestContext for MockTestContext {
        type Writing = MockBufferForWriting;

        type Reading = MockBufferForReading;

        fn writing(&self) -> Self::Writing {
            MockBufferForWriting::new(self.state.clone())
        }

        async fn reading(&self) -> Self::Reading {
            MockBufferForReading::new(self.state.clone())
        }
    }

    #[tokio::test]
    async fn test_generic() {
        perform_generic_tests(MockTestAdapter {}).await;
    }

    #[test]
    #[should_panic(expected = "entry must be sequenced")]
    fn test_state_push_entry_panic_unsequenced() {
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
        let entry = lp_to_entry("upc,region=east user=1 100");
        state.push_entry(SequencedEntry::new_unsequenced(entry));
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_push_entry_panic_wrong_sequencer() {
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence = Sequence::new(2, 0);
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
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
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
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
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
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
        let error = "foo".to_string().into();
        state.push_error(error, 2);
    }

    #[test]
    #[should_panic(expected = "invalid sequencer ID")]
    fn test_state_get_messages_panic_wrong_sequencer() {
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
        state.get_messages(2);
    }
}
