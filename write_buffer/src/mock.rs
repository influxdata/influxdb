use std::{collections::BTreeMap, sync::Arc, task::Poll};

use async_trait::async_trait;
use entry::{Entry, Sequence, SequencedEntry};
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use parking_lot::Mutex;

use crate::core::{WriteBufferError, WriteBufferReading, WriteBufferWriting};

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
    ) -> Result<Sequence, WriteBufferError> {
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
        sequencer_entries.push(Ok(SequencedEntry::new_from_sequence(
            sequence,
            entry.clone(),
        )
        .unwrap()));

        Ok(sequence)
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
    ) -> Result<Sequence, WriteBufferError> {
        Err(String::from(
            "Something bad happened on the way to writing an entry in the write buffer",
        )
        .into())
    }
}

pub struct MockBufferForReading {
    state: MockBufferSharedState,
    positions: Arc<Mutex<BTreeMap<u32, usize>>>,
}

impl MockBufferForReading {
    pub fn new(state: MockBufferSharedState) -> Self {
        let n_sequencers = state.entries.lock().len() as u32;
        let positions: BTreeMap<_, _> = (0..n_sequencers)
            .map(|sequencer_id| (sequencer_id, 0))
            .collect();

        Self {
            state,
            positions: Arc::new(Mutex::new(positions)),
        }
    }
}

impl std::fmt::Debug for MockBufferForReading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBufferForReading").finish()
    }
}

impl WriteBufferReading for MockBufferForReading {
    fn stream<'life0, 'async_trait>(
        &'life0 self,
    ) -> BoxStream<'async_trait, Result<SequencedEntry, WriteBufferError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let state = self.state.clone();
        let positions = Arc::clone(&self.positions);

        stream::poll_fn(move |_ctx| {
            let entries = state.entries.lock();
            let mut positions = positions.lock();

            for (sequencer_id, position) in positions.iter_mut() {
                let entry_vec = entries.get(sequencer_id).unwrap();
                if entry_vec.len() > *position {
                    let entry = match &entry_vec[*position] {
                        Ok(entry) => Ok(entry.clone()),
                        Err(e) => Err(e.to_string().into()),
                    };
                    *position += 1;
                    return Poll::Ready(Some(entry));
                }
            }

            Poll::Pending
        })
        .boxed()
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

    impl TestContext for MockTestContext {
        type Writing = MockBufferForWriting;

        type Reading = MockBufferForReading;

        fn writing(&self) -> Self::Writing {
            MockBufferForWriting::new(self.state.clone())
        }

        fn reading(&self) -> Self::Reading {
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
        state.push_entry(SequencedEntry::new_from_sequence(sequence, entry).unwrap());
    }

    #[test]
    #[should_panic(
        expected = "sequence number 13 is less/equal than current max sequencer number 13"
    )]
    fn test_state_push_entry_panic_wrong_sequence_number_equal() {
        let state = MockBufferSharedState::empty_with_n_sequencers(2);
        let entry = lp_to_entry("upc,region=east user=1 100");
        let sequence = Sequence::new(1, 13);
        state.push_entry(SequencedEntry::new_from_sequence(sequence, entry.clone()).unwrap());
        state.push_entry(SequencedEntry::new_from_sequence(sequence, entry).unwrap());
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
        state.push_entry(SequencedEntry::new_from_sequence(sequence_1, entry.clone()).unwrap());
        state.push_entry(SequencedEntry::new_from_sequence(sequence_2, entry).unwrap());
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
