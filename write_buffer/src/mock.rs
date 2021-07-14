use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use entry::{Entry, Sequence, SequencedEntry};
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};

use crate::core::{WriteBufferError, WriteBufferReading, WriteBufferWriting};

#[derive(Debug, Default)]
pub struct MockBufferForWriting {
    pub entries: Arc<Mutex<Vec<Entry>>>,
}

#[async_trait]
impl WriteBufferWriting for MockBufferForWriting {
    async fn store_entry(&self, entry: &Entry) -> Result<Sequence, WriteBufferError> {
        let mut entries = self.entries.lock().unwrap();
        let offset = entries.len() as u64;
        entries.push(entry.clone());

        Ok(Sequence {
            id: 0,
            number: offset,
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MockBufferForWritingThatAlwaysErrors;

#[async_trait]
impl WriteBufferWriting for MockBufferForWritingThatAlwaysErrors {
    async fn store_entry(&self, _entry: &Entry) -> Result<Sequence, WriteBufferError> {
        Err(String::from(
            "Something bad happened on the way to writing an entry in the write buffer",
        )
        .into())
    }
}

type MoveableEntries = Arc<Mutex<Vec<Result<SequencedEntry, WriteBufferError>>>>;
pub struct MockBufferForReading {
    entries: MoveableEntries,
}

impl std::fmt::Debug for MockBufferForReading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBufferForReading").finish()
    }
}

impl MockBufferForReading {
    pub fn new(entries: Vec<Result<SequencedEntry, WriteBufferError>>) -> Self {
        Self {
            entries: Arc::new(Mutex::new(entries)),
        }
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
        // move the entries out of `self` to move them into the stream
        let entries: Vec<_> = self.entries.lock().unwrap().drain(..).collect();

        stream::iter(entries.into_iter())
            .chain(stream::pending())
            .boxed()
    }
}
