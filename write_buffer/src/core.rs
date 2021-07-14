use async_trait::async_trait;
use entry::{Entry, Sequence, SequencedEntry};
use futures::stream::BoxStream;

/// Generic boxed error type that is used in this crate.
///
/// The dynamic boxing makes it easier to deal with error from different implementations.
pub type WriteBufferError = Box<dyn std::error::Error + Sync + Send>;

/// Writing to a Write Buffer takes an `Entry` and returns `Sequence` data that facilitates reading
/// entries from the Write Buffer at a later time.
#[async_trait]
pub trait WriteBufferWriting: Sync + Send + std::fmt::Debug + 'static {
    /// Send an `Entry` to the write buffer and return information that can be used to restore
    /// entries at a later time.
    async fn store_entry(&self, entry: &Entry) -> Result<Sequence, WriteBufferError>;
}

/// Produce a stream of `SequencedEntry` that a `Db` can add to the mutable buffer by using
/// `Db::stream_in_sequenced_entries`.
pub trait WriteBufferReading: Sync + Send + std::fmt::Debug + 'static {
    fn stream<'life0, 'async_trait>(
        &'life0 self,
    ) -> BoxStream<'async_trait, Result<SequencedEntry, WriteBufferError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
}
