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
    /// Send an `Entry` to the write buffer using the specified sequencer ID.
    ///
    /// Returns information that can be used to restore entries at a later time.
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
    ) -> Result<Sequence, WriteBufferError>;
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

pub mod test_utils {
    use async_trait::async_trait;
    use entry::{test_helpers::lp_to_entry, Entry};
    use futures::{StreamExt, TryStreamExt};

    use super::{WriteBufferReading, WriteBufferWriting};

    #[async_trait]
    pub trait TestAdapter: Send + Sync {
        type Context: TestContext;

        async fn new_context(&self, n_sequencers: u32) -> Self::Context;
    }

    pub trait TestContext: Send + Sync {
        type Writing: WriteBufferWriting;
        type Reading: WriteBufferReading;

        fn writing(&self) -> Self::Writing;

        fn reading(&self) -> Self::Reading;
    }

    pub async fn perform_generic_tests<T>(adapter: T)
    where
        T: TestAdapter,
    {
        test_single_stream_io(&adapter).await;
        test_multi_stream_io(&adapter).await;
        test_multi_writer_multi_reader(&adapter).await;
    }

    async fn test_single_stream_io<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(1).await;

        let entry_1 = lp_to_entry("upc user=1 100");
        let entry_2 = lp_to_entry("upc user=2 200");
        let entry_3 = lp_to_entry("upc user=3 300");

        let writer = context.writing();
        let reader = context.reading();

        let mut stream = reader.stream();
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        // empty stream is pending
        assert!(stream.poll_next_unpin(&mut cx).is_pending());

        // adding content allows us to get results
        writer.store_entry(&entry_1, 0).await.unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().entry(), &entry_1);

        // stream is pending again
        assert!(stream.poll_next_unpin(&mut cx).is_pending());

        // adding more data unblocks the stream
        writer.store_entry(&entry_2, 0).await.unwrap();
        writer.store_entry(&entry_3, 0).await.unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().entry(), &entry_2);
        assert_eq!(stream.next().await.unwrap().unwrap().entry(), &entry_3);

        // stream is pending again
        assert!(stream.poll_next_unpin(&mut cx).is_pending());
    }

    async fn test_multi_stream_io<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(1).await;

        let entry_1 = lp_to_entry("upc user=1 100");
        let entry_2 = lp_to_entry("upc user=2 200");
        let entry_3 = lp_to_entry("upc user=3 300");

        let writer = context.writing();
        let reader = context.reading();

        let mut stream_1 = reader.stream();
        let mut stream_2 = reader.stream();
        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        // empty streams is pending
        assert!(stream_1.poll_next_unpin(&mut cx).is_pending());
        assert!(stream_2.poll_next_unpin(&mut cx).is_pending());

        // streams poll from same source
        writer.store_entry(&entry_1, 0).await.unwrap();
        writer.store_entry(&entry_2, 0).await.unwrap();
        writer.store_entry(&entry_3, 0).await.unwrap();
        assert_eq!(stream_1.next().await.unwrap().unwrap().entry(), &entry_1);
        assert_eq!(stream_2.next().await.unwrap().unwrap().entry(), &entry_2);
        assert_eq!(stream_1.next().await.unwrap().unwrap().entry(), &entry_3);

        // stream1 is pending again
        assert!(stream_1.poll_next_unpin(&mut cx).is_pending());
        assert!(stream_2.poll_next_unpin(&mut cx).is_pending());
    }

    async fn test_multi_writer_multi_reader<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(2).await;

        let entry_east_1 = lp_to_entry("upc,region=east user=1 100");
        let entry_east_2 = lp_to_entry("upc,region=east user=2 200");
        let entry_west_1 = lp_to_entry("upc,region=west user=1 200");

        let writer_1 = context.writing();
        let writer_2 = context.writing();
        let reader_1 = context.reading();
        let reader_2 = context.reading();

        // TODO: do not hard-code sequencer IDs here but provide a proper interface
        writer_1.store_entry(&entry_east_1, 0).await.unwrap();
        writer_1.store_entry(&entry_west_1, 1).await.unwrap();
        writer_2.store_entry(&entry_east_2, 0).await.unwrap();

        assert_reader_content(reader_1, &[&entry_east_1, &entry_east_2, &entry_west_1]).await;
        assert_reader_content(reader_2, &[&entry_east_1, &entry_east_2, &entry_west_1]).await;
    }

    async fn assert_reader_content<R>(reader: R, expected: &[&Entry])
    where
        R: WriteBufferReading,
    {
        // we need to limit the stream to `expected.len()` elements, otherwise it might be pending forever
        let mut results: Vec<_> = reader
            .stream()
            .take(expected.len())
            .try_collect()
            .await
            .unwrap();
        results.sort_by_key(|entry| {
            let sequence = entry.sequence().unwrap();
            (sequence.id, sequence.number)
        });
        let actual: Vec<_> = results.iter().map(|entry| entry.entry()).collect();
        assert_eq!(&actual[..], expected);
    }
}
