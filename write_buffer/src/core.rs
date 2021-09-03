use std::fmt::Debug;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use entry::{Entry, Sequence, SequencedEntry};
use futures::{future::BoxFuture, stream::BoxStream};

/// Generic boxed error type that is used in this crate.
///
/// The dynamic boxing makes it easier to deal with error from different implementations.
pub type WriteBufferError = Box<dyn std::error::Error + Sync + Send>;

/// Writing to a Write Buffer takes an [`Entry`] and returns [`Sequence`] data that facilitates reading
/// entries from the Write Buffer at a later time.
#[async_trait]
pub trait WriteBufferWriting: Sync + Send + Debug + 'static {
    /// Send an `Entry` to the write buffer using the specified sequencer ID.
    ///
    /// Returns information that can be used to restore entries at a later time.
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
    ) -> Result<(Sequence, DateTime<Utc>), WriteBufferError>;

    /// Return type (like `"mock"` or `"kafka"`) of this writer.
    fn type_name(&self) -> &'static str;
}

pub type FetchHighWatermarkFut<'a> = BoxFuture<'a, Result<u64, WriteBufferError>>;
pub type FetchHighWatermark<'a> = Box<dyn (Fn() -> FetchHighWatermarkFut<'a>) + Send + Sync>;

/// Output stream of [`WriteBufferReading`].
pub struct EntryStream<'a> {
    /// Stream that produces entries.
    pub stream: BoxStream<'a, Result<SequencedEntry, WriteBufferError>>,

    /// Get high watermark (= what we believe is the next sequence number to be added).
    ///
    /// Can be used to calculate lag. Note that since the watermark is "next sequence ID number to be added", it starts
    /// at 0 and after the entry with sequence number 0 is added to the buffer, it is 1.
    pub fetch_high_watermark: FetchHighWatermark<'a>,
}

impl<'a> Debug for EntryStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntryStream").finish_non_exhaustive()
    }
}

/// Produce streams (one per sequencer) of [`SequencedEntry`]s.
#[async_trait]
pub trait WriteBufferReading: Sync + Send + Debug + 'static {
    /// Returns a stream per sequencer.
    ///
    /// Note that due to the mutable borrow, it is not possible to have multiple streams from the same
    /// [`WriteBufferReading`] instance at the same time. If all streams are dropped and requested again, the last
    /// offsets of the old streams will be the start offsets for the new streams. If you want to prevent that either
    /// create a new [`WriteBufferReading`] or use [`seek`](Self::seek).
    fn streams(&mut self) -> Vec<(u32, EntryStream<'_>)>;

    /// Seek given sequencer to given sequence number. The next output of related streams will be an entry with at least
    /// the given sequence number (the actual sequence number might be skipped due to "holes" in the stream).
    ///
    /// Note that due to the mutable borrow, it is not possible to seek while streams exists.
    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError>;

    /// Return type (like `"mock"` or `"kafka"`) of this reader.
    fn type_name(&self) -> &'static str;
}

pub mod test_utils {
    //! Generic tests for all write buffer implementations.
    use std::{convert::TryFrom, num::NonZeroU32, time::Duration};

    use async_trait::async_trait;
    use chrono::{DateTime, TimeZone, Utc};
    use entry::{test_helpers::lp_to_entry, Entry};
    use futures::{StreamExt, TryStreamExt};

    use super::{WriteBufferError, WriteBufferReading, WriteBufferWriting};

    /// Adapter to make a concrete write buffer implementation work w/ [`perform_generic_tests`].
    #[async_trait]
    pub trait TestAdapter: Send + Sync {
        /// The context type that is used.
        type Context: TestContext;

        /// Create a new context.
        ///
        /// This will be called multiple times during the test suite. Each resulting context must represent an isolated
        /// environment.
        async fn new_context(&self, n_sequencers: NonZeroU32) -> Self::Context;
    }

    /// Context used during testing.
    ///
    /// Represents an isolated environment. Actions like sequencer creations and writes must not leak across context boundaries.
    #[async_trait]
    pub trait TestContext: Send + Sync {
        /// Write buffer writer implementation specific to this context and adapter.
        type Writing: WriteBufferWriting;

        /// Write buffer reader implementation specific to this context and adapter.
        type Reading: WriteBufferReading;

        /// Create new writer.
        async fn writing(
            &self,
            auto_create_sequencers: bool,
        ) -> Result<Self::Writing, WriteBufferError>;

        /// Create new reader.
        async fn reading(
            &self,
            auto_create_sequencers: bool,
        ) -> Result<Self::Reading, WriteBufferError>;
    }

    /// Generic test suite that must be passed by all proper write buffer implementations.
    ///
    /// See [`TestAdapter`] for how to make a concrete write buffer implementation work with this test suite.
    ///
    /// Note that you might need more tests on top of this to assert specific implementation behaviors, edge cases, and
    /// error handling.
    pub async fn perform_generic_tests<T>(adapter: T)
    where
        T: TestAdapter,
    {
        test_single_stream_io(&adapter).await;
        test_multi_stream_io(&adapter).await;
        test_multi_sequencer_io(&adapter).await;
        test_multi_writer_multi_reader(&adapter).await;
        test_seek(&adapter).await;
        test_watermark(&adapter).await;
        test_timestamp(&adapter).await;
        test_sequencer_auto_creation(&adapter).await;
    }

    /// Test IO with a single writer and single reader stream.
    ///
    /// This tests that:
    /// - streams process data in order
    /// - readers can handle the "pending" state w/o erroring
    /// - readers unblock after being in "pending" state
    async fn test_single_stream_io<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;

        let entry_1 = lp_to_entry("upc user=1 100");
        let entry_2 = lp_to_entry("upc user=2 200");
        let entry_3 = lp_to_entry("upc user=3 300");

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (sequencer_id, mut stream) = streams.pop().unwrap();

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        // empty stream is pending
        assert!(stream.stream.poll_next_unpin(&mut cx).is_pending());

        // adding content allows us to get results
        writer.store_entry(&entry_1, sequencer_id).await.unwrap();
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_1
        );

        // stream is pending again
        assert!(stream.stream.poll_next_unpin(&mut cx).is_pending());

        // adding more data unblocks the stream
        writer.store_entry(&entry_2, sequencer_id).await.unwrap();
        writer.store_entry(&entry_3, sequencer_id).await.unwrap();
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_2
        );
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_3
        );

        // stream is pending again
        assert!(stream.stream.poll_next_unpin(&mut cx).is_pending());
    }

    /// Tests multiple subsequently created streams from a single reader.
    ///
    /// This tests that:
    /// - readers remember their offset (and "pending" state) even when streams are dropped
    async fn test_multi_stream_io<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;

        let entry_1 = lp_to_entry("upc user=1 100");
        let entry_2 = lp_to_entry("upc user=2 200");
        let entry_3 = lp_to_entry("upc user=3 300");

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        writer.store_entry(&entry_1, 0).await.unwrap();
        writer.store_entry(&entry_2, 0).await.unwrap();
        writer.store_entry(&entry_3, 0).await.unwrap();

        // creating stream, drop stream, re-create it => still starts at first entry
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, stream) = streams.pop().unwrap();
        drop(stream);
        drop(streams);
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = streams.pop().unwrap();
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_1
        );

        // re-creating stream after reading remembers offset
        drop(stream);
        drop(streams);
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = streams.pop().unwrap();
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_2
        );
        assert_eq!(
            stream.stream.next().await.unwrap().unwrap().entry(),
            &entry_3
        );

        // re-creating stream after reading everything makes it pending
        drop(stream);
        drop(streams);
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = streams.pop().unwrap();
        assert!(stream.stream.poll_next_unpin(&mut cx).is_pending());
    }

    /// Test single reader-writer IO w/ multiple sequencers.
    ///
    /// This tests that:
    /// - writes go to and reads come from the right sequencer, aka that sequencers provide a namespace-like isolation
    /// - "pending" states are specific to a sequencer
    async fn test_multi_sequencer_io<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(2).unwrap()).await;

        let entry_1 = lp_to_entry("upc user=1 100");
        let entry_2 = lp_to_entry("upc user=2 200");
        let entry_3 = lp_to_entry("upc user=3 300");

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let mut streams = reader.streams();
        assert_eq!(streams.len(), 2);
        let (sequencer_id_1, mut stream_1) = streams.pop().unwrap();
        let (sequencer_id_2, mut stream_2) = streams.pop().unwrap();
        assert_ne!(sequencer_id_1, sequencer_id_2);

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        // empty streams are pending
        assert!(stream_1.stream.poll_next_unpin(&mut cx).is_pending());
        assert!(stream_2.stream.poll_next_unpin(&mut cx).is_pending());

        // entries arrive at the right target stream
        writer.store_entry(&entry_1, sequencer_id_1).await.unwrap();
        assert_eq!(
            stream_1.stream.next().await.unwrap().unwrap().entry(),
            &entry_1
        );
        assert!(stream_2.stream.poll_next_unpin(&mut cx).is_pending());

        writer.store_entry(&entry_2, sequencer_id_2).await.unwrap();
        assert!(stream_1.stream.poll_next_unpin(&mut cx).is_pending());
        assert_eq!(
            stream_2.stream.next().await.unwrap().unwrap().entry(),
            &entry_2
        );

        writer.store_entry(&entry_3, sequencer_id_1).await.unwrap();
        assert!(stream_2.stream.poll_next_unpin(&mut cx).is_pending());
        assert_eq!(
            stream_1.stream.next().await.unwrap().unwrap().entry(),
            &entry_3
        );

        // streams are pending again
        assert!(stream_1.stream.poll_next_unpin(&mut cx).is_pending());
        assert!(stream_2.stream.poll_next_unpin(&mut cx).is_pending());
    }

    /// Test multiple multiple writers and multiple readers on multiple sequencers.
    ///
    /// This tests that:
    /// - writes go to and reads come from the right sequencer, similar to [`test_multi_sequencer_io`] but less
    ///   detailled
    /// - multiple writers can write to a single sequencer
    async fn test_multi_writer_multi_reader<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(2).unwrap()).await;

        let entry_east_1 = lp_to_entry("upc,region=east user=1 100");
        let entry_east_2 = lp_to_entry("upc,region=east user=2 200");
        let entry_west_1 = lp_to_entry("upc,region=west user=1 200");

        let writer_1 = context.writing(true).await.unwrap();
        let writer_2 = context.writing(true).await.unwrap();
        let mut reader_1 = context.reading(true).await.unwrap();
        let mut reader_2 = context.reading(true).await.unwrap();

        // TODO: do not hard-code sequencer IDs here but provide a proper interface
        writer_1.store_entry(&entry_east_1, 0).await.unwrap();
        writer_1.store_entry(&entry_west_1, 1).await.unwrap();
        writer_2.store_entry(&entry_east_2, 0).await.unwrap();

        assert_reader_content(
            &mut reader_1,
            &[(0, &[&entry_east_1, &entry_east_2]), (1, &[&entry_west_1])],
        )
        .await;
        assert_reader_content(
            &mut reader_2,
            &[(0, &[&entry_east_1, &entry_east_2]), (1, &[&entry_west_1])],
        )
        .await;
    }

    /// Test seek implemention of readers.
    ///
    /// This tests that:
    /// - seeking is specific to the reader AND sequencer
    /// - forward and backwards seeking works
    /// - seeking past the end of the known content works (results in "pending" status and remembers sequence number and
    ///   not just "next entry")
    async fn test_seek<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(2).unwrap()).await;

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        let entry_east_1 = lp_to_entry("upc,region=east user=1 100");
        let entry_east_2 = lp_to_entry("upc,region=east user=2 200");
        let entry_east_3 = lp_to_entry("upc,region=east user=3 300");
        let entry_west_1 = lp_to_entry("upc,region=west user=1 200");

        let writer = context.writing(true).await.unwrap();
        let _sequence_number_east_1 = writer.store_entry(&entry_east_1, 0).await.unwrap().0.number;
        let sequence_number_east_2 = writer.store_entry(&entry_east_2, 0).await.unwrap().0.number;
        let _sequence_number_west_1 = writer.store_entry(&entry_west_1, 1).await.unwrap().0.number;

        let mut reader_1 = context.reading(true).await.unwrap();
        let mut reader_2 = context.reading(true).await.unwrap();

        // forward seek
        reader_1.seek(0, sequence_number_east_2).await.unwrap();
        assert_reader_content(
            &mut reader_1,
            &[(0, &[&entry_east_2]), (1, &[&entry_west_1])],
        )
        .await;
        assert_reader_content(
            &mut reader_2,
            &[(0, &[&entry_east_1, &entry_east_2]), (1, &[&entry_west_1])],
        )
        .await;

        // backward seek
        reader_1.seek(0, 0).await.unwrap();
        assert_reader_content(
            &mut reader_1,
            &[(0, &[&entry_east_1, &entry_east_2]), (1, &[])],
        )
        .await;

        // seek to far end and then at data
        reader_1.seek(0, 1_000_000).await.unwrap();
        let _sequence_number_east_3 = writer.store_entry(&entry_east_3, 0).await.unwrap().0.number;
        let mut streams = reader_1.streams();
        assert_eq!(streams.len(), 2);
        let (_sequencer_id, mut stream_1) = streams.pop().unwrap();
        let (_sequencer_id, mut stream_2) = streams.pop().unwrap();
        assert!(stream_1.stream.poll_next_unpin(&mut cx).is_pending());
        assert!(stream_2.stream.poll_next_unpin(&mut cx).is_pending());
        drop(stream_1);
        drop(stream_2);
        drop(streams);

        // seeking unknown sequencer is NOT an error
        reader_1.seek(0, 42).await.unwrap();
    }

    /// Test watermark fetching.
    ///
    /// This tests that:
    /// - watermarks for empty sequencers is 0
    /// - watermarks for non-empty sequencers is "last sequence ID plus 1"
    async fn test_watermark<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(2).unwrap()).await;

        let entry_east_1 = lp_to_entry("upc,region=east user=1 100");
        let entry_east_2 = lp_to_entry("upc,region=east user=2 200");
        let entry_west_1 = lp_to_entry("upc,region=west user=1 200");

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let mut streams = reader.streams();
        assert_eq!(streams.len(), 2);
        let (sequencer_id_1, stream_1) = streams.pop().unwrap();
        let (sequencer_id_2, stream_2) = streams.pop().unwrap();

        // start at watermark 0
        assert_eq!((stream_1.fetch_high_watermark)().await.unwrap(), 0);
        assert_eq!((stream_2.fetch_high_watermark)().await.unwrap(), 0);

        // high water mark moves
        writer
            .store_entry(&entry_east_1, sequencer_id_1)
            .await
            .unwrap();
        let mark_1 = writer
            .store_entry(&entry_east_2, sequencer_id_1)
            .await
            .unwrap()
            .0
            .number;
        let mark_2 = writer
            .store_entry(&entry_west_1, sequencer_id_2)
            .await
            .unwrap()
            .0
            .number;
        assert_eq!((stream_1.fetch_high_watermark)().await.unwrap(), mark_1 + 1);
        assert_eq!((stream_2.fetch_high_watermark)().await.unwrap(), mark_2 + 1);
    }

    /// Test that timestamps reported by the readers are sane.
    async fn test_timestamp<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;

        let entry = lp_to_entry("upc user=1 100");

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (sequencer_id, mut stream) = streams.pop().unwrap();

        // ingest data
        //
        // We want to capture the time of `store_entry`. However for certain sequencers (like Kafka) the time is
        // slightly imprecise in a way that it truncates the time to milliseconds. So the workaround in the test is:
        //
        // 1. Capture a `ts_pre` from which we know that it is close but less or equal to the store time. We use the
        //    wallclock for that but truncate to milliseconds.
        // 2. Capture a `ts_post` from which we know that it is close but greater or equal to the store time. We use
        //    the wallclock but if it has a sub-millisecond part we use the next millisecond (it's like a ceil
        //    operation).
        // 3. Wait a bit between step 2 and the restore operation so that we can be really sure that the restore
        //    operation must know the timestamp of the store operation and cannot just "guess" it.
        let ts_pre = timestamp_floor_millis(Utc::now());
        let reported_ts = writer.store_entry(&entry, sequencer_id).await.unwrap().1;
        let ts_post = timestamp_ceil_millis(Utc::now());

        // wait a bit
        tokio::time::sleep(Duration::from_millis(100)).await;

        // check that the timestamp records the ingestion time, not the read time
        let sequenced_entry = stream.stream.next().await.unwrap().unwrap();
        let ts_entry = sequenced_entry.producer_wallclock_timestamp().unwrap();
        assert!(ts_entry >= ts_pre, "{} >= {}", ts_entry, ts_pre);
        assert!(ts_entry <= ts_post, "{} <= {}", ts_entry, ts_post);
        assert_eq!(ts_entry, reported_ts);
    }

    /// Test that sequencer auto-creation works.
    ///
    /// This tests that:
    /// - both writer and reader cannot be constructed when sequencers are missing
    /// - both writer and reader cannot be auto-create sequencers
    async fn test_sequencer_auto_creation<T>(adapter: &T)
    where
        T: TestAdapter,
    {
        // fail when sequencers are missing
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;
        context.writing(false).await.unwrap_err();
        context.reading(false).await.unwrap_err();

        // writer can create sequencers
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;
        context.writing(true).await.unwrap();
        context.writing(false).await.unwrap();
        context.reading(false).await.unwrap();

        // reader can create sequencers
        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;
        context.reading(true).await.unwrap();
        context.reading(false).await.unwrap();
        context.writing(false).await.unwrap();
    }

    /// Assert that the content of the reader is as expected.
    ///
    /// This will read `expected.len()` from the reader and then ensures that the stream is pending.
    async fn assert_reader_content<R>(reader: &mut R, expected: &[(u32, &[&Entry])])
    where
        R: WriteBufferReading,
    {
        // Ensure content of the streams
        let mut streams = reader.streams();
        assert_eq!(streams.len(), expected.len());
        streams.sort_by_key(|(sequencer_id, _stream)| *sequencer_id);

        for ((actual_sequencer_id, actual_stream), (expected_sequencer_id, expected_entries)) in
            streams.into_iter().zip(expected.iter())
        {
            assert_eq!(actual_sequencer_id, *expected_sequencer_id);

            // we need to limit the stream to `expected.len()` elements, otherwise it might be pending forever
            let results: Vec<_> = actual_stream
                .stream
                .take(expected_entries.len())
                .try_collect()
                .await
                .unwrap();
            let actual_entries: Vec<_> = results.iter().map(|entry| entry.entry()).collect();
            assert_eq!(&&actual_entries[..], expected_entries);
        }

        // Ensure that streams a pending
        let mut streams = reader.streams();
        assert_eq!(streams.len(), expected.len());
        streams.sort_by_key(|(sequencer_id, _stream)| *sequencer_id);

        let waker = futures::task::noop_waker();
        let mut cx = futures::task::Context::from_waker(&waker);

        for (
            (actual_sequencer_id, mut actual_stream),
            (expected_sequencer_id, _expected_entries),
        ) in streams.into_iter().zip(expected.iter())
        {
            assert_eq!(actual_sequencer_id, *expected_sequencer_id);

            // empty stream is pending
            assert!(actual_stream.stream.poll_next_unpin(&mut cx).is_pending());
        }
    }

    /// Return largest "milliseconds only" timestamp less than or equal to the given timestamp.
    ///
    /// The result will not have micro- or nanoseconds attached.
    fn timestamp_floor_millis(ts: DateTime<Utc>) -> DateTime<Utc> {
        let millis = ts.timestamp_millis();
        Utc.timestamp_millis(millis)
    }

    /// Return smallest "milliseconds only" timestamp greater than or equal to the given timestamp.
    ///
    /// The result will not have micro- or nanoseconds attached.
    fn timestamp_ceil_millis(ts: DateTime<Utc>) -> DateTime<Utc> {
        let millis = ts.timestamp_millis();
        let ts2 = Utc.timestamp_millis(millis);
        if ts2 != ts {
            // ts has sub-milli precision, increase millis by 1 (ceiling)
            Utc.timestamp_millis(millis + 1)
        } else {
            ts2
        }
    }
}
