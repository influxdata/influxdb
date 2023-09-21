//! Memory tracked parquet writer
use std::{fmt::Debug, io::Write, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    error::DataFusionError,
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
};
use observability_deps::tracing::warn;
use parquet::{arrow::ArrowWriter, errors::ParquetError, file::properties::WriterProperties};
use thiserror::Error;

/// Errors related to [`TrackedMemoryArrowWriter`]
#[derive(Debug, Error)]
pub enum Error {
    /// Writing the parquet file failed with the specified error.
    #[error("failed to write parquet file: {0}")]
    Writer(#[from] ParquetError),

    /// Could not allocate sufficient memory
    #[error("failed to allocate buffer while writing parquet: {0}")]
    OutOfMemory(#[from] DataFusionError),
}

/// Results!
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Wraps an [`ArrowWriter`] to track its buffered memory in a
/// DataFusion [`MemoryPool`]
///
/// If the memory used by the `ArrowWriter` exceeds the memory allowed
/// by the `MemoryPool`, subsequent writes will fail.
///
/// Note no attempt is made to cap the memory used by the
/// `ArrowWriter` (for example by flushing earlier), which might be a
/// useful exercise.
#[derive(Debug)]
pub struct TrackedMemoryArrowWriter<W: Write + Send> {
    /// The inner ArrowWriter
    inner: ArrowWriter<W>,
    /// DataFusion memory reservation with
    reservation: MemoryReservation,
}

impl<W: Write + Send> TrackedMemoryArrowWriter<W> {
    /// create a new `TrackedMemoryArrowWriter<`
    pub fn try_new(
        sink: W,
        schema: SchemaRef,
        props: WriterProperties,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        let inner = ArrowWriter::try_new(sink, schema, Some(props))?;
        let consumer = MemoryConsumer::new("IOx ParquetWriter (TrackedMemoryArrowWriter)");
        let reservation = consumer.register(&pool);

        Ok(Self { inner, reservation })
    }

    /// Push a `RecordBatch` into the underlying writer, updating the
    /// tracked allocation
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // writer encodes the batch into its internal buffers
        let result = self.inner.write(&batch);

        // In progress memory, in bytes
        let in_progress_size = self.inner.in_progress_size();

        // update the allocation with the pool.
        let reservation_result = self
            .reservation
            .try_resize(in_progress_size)
            .map_err(Error::OutOfMemory);

        // Log any errors
        if let Err(e) = &reservation_result {
            warn!(
                %e,
                in_progress_size,
                in_progress_rows = self.inner.in_progress_rows(),
                existing_allocation = self.reservation.size(),
                "Could not allocate sufficient buffer memory for writing parquet data"
            );
        }

        reservation_result?;
        result?;
        Ok(())
    }

    /// closes the writer, flushing any remaining data and returning
    /// the written [`FileMetaData`]
    ///
    /// [`FileMetaData`]: parquet::format::FileMetaData
    pub fn close(self) -> Result<parquet::format::FileMetaData> {
        // reservation is returned on drop
        Ok(self.inner.close()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use datafusion::{common::assert_contains, execution::memory_pool::GreedyMemoryPool};
    use rand::{distributions::Standard, rngs::StdRng, Rng, SeedableRng};

    /// Number of rows to trigger writer flush
    const TEST_MAX_ROW_GROUP_SIZE: usize = 100;

    /// Ensure the writer can successfully write when configured with
    /// a sufficiently sized pool
    #[tokio::test]
    async fn test_pool_allocation() {
        test_helpers::maybe_start_logging();
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .set_data_page_size_limit(10) // ensure each batch is written as a page
            .build();

        let mut data_gen = DataGenerator::new();

        let pool_size = 10000;
        let pool = memory_pool(pool_size);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], data_gen.schema(), props, Arc::clone(&pool))
                .unwrap();

        // first batch exceeds page limit, so wrote to a page
        writer.write(data_gen.batch(10)).unwrap();
        assert!(writer.reservation.size() > 0);
        assert_eq!(writer.reservation.size(), writer.inner.in_progress_size());
        let previous_reservation = writer.reservation.size();

        // Feed in more data to force more data to be written
        writer.write(data_gen.batch(20)).unwrap();
        assert!(
            writer.reservation.size() > previous_reservation,
            "reservation_size: {} > {previous_reservation}",
            writer.reservation.size()
        );

        // Feed in 50 more batches of 5 rows each, and expect that the reservation
        // continues to match (may not grow as pages are flushed)
        for _ in 0..50 {
            writer.write(data_gen.batch(5)).unwrap();
            assert_eq!(writer.reservation.size(), writer.inner.in_progress_size())
        }

        println!("Final reservation is {}", pool.reserved());
        assert_ne!(pool.reserved(), 0);
        assert_eq!(pool.reserved(), writer.inner.in_progress_size());

        // drop the writer and verify the memory is returned to the pool
        std::mem::drop(writer);
        assert_eq!(pool.reserved(), 0);
    }

    /// Ensure the writer errors if it needs to buffer more data than
    /// allowed by pool
    #[tokio::test]
    async fn test_pool_memory_pressure() {
        test_helpers::maybe_start_logging();
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .set_data_page_size_limit(10) // ensure each batch is written as a page
            .build();

        let mut data_gen = DataGenerator::new();

        let pool_size = 1000;
        let pool = memory_pool(pool_size);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], data_gen.schema(), props, Arc::clone(&pool))
                .unwrap();

        for _ in 0..100 {
            match writer.write(data_gen.batch(10)) {
                Err(Error::OutOfMemory(e)) => {
                    println!("Test errored as expected: {e}");
                    assert_contains!(
                        e.to_string(),
                        "IOx ParquetWriter (TrackedMemoryArrowWriter)"
                    );
                    return;
                }
                Err(e) => {
                    panic!("Unexpected error. Expected OOM, got: {e}");
                }
                Ok(_) => {}
            }
        }
        panic!("Writer did not error when pool limit exceeded");
    }

    /// Ensure the writer can successfully write even after an error
    #[tokio::test]
    async fn test_allocation_after_error() {
        test_helpers::maybe_start_logging();
        let props = WriterProperties::builder()
            .set_max_row_group_size(TEST_MAX_ROW_GROUP_SIZE)
            .set_data_page_size_limit(10) // ensure each batch is written as a page
            .build();

        let mut data_gen = DataGenerator::new();

        let pool_size = 10000;
        let pool = memory_pool(pool_size);
        let mut writer =
            TrackedMemoryArrowWriter::try_new(vec![], data_gen.schema(), props, Arc::clone(&pool))
                .unwrap();

        writer.write(data_gen.batch(10)).unwrap();
        assert_ne!(writer.reservation.size(), 0);
        assert_eq!(writer.reservation.size(), writer.inner.in_progress_size());

        // write a bad batch and accounting should still add up
        writer.write(data_gen.bad_batch(3)).unwrap();
        assert_eq!(writer.reservation.size(), writer.inner.in_progress_size());

        // feed more good batches and allocation should still match
        for _ in 0..15 {
            writer.write(data_gen.batch(13)).unwrap();
            assert_ne!(writer.reservation.size(), 0);
            assert_eq!(writer.reservation.size(), writer.inner.in_progress_size());
        }
    }

    /// Creates arrays of psuedo random 16 digit strings. Since
    /// parquet is excellent at compression, psudo random strings are
    /// required to make page flusing work in reasonable ways.
    struct DataGenerator {
        rng: StdRng,
    }

    impl DataGenerator {
        fn new() -> Self {
            let seed = 42;
            Self {
                rng: SeedableRng::seed_from_u64(seed),
            }
        }

        /// Returns a batch with the specified number of random strings
        fn batch(&mut self, count: usize) -> RecordBatch {
            RecordBatch::try_from_iter([("a", self.string_array(count))]).unwrap()
        }

        /// Returns a batch with a different scheam
        fn bad_batch(&mut self, count: usize) -> RecordBatch {
            RecordBatch::try_from_iter([("b", self.string_array(count))]).unwrap()
        }

        /// Makes a string array with `count` entries of data with
        /// different values (parquet is super efficient at encoding the
        /// same value)
        fn string_array(&mut self, count: usize) -> ArrayRef {
            let array: StringArray = (0..count).map(|_| Some(self.rand_string())).collect();
            Arc::new(array)
        }

        /// Return the schema of the generated batches
        fn schema(&mut self) -> SchemaRef {
            self.batch(1).schema()
        }

        /// Make  random 16 digit string
        fn rand_string(&mut self) -> String {
            // sample_iter consumes the random generator so use
            // self.rng to seed one
            let seed: u64 = self.rng.gen_range(0..u64::MAX);
            let rng: StdRng = SeedableRng::seed_from_u64(seed);
            rng.sample_iter(&Standard)
                .filter_map(|c: u8| {
                    if c.is_ascii_digit() {
                        Some(char::from(c))
                    } else {
                        // discard if out of range
                        None
                    }
                })
                .take(16)
                .collect()
        }
    }

    /// make a MemoryPool with the specified max size
    fn memory_pool(max_size: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(max_size))
    }
}
