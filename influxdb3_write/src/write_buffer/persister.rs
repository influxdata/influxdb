//! This module contains the logic for persisting buffer segments when closed and persisting
//! individual tables in advance of closing a buffer segment based on memory limits.

use crate::catalog::TIME_COLUMN_NAME;
use crate::chunk::BufferChunk;
use crate::paths::ParquetFilePath;
use crate::write_buffer::buffer_segment::{ClosedBufferSegment, SegmentSizes};
use crate::write_buffer::segment_state::SegmentState;
use crate::{persister, write_buffer, ParquetFile, Persister, SegmentDuration, SegmentId, Wal};
use arrow::array::TimestampNanosecondArray;
use arrow::record_batch::RecordBatch;
use data_types::{
    ChunkId, ChunkOrder, PartitionKey, TableId, TimestampMinMax, TransitionPartitionId,
};
use datafusion_util::stream_from_batches;
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use iox_time::TimeProvider;
use observability_deps::tracing::{error, info};
use parking_lot::RwLock;
use parquet::format::FileMetaData;
use schema::sort::SortKey;
use schema::Schema;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

#[cfg(test)]
const PERSISTER_CHECK_INTERVAL: Duration = Duration::from_millis(10);

#[cfg(not(test))]
const PERSISTER_CHECK_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) async fn run_buffer_segment_persist_and_cleanup<P, T, W>(
    persister: Arc<P>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    mut shutdown_rx: watch::Receiver<()>,
    time_provider: Arc<T>,
    wal: Option<Arc<W>>,
    executor: Arc<iox_query::exec::Executor>,
) where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    T: TimeProvider,
    W: Wal,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                break;
            }
            _ = tokio::time::sleep(PERSISTER_CHECK_INTERVAL) => {
                if let Err(e) = persist_and_cleanup_ready_segments(Arc::clone(&persister), Arc::clone(&segment_state), Arc::clone(&time_provider), wal.clone(), Arc::clone(&executor)).await {
                    error!("Error persisting and cleaning up segments: {}", e);
                }
            }
        }
    }
}

async fn persist_and_cleanup_ready_segments<P, T, W>(
    persister: Arc<P>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    time_provider: Arc<T>,
    wal: Option<Arc<W>>,
    executor: Arc<iox_query::exec::Executor>,
) -> Result<(), crate::Error>
where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    T: TimeProvider,
    W: Wal,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    // this loop is where persistence happens so if anything is in persisting,
    // it's either been dropped or remaining from a restart, so clear those out first.
    let persisting_segments = {
        let segment_state = segment_state.read();
        segment_state.persisting_segments()
    };

    for segment in persisting_segments {
        persist_closed_segment_and_cleanup(
            segment,
            Arc::clone(&persister),
            Arc::clone(&segment_state),
            wal.clone(),
            Arc::clone(&executor),
        )
        .await?
    }

    // check for open segments to persist
    let current_time = time_provider.now();
    let segments_to_persist = {
        let segment_state = segment_state.read();
        segment_state.segments_to_persist(current_time)
    };

    // close and persist each one in turn
    for segment_start in segments_to_persist {
        let closed_segment = {
            let mut segment_state = segment_state.write();
            segment_state.close_segment(segment_start)
        };

        if let Some(closed_segment) = closed_segment {
            persist_closed_segment_and_cleanup(
                closed_segment,
                Arc::clone(&persister),
                Arc::clone(&segment_state),
                wal.clone(),
                Arc::clone(&executor),
            )
            .await?
        }
    }

    Ok(())
}

// Performs the following:
// 1. persist the segment to the object store
// 2. remove the segment from the persisting_segments map and add it to the persisted_segments map
// 3. remove the wal segment file
async fn persist_closed_segment_and_cleanup<P, T, W>(
    closed_segment: Arc<ClosedBufferSegment>,
    persister: Arc<P>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    wal: Option<Arc<W>>,
    executor: Arc<iox_query::exec::Executor>,
) -> Result<(), crate::Error>
where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    T: TimeProvider,
    W: Wal,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    let closed_segment_start_time = closed_segment.segment_range.start_time;
    let closed_segment_id = closed_segment.segment_id;
    let persisted_segment = closed_segment.persist(persister, executor, None).await?;

    {
        let mut segment_state = segment_state.write();
        segment_state.mark_segment_as_persisted(closed_segment_start_time, persisted_segment);
    }

    if let Some(wal) = wal {
        wal.delete_wal_segment(closed_segment_id)?;
    }

    Ok(())
}

#[cfg(test)]
const BUFFER_SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(not(test))]
const BUFFER_SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Periodically checks the buffer size and persists segments or tables to free up memory if the
/// buffer size is over the limit.
pub(crate) async fn run_buffer_size_check_and_persist<P, T, W>(
    persister: Arc<P>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    mut shutdown_rx: watch::Receiver<()>,
    executor: Arc<iox_query::exec::Executor>,
    buffer_limit_mb: usize,
) where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    T: TimeProvider,
    W: Wal,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    let mut interval = tokio::time::interval(BUFFER_SIZE_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut buffer_sizes = BufferSizeRingBuffer::new(10);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                break;
            }
            _ = interval.tick() => {
                check_buffer_size_and_persist(Arc::clone(&persister), Arc::clone(&segment_state), Arc::clone(&executor), &mut buffer_sizes, buffer_limit_mb).await;
            }
        }
    }
}

// Performs the following:
// 1. Get the total of all open segments
// 2. Compute the growth rate based on the buffer size from the last 10 measurements
// 3. If the growth rate and the current size will put the buffer over the limit in the next 5
//    minutes, persist the oldest cold segment, or persist the largest tables in the open segments
//    until we get under a size that will not exceed the limit in the next 5 minutes.
async fn check_buffer_size_and_persist<P, T, W>(
    persister: Arc<P>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    executor: Arc<iox_query::exec::Executor>,
    buffer_sizes: &mut BufferSizeRingBuffer,
    buffer_limit_mb: usize,
) where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    T: TimeProvider,
    W: Wal,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    let mut segment_sizes = {
        let segment_state = segment_state.read();
        segment_state.open_segments_sizes()
    };
    let buffer_size = segment_sizes.iter().map(|s| s.size()).sum::<usize>();

    buffer_sizes.push(buffer_size, Instant::now());

    let mut size_to_shed = buffer_sizes.size_to_shed(buffer_limit_mb);

    while let Some(target) = next_to_persist(size_to_shed, &mut segment_sizes) {
        match target {
            PersistTarget::SegmentToClose(segment) => {
                // close the segment and let the regular segment persist checker pick it up
                info!(
                    "Closing segment early to free memory {:?} {}",
                    segment.segment_id, segment.segment_start_time
                );
                let mut state = segment_state.write();

                if state.close_segment(segment.segment_start_time).is_none() {
                    error!(
                        "Segment {:?} {} not found in open segments",
                        segment.segment_id, segment.segment_start_time
                    );
                }

                size_to_shed -= segment.size();
            }
            PersistTarget::Table(table) => {
                info!(
                    "Persisting table {} in database {} in segment {:?} to free memory",
                    table.table_name, table.database_name, table.segment_id
                );

                let data_to_persist = {
                    let mut state = segment_state.write();
                    state.split_table_for_persistence(
                        table.segment_id,
                        &table.database_name,
                        &table.table_name,
                    )
                };

                if let Some((path, batch)) = data_to_persist {
                    let (min_time, max_time) = min_max_time_from_batch(&batch);

                    let schema =
                        Schema::try_from(batch.schema()).expect("schema should always be valid");
                    let sort_key = SortKey::from(
                        schema
                            .primary_key()
                            .iter()
                            .map(|k| k.to_string())
                            .collect::<Vec<String>>(),
                    );

                    let path_string = path.to_string();
                    let (size_bytes, meta) = sort_dedupe_persist(
                        &table.table_name,
                        path,
                        batch,
                        &schema,
                        TimestampMinMax::new(min_time, max_time),
                        &table.segment_key,
                        sort_key,
                        Arc::clone(&persister),
                        Arc::clone(&executor),
                    )
                    .await;

                    let parquet_file = ParquetFile {
                        path: path_string,
                        size_bytes,
                        row_count: meta.num_rows as u64,
                        min_time,
                        max_time,
                    };

                    // grab a lock on segment state and insert the parquet file in the list of files while clearing out the persisting data from the buffer
                    if let Err(e) = segment_state.write().clear_persisting_table_buffer(
                        parquet_file,
                        table.segment_id,
                        &table.database_name,
                        &table.table_name,
                    ) {
                        // if there's an error here, it just means there was a problem logging this in the WAL. The data has already been persisted so it's safe.
                        error!("Error clearing persisted table buffer: {}", e);
                    }
                }
                size_to_shed -= table.size_bytes;
            }
        }
    }
}

fn min_max_time_from_batch(batch: &RecordBatch) -> (i64, i64) {
    batch
        .column_by_name(TIME_COLUMN_NAME)
        .map(|c| {
            let times = c
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("time column was an unexpected type")
                .values()
                .inner()
                .typed_data();
            let mut min_timestamp = i64::MAX;
            let mut max_timestamp = i64::MIN;
            for t in times {
                min_timestamp = min_timestamp.min(*t);
                max_timestamp = max_timestamp.max(*t);
            }
            (min_timestamp, max_timestamp)
        })
        .unwrap_or_else(|| (i64::MAX, i64::MIN))
}

#[allow(clippy::too_many_arguments)]
async fn sort_dedupe_persist<P>(
    table_name: &str,
    path: ParquetFilePath,
    batch: RecordBatch,
    schema: &Schema,
    time_min_max: TimestampMinMax,
    segment_key: &PartitionKey,
    sort_key: SortKey,
    persister: Arc<P>,
    executor: Arc<iox_query::exec::Executor>,
) -> (u64, FileMetaData)
where
    P: Persister,
    persister::Error: From<<P as Persister>::Error>,
    write_buffer::Error: From<<P as Persister>::Error>,
{
    // Dedupe and sort using the COMPACT query built into
    // iox_query
    let row_count = batch.num_rows();

    let chunk_stats =
        create_chunk_statistics(Some(row_count), schema, Some(time_min_max), &NoColumnRanges);

    let chunks: Vec<Arc<dyn QueryChunk>> = vec![Arc::new(BufferChunk {
        batches: vec![batch],
        schema: schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id: TransitionPartitionId::new(TableId::new(0), segment_key),
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(1),
    })];

    let ctx = executor.new_context();

    let logical_plan = ReorgPlanner::new()
        .compact_plan(Arc::from(table_name), schema, chunks, sort_key)
        .unwrap();

    // Build physical plan
    let physical_plan = ctx.create_physical_plan(&logical_plan).await.unwrap();

    // Execute the plan and return compacted record batches
    let data = ctx.collect(physical_plan).await.unwrap();

    // keep attempting to persist forever. If we can't reach the object store, we'll stop accepting
    // writes elsewhere in the system, so we need to keep trying to persist.
    loop {
        let batch_stream = stream_from_batches(schema.as_arrow(), data.clone());

        match persister
            .persist_parquet_file(path.clone(), batch_stream)
            .await
        {
            Ok((size_bytes, meta)) => {
                info!("Persisted parquet file: {}", path.to_string());
                return (size_bytes, meta);
            }
            Err(_) => {
                error!("Error persisting parquet file: (TODO: figure out why we can't output the error), sleeping and retrying...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// The interval from the last write to a segment at which to close the segment if the buffer size
// check indicates that we need to free up space.
const SEGMENT_CLOSE_INTERVAL: Duration = Duration::from_secs(60);

// The interval in the future to see if our growth rate will cause us to exceed the buffer limit
const GROWTH_RATE_INTERVAL: Duration = Duration::from_secs(300);

// Returns the next thing to persist based on the buffer growth rate. Can be:
// * None
// * Close a segment
// * Specific table in a specific segment
fn next_to_persist(
    size_to_shed: usize,
    segment_sizes: &mut Vec<SegmentSizes>,
) -> Option<PersistTarget> {
    if size_to_shed == 0 {
        return None;
    }

    // first check is to return the oldest cold segment
    let mut oldest_cold_index_and_elapsed = None;
    for (index, segment_size) in segment_sizes.iter().enumerate() {
        if segment_is_cold(
            segment_size.last_write_time.elapsed(),
            segment_size.segment_duration,
        ) {
            match oldest_cold_index_and_elapsed {
                Some((oldest_index, oldest_elapsed)) => {
                    if oldest_elapsed < segment_size.last_write_time.elapsed() {
                        oldest_cold_index_and_elapsed = Some((oldest_index, oldest_elapsed))
                    }
                }
                None => {
                    oldest_cold_index_and_elapsed =
                        Some((index, segment_size.last_write_time.elapsed()))
                }
            };
        }
    }

    // If we have a segment, remove this segment from the list so that if we have to call in here
    // again to clear up more space, it won't be considered again. Then return the info.
    if let Some((index, _)) = oldest_cold_index_and_elapsed {
        let segment = segment_sizes.remove(index);
        return Some(PersistTarget::SegmentToClose(segment));
    }

    // if no segment is cold, return the largest table
    let max_segment_db_table = {
        let mut max_segment_db_table = None;

        for (index, segment_size) in segment_sizes.iter_mut().enumerate() {
            for (db, db_sizes) in &segment_size.database_buffer_sizes {
                for (table, size) in &db_sizes.table_sizes {
                    match max_segment_db_table {
                        Some((_, _, _, max_size)) => {
                            if max_size < *size {
                                max_segment_db_table = Some((index, db, table, *size))
                            }
                        }
                        None => max_segment_db_table = Some((index, db, table, *size)),
                    };
                }
            }
        }

        max_segment_db_table
            .map(|(index, db, table, size)| (index, db.to_string(), table.to_string(), size))
    };

    // If we have a table, remove it from the segment so that if we have to call in here again to
    // clear up more space, it won't be considered again. Then return the info.
    if let Some((index, db, table, size)) = max_segment_db_table {
        let segment = segment_sizes.get_mut(index).unwrap();
        segment
            .database_buffer_sizes
            .get_mut(&db)
            .unwrap()
            .table_sizes
            .remove(&table);
        return Some(PersistTarget::Table(TableToPersist {
            segment_id: segment.segment_id,
            segment_key: segment.segment_key.clone(),
            database_name: db,
            table_name: table,
            size_bytes: size,
        }));
    }

    None
}

fn segment_is_cold(since_last_write: Duration, segment_duration: SegmentDuration) -> bool {
    since_last_write > segment_duration.as_duration() / 2
        || since_last_write > SEGMENT_CLOSE_INTERVAL
}

#[derive(Debug)]
enum PersistTarget {
    SegmentToClose(SegmentSizes),
    Table(TableToPersist),
}

#[derive(Debug)]
struct TableToPersist {
    segment_id: SegmentId,
    segment_key: PartitionKey,
    database_name: String,
    table_name: String,
    size_bytes: usize,
}

// Ring buffer to keep buffer sizes and predict when the buffer might exceed the limit
#[derive(Debug)]
struct BufferSizeRingBuffer {
    buffer: VecDeque<BufferSize>,
}

#[derive(Debug, Copy, Clone)]
struct BufferSize {
    size: usize,
    time: Instant,
}

impl BufferSizeRingBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: usize, time: Instant) {
        if self.buffer.len() == self.buffer.capacity() {
            self.buffer.pop_front();
        }

        self.buffer.push_back(BufferSize { size: value, time });
    }

    /// Returns the growth rate of the buffer size in bytes per minute for each measurement
    /// compared to the most recent measurement
    fn per_minute_growth_rates(&self) -> Vec<usize> {
        if self.buffer.len() < 2 {
            return vec![];
        }

        let latest = self.buffer.back().unwrap();

        self.buffer
            .iter()
            .take(self.buffer.len() - 1)
            .map(|measurement| {
                let duration = latest.time.duration_since(measurement.time);
                let minutes = duration.as_secs_f64() / 60.0;
                let growth = latest.size as f64 - measurement.size as f64;
                (growth / minutes) as usize
            })
            .collect()
    }

    /// The size in bytes to shed from the write buffer in order to get the projected size under the
    /// limit in the next 5 minutes
    fn size_to_shed(&self, limit_mb: usize) -> usize {
        if limit_mb == 0 {
            return 0;
        }

        let minutes_forward = (GROWTH_RATE_INTERVAL.as_secs() / 60) as usize;
        let last_buffer_size = self.buffer.back().map(|s| s.size).unwrap_or(0);

        let mut max_shed = 0;

        for growth_per_minute in self.per_minute_growth_rates() {
            let projected_size_mb =
                (growth_per_minute * minutes_forward + last_buffer_size) / 1024 / 1024;

            if projected_size_mb > limit_mb {
                max_shed = max_shed.max(projected_size_mb - limit_mb)
            }
        }

        max_shed * 1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::persister::PersisterImpl;
    use crate::test_helpers::lp_to_write_batch;
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::write_buffer::buffer_segment::tests::TestPersister;
    use crate::write_buffer::buffer_segment::OpenBufferSegment;
    use crate::{
        wal, SegmentDuration, SegmentFile, SegmentId, SegmentRange, WalSegmentReader,
        WalSegmentWriter,
    };
    use arrow_util::assert_batches_eq;
    use datafusion_util::config::register_iox_object_store;
    use iox_time::{MockProvider, Time};
    use object_store::local::LocalFileSystem;
    use parking_lot::Mutex;
    use std::any::Any;

    #[tokio::test]
    async fn persist_and_cleanup_ready_segments_handles_persisting_and_rotates_old() {
        let catalog = Arc::new(Catalog::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let first_segment_range = SegmentRange::from_time_and_duration(
            Time::from_timestamp_nanos(0),
            segment_duration,
            false,
        );

        let mut open_segment1 = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(1),
            first_segment_range,
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(1))),
            None,
        );
        open_segment1
            .buffer_writes(lp_to_write_batch(
                Arc::clone(&catalog),
                "foo",
                "cpu bar=1 10",
            ))
            .unwrap();

        let mut open_segment2 = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(2),
            SegmentRange::from_time_and_duration(
                Time::from_timestamp(300, 0).unwrap(),
                segment_duration,
                false,
            ),
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(2))),
            None,
        );
        open_segment2
            .buffer_writes(lp_to_write_batch(
                Arc::clone(&catalog),
                "foo",
                "cpu bar=2 300000000000",
            ))
            .unwrap();

        let mut open_segment3 = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(3),
            SegmentRange::from_time_and_duration(
                Time::from_timestamp(600, 0).unwrap(),
                segment_duration,
                false,
            ),
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(3))),
            None,
        );
        open_segment3
            .buffer_writes(lp_to_write_batch(
                Arc::clone(&catalog),
                "foo",
                "cpu bar=3 700000000000",
            ))
            .unwrap();

        let wal = Arc::new(TestWal::default());

        let segment_state: SegmentState<MockProvider, TestWal> = SegmentState::new(
            SegmentDuration::new_5m(),
            SegmentId::new(4),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
            vec![open_segment2, open_segment3],
            vec![open_segment1.into_closed_segment(Arc::clone(&catalog))],
            vec![],
            Some(Arc::clone(&wal)),
        );
        let segment_state = Arc::new(RwLock::new(segment_state));

        let persister = Arc::new(TestPersister::default());

        time_provider.set(Time::from_timestamp(900, 0).unwrap());

        persist_and_cleanup_ready_segments(
            Arc::clone(&persister),
            Arc::clone(&segment_state),
            Arc::clone(&time_provider),
            Some(Arc::clone(&wal)),
            crate::test_help::make_exec(),
        )
        .await
        .unwrap();

        let persisted_state = persister
            .as_any()
            .downcast_ref::<TestPersister>()
            .unwrap()
            .state
            .lock();

        assert_eq!(persisted_state.catalog.len(), 1);
        assert_eq!(persisted_state.segments.len(), 2);
        assert_eq!(persisted_state.parquet_files.len(), 2);

        let wal_state = wal.as_any().downcast_ref::<TestWal>().unwrap();
        let deleted_segments = wal_state.deleted_wal_segments.lock().clone();

        assert_eq!(deleted_segments, vec![SegmentId::new(1), SegmentId::new(2)]);
    }

    #[tokio::test]
    async fn test_check_buffer_size_and_persist() {
        let catalog = Arc::new(Catalog::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let first_segment_range = SegmentRange::from_time_and_duration(
            Time::from_timestamp_nanos(0),
            segment_duration,
            false,
        );

        let mut open_segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(1),
            first_segment_range,
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(1))),
            None,
        );
        for x in 0..10 {
            open_segment
                .buffer_writes(lp_to_write_batch(
                    Arc::clone(&catalog),
                    "foo",
                    format!("cpu bar={} {}", x, x).as_str(),
                ))
                .unwrap();
        }

        let wal = Arc::new(TestWal::default());

        let segment_state: SegmentState<MockProvider, TestWal> = SegmentState::new(
            SegmentDuration::new_5m(),
            SegmentId::new(4),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
            vec![open_segment],
            vec![],
            vec![],
            Some(Arc::clone(&wal)),
        );
        let segment_state = Arc::new(RwLock::new(segment_state));

        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister: Arc<PersisterImpl> = Arc::new(PersisterImpl::new(Arc::new(local_disk)));

        time_provider.set(Time::from_timestamp(900, 0).unwrap());

        let mut buffer_sizes = BufferSizeRingBuffer::new(2);
        buffer_sizes.push(0, Instant::now());

        check_buffer_size_and_persist(
            Arc::clone(&persister),
            Arc::clone(&segment_state),
            crate::test_help::make_exec(),
            &mut buffer_sizes,
            1,
        )
        .await;

        let db_schema = catalog.db_schema("foo").unwrap();
        let table_schema = db_schema.get_table_schema("cpu").unwrap();
        let non_persisted_data =
            segment_state
                .read()
                .open_segments_table_record_batches("foo", "cpu", table_schema);

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 9.0 | 1970-01-01T00:00:00.000000009Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &non_persisted_data);

        let exec = crate::test_help::make_exec();
        let session_context = exec.new_context();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", persister.object_store());
        let ctx = session_context.inner().state();

        let chunks = segment_state
            .read()
            .get_table_chunks(
                Arc::clone(&db_schema),
                "cpu",
                &[],
                None,
                persister.object_store_url(),
                persister.object_store(),
                &ctx,
            )
            .unwrap();

        let mut batches = vec![];
        for chunk in chunks {
            let chunk = chunk
                .data()
                .read_to_batches(chunk.schema(), session_context.inner())
                .await;
            batches.extend(chunk);
        }

        let expected = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 0.0 | 1970-01-01T00:00:00Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000001Z |",
            "| 2.0 | 1970-01-01T00:00:00.000000002Z |",
            "| 3.0 | 1970-01-01T00:00:00.000000003Z |",
            "| 4.0 | 1970-01-01T00:00:00.000000004Z |",
            "| 5.0 | 1970-01-01T00:00:00.000000005Z |",
            "| 6.0 | 1970-01-01T00:00:00.000000006Z |",
            "| 7.0 | 1970-01-01T00:00:00.000000007Z |",
            "| 8.0 | 1970-01-01T00:00:00.000000008Z |",
            "| 9.0 | 1970-01-01T00:00:00.000000009Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        // now close out the segment and validate that two parquet files are in the segment info file
        let closed_segment = segment_state
            .write()
            .close_segment(first_segment_range.start_time)
            .unwrap();
        let persisted = closed_segment
            .persist(Arc::clone(&persister), exec, None)
            .await
            .unwrap();
        segment_state
            .write()
            .mark_segment_as_persisted(first_segment_range.start_time, persisted);
        let segments = persister.load_segments(10).await.unwrap();
        assert_eq!(segments.len(), 1);
        let segment = segments.first().unwrap();
        assert_eq!(segment.segment_row_count, 10);
        let db = segment.databases.get("foo").unwrap();
        let table = db.tables.get("cpu").unwrap();
        assert_eq!(table.parquet_files.len(), 2);
        assert_eq!(table.parquet_files[0].row_count, 9);
        assert_eq!(table.parquet_files[0].min_time, 0);
        assert_eq!(table.parquet_files[0].max_time, 8);
        assert_eq!(table.parquet_files[1].row_count, 1);
        assert_eq!(table.parquet_files[1].min_time, 9);
        assert_eq!(table.parquet_files[1].max_time, 9);
    }

    #[test]
    fn buffer_size_ring_buffer() {
        let mut buffer = BufferSizeRingBuffer::new(3);
        let start = Instant::now();
        buffer.push(1024 * 1024, start);
        assert!(buffer.per_minute_growth_rates().is_empty());
        assert_eq!(buffer.size_to_shed(0), 0);

        buffer.push(1024 * 1024 * 100, start + Duration::from_secs(60));
        assert!(*buffer.per_minute_growth_rates().first().unwrap() > 1024 * 1024);
        assert_eq!(buffer.size_to_shed(10000), 0);
        assert_eq!(buffer.size_to_shed(400), 204472320);

        buffer.push(1024 * 1024 * 200, start + Duration::from_secs(120));
        buffer.push(1024 * 1024, start + Duration::from_secs(180));
        buffer.push(1024 * 1024 * 300, start + Duration::from_secs(240));

        let rates = buffer.per_minute_growth_rates();
        let expected = vec![52428800, 313524224];
        assert_eq!(rates, expected);
        assert_eq!(buffer.size_to_shed(10000), 0);
        assert_eq!(buffer.size_to_shed(400), 1462763520);
    }

    #[derive(Debug, Default)]
    struct TestWal {
        deleted_wal_segments: Mutex<Vec<SegmentId>>,
    }

    impl Wal for TestWal {
        fn new_segment_writer(
            &self,
            _segment_id: SegmentId,
            _range: SegmentRange,
        ) -> wal::Result<Box<dyn WalSegmentWriter>> {
            todo!()
        }

        fn open_segment_writer(
            &self,
            _segment_id: SegmentId,
        ) -> wal::Result<Box<dyn WalSegmentWriter>> {
            todo!()
        }

        fn open_segment_reader(
            &self,
            _segment_id: SegmentId,
        ) -> wal::Result<Box<dyn WalSegmentReader>> {
            todo!()
        }

        fn segment_files(&self) -> wal::Result<Vec<SegmentFile>> {
            todo!()
        }

        fn delete_wal_segment(&self, segment_id: SegmentId) -> wal::Result<()> {
            self.deleted_wal_segments.lock().push(segment_id);
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self as &dyn Any
        }
    }
}
