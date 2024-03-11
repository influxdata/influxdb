//! State for the write buffer segments.

use crate::catalog::{Catalog, DatabaseSchema};
use crate::chunk::BufferChunk;
use crate::wal::WalSegmentWriterNoopImpl;
use crate::write_buffer::buffer_segment::{
    ClosedBufferSegment, OpenBufferSegment, TableBuffer, WriteBatch,
};
use crate::{
    persister, wal, write_buffer, ParquetFile, PersistedSegment, Persister, SegmentDuration,
    SegmentId, SegmentRange, Wal, WalOp,
};
use data_types::{ChunkId, ChunkOrder, TableId, TransitionPartitionId};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::error;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

// The maximum number of open segments that can be open at any one time. Each one of these will
// have an open wal file and a buffer segment in memory.
const OPEN_SEGMENT_LIMIT: usize = 100;

#[derive(Debug)]
pub(crate) struct SegmentState<T, W> {
    segment_duration: SegmentDuration,
    last_segment_id: SegmentId,
    catalog: Arc<Catalog>,
    wal: Option<Arc<W>>,
    time_provider: Arc<T>,
    // Map of segment start times to open segments. Should always have a segment open for the
    // start time that time.now falls into.
    segments: BTreeMap<Time, OpenBufferSegment>,
    persisting_segments: BTreeMap<Time, Arc<ClosedBufferSegment>>,
    persisted_segments: BTreeMap<Time, Arc<PersistedSegment>>,
}

impl<T: TimeProvider, W: Wal> SegmentState<T, W> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        segment_duration: SegmentDuration,
        last_segment_id: SegmentId,
        catalog: Arc<Catalog>,
        time_provider: Arc<T>,
        open_segments: Vec<OpenBufferSegment>,
        persisting_segments: Vec<ClosedBufferSegment>,
        persisted_segments: Vec<PersistedSegment>,
        wal: Option<Arc<W>>,
    ) -> Self {
        let mut segments = BTreeMap::new();
        for segment in open_segments {
            segments.insert(segment.segment_range().start_time, segment);
        }

        let mut persisting_segments_map = BTreeMap::new();
        for segment in persisting_segments {
            persisting_segments_map.insert(segment.segment_range.start_time, Arc::new(segment));
        }

        let mut persisted_segments_map = BTreeMap::new();
        for segment in persisted_segments {
            persisted_segments_map.insert(
                Time::from_timestamp_nanos(segment.segment_min_time),
                Arc::new(segment),
            );
        }

        Self {
            segment_duration,
            last_segment_id,
            catalog,
            time_provider,
            wal,
            segments,
            persisting_segments: persisting_segments_map,
            persisted_segments: persisted_segments_map,
        }
    }

    pub(crate) fn write_ops_to_segment(
        &mut self,
        segment_start: Time,
        ops: Vec<WalOp>,
    ) -> wal::Result<()> {
        let segment = self.get_or_create_segment_for_time(segment_start)?;
        segment.write_wal_ops(ops)
    }

    pub(crate) fn write_batch_to_segment(
        &mut self,
        segment_start: Time,
        write_batch: WriteBatch,
    ) -> crate::write_buffer::Result<()> {
        let segment = self.get_or_create_segment_for_time(segment_start)?;
        segment.buffer_writes(write_batch)
    }

    pub(crate) fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        _filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let table = db_schema
            .tables
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;
        let schema = table.schema.clone();

        let mut table_buffers = self.clone_table_buffers(&db_schema.name, table_name);
        table_buffers.extend(
            self.persisting_segments
                .values()
                .filter_map(|segment| segment.table_buffer(&db_schema.name, table_name))
                .collect::<Vec<_>>(),
        );

        let mut chunk_order = 0;

        let chunks = table_buffers
            .into_iter()
            .map(|table_buffer| {
                let batch = table_buffer.rows_to_record_batch(&schema, table.columns());
                let batch_stats = create_chunk_statistics(
                    Some(table_buffer.row_count()),
                    &schema,
                    Some(table_buffer.timestamp_min_max()),
                    None,
                );

                let chunk: Arc<dyn QueryChunk> = Arc::new(BufferChunk {
                    batches: vec![batch],
                    schema: schema.clone(),
                    stats: Arc::new(batch_stats),
                    partition_id: TransitionPartitionId::new(
                        TableId::new(0),
                        &table_buffer.segment_key,
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: ChunkOrder::new(chunk_order),
                });

                chunk_order += 1;

                chunk
            })
            .collect();

        Ok(chunks)
    }

    pub(crate) fn get_parquet_files(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Vec<ParquetFile> {
        let mut parquet_files = vec![];

        println!(
            "Getting parquet files for database: {} and table: {}",
            database_name, table_name
        );

        for segment in self.persisted_segments.values() {
            println!("Segment: {:?}", segment.segment_min_time);

            if let Some(db) = segment.databases.get(database_name) {
                println!("Got database: {:?}", db);
                if let Some(table) = db.tables.get(table_name) {
                    println!("Got table: {:?}", table);
                    let segment_parquet_files = table.parquet_files.clone();
                    parquet_files.extend(segment_parquet_files);
                }
            }
        }

        println!("Got parquet files: {:?}", parquet_files);

        parquet_files
    }

    pub(crate) fn clone_table_buffers(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Vec<TableBuffer> {
        self.segments
            .values()
            .filter_map(|segment| segment.table_buffer(database_name, table_name))
            .collect::<Vec<_>>()
    }

    #[cfg(test)]
    pub(crate) fn persisted_segments(&self) -> Vec<Arc<PersistedSegment>> {
        self.persisted_segments.values().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn open_segment_times(&self) -> Vec<Time> {
        self.segments.keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub(crate) fn segment_for_time(&self, time: Time) -> Option<&OpenBufferSegment> {
        self.segments.get(&time)
    }

    // Looks at the open buffer segments and returns the start `Time` of any that meet the following
    // criteria (in time ascending order):
    // 1. The segment is not in the current time or next time
    // 2. The segment has been open for longer than half the segment duration
    fn segments_to_persist(&self, current_time: Time) -> Vec<Time> {
        let mut segments_to_persist = vec![];

        for (start_time, segment) in &self.segments {
            if segment.should_persist(current_time) {
                segments_to_persist.push(*start_time);
            }
        }

        segments_to_persist.sort();
        segments_to_persist
    }

    fn close_segment(&mut self, segment_start: Time) -> Option<Arc<ClosedBufferSegment>> {
        self.segments.remove(&segment_start).map(|segment| {
            let closed_segment = Arc::new(segment.into_closed_segment(Arc::clone(&self.catalog)));

            self.persisting_segments
                .insert(segment_start, Arc::clone(&closed_segment));

            closed_segment
        })
    }

    // return the segment with this start time or open up a new one if it isn't currently open.
    fn get_or_create_segment_for_time(
        &mut self,
        time: Time,
    ) -> wal::Result<&mut OpenBufferSegment> {
        if !self.segments.contains_key(&time) {
            if self.segments.len() >= OPEN_SEGMENT_LIMIT {
                return Err(wal::Error::OpenSegmentLimitReached(OPEN_SEGMENT_LIMIT));
            }

            self.last_segment_id = self.last_segment_id.next();
            let segment_id = self.last_segment_id;
            let segment_range =
                SegmentRange::from_time_and_duration(time, self.segment_duration, false);

            let segment_writer = match &self.wal {
                Some(wal) => wal.new_segment_writer(segment_id, segment_range)?,
                None => Box::new(WalSegmentWriterNoopImpl::new(segment_id)),
            };

            let segment = OpenBufferSegment::new(
                segment_id,
                segment_range,
                self.time_provider.now(),
                self.catalog.sequence_number(),
                segment_writer,
                None,
            );
            self.segments.insert(time, segment);
        }

        Ok(self.segments.get_mut(&time).unwrap())
    }
}

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
                if let Err(e) = persist_and_cleanup_ready_segments(Arc::clone(&persister), Arc::clone(&segment_state), Arc::clone(&time_provider), wal.clone()).await {
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
        segment_state
            .persisting_segments
            .values()
            .cloned()
            .collect::<Vec<_>>()
    };

    for segment in persisting_segments {
        persist_closed_segment_and_cleanup(
            segment,
            Arc::clone(&persister),
            Arc::clone(&segment_state),
            wal.clone(),
        )
        .await
        .unwrap()
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
            )
            .await
            .unwrap()
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
    let persisted_segment = closed_segment.persist(persister).await?;

    {
        let mut segment_state = segment_state.write();
        segment_state
            .persisting_segments
            .remove(&closed_segment_start_time);
        segment_state
            .persisted_segments
            .insert(closed_segment_start_time, Arc::new(persisted_segment));
    }

    if let Some(wal) = wal {
        wal.delete_wal_segment(closed_segment_id)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::lp_to_write_batch;
    use crate::wal::WalImpl;
    use crate::{SegmentFile, WalSegmentReader, WalSegmentWriter};
    use iox_time::MockProvider;
    use parking_lot::Mutex;
    use std::any::Any;
    use std::fmt::Debug;
    use write_buffer::buffer_segment::tests::TestPersister;

    #[test]
    fn segments_to_persist_sorts_oldest_first() {
        let catalog = Arc::new(Catalog::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let first_segment_range = SegmentRange::from_time_and_duration(
            Time::from_timestamp_nanos(0),
            segment_duration,
            false,
        );

        let open_segment1 = OpenBufferSegment::new(
            SegmentId::new(1),
            first_segment_range,
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(1))),
            None,
        );

        let open_segment2 = OpenBufferSegment::new(
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

        let open_segment3 = OpenBufferSegment::new(
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

        let segment_state: SegmentState<MockProvider, WalImpl> = SegmentState::new(
            SegmentDuration::new_5m(),
            SegmentId::new(4),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
            vec![open_segment1, open_segment2, open_segment3],
            vec![],
            vec![],
            None,
        );

        let segments_to_persist =
            segment_state.segments_to_persist(Time::from_timestamp(800, 0).unwrap());

        assert_eq!(
            segments_to_persist,
            vec![
                Time::from_timestamp_nanos(0),
                Time::from_timestamp(300, 0).unwrap()
            ]
        );
    }

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
            SegmentId::new(1),
            first_segment_range,
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(1))),
            None,
        );
        open_segment1
            .buffer_writes(lp_to_write_batch(&catalog, "foo", "cpu bar=1 10"))
            .unwrap();

        let mut open_segment2 = OpenBufferSegment::new(
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
            .buffer_writes(lp_to_write_batch(&catalog, "foo", "cpu bar=2 300000000000"))
            .unwrap();

        let mut open_segment3 = OpenBufferSegment::new(
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
            .buffer_writes(lp_to_write_batch(&catalog, "foo", "cpu bar=3 700000000000"))
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
