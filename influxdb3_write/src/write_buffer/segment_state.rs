//! State for the write buffer segments.

use crate::catalog::{Catalog, DatabaseSchema};
use crate::chunk::BufferChunk;
use crate::paths::ParquetFilePath;
use crate::wal::WalSegmentWriterNoopImpl;
use crate::write_buffer::buffer_segment::{
    ClosedBufferSegment, OpenBufferSegment, SegmentSizes, WriteBatch,
};
use crate::write_buffer::parquet_chunk_from_file;
use crate::{
    wal, write_buffer, ParquetFile, SegmentDuration, SegmentId, SegmentRange, SequenceNumber, Wal,
    WalOp,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use data_types::{ChunkId, ChunkOrder, TableId, TransitionPartitionId};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::ObjectStore;
use observability_deps::tracing::error;
#[cfg(test)]
use schema::Schema;
use std::collections::BTreeMap;
use std::sync::Arc;

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
}

impl<T: TimeProvider, W: Wal> SegmentState<T, W> {
    pub(crate) fn new(
        segment_duration: SegmentDuration,
        last_segment_id: SegmentId,
        catalog: Arc<Catalog>,
        time_provider: Arc<T>,
        open_segments: Vec<OpenBufferSegment>,
        persisting_segments: Vec<ClosedBufferSegment>,
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

        Self {
            segment_duration,
            last_segment_id,
            catalog,
            time_provider,
            wal,
            segments,
            persisting_segments: persisting_segments_map,
        }
    }

    /// Get the [`SegmentId`] for the currently open segment
    pub(crate) fn current_segment_id(&self) -> SegmentId {
        self.segments.iter().rev().next().unwrap().1.segment_id()
    }

    pub(crate) fn write_ops_to_segment(
        &mut self,
        segment_start: Time,
        ops: Vec<WalOp>,
        starting_catalog_sequence_number: SequenceNumber,
    ) -> wal::Result<()> {
        let segment =
            self.get_or_create_segment_for_time(segment_start, starting_catalog_sequence_number)?;
        segment.write_wal_ops(ops)
    }

    pub(crate) fn write_batch_to_segment(
        &mut self,
        segment_start: Time,
        write_batch: WriteBatch,
        starting_catalog_sequence_number: SequenceNumber,
    ) -> crate::write_buffer::Result<()> {
        let segment =
            self.get_or_create_segment_for_time(segment_start, starting_catalog_sequence_number)?;
        segment.buffer_writes(write_batch)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        object_store_url: ObjectStoreUrl,
        object_store: Arc<dyn ObjectStore>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let table = db_schema
            .tables
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;

        let arrow_schema: SchemaRef = match projection {
            Some(projection) => Arc::new(table.schema.as_arrow().project(projection).unwrap()),
            None => table.schema.as_arrow(),
        };

        let schema = schema::Schema::try_from(Arc::clone(&arrow_schema))
            .map_err(|e| DataFusionError::Execution(format!("schema error {}", e)))?;

        let mut chunks: Vec<Arc<dyn QueryChunk>> = vec![];

        for segment in self.segments.values() {
            // output the older persisted stuff first
            if let Some(table_paruqet_files) =
                segment.table_persisted_parquet_files(&db_schema.name, table_name)
            {
                for parquet_file in &table_paruqet_files.parquet_files {
                    let parquet_chunk = parquet_chunk_from_file(
                        parquet_file,
                        &schema,
                        object_store_url.clone(),
                        Arc::clone(&object_store),
                        chunks
                            .len()
                            .try_into()
                            .expect("should never have this many chunks"),
                    );

                    chunks.push(Arc::new(parquet_chunk));
                }
            }

            // now add the in-memory stuff
            if let Some(batches) = segment.table_record_batches(
                &db_schema.name,
                table_name,
                Arc::clone(&arrow_schema),
                filters,
            ) {
                let batches = batches.map_err(|e| {
                    DataFusionError::Execution(format!("error getting batches {}", e))
                })?;
                let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();

                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &schema,
                    Some(segment.segment_range().timestamp_min_max()),
                    &NoColumnRanges,
                );

                chunks.push(Arc::new(BufferChunk {
                    batches,
                    schema: schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::new(
                        TableId::new(0),
                        segment.segment_key(),
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: ChunkOrder::new(
                        chunks
                            .len()
                            .try_into()
                            .expect("should never have this many chunks"),
                    ),
                }));
            }
        }

        for persisting_segment in self.persisting_segments.values() {
            if let Some(batches) = persisting_segment.buffered_data.table_record_batches(
                &db_schema.name,
                table_name,
                Arc::clone(&arrow_schema),
                filters,
            ) {
                let batches = batches.map_err(|e| {
                    DataFusionError::Execution(format!("error getting batches {}", e))
                })?;
                let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();

                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &schema,
                    Some(persisting_segment.segment_range.timestamp_min_max()),
                    &NoColumnRanges,
                );

                chunks.push(Arc::new(BufferChunk {
                    batches,
                    schema: schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::new(
                        TableId::new(0),
                        &persisting_segment.segment_key,
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: ChunkOrder::new(
                        chunks
                            .len()
                            .try_into()
                            .expect("should never have this many chunks"),
                    ),
                }));
            }
        }

        Ok(chunks)
    }

    pub(crate) fn split_table_for_persistence(
        &mut self,
        segment_id: SegmentId,
        database_name: &str,
        table_name: &str,
    ) -> Option<(ParquetFilePath, RecordBatch)> {
        let db_schema = self.catalog.db_schema(database_name)?;
        let table_schema = db_schema.get_table_schema(table_name)?;

        let segment = self
            .segments
            .values_mut()
            .find(|segment| segment.segment_id() == segment_id)?;
        segment.split_table_for_persistence(database_name, table_name, table_schema)
    }

    pub(crate) fn clear_persisting_table_buffer(
        &mut self,
        parquet_file: ParquetFile,
        segment_id: SegmentId,
        database_name: &str,
        table_name: &str,
    ) -> write_buffer::Result<()> {
        if let Some(segment) = self
            .segments
            .values_mut()
            .find(|segment| segment.segment_id() == segment_id)
        {
            segment.clear_persisting_table_buffer(parquet_file, database_name, table_name)
        } else {
            error!("Failed to find segment with id {:?}", segment_id);
            // caller can't call back in with the same id and get any different result, so log
            // and say it's ok.
            Ok(())
        }
    }

    #[cfg(test)]
    pub(crate) fn open_segment_times(&self) -> Vec<Time> {
        self.segments.keys().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn open_segments_table_record_batches(
        &self,
        db_name: &str,
        table_name: &str,
        schema: &Schema,
    ) -> Vec<RecordBatch> {
        self.segments
            .values()
            .flat_map(|segment| {
                segment
                    .table_record_batches(db_name, table_name, schema.as_arrow(), &[])
                    .unwrap()
                    .unwrap()
            })
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn segment_for_time(&self, time: Time) -> Option<&OpenBufferSegment> {
        self.segments.get(&time)
    }

    // Looks at the open buffer segments and returns the start `Time` of any that meet the following
    // criteria (in time ascending order):
    // 1. The segment is not in the current time or next time
    // 2. The segment has been open for longer than half the segment duration
    pub(crate) fn segments_to_persist(&self, current_time: Time) -> Vec<Time> {
        let mut segments_to_persist = vec![];

        for (start_time, segment) in &self.segments {
            if segment.should_persist(current_time) {
                segments_to_persist.push(*start_time);
            }
        }

        segments_to_persist.sort();
        segments_to_persist
    }

    pub(crate) fn persisting_segments(&self) -> Vec<Arc<ClosedBufferSegment>> {
        self.persisting_segments.values().cloned().collect()
    }

    pub(crate) fn close_segment(
        &mut self,
        segment_start: Time,
    ) -> Option<Arc<ClosedBufferSegment>> {
        self.segments.remove(&segment_start).map(|segment| {
            let closed_segment = Arc::new(segment.into_closed_segment(Arc::clone(&self.catalog)));

            self.persisting_segments
                .insert(segment_start, Arc::clone(&closed_segment));

            closed_segment
        })
    }

    pub(crate) fn remove_persisting_segment(&mut self, segment_start: Time) {
        self.persisting_segments.remove(&segment_start);
    }

    // return the segment with this start time or open up a new one if it isn't currently open.
    fn get_or_create_segment_for_time(
        &mut self,
        time: Time,
        starting_catalog_sequence_number: SequenceNumber,
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
                Arc::clone(&self.catalog),
                segment_id,
                segment_range,
                self.time_provider.now(),
                starting_catalog_sequence_number,
                segment_writer,
                None,
            );
            self.segments.insert(time, segment);
        }

        Ok(self.segments.get_mut(&time).unwrap())
    }

    // Returns the details of open segments with their last write time and their individual table
    // buffer sizes.
    pub(crate) fn open_segments_sizes(&self) -> Vec<SegmentSizes> {
        self.segments
            .values()
            .map(|segment| segment.sizes())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalImpl;
    use iox_time::MockProvider;

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
            Arc::clone(&catalog),
            SegmentId::new(1),
            first_segment_range,
            time_provider.now(),
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(1))),
            None,
        );

        let open_segment2 = OpenBufferSegment::new(
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

        let open_segment3 = OpenBufferSegment::new(
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

        let segment_state: SegmentState<MockProvider, WalImpl> = SegmentState::new(
            SegmentDuration::new_5m(),
            SegmentId::new(4),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
            vec![open_segment1, open_segment2, open_segment3],
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
}
