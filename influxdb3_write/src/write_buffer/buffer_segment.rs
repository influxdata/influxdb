//! A single buffer segment used by the write buffer. This is all the data in memory for a
//! single WAL segment. Only one segment should be open for writes in the write buffer at any
//! given time.

use crate::catalog::{Catalog, DatabaseSchema};
use crate::chunk::BufferChunk;
use crate::paths::ParquetFilePath;
use crate::write_buffer::flusher::BufferedWriteResult;
use crate::write_buffer::table_buffer::{Builder, Result as TableBufferResult, TableBuffer};
use crate::write_buffer::{
    parse_validate_and_update_catalog, Error, TableBatch, ValidSegmentedData,
};
use crate::{
    wal, write_buffer, write_buffer::Result, DatabaseTables, ParquetFile, PersistedSegment,
    Persister, Precision, SegmentDuration, SegmentId, SegmentRange, SequenceNumber,
    TableParquetFiles, WalOp, WalSegmentReader, WalSegmentWriter,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use data_types::ChunkId;
use data_types::ChunkOrder;
use data_types::TableId;
use data_types::TransitionPartitionId;
use data_types::{NamespaceName, PartitionKey};
use datafusion::logical_expr::Expr;
use datafusion_util::stream_from_batches;
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use iox_time::Time;
use schema::sort::SortKey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct OpenBufferSegment {
    segment_writer: Box<dyn WalSegmentWriter>,
    segment_id: SegmentId,
    segment_range: SegmentRange,
    segment_duration: SegmentDuration,
    segment_key: PartitionKey,
    buffered_data: BufferedData,
    segment_open_time: Time,
    catalog: Arc<Catalog>,
    #[allow(dead_code)]
    starting_catalog_sequence_number: SequenceNumber,
    // TODO: This is temporarily just the number of rows in the segment. When the buffer gets refactored to use
    //       different structures, we want this to be a representation of approximate memory usage.
    segment_size: usize,
}

impl OpenBufferSegment {
    pub fn new(
        catalog: Arc<Catalog>,
        segment_id: SegmentId,
        segment_range: SegmentRange,
        segment_open_time: Time,
        starting_catalog_sequence_number: SequenceNumber,
        segment_writer: Box<dyn WalSegmentWriter>,
        buffered_data: Option<(BufferedData, usize)>,
    ) -> Self {
        let (buffered_data, segment_size) = buffered_data.unwrap_or_default();
        let segment_key = PartitionKey::from(segment_range.key());
        let segment_duration = SegmentDuration::from_range(segment_range);

        Self {
            catalog,
            segment_writer,
            segment_id,
            segment_range,
            segment_duration,
            segment_open_time,
            segment_key,
            starting_catalog_sequence_number,
            segment_size,
            buffered_data,
        }
    }

    #[cfg(test)]
    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    pub fn segment_range(&self) -> &SegmentRange {
        &self.segment_range
    }

    pub fn segment_key(&self) -> &PartitionKey {
        &self.segment_key
    }

    pub fn write_wal_ops(&mut self, write_batch: Vec<WalOp>) -> wal::Result<()> {
        self.segment_writer.write_batch(write_batch)
    }

    #[cfg(test)]
    pub fn starting_catalog_sequence_number(&self) -> SequenceNumber {
        self.starting_catalog_sequence_number
    }

    /// Adds the batch into the in memory buffer.
    pub(crate) fn buffer_writes(&mut self, write_batch: WriteBatch) -> Result<()> {
        for (db_name, db_batch) in write_batch.database_batches {
            let db_buffer = self
                .buffered_data
                .database_buffers
                .entry(db_name.to_string())
                .or_insert_with(|| {
                    let db_schema = self
                        .catalog
                        .db_schema(&db_name)
                        .expect("db schema should exist");
                    DatabaseBuffer {
                        table_buffers: HashMap::new(),
                        db_schema,
                    }
                });

            for (table_name, table_batch) in db_batch.table_batches {
                // TODO: for now we'll just have the number of rows represent the segment size. The entire
                //       buffer is going to get refactored to use different structures, so this will change.
                self.segment_size += table_batch.rows.len();

                db_buffer.buffer_table_batch(table_name, &self.segment_key, table_batch);
            }
        }

        Ok(())
    }

    /// Returns the table data as record batches
    pub(crate) fn table_record_batch(
        &self,
        db_name: &str,
        table_name: &str,
        schema: SchemaRef,
        filter: &[Expr],
    ) -> Option<TableBufferResult<RecordBatch>> {
        self.buffered_data
            .table_record_batches(db_name, table_name, schema, filter)
    }

    /// Returns true if the segment should be persisted. A segment should be persisted if both of
    /// the following are true:
    /// 1. The segment has been open longer than half its duration
    /// 2. The current time is past the end time of the segment + half its duration
    pub fn should_persist(&self, current_time: Time) -> bool {
        let half_duration_seconds = self.segment_duration.duration_seconds() / 2;
        let open_duration_seconds = current_time
            .checked_duration_since(self.segment_open_time)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;

        let segment_end_epoch = self.segment_range.end_time.timestamp();
        let end_time_age_out_epoch = segment_end_epoch + half_duration_seconds;

        open_duration_seconds > half_duration_seconds
            && current_time.timestamp() > end_time_age_out_epoch
    }

    #[allow(dead_code)]
    pub fn into_closed_segment(self, catalog: Arc<Catalog>) -> ClosedBufferSegment {
        ClosedBufferSegment::new(
            self.segment_id,
            self.segment_range,
            self.segment_key,
            self.starting_catalog_sequence_number,
            catalog.sequence_number(),
            self.buffered_data,
            self.segment_writer.bytes_written(),
            catalog,
        )
    }
}

pub(crate) fn load_buffer_from_segment(
    catalog: &Catalog,
    mut segment_reader: Box<dyn WalSegmentReader>,
) -> Result<(BufferedData, usize)> {
    let mut segment_size = 0;
    let mut buffered_data = BufferedData::default();
    let segment_key = PartitionKey::from(segment_reader.header().range.key());
    let segment_duration = SegmentDuration::from_range(segment_reader.header().range);

    while let Some(batch) = segment_reader.next_batch()? {
        for wal_op in batch.ops {
            match wal_op {
                WalOp::LpWrite(write) => {
                    let mut validated_write = parse_validate_and_update_catalog(
                        NamespaceName::new(write.db_name.clone())?,
                        &write.lp,
                        catalog,
                        Time::from_timestamp_nanos(write.default_time),
                        segment_duration,
                        false,
                        Precision::Nanosecond,
                    )?;

                    let db_name = &write.db_name;
                    if !buffered_data.database_buffers.contains_key(db_name) {
                        let db_schema = catalog.db_schema(db_name).expect("db schema should exist");
                        buffered_data.database_buffers.insert(
                            db_name.clone(),
                            DatabaseBuffer {
                                table_buffers: HashMap::new(),
                                db_schema,
                            },
                        );
                    }
                    let db_buffer = buffered_data.database_buffers.get_mut(db_name).unwrap();

                    // there should only ever be data for a single segment as this is all read
                    // from one segment file
                    if validated_write.valid_segmented_data.len() != 1 {
                        return Err(Error::WalOpForMultipleSegments(
                            segment_reader.path().to_string(),
                        ));
                    }
                    let segment_data = validated_write.valid_segmented_data.pop().unwrap();

                    for (table_name, table_batch) in segment_data.table_batches {
                        // TODO: for now we'll just have the number of rows represent the segment size. The entire
                        //       buffer is going to get refactored to use different structures, so this will change.
                        segment_size += table_batch.rows.len();

                        db_buffer.buffer_table_batch(table_name, &segment_key, table_batch);
                    }
                }
            }
        }
    }

    Ok((buffered_data, segment_size))
}

#[derive(Debug, Default)]
pub(crate) struct WriteBatch {
    database_batches: HashMap<NamespaceName<'static>, DatabaseBatch>,
}

impl WriteBatch {
    pub(crate) fn add_db_write(
        &mut self,
        db_name: NamespaceName<'static>,
        table_batches: HashMap<String, TableBatch>,
    ) {
        let db_batch = self.database_batches.entry(db_name).or_default();
        db_batch.add_table_batches(table_batches);
    }
}

#[derive(Debug, Default)]
struct DatabaseBatch {
    table_batches: HashMap<String, TableBatch>,
}

impl DatabaseBatch {
    fn add_table_batches(&mut self, table_batches: HashMap<String, TableBatch>) {
        for (table_name, table_batch) in table_batches {
            let write_table_batch = self.table_batches.entry(table_name).or_default();
            write_table_batch.rows.extend(table_batch.rows);
        }
    }
}

pub struct BufferedWrite {
    pub segmented_data: Vec<ValidSegmentedData>,
    pub response_tx: oneshot::Sender<BufferedWriteResult>,
}

#[derive(Debug, Default)]
pub struct BufferedData {
    database_buffers: HashMap<String, DatabaseBuffer>,
}

impl BufferedData {
    /// Returns the table data as record batches
    pub(crate) fn table_record_batches(
        &self,
        db_name: &str,
        table_name: &str,
        schema: SchemaRef,
        filter: &[Expr],
    ) -> Option<TableBufferResult<RecordBatch>> {
        self.database_buffers
            .get(db_name)
            .and_then(|db_buffer| db_buffer.table_buffers.get(table_name))
            .map(|table_buffer| table_buffer.record_batch(schema, filter))
    }

    /// Verifies that the passed in buffer has the same data as this buffer
    #[cfg(test)]
    pub(crate) fn verify_matches(&self, other: &BufferedData, catalog: &Catalog) {
        assert_eq!(self.database_buffers.len(), other.database_buffers.len());
        for (db_name, db_buffer) in &self.database_buffers {
            let other_db_buffer = other.database_buffers.get(db_name).unwrap();
            let db_schema = catalog.db_schema(db_name).unwrap();

            for table_name in db_buffer.table_buffers.keys() {
                let table_buffer = db_buffer.table_buffers.get(table_name).unwrap();
                let other_table_buffer = other_db_buffer.table_buffers.get(table_name).unwrap();
                let schema = db_schema.get_table_schema(table_name).unwrap();

                let table_data = table_buffer.record_batch(schema.as_arrow(), &[]).unwrap();
                let other_table_data = other_table_buffer
                    .record_batch(schema.as_arrow(), &[])
                    .unwrap();

                assert_eq!(table_data, other_table_data);
            }
        }
    }
}

#[derive(Debug)]
struct DatabaseBuffer {
    table_buffers: HashMap<String, TableBuffer>,
    db_schema: Arc<DatabaseSchema>,
}

impl DatabaseBuffer {
    fn buffer_table_batch(
        &mut self,
        table_name: String,
        segment_key: &PartitionKey,
        table_batch: TableBatch,
    ) {
        if !self.table_buffers.contains_key(&table_name) {
            // TODO: this check shouldn't be necessary. If the table doesn't exist in the catalog
            //      and we've gotten here, it means we're dropping a write.
            if let Some(table) = self.db_schema.get_table(&table_name) {
                self.table_buffers.insert(
                    table_name.clone(),
                    TableBuffer::new(segment_key.clone(), &table.index_columns()),
                );
            } else {
                return;
            }
        }
        let table_buffer = self
            .table_buffers
            .get_mut(&table_name)
            .expect("table buffer should exist");

        table_buffer.add_rows(table_batch.rows);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ClosedBufferSegment {
    pub segment_id: SegmentId,
    pub segment_range: SegmentRange,
    pub segment_key: PartitionKey,
    pub catalog_start_sequence_number: SequenceNumber,
    pub catalog_end_sequence_number: SequenceNumber,
    pub buffered_data: BufferedData,
    pub segment_wal_bytes: u64,
    catalog: Arc<Catalog>,
}

impl ClosedBufferSegment {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    fn new(
        segment_id: SegmentId,
        segment_range: SegmentRange,
        segment_key: PartitionKey,
        catalog_start_sequence_number: SequenceNumber,
        catalog_end_sequence_number: SequenceNumber,
        buffered_data: BufferedData,
        segment_wal_bytes: u64,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            segment_id,
            segment_range,
            segment_key,
            catalog_start_sequence_number,
            catalog_end_sequence_number,
            buffered_data,
            segment_wal_bytes,
            catalog,
        }
    }

    pub(crate) async fn persist<P>(
        &self,
        persister: Arc<P>,
        executor: Arc<iox_query::exec::Executor>,
        sort_key: Option<SortKey>,
    ) -> Result<PersistedSegment>
    where
        P: Persister,
        write_buffer::Error: From<<P as Persister>::Error>,
    {
        if self.catalog_start_sequence_number != self.catalog_end_sequence_number {
            let inner_catalog = self.catalog.clone_inner();

            persister
                .persist_catalog(self.segment_id, Catalog::from_inner(inner_catalog))
                .await?;
        }

        let mut persisted_database_files = HashMap::new();
        let mut segment_parquet_size_bytes = 0;
        let mut segment_row_count = 0;
        let mut segment_min_time = i64::MAX;
        let mut segment_max_time = i64::MIN;

        // persist every partition buffer
        for (db_name, db_buffer) in &self.buffered_data.database_buffers {
            let mut database_tables = DatabaseTables::default();

            if let Some(db_schema) = self.catalog.db_schema(db_name) {
                for (table_name, table_buffer) in &db_buffer.table_buffers {
                    if let Some(table) = db_schema.get_table(table_name) {
                        let mut table_parquet_files = TableParquetFiles {
                            table_name: table_name.to_string(),
                            parquet_files: vec![],
                            sort_key: vec![],
                        };

                        // All of the record batches for this table that we will
                        // want to dedupe
                        let batch = table_buffer.record_batch(table.schema().as_arrow(), &[])?;
                        let row_count = batch.num_rows();

                        // Dedupe and sort using the COMPACT query built into
                        // iox_query
                        let mut chunks: Vec<Arc<dyn QueryChunk>> = vec![];
                        let time_min_max = table_buffer.timestamp_min_max();
                        let schema = table.schema();

                        let chunk_stats = create_chunk_statistics(
                            Some(row_count),
                            schema,
                            Some(time_min_max),
                            None,
                        );

                        chunks.push(Arc::new(BufferChunk {
                            batches: vec![batch],
                            schema: schema.clone(),
                            stats: Arc::new(chunk_stats),
                            partition_id: TransitionPartitionId::new(
                                TableId::new(0),
                                &self.segment_key,
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

                        let ctx = executor.new_context();

                        let sort_key = match sort_key.as_ref() {
                            Some(key) => key.clone(),
                            // Default to using tags sorted in lexographical
                            // order as the sort key
                            None => {
                                let mut tags = table_buffer
                                    .data
                                    .iter()
                                    .filter(|(_, v)| matches!(v, Builder::Tag(_)))
                                    .map(|(k, _)| k)
                                    .cloned()
                                    .collect::<Vec<String>>();
                                tags.sort();
                                SortKey::from(tags)
                            }
                        };

                        let logical_plan = ReorgPlanner::new()
                            .compact_plan(
                                Arc::from(table_name.clone()),
                                table.schema(),
                                chunks,
                                sort_key,
                            )
                            .unwrap();

                        // Build physical plan
                        let physical_plan = ctx.create_physical_plan(&logical_plan).await.unwrap();

                        // Execute the plan and return compacted record batches
                        let data = ctx.collect(physical_plan).await.unwrap();

                        // Get the new row count before turning it into a
                        // stream. We couldn't turn the data directly into a
                        // stream since we needed the row count for
                        // `ParquetFile` below
                        let row_count = data.iter().map(|b| b.num_rows()).sum::<usize>();

                        let batch_stream = stream_from_batches(table.schema().as_arrow(), data);
                        let parquet_file_path = ParquetFilePath::new_with_partition_key(
                            db_name,
                            &table.name,
                            &table_buffer.segment_key.to_string(),
                            self.segment_id.0,
                        );
                        let path = parquet_file_path.to_string();
                        let (size_bytes, meta) = persister
                            .persist_parquet_file(parquet_file_path, batch_stream)
                            .await?;

                        let parquet_file = ParquetFile {
                            path,
                            size_bytes,
                            row_count: row_count as u64,
                            min_time: time_min_max.min,
                            max_time: time_min_max.max,
                        };
                        table_parquet_files.parquet_files.push(parquet_file);

                        segment_parquet_size_bytes += size_bytes;
                        segment_row_count += meta.num_rows as u64;
                        segment_max_time = segment_max_time.max(time_min_max.max);
                        segment_min_time = segment_min_time.min(time_min_max.min);

                        if !table_parquet_files.parquet_files.is_empty() {
                            database_tables
                                .tables
                                .insert(table_name.to_string(), table_parquet_files);
                        }
                    }
                }
            }

            if !database_tables.tables.is_empty() {
                persisted_database_files.insert(db_name.to_string(), database_tables);
            }
        }

        let persisted_segment = PersistedSegment {
            segment_id: self.segment_id,
            segment_wal_size_bytes: self.segment_wal_bytes,
            segment_parquet_size_bytes,
            segment_row_count,
            segment_min_time,
            segment_max_time,
            databases: persisted_database_files,
        };

        persister.persist_segment(&persisted_segment).await?;

        Ok(persisted_segment)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::test_helpers::{lp_to_table_batches, lp_to_write_batch};
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::{persister, LpWriteOp, PersistedCatalog};
    use arrow_util::assert_batches_eq;
    use bytes::Bytes;
    use datafusion::execution::SendableRecordBatchStream;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use parquet::format::FileMetaData;
    use std::any::Any;
    use std::str::FromStr;

    #[test]
    fn buffers_rows() {
        let catalog = Arc::new(Catalog::new());
        let mut open_segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(0),
            SegmentRange::test_range(),
            Time::from_timestamp_nanos(0),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
            None,
        );

        let db_name: NamespaceName<'static> = NamespaceName::new("db1").unwrap();

        let batches = lp_to_table_batches(
            &catalog,
            "db1",
            "cpu,tag1=cupcakes bar=1 10\nmem,tag2=snakes bar=2 20",
            10,
        );
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let batches = lp_to_table_batches(&catalog, "db1", "cpu,tag1=cupcakes bar=2 30", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let db_schema = catalog.db_schema("db1").unwrap();
        let cpu_table = open_segment
            .table_record_batch(
                "db1",
                "cpu",
                db_schema.get_table_schema("cpu").unwrap().as_arrow(),
                &[],
            )
            .unwrap()
            .unwrap();
        let expected_cpu_table = vec![
            "+-----+----------+--------------------------------+",
            "| bar | tag1     | time                           |",
            "+-----+----------+--------------------------------+",
            "| 1.0 | cupcakes | 1970-01-01T00:00:00.000000010Z |",
            "| 2.0 | cupcakes | 1970-01-01T00:00:00.000000030Z |",
            "+-----+----------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_cpu_table, &[cpu_table]);

        let mem_table = open_segment
            .table_record_batch(
                "db1",
                "mem",
                db_schema.get_table_schema("mem").unwrap().as_arrow(),
                &[],
            )
            .unwrap()
            .unwrap();
        let expected_mem_table = vec![
            "+-----+--------+--------------------------------+",
            "| bar | tag2   | time                           |",
            "+-----+--------+--------------------------------+",
            "| 2.0 | snakes | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_mem_table, &[mem_table]);
    }

    #[tokio::test]
    async fn buffers_schema_update() {
        let catalog = Arc::new(Catalog::new());
        let mut open_segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(0),
            SegmentRange::test_range(),
            Time::from_timestamp_nanos(0),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
            None,
        );

        let db_name: NamespaceName<'static> = NamespaceName::new("db1").unwrap();

        let batches = lp_to_table_batches(&catalog, "db1", "cpu,tag1=cupcakes bar=1 10", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let batches = lp_to_table_batches(&catalog, "db1", "cpu,tag2=asdf bar=2 30", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let batches = lp_to_table_batches(&catalog, "db1", "cpu bar=2,ival=7i 30", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let batches =
            lp_to_table_batches(&catalog, "db1", "cpu bar=2,ival=9i 40\ncpu fval=2.1 40", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let db_schema = catalog.db_schema("db1").unwrap();
        println!("{:?}", db_schema);
        let cpu_table = open_segment
            .table_record_batch(
                "db1",
                "cpu",
                db_schema.get_table_schema("cpu").unwrap().as_arrow(),
                &[],
            )
            .unwrap()
            .unwrap();
        let expected_cpu_table = vec![
            "+-----+------+------+----------+------+--------------------------------+",
            "| bar | fval | ival | tag1     | tag2 | time                           |",
            "+-----+------+------+----------+------+--------------------------------+",
            "| 1.0 |      |      | cupcakes |      | 1970-01-01T00:00:00.000000010Z |",
            "| 2.0 |      |      |          | asdf | 1970-01-01T00:00:00.000000030Z |",
            "| 2.0 |      | 7    |          |      | 1970-01-01T00:00:00.000000030Z |",
            "| 2.0 |      | 9    |          |      | 1970-01-01T00:00:00.000000040Z |",
            "|     | 2.1  |      |          |      | 1970-01-01T00:00:00.000000040Z |",
            "+-----+------+------+----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected_cpu_table, &[cpu_table]);
    }

    #[tokio::test]
    async fn persist_closed_buffer() {
        const SEGMENT_KEY: &str = "1970-01-01T00-00";

        let segment_id = SegmentId::new(4);
        let segment_writer = Box::new(WalSegmentWriterNoopImpl::new(segment_id));
        let catalog = Arc::new(Catalog::new());
        let mut open_segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            segment_id,
            SegmentRange::test_range(),
            Time::from_timestamp_nanos(0),
            SequenceNumber::new(0),
            segment_writer,
            None,
        );

        // When we persist the data all of these duplicates should be removed
        let lp = "cpu,tag1=cupcakes bar=1 10\n\
                  cpu,tag1=cupcakes bar=1 10\n\
                  cpu,tag1=something bar=5 10\n\
                  cpu,tag1=cupcakes bar=1 10\n\
                  mem,tag2=turtles bar=3 15\n\
                  cpu,tag1=cupcakes bar=1 10\n\
                  cpu,tag1=cupcakes bar=1 10\n\
                  mem,tag2=turtles bar=3 15\n\
                  mem,tag2=turtles bar=3 15\n\
                  mem,tag2=snakes bar=2 20\n\
                  mem,tag2=turtles bar=3 15\n\
                  mem,tag2=turtles bar=3 15";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "db1".to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, "db1", lp);

        open_segment.write_wal_ops(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let catalog = Arc::new(catalog);
        let closed_buffer_segment = open_segment.into_closed_segment(Arc::clone(&catalog));

        let persister = Arc::new(TestPersister::default());
        closed_buffer_segment
            .persist(Arc::clone(&persister), crate::test_help::make_exec(), None)
            .await
            .unwrap();

        let persisted_state = persister
            .as_any()
            .downcast_ref::<TestPersister>()
            .unwrap()
            .state
            .lock();
        assert_eq!(
            persisted_state.catalog.first().unwrap().clone_inner(),
            catalog.clone_inner()
        );
        let segment_info = persisted_state.segments.first().unwrap();

        assert_eq!(segment_info.segment_id, segment_id);
        assert_eq!(segment_info.segment_min_time, 10);
        assert_eq!(segment_info.segment_max_time, 20);
        assert_eq!(segment_info.segment_row_count, 2);
        // in the mock each parquet file is 1 byte, so should have 2
        assert_eq!(segment_info.segment_parquet_size_bytes, 2);
        // should have been one write into the wal
        assert_eq!(segment_info.segment_wal_size_bytes, 1);
        // one file for cpu, one for mem
        assert_eq!(persisted_state.parquet_files.len(), 2);

        println!("segment_info:\n{:#?}", segment_info);
        let db = segment_info.databases.get("db1").unwrap();
        let cpu = db.tables.get("cpu").unwrap();
        let cpu_parqet = &cpu.parquet_files[0];

        // file number of the path should match the segment id
        assert_eq!(
            cpu_parqet.path,
            ParquetFilePath::new_with_partition_key("db1", "cpu", SEGMENT_KEY, 4).to_string()
        );
        assert_eq!(cpu_parqet.row_count, 2);
        assert_eq!(cpu_parqet.min_time, 10);
        assert_eq!(cpu_parqet.max_time, 10);

        let mem = db.tables.get("mem").unwrap();
        let mem_parqet = &mem.parquet_files[0];

        // file number of the path should match the segment id
        assert_eq!(
            mem_parqet.path,
            ParquetFilePath::new_with_partition_key("db1", "mem", SEGMENT_KEY, 4).to_string()
        );
        assert_eq!(mem_parqet.row_count, 2);
        assert_eq!(mem_parqet.min_time, 15);
        assert_eq!(mem_parqet.max_time, 20);
    }

    #[test]
    fn should_persist() {
        let catalog = Arc::new(Catalog::new());
        let segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(0),
            SegmentRange::from_time_and_duration(
                Time::from_timestamp_nanos(0),
                SegmentDuration::from_str("1m").unwrap(),
                false,
            ),
            Time::from_timestamp_nanos(0),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
            None,
        );

        // time is in current segment
        assert!(!segment.should_persist(Time::from_timestamp(30, 0).unwrap()));

        // time is in next segment, but before half duration
        assert!(!segment.should_persist(Time::from_timestamp(61, 0).unwrap()));

        // time is in next segment, and after half duration
        assert!(segment.should_persist(Time::from_timestamp(61 + 30, 0).unwrap()));

        let segment = OpenBufferSegment::new(
            Arc::clone(&catalog),
            SegmentId::new(0),
            SegmentRange::from_time_and_duration(
                Time::from_timestamp_nanos(0),
                SegmentDuration::from_str("1m").unwrap(),
                false,
            ),
            Time::from_timestamp(500, 0).unwrap(),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
            None,
        );

        // time has advanced from segment open time, but is still less than half duration away
        assert!(!segment.should_persist(Time::from_timestamp(500 + 29, 0).unwrap()));

        // time has advanced from segment open time, and is now half duration away
        assert!(segment.should_persist(Time::from_timestamp(500 + 31, 0).unwrap()));
    }

    #[derive(Debug, Default)]
    pub(crate) struct TestPersister {
        pub(crate) state: Mutex<PersistedState>,
    }

    #[derive(Debug, Default)]
    pub(crate) struct PersistedState {
        pub(crate) catalog: Vec<Catalog>,
        pub(crate) segments: Vec<PersistedSegment>,
        pub(crate) parquet_files: Vec<ParquetFilePath>,
    }

    #[async_trait::async_trait]
    impl Persister for TestPersister {
        type Error = persister::Error;

        async fn persist_catalog(
            &self,
            _segment_id: SegmentId,
            catalog: Catalog,
        ) -> persister::Result<()> {
            self.state.lock().catalog.push(catalog);
            Ok(())
        }

        async fn persist_parquet_file(
            &self,
            path: ParquetFilePath,
            _data: SendableRecordBatchStream,
        ) -> persister::Result<(u64, FileMetaData)> {
            self.state.lock().parquet_files.push(path);
            let meta = FileMetaData::new(1, vec![], 1, vec![], None, None, None, None, None);
            Ok((1, meta))
        }

        async fn persist_segment(&self, segment: &PersistedSegment) -> persister::Result<()> {
            self.state.lock().segments.push(segment.clone());
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self as &dyn Any
        }

        async fn load_catalog(&self) -> persister::Result<Option<PersistedCatalog>> {
            todo!()
        }

        async fn load_segments(
            &self,
            _most_recent_n: usize,
        ) -> persister::Result<Vec<PersistedSegment>> {
            todo!()
        }

        async fn load_parquet_file(&self, _path: ParquetFilePath) -> persister::Result<Bytes> {
            todo!()
        }

        fn object_store(&self) -> Arc<dyn ObjectStore> {
            todo!()
        }
    }
}
