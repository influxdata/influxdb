//! A single buffer segment used by the write buffer. This is all the data in memory for a
//! single WAL segment. Only one segment should be open for writes in the write buffer at any
//! given time.

use crate::catalog::Catalog;
use crate::paths::ParquetFilePath;
use crate::write_buffer::flusher::BufferedWriteResult;
use crate::write_buffer::{FieldData, Row, TableBatch};
use crate::{
    wal, write_buffer::Result, DatabaseTables, ParquetFile, PersistedSegment, Persister, SegmentId,
    SequenceNumber, TableParquetFiles, WalOp, WalSegmentWriter,
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::{ColumnType, NamespaceName, TimestampMinMax};
use datafusion_util::stream_from_batch;
use schema::Schema;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct OpenBufferSegment {
    segment_writer: Box<dyn WalSegmentWriter>,
    segment_id: SegmentId,
    buffered_data: HashMap<String, DatabaseBuffer>,
    #[allow(dead_code)]
    starting_catalog_sequence_number: SequenceNumber,
    // TODO: This is temporarily just the number of rows in the segment. When the buffer gets refactored to use
    //       different structures, we want this to be a representation of approximate memory usage.
    segment_size: usize,
}

impl OpenBufferSegment {
    pub fn new(
        segment_id: SegmentId,
        starting_catalog_sequence_number: SequenceNumber,
        segment_writer: Box<dyn WalSegmentWriter>,
    ) -> Self {
        Self {
            buffered_data: Default::default(),
            segment_writer,
            segment_id,
            starting_catalog_sequence_number,
            segment_size: 0,
        }
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }
    pub fn write_batch(&mut self, write_batch: Vec<WalOp>) -> wal::Result<SequenceNumber> {
        self.segment_writer.write_batch(write_batch)
    }

    pub fn table_buffer(&self, db_name: &str, table_name: &str) -> Option<TableBuffer> {
        self.buffered_data
            .get(db_name)
            .and_then(|db_buffer| db_buffer.table_buffers.get(table_name).cloned())
    }

    /// Adds the batch into the in memory buffer. Returns the number of rows in the segment after the write.
    pub(crate) fn buffer_writes(&mut self, write_batch: WriteBatch) -> Result<usize> {
        for (db_name, db_batch) in write_batch.database_batches {
            let db_buffer = self.buffered_data.entry(db_name.to_string()).or_default();

            for (table_name, table_batch) in db_batch.table_batches {
                let table_buffer = db_buffer.table_buffers.entry(table_name).or_default();

                for (partition_key, partition_batch) in table_batch.partition_batches {
                    let partition_buffer = table_buffer
                        .partition_buffers
                        .entry(partition_key)
                        .or_default();

                    // TODO: for now we'll just have the number of rows represent the segment size. The entire
                    //       buffer is going to get refactored to use different structures, so this will change.
                    self.segment_size += partition_batch.rows.len();

                    partition_buffer.add_rows(partition_batch.rows);
                }
            }
        }

        Ok(self.segment_size)
    }

    #[allow(dead_code)]
    pub fn into_closed_segment(self, catalog: Arc<Catalog>) -> ClosedBufferSegment {
        ClosedBufferSegment::new(
            self.segment_id,
            self.starting_catalog_sequence_number,
            catalog.sequence_number(),
            self.segment_writer,
            self.buffered_data,
            catalog,
        )
    }
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

            for (partition_key, partition_batch) in table_batch.partition_batches {
                let write_partition_batch = write_table_batch
                    .partition_batches
                    .entry(partition_key)
                    .or_default();
                write_partition_batch.rows.extend(partition_batch.rows);
            }
        }
    }
}

pub struct BufferedWrite {
    pub wal_op: WalOp,
    pub database_write: DatabaseWrite,
    pub response_tx: oneshot::Sender<BufferedWriteResult>,
}

pub struct DatabaseWrite {
    pub(crate) db_name: NamespaceName<'static>,
    pub(crate) table_batches: HashMap<String, TableBatch>,
}

impl DatabaseWrite {
    pub fn new(
        db_name: NamespaceName<'static>,
        table_batches: HashMap<String, TableBatch>,
    ) -> Self {
        Self {
            db_name,
            table_batches,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WriteSummary {
    pub segment_id: SegmentId,
    pub sequence_number: SequenceNumber,
    pub buffer_size: usize,
}

#[derive(Debug, Default)]
struct DatabaseBuffer {
    table_buffers: HashMap<String, TableBuffer>,
}

#[derive(Debug, Default, Clone)]
pub struct TableBuffer {
    pub partition_buffers: HashMap<String, PartitionBuffer>,
}

impl TableBuffer {
    #[allow(dead_code)]
    pub fn partition_buffer(&self, partition_key: &str) -> Option<&PartitionBuffer> {
        self.partition_buffers.get(partition_key)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionBuffer {
    rows: Vec<Row>,
    timestamp_min: i64,
    timestamp_max: i64,
}

impl Default for PartitionBuffer {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            timestamp_min: i64::MAX,
            timestamp_max: i64::MIN,
        }
    }
}

impl PartitionBuffer {
    pub fn add_rows(&mut self, rows: Vec<Row>) {
        self.rows.reserve(rows.len());
        for row in rows {
            self.timestamp_min = self.timestamp_min.min(row.time);
            self.timestamp_max = self.timestamp_max.max(row.time);
            self.rows.push(row);
        }
    }

    pub fn timestamp_min_max(&self) -> TimestampMinMax {
        TimestampMinMax {
            min: self.timestamp_min,
            max: self.timestamp_max,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn rows_to_record_batch(
        &self,
        schema: &Schema,
        column_types: &BTreeMap<String, i16>,
    ) -> RecordBatch {
        let row_count = self.rows.len();
        let mut columns = BTreeMap::new();
        for (name, column_type) in column_types {
            match ColumnType::try_from(*column_type).unwrap() {
                ColumnType::Bool => columns.insert(
                    name,
                    Builder::Bool(BooleanBuilder::with_capacity(row_count)),
                ),
                ColumnType::F64 => {
                    columns.insert(name, Builder::F64(Float64Builder::with_capacity(row_count)))
                }
                ColumnType::I64 => {
                    columns.insert(name, Builder::I64(Int64Builder::with_capacity(row_count)))
                }
                ColumnType::U64 => {
                    columns.insert(name, Builder::U64(UInt64Builder::with_capacity(row_count)))
                }
                ColumnType::String => columns.insert(name, Builder::String(StringBuilder::new())),
                ColumnType::Tag => {
                    columns.insert(name, Builder::Tag(StringDictionaryBuilder::new()))
                }
                ColumnType::Time => columns.insert(
                    name,
                    Builder::Time(TimestampNanosecondBuilder::with_capacity(row_count)),
                ),
            };
        }

        for r in &self.rows {
            let mut value_added = HashSet::with_capacity(r.fields.len());

            for f in &r.fields {
                let builder = columns.get_mut(&f.name).unwrap();
                match (&f.value, builder) {
                    (FieldData::Timestamp(v), Builder::Time(b)) => b.append_value(*v),
                    (FieldData::Tag(v), Builder::Tag(b)) => {
                        b.append(v).unwrap();
                    }
                    (FieldData::String(v), Builder::String(b)) => b.append_value(v),
                    (FieldData::Integer(v), Builder::I64(b)) => b.append_value(*v),
                    (FieldData::UInteger(v), Builder::U64(b)) => b.append_value(*v),
                    (FieldData::Float(v), Builder::F64(b)) => b.append_value(*v),
                    (FieldData::Boolean(v), Builder::Bool(b)) => b.append_value(*v),
                    _ => panic!("unexpected field type"),
                }
                value_added.insert(&f.name);
            }

            for (name, builder) in &mut columns {
                if !value_added.contains(name) {
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::F64(b) => b.append_null(),
                        Builder::I64(b) => b.append_null(),
                        Builder::U64(b) => b.append_null(),
                        Builder::String(b) => b.append_null(),
                        Builder::Tag(b) => b.append_null(),
                        Builder::Time(b) => b.append_null(),
                    }
                }
            }
        }

        // ensure the order of the columns matches their order in the Arrow schema definition
        let mut cols = Vec::with_capacity(columns.len());
        let schema = schema.as_arrow();
        for f in &schema.fields {
            cols.push(columns.remove(f.name()).unwrap().into_arrow());
        }

        RecordBatch::try_new(schema, cols).unwrap()
    }
}

enum Builder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    U64(UInt64Builder),
    String(StringBuilder),
    Tag(StringDictionaryBuilder<Int32Type>),
    Time(TimestampNanosecondBuilder),
}

impl Builder {
    fn into_arrow(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::U64(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Tag(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ClosedBufferSegment {
    segment_id: SegmentId,
    catalog_start_sequence_number: SequenceNumber,
    catalog_end_sequence_number: SequenceNumber,
    segment_writer: Box<dyn WalSegmentWriter>,
    buffered_data: HashMap<String, DatabaseBuffer>,
    catalog: Arc<Catalog>,
}

impl ClosedBufferSegment {
    #[allow(dead_code)]
    fn new(
        segment_id: SegmentId,
        catalog_start_sequence_number: SequenceNumber,
        catalog_end_sequence_number: SequenceNumber,
        segment_writer: Box<dyn WalSegmentWriter>,
        buffered_data: HashMap<String, DatabaseBuffer>,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            segment_id,
            catalog_start_sequence_number,
            catalog_end_sequence_number,
            segment_writer,
            buffered_data,
            catalog,
        }
    }

    #[allow(dead_code)]
    pub async fn persist(&self, persister: Arc<dyn Persister>) -> crate::Result<()> {
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
        for (db_name, db_buffer) in &self.buffered_data {
            let mut database_tables = DatabaseTables::default();

            if let Some(db_schema) = self.catalog.db_schema(db_name) {
                for (table_name, table_buffer) in &db_buffer.table_buffers {
                    if let Some(table) = db_schema.get_table(table_name) {
                        let mut table_parquet_files = TableParquetFiles {
                            table_name: table_name.to_string(),
                            parquet_files: vec![],
                            sort_key: vec![],
                        };

                        // persist every partition buffer
                        for (partition_key, partition_buffer) in &table_buffer.partition_buffers {
                            let data = partition_buffer
                                .rows_to_record_batch(table.schema(), table.columns());
                            let row_count = data.num_rows();
                            let batch_stream = stream_from_batch(table.schema().as_arrow(), data);
                            let parquet_file_path = ParquetFilePath::new_with_parititon_key(
                                db_name,
                                &table.name,
                                partition_key,
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
                                min_time: partition_buffer.timestamp_min,
                                max_time: partition_buffer.timestamp_max,
                            };
                            table_parquet_files.parquet_files.push(parquet_file);

                            segment_parquet_size_bytes += size_bytes;
                            segment_row_count += meta.num_rows as u64;
                            segment_max_time = segment_max_time.max(partition_buffer.timestamp_max);
                            segment_min_time = segment_min_time.min(partition_buffer.timestamp_min);
                        }

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
            segment_wal_size_bytes: self.segment_writer.bytes_written(),
            segment_parquet_size_bytes,
            segment_row_count,
            segment_min_time,
            segment_max_time,
            databases: persisted_database_files,
        };

        persister.persist_segment(persisted_segment).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::write_buffer::tests::lp_to_table_batches;
    use crate::write_buffer::{parse_validate_and_update_schema, Partitioner};
    use crate::Precision;
    use crate::{LpWriteOp, PersistedCatalog};
    use bytes::Bytes;
    use datafusion::execution::SendableRecordBatchStream;
    use object_store::ObjectStore;
    use parking_lot::Mutex;
    use parquet::format::FileMetaData;
    use std::any::Any;

    #[test]
    fn buffers_rows() {
        let mut open_segment = OpenBufferSegment::new(
            SegmentId::new(0),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
        );

        let db_name: NamespaceName<'static> = NamespaceName::new("db1").unwrap();

        let batches =
            lp_to_table_batches("cpu,tag1=cupcakes bar=1 10\nmem,tag2=snakes bar=2 20", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let batches = lp_to_table_batches("cpu,tag1=cupcakes bar=2 30", 10);
        let mut write_batch = WriteBatch::default();
        write_batch.add_db_write(db_name.clone(), batches);
        open_segment.buffer_writes(write_batch).unwrap();

        let cpu_table = open_segment.table_buffer(&db_name, "cpu").unwrap();
        let cpu_partition = cpu_table.partition_buffers.get("1970-01-01").unwrap();
        assert_eq!(cpu_partition.rows.len(), 2);
        assert_eq!(cpu_partition.timestamp_min, 10);
        assert_eq!(cpu_partition.timestamp_max, 30);

        let mem_table = open_segment.table_buffer(&db_name, "mem").unwrap();
        let mem_partition = mem_table.partition_buffers.get("1970-01-01").unwrap();
        assert_eq!(mem_partition.rows.len(), 1);
        assert_eq!(mem_partition.timestamp_min, 20);
        assert_eq!(mem_partition.timestamp_max, 20);
    }

    #[tokio::test]
    async fn persist_closed_buffer() {
        let segment_id = SegmentId::new(4);
        let segment_writer = Box::new(WalSegmentWriterNoopImpl::new(segment_id));
        let mut open_segment =
            OpenBufferSegment::new(segment_id, SequenceNumber::new(0), segment_writer);

        let catalog = Catalog::new();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "db1".to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, "db1", lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let catalog = Arc::new(catalog);
        let closed_buffer_segment = open_segment.into_closed_segment(Arc::clone(&catalog));

        let persister: Arc<dyn Persister> = Arc::new(TestPersister::default());
        closed_buffer_segment
            .persist(Arc::clone(&persister))
            .await
            .unwrap();

        let persisted_state = persister
            .as_any()
            .downcast_ref::<TestPersister>()
            .unwrap()
            .state
            .lock();
        assert_eq!(
            persisted_state.catalog.as_ref().unwrap().clone_inner(),
            catalog.clone_inner()
        );
        let segment_info = persisted_state.segment.as_ref().unwrap();

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
            ParquetFilePath::new_with_parititon_key("db1", "cpu", "1970-01-01", 4).to_string()
        );
        assert_eq!(cpu_parqet.row_count, 1);
        assert_eq!(cpu_parqet.min_time, 10);
        assert_eq!(cpu_parqet.max_time, 10);

        let mem = db.tables.get("mem").unwrap();
        let mem_parqet = &mem.parquet_files[0];

        // file number of the path should match the segment id
        assert_eq!(
            mem_parqet.path,
            ParquetFilePath::new_with_parititon_key("db1", "mem", "1970-01-01", 4).to_string()
        );
        assert_eq!(mem_parqet.row_count, 2);
        assert_eq!(mem_parqet.min_time, 15);
        assert_eq!(mem_parqet.max_time, 20);
    }

    #[derive(Debug, Default)]
    struct TestPersister {
        state: Mutex<PersistedState>,
    }

    #[derive(Debug, Default)]
    struct PersistedState {
        catalog: Option<Catalog>,
        segment: Option<PersistedSegment>,
        parquet_files: Vec<ParquetFilePath>,
    }

    #[async_trait::async_trait]
    impl Persister for TestPersister {
        async fn persist_catalog(
            &self,
            _segment_id: SegmentId,
            catalog: Catalog,
        ) -> crate::Result<()> {
            self.state.lock().catalog = Some(catalog);
            Ok(())
        }

        async fn persist_parquet_file(
            &self,
            path: ParquetFilePath,
            _data: SendableRecordBatchStream,
        ) -> crate::Result<(u64, FileMetaData)> {
            self.state.lock().parquet_files.push(path);
            let meta = FileMetaData::new(1, vec![], 1, vec![], None, None, None, None, None);
            Ok((1, meta))
        }

        async fn persist_segment(&self, segment: PersistedSegment) -> crate::Result<()> {
            self.state.lock().segment = Some(segment);
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self as &dyn Any
        }

        async fn load_catalog(&self) -> crate::Result<Option<PersistedCatalog>> {
            todo!()
        }

        async fn load_segments(
            &self,
            _most_recent_n: usize,
        ) -> crate::Result<Vec<PersistedSegment>> {
            todo!()
        }

        async fn load_parquet_file(&self, _path: ParquetFilePath) -> crate::Result<Bytes> {
            todo!()
        }

        fn object_store(&self) -> Arc<dyn ObjectStore> {
            todo!()
        }
    }

    fn lp_to_write_batch(catalog: &Catalog, db_name: &'static str, lp: &str) -> WriteBatch {
        let mut write_batch = WriteBatch::default();
        let (seq, db) = catalog.db_or_create(db_name);
        let partitioner = Partitioner::new_per_day_partitioner();
        let result = parse_validate_and_update_schema(
            lp,
            &db,
            &partitioner,
            0,
            false,
            Precision::Nanosecond,
        )
        .unwrap();
        if let Some(db) = result.schema {
            catalog.replace_database(seq, Arc::new(db)).unwrap();
        }
        let db_name = NamespaceName::new(db_name).unwrap();
        write_batch.add_db_write(db_name, result.table_batches);
        write_batch
    }
}
