//! A single buffer segment used by the write buffer. This is all the data in memory for a
//! single WAL segment. Only one segment should be open for writes in the write buffer at any
//! given time.

use crate::catalog::Catalog;
use crate::paths::ParquetFilePath;
use crate::write_buffer::flusher::BufferedWriteResult;
use crate::write_buffer::{
    parse_validate_and_update_catalog, FieldData, Row, TableBatch, ValidSegmentedData,
};
use crate::{
    wal, write_buffer::Result, DatabaseTables, ParquetFile, PersistedSegment, Persister, Precision,
    SegmentDuration, SegmentId, SegmentRange, SequenceNumber, TableParquetFiles, WalOp,
    WalSegmentReader, WalSegmentWriter,
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::{ColumnType, NamespaceName, PartitionKey, TimestampMinMax};
use datafusion_util::stream_from_batch;
use iox_time::Time;
use schema::Schema;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct OpenBufferSegment {
    segment_writer: Box<dyn WalSegmentWriter>,
    segment_id: SegmentId,
    segment_range: SegmentRange,
    segment_key: PartitionKey,
    buffered_data: BufferedData,
    #[allow(dead_code)]
    starting_catalog_sequence_number: SequenceNumber,
    // TODO: This is temporarily just the number of rows in the segment. When the buffer gets refactored to use
    //       different structures, we want this to be a representation of approximate memory usage.
    segment_size: usize,
}

impl OpenBufferSegment {
    pub fn new(
        segment_id: SegmentId,
        segment_range: SegmentRange,
        starting_catalog_sequence_number: SequenceNumber,
        segment_writer: Box<dyn WalSegmentWriter>,
        buffered_data: Option<(BufferedData, usize)>,
    ) -> Self {
        let (buffered_data, segment_size) = buffered_data.unwrap_or_default();
        let segment_key = PartitionKey::from(segment_range.key());

        Self {
            segment_writer,
            segment_id,
            segment_range,
            segment_key,
            starting_catalog_sequence_number,
            segment_size,
            buffered_data,
        }
    }

    pub fn start_time_matches(&self, t: Time) -> bool {
        self.segment_range.start_time == t
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }
    pub fn write_wal_ops(&mut self, write_batch: Vec<WalOp>) -> wal::Result<()> {
        self.segment_writer.write_batch(write_batch)
    }

    pub fn table_buffer(&self, db_name: &str, table_name: &str) -> Option<TableBuffer> {
        self.buffered_data
            .database_buffers
            .get(db_name)
            .and_then(|db_buffer| db_buffer.table_buffers.get(table_name).cloned())
    }

    /// Adds the batch into the in memory buffer.
    pub(crate) fn buffer_writes(&mut self, write_batch: WriteBatch) -> Result<()> {
        for (db_name, db_batch) in write_batch.database_batches {
            let db_buffer = self
                .buffered_data
                .database_buffers
                .entry(db_name.to_string())
                .or_default();

            for (table_name, table_batch) in db_batch.table_batches {
                let table_buffer = db_buffer
                    .table_buffers
                    .entry(table_name)
                    .or_insert_with(|| TableBuffer {
                        segment_key: self.segment_key.clone(),
                        rows: vec![],
                        timestamp_min: i64::MAX,
                        timestamp_max: i64::MIN,
                    });
                // TODO: for now we'll just have the number of rows represent the segment size. The entire
                //       buffer is going to get refactored to use different structures, so this will change.
                self.segment_size += table_batch.rows.len();
                table_buffer.add_rows(table_batch.rows);
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn into_closed_segment(self, catalog: Arc<Catalog>) -> ClosedBufferSegment {
        ClosedBufferSegment::new(
            self.segment_id,
            self.segment_range,
            self.segment_key,
            self.starting_catalog_sequence_number,
            catalog.sequence_number(),
            self.segment_writer,
            self.buffered_data,
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

                    let db_buffer = buffered_data
                        .database_buffers
                        .entry(write.db_name)
                        .or_default();

                    // there should only ever be data for a single segment as this is all read
                    // from one segment file
                    assert!(validated_write.valid_segmented_data.len() == 1);
                    let segment_data = validated_write.valid_segmented_data.pop().unwrap();

                    for (table_name, table_batch) in segment_data.table_batches {
                        let table_buffer = db_buffer
                            .table_buffers
                            .entry(table_name)
                            .or_insert_with(|| TableBuffer {
                                segment_key: segment_key.clone(),
                                rows: vec![],
                                timestamp_min: i64::MAX,
                                timestamp_max: i64::MIN,
                            });

                        // TODO: for now we'll just have the number of rows represent the segment size. The entire
                        //       buffer is going to get refactored to use different structures, so this will change.
                        segment_size += table_batch.rows.len();

                        table_buffer.add_rows(table_batch.rows);
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

#[derive(Debug, Default, Eq, PartialEq)]
pub struct BufferedData {
    database_buffers: HashMap<String, DatabaseBuffer>,
}

#[derive(Debug, Default, Eq, PartialEq)]
struct DatabaseBuffer {
    table_buffers: HashMap<String, TableBuffer>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableBuffer {
    pub segment_key: PartitionKey,
    rows: Vec<Row>,
    timestamp_min: i64,
    timestamp_max: i64,
}

impl TableBuffer {
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

// impl Default for TableBuffer {
//     fn default() -> Self {
//         Self {
//             rows: Vec::new(),
//             timestamp_min: i64::MAX,
//             timestamp_max: i64::MIN,
//         }
//     }
// }

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
    pub segment_id: SegmentId,
    pub segment_range: SegmentRange,
    pub segment_key: PartitionKey,
    pub catalog_start_sequence_number: SequenceNumber,
    pub catalog_end_sequence_number: SequenceNumber,
    segment_writer: Box<dyn WalSegmentWriter>,
    pub buffered_data: BufferedData,
    catalog: Arc<Catalog>,
}

impl ClosedBufferSegment {
    #[allow(dead_code)]
    fn new(
        segment_id: SegmentId,
        segment_range: SegmentRange,
        segment_key: PartitionKey,
        catalog_start_sequence_number: SequenceNumber,
        catalog_end_sequence_number: SequenceNumber,
        segment_writer: Box<dyn WalSegmentWriter>,
        buffered_data: BufferedData,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            segment_id,
            segment_range,
            segment_key,
            catalog_start_sequence_number,
            catalog_end_sequence_number,
            segment_writer,
            buffered_data,
            catalog,
        }
    }

    #[allow(dead_code)]
    pub async fn persist<P>(&self, persister: Arc<P>) -> crate::Result<()>
    where
        P: Persister,
        crate::Error: From<P::Error>,
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

                        // persist every table buffer
                        let data =
                            table_buffer.rows_to_record_batch(table.schema(), table.columns());
                        let row_count = data.num_rows();
                        let batch_stream = stream_from_batch(table.schema().as_arrow(), data);
                        let parquet_file_path = ParquetFilePath::new_with_parititon_key(
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
                            min_time: table_buffer.timestamp_min,
                            max_time: table_buffer.timestamp_max,
                        };
                        table_parquet_files.parquet_files.push(parquet_file);

                        segment_parquet_size_bytes += size_bytes;
                        segment_row_count += meta.num_rows as u64;
                        segment_max_time = segment_max_time.max(table_buffer.timestamp_max);
                        segment_min_time = segment_min_time.min(table_buffer.timestamp_min);

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
    use crate::test_helpers::{lp_to_table_batches, lp_to_write_batch};
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::{persister, LpWriteOp, PersistedCatalog};
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
            SegmentRange::test_range(),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(SegmentId::new(0))),
            None,
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
        assert_eq!(cpu_table.rows.len(), 2);
        assert_eq!(cpu_table.timestamp_min, 10);
        assert_eq!(cpu_table.timestamp_max, 30);

        let mem_table = open_segment.table_buffer(&db_name, "mem").unwrap();
        assert_eq!(mem_table.rows.len(), 1);
        assert_eq!(mem_table.timestamp_min, 20);
        assert_eq!(mem_table.timestamp_max, 20);
    }

    #[tokio::test]
    async fn persist_closed_buffer() {
        const SEGMENT_KEY: &str = "1970-01-01T00-00";

        let segment_id = SegmentId::new(4);
        let segment_writer = Box::new(WalSegmentWriterNoopImpl::new(segment_id));
        let mut open_segment = OpenBufferSegment::new(
            segment_id,
            SegmentRange::test_range(),
            SequenceNumber::new(0),
            segment_writer,
            None,
        );

        let catalog = Catalog::new();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

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
            ParquetFilePath::new_with_parititon_key("db1", "cpu", SEGMENT_KEY, 4).to_string()
        );
        assert_eq!(cpu_parqet.row_count, 1);
        assert_eq!(cpu_parqet.min_time, 10);
        assert_eq!(cpu_parqet.max_time, 10);

        let mem = db.tables.get("mem").unwrap();
        let mem_parqet = &mem.parquet_files[0];

        // file number of the path should match the segment id
        assert_eq!(
            mem_parqet.path,
            ParquetFilePath::new_with_parititon_key("db1", "mem", SEGMENT_KEY, 4).to_string()
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
        type Error = persister::Error;

        async fn persist_catalog(
            &self,
            _segment_id: SegmentId,
            catalog: Catalog,
        ) -> persister::Result<()> {
            self.state.lock().catalog = Some(catalog);
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

        async fn persist_segment(&self, segment: PersistedSegment) -> persister::Result<()> {
            self.state.lock().segment = Some(segment);
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
