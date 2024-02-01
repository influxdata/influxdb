//! A single buffer segment used by the write buffer. This is all the data in memory for a
//! single WAL segment. Only one segment should be open for writes in the write buffer at any
//! given time.

use crate::write_buffer::flusher::BufferedWriteResult;
use crate::write_buffer::{FieldData, Row, TableBatch};
use crate::{wal, write_buffer::Result, SegmentId, SequenceNumber, WalOp, WalSegmentWriter};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use data_types::{ColumnType, NamespaceName, TimestampMinMax};
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

#[derive(Debug)]
pub struct ClosedBufferSegment {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::write_buffer::tests::lp_to_table_batches;

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
}
