use std::{num::NonZeroU32, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{Int32Type, SchemaRef},
    record_batch::RecordBatch,
};
use chrono::{TimeZone, Utc};
use datafusion::physical_plan::SendableRecordBatchStream;

use data_types::{
    partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
    server_id::ServerId,
};
use datafusion_util::MemoryStream;
use futures::TryStreamExt;
use internal_types::{
    schema::{builder::SchemaBuilder, Schema, TIME_COLUMN_NAME},
    selection::Selection,
};
use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreApi};
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::serialized_reader::{SerializedFileReader, SliceableCursor},
};

use crate::{
    chunk::ChunkMetrics,
    metadata::{IoxMetadata, IoxParquetMetaData},
};
use crate::{
    chunk::{self, ParquetChunk},
    storage::Storage,
};
use data_types::chunk_metadata::ChunkAddr;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error getting data from object store: {}", source))]
    GettingDataFromObjectStore { source: object_store::Error },

    #[snafu(display("Error reading chunk dato from object store: {}", source))]
    ReadingChunk { source: chunk::Error },

    #[snafu(display("Error loading data from object store"))]
    LoadingFromObjectStore {},
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Load parquet from store and return table name and parquet bytes.
// This function is for test only
pub async fn load_parquet_from_store(
    chunk: &ParquetChunk,
    store: Arc<ObjectStore>,
) -> Result<(String, Vec<u8>)> {
    load_parquet_from_store_for_chunk(chunk, store).await
}

pub async fn load_parquet_from_store_for_chunk(
    chunk: &ParquetChunk,
    store: Arc<ObjectStore>,
) -> Result<(String, Vec<u8>)> {
    let path = chunk.path();
    let table_name = chunk.table_name().to_string();
    Ok((
        table_name,
        load_parquet_from_store_for_path(&path, store).await?,
    ))
}

pub async fn load_parquet_from_store_for_path(
    path: &Path,
    store: Arc<ObjectStore>,
) -> Result<Vec<u8>> {
    let parquet_data = store
        .get(path)
        .await
        .context(GettingDataFromObjectStore)?
        .map_ok(|bytes| bytes.to_vec())
        .try_concat()
        .await
        .context(GettingDataFromObjectStore)?;

    Ok(parquet_data)
}

/// The db name to use for testing
pub fn db_name() -> &'static str {
    "db1"
}

/// Creates a test chunk address for a given chunk id
pub fn chunk_addr(id: u32) -> ChunkAddr {
    ChunkAddr {
        db_name: Arc::from(db_name()),
        table_name: Arc::from("table1"),
        partition_key: Arc::from("part1"),
        chunk_id: id,
    }
}

/// Same as [`make_chunk`] but parquet file does not contain any row group.
pub async fn make_chunk(
    store: Arc<ObjectStore>,
    column_prefix: &str,
    addr: ChunkAddr,
) -> ParquetChunk {
    let (record_batches, schema, column_summaries, _num_rows) = make_record_batch(column_prefix);
    make_chunk_given_record_batch(store, record_batches, schema, addr, column_summaries).await
}

/// Same as [`make_chunk`] but parquet file does not contain any row group.
pub async fn make_chunk_no_row_group(
    store: Arc<ObjectStore>,
    column_prefix: &str,
    addr: ChunkAddr,
) -> ParquetChunk {
    let (_, schema, column_summaries, _num_rows) = make_record_batch(column_prefix);
    make_chunk_given_record_batch(store, vec![], schema, addr, column_summaries).await
}

/// Create a test chunk by writing data to object store.
///
/// TODO: This code creates a chunk that isn't hooked up with metrics
pub async fn make_chunk_given_record_batch(
    store: Arc<ObjectStore>,
    record_batches: Vec<RecordBatch>,
    schema: Schema,
    addr: ChunkAddr,
    column_summaries: Vec<ColumnSummary>,
) -> ParquetChunk {
    let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
    let storage = Storage::new(Arc::clone(&store), server_id);

    let mut table_summary = TableSummary::new(addr.table_name.to_string());
    table_summary.columns = column_summaries;
    let stream: SendableRecordBatchStream = if record_batches.is_empty() {
        Box::pin(MemoryStream::new_with_schema(
            record_batches,
            Arc::clone(schema.inner()),
        ))
    } else {
        Box::pin(MemoryStream::new(record_batches))
    };
    let metadata = IoxMetadata {
        creation_timestamp: Utc.timestamp(10, 20),
        table_name: addr.table_name.to_string(),
        partition_key: addr.partition_key.to_string(),
        chunk_id: addr.chunk_id,
    };
    let (path, parquet_metadata) = storage
        .write_to_object_store(addr.clone(), stream, metadata)
        .await
        .unwrap();

    ParquetChunk::new_from_parts(
        addr.partition_key.to_string(),
        Arc::new(table_summary),
        Arc::new(schema),
        path,
        Arc::clone(&store),
        Arc::new(parquet_metadata),
        ChunkMetrics::new_unregistered(),
    )
}

fn create_column_tag(
    name: &str,
    data: Vec<Vec<Option<&str>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    assert_eq!(data.len(), arrow_cols.len());

    for (arrow_cols_sub, data_sub) in arrow_cols.iter_mut().zip(data.iter()) {
        let array: DictionaryArray<Int32Type> = data_sub.iter().cloned().collect();
        let array: Arc<dyn Array> = Arc::new(array);
        arrow_cols_sub.push((name.to_string(), Arc::clone(&array), true));
    }

    summaries.push(ColumnSummary {
        name: name.to_string(),
        influxdb_type: Some(InfluxDbType::Tag),
        stats: Statistics::String(StatValues {
            min: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .min()
                .map(|x| x.to_string()),
            max: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .max()
                .map(|x| x.to_string()),
            count: data.iter().flatten().filter_map(|x| x.as_ref()).count() as u64,
            distinct_count: None,
        }),
    });

    schema_builder.tag(name);
}

fn create_column_field_string(
    name: &str,
    data: Vec<Vec<Option<&str>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    create_column_field_generic::<StringArray, _, _>(
        name,
        data,
        arrow_cols,
        summaries,
        schema_builder,
        |StatValues {
             min,
             max,
             count,
             distinct_count,
         }| {
            Statistics::String(StatValues {
                min: min.map(|x| x.to_string()),
                max: max.map(|x| x.to_string()),
                distinct_count,
                count,
            })
        },
    )
}

fn create_column_field_i64(
    name: &str,
    data: Vec<Vec<Option<i64>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    create_column_field_generic::<Int64Array, _, _>(
        name,
        data,
        arrow_cols,
        summaries,
        schema_builder,
        Statistics::I64,
    )
}

fn create_column_field_u64(
    name: &str,
    data: Vec<Vec<Option<u64>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    create_column_field_generic::<UInt64Array, _, _>(
        name,
        data,
        arrow_cols,
        summaries,
        schema_builder,
        Statistics::U64,
    )
}

fn create_column_field_f64(
    name: &str,
    data: Vec<Vec<Option<f64>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    assert_eq!(data.len(), arrow_cols.len());

    let mut array_data_type = None;
    for (arrow_cols_sub, data_sub) in arrow_cols.iter_mut().zip(data.iter()) {
        let array: Arc<dyn Array> = Arc::new(Float64Array::from(data_sub.clone()));
        arrow_cols_sub.push((name.to_string(), Arc::clone(&array), true));
        array_data_type = Some(array.data_type().clone());
    }

    summaries.push(ColumnSummary {
        name: name.to_string(),
        influxdb_type: Some(InfluxDbType::Field),
        stats: Statistics::F64(StatValues {
            min: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .filter(|x| !x.is_nan())
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .cloned(),
            max: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .filter(|x| !x.is_nan())
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .cloned(),
            count: data.iter().flatten().filter_map(|x| x.as_ref()).count() as u64,
            distinct_count: None,
        }),
    });

    schema_builder.field(name, array_data_type.unwrap());
}

fn create_column_field_bool(
    name: &str,
    data: Vec<Vec<Option<bool>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    create_column_field_generic::<BooleanArray, _, _>(
        name,
        data,
        arrow_cols,
        summaries,
        schema_builder,
        Statistics::Bool,
    )
}

fn create_column_field_generic<A, T, F>(
    name: &str,
    data: Vec<Vec<Option<T>>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
    f: F,
) where
    A: 'static + Array,
    A: From<Vec<Option<T>>>,
    T: Clone + Ord,
    F: Fn(StatValues<T>) -> Statistics,
{
    assert_eq!(data.len(), arrow_cols.len());

    let mut array_data_type = None;
    for (arrow_cols_sub, data_sub) in arrow_cols.iter_mut().zip(data.iter()) {
        let array: Arc<dyn Array> = Arc::new(A::from(data_sub.clone()));
        arrow_cols_sub.push((name.to_string(), Arc::clone(&array), true));
        array_data_type = Some(array.data_type().clone());
    }

    summaries.push(ColumnSummary {
        name: name.to_string(),
        influxdb_type: Some(InfluxDbType::Field),
        stats: f(StatValues {
            min: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .min()
                .cloned(),
            max: data
                .iter()
                .flatten()
                .filter_map(|x| x.as_ref())
                .max()
                .cloned(),
            count: data.iter().flatten().filter_map(|x| x.as_ref()).count() as u64,
            distinct_count: None,
        }),
    });

    schema_builder.field(name, array_data_type.unwrap());
}

fn create_column_timestamp(
    data: Vec<Vec<i64>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: &mut SchemaBuilder,
) {
    assert_eq!(data.len(), arrow_cols.len());

    for (arrow_cols_sub, data_sub) in arrow_cols.iter_mut().zip(data.iter()) {
        let array: Arc<dyn Array> =
            Arc::new(TimestampNanosecondArray::from_vec(data_sub.clone(), None));
        arrow_cols_sub.push((TIME_COLUMN_NAME.to_string(), Arc::clone(&array), true));
    }

    let min = data.iter().flatten().min().cloned();
    let max = data.iter().flatten().max().cloned();

    summaries.push(ColumnSummary {
        name: TIME_COLUMN_NAME.to_string(),
        influxdb_type: Some(InfluxDbType::Timestamp),
        stats: Statistics::I64(StatValues {
            min,
            max,
            count: data.iter().map(Vec::len).sum::<usize>() as u64,
            distinct_count: None,
        }),
    });

    schema_builder.timestamp();
}

/// Creates an Arrow RecordBatches with schema and IOx statistics.
///
/// Generated columns are prefixes using `column_prefix`.
///
/// RecordBatches, schema and IOx statistics will be generated in separate ways to emulate what the normal data
/// ingestion would do. This also ensures that the Parquet data that will later be created out of the RecordBatch is
/// indeed self-contained and can act as a source to recorder schema and statistics.
pub fn make_record_batch(
    column_prefix: &str,
) -> (Vec<RecordBatch>, Schema, Vec<ColumnSummary>, usize) {
    // (name, array, nullable)
    let mut arrow_cols: Vec<Vec<(String, ArrayRef, bool)>> = vec![vec![], vec![], vec![]];
    let mut summaries = vec![];
    let mut schema_builder = SchemaBuilder::new();

    // tag
    create_column_tag(
        &format!("{}_tag_normal", column_prefix),
        vec![
            vec![Some("foo")],
            vec![Some("bar")],
            vec![Some("baz"), Some("foo")],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_tag(
        &format!("{}_tag_empty", column_prefix),
        vec![vec![Some("")], vec![Some("")], vec![Some(""), Some("")]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_tag(
        &format!("{}_tag_null_some", column_prefix),
        vec![vec![None], vec![Some("bar")], vec![Some("baz"), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_tag(
        &format!("{}_tag_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // field: string
    create_column_field_string(
        &format!("{}_field_string_normal", column_prefix),
        vec![
            vec![Some("foo")],
            vec![Some("bar")],
            vec![Some("baz"), Some("foo")],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_string(
        &format!("{}_field_string_empty", column_prefix),
        vec![vec![Some("")], vec![Some("")], vec![Some(""), Some("")]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_string(
        &format!("{}_field_string_null_some", column_prefix),
        vec![vec![None], vec![Some("bar")], vec![Some("baz"), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_string(
        &format!("{}_field_string_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // field: i64
    create_column_field_i64(
        &format!("{}_field_i64_normal", column_prefix),
        vec![vec![Some(-1)], vec![Some(2)], vec![Some(3), Some(4)]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_i64(
        &format!("{}_field_i64_range", column_prefix),
        vec![
            vec![Some(i64::MIN)],
            vec![Some(i64::MAX)],
            vec![Some(i64::MIN), Some(i64::MAX)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_i64(
        &format!("{}_field_i64_null_some", column_prefix),
        vec![vec![None], vec![Some(2)], vec![Some(3), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_i64(
        &format!("{}_field_i64_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // field: u64
    create_column_field_u64(
        &format!("{}_field_u64_normal", column_prefix),
        vec![vec![Some(1u64)], vec![Some(2)], vec![Some(3), Some(4)]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_u64(
        &format!("{}_field_u64_range", column_prefix),
        vec![
            vec![Some(u64::MIN)],
            vec![Some(u64::MAX)],
            vec![Some(u64::MIN), Some(u64::MAX)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_u64(
        &format!("{}_field_u64_null_some", column_prefix),
        vec![vec![None], vec![Some(2)], vec![Some(3), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_u64(
        &format!("{}_field_u64_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // field: f64
    create_column_field_f64(
        &format!("{}_field_f64_normal", column_prefix),
        vec![
            vec![Some(10.1)],
            vec![Some(20.1)],
            vec![Some(30.1), Some(40.1)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_f64(
        &format!("{}_field_f64_inf", column_prefix),
        vec![
            vec![Some(0.0)],
            vec![Some(f64::INFINITY)],
            vec![Some(f64::NEG_INFINITY), Some(1.0)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_f64(
        &format!("{}_field_f64_zero", column_prefix),
        vec![
            vec![Some(0.0)],
            vec![Some(-0.0)],
            vec![Some(0.0), Some(-0.0)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    let nan1 = f64::from_bits(0x7ff8000000000001);
    let nan2 = f64::from_bits(0x7ff8000000000002);
    assert!(nan1.is_nan());
    assert!(nan2.is_nan());
    create_column_field_f64(
        &format!("{}_field_f64_nan_some", column_prefix),
        vec![
            vec![Some(nan1)],
            vec![Some(2.0)],
            vec![Some(1.0), Some(nan2)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_f64(
        &format!("{}_field_f64_nan_all", column_prefix),
        vec![
            vec![Some(nan1)],
            vec![Some(nan2)],
            vec![Some(nan1), Some(nan2)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_f64(
        &format!("{}_field_f64_null_some", column_prefix),
        vec![vec![None], vec![Some(20.1)], vec![Some(30.1), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_f64(
        &format!("{}_field_f64_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // field: bool
    create_column_field_bool(
        &format!("{}_field_bool_normal", column_prefix),
        vec![
            vec![Some(true)],
            vec![Some(false)],
            vec![Some(true), Some(false)],
        ],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_bool(
        &format!("{}_field_bool_null_some", column_prefix),
        vec![vec![None], vec![Some(false)], vec![Some(true), None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );
    create_column_field_bool(
        &format!("{}_field_bool_null_all", column_prefix),
        vec![vec![None], vec![None], vec![None, None]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // time
    create_column_timestamp(
        vec![vec![1000], vec![2000], vec![3000, 4000]],
        &mut arrow_cols,
        &mut summaries,
        &mut schema_builder,
    );

    // build record batches
    let mut num_rows = 0;
    let schema = schema_builder.build().expect("schema building");
    let mut record_batches = vec![];
    for arrow_cols_sub in arrow_cols {
        let record_batch = RecordBatch::try_from_iter_with_nullable(arrow_cols_sub)
            .expect("created new record batch");
        // The builder-generated schema contains some extra metadata that we need in our recordbatch
        let record_batch =
            RecordBatch::try_new(Arc::clone(schema.inner()), record_batch.columns().to_vec())
                .expect("record-batch re-creation");
        num_rows += record_batch.num_rows();
        record_batches.push(record_batch);
    }

    (record_batches, schema, summaries, num_rows)
}

/// Creates new in-memory object store for testing.
pub fn make_object_store() -> Arc<ObjectStore> {
    Arc::new(ObjectStore::new_in_memory(InMemory::new()))
}

pub fn read_data_from_parquet_data(schema: SchemaRef, parquet_data: Vec<u8>) -> Vec<RecordBatch> {
    let mut record_batches = vec![];

    let cursor = SliceableCursor::new(parquet_data);
    let reader = SerializedFileReader::new(cursor).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

    // Indices of columns in the schema needed to read
    let projection: Vec<usize> = Storage::column_indices(Selection::All, Arc::clone(&schema));
    let mut batch_reader = arrow_reader
        .get_record_reader_by_columns(projection, 1024)
        .unwrap();
    loop {
        match batch_reader.next() {
            Some(Ok(batch)) => {
                // TODO: remove this when arow-rs' ticket https://github.com/apache/arrow-rs/issues/252#252 is done
                let columns = batch.columns().to_vec();
                let new_batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
                record_batches.push(new_batch);
            }
            None => {
                break;
            }
            Some(Err(e)) => {
                println!("Error reading batch: {}", e.to_string());
            }
        }
    }

    record_batches
}

/// Create test metadata by creating a parquet file and reading it back into memory.
///
/// See [`make_chunk`] for details.
pub async fn make_metadata(
    object_store: &Arc<ObjectStore>,
    column_prefix: &str,
    addr: ChunkAddr,
) -> (Path, IoxParquetMetaData) {
    let chunk = make_chunk(Arc::clone(object_store), column_prefix, addr).await;
    let (_, parquet_data) = load_parquet_from_store(&chunk, Arc::clone(object_store))
        .await
        .unwrap();
    (
        chunk.path(),
        IoxParquetMetaData::from_file_bytes(parquet_data).unwrap(),
    )
}
