use std::{num::NonZeroU32, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{Int32Type, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::SendableRecordBatchStream;

use data_types::{
    partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
    server_id::ServerId,
    timestamp::TimestampRange,
};
use datafusion_util::MemoryStream;
use futures::TryStreamExt;
use internal_types::{
    schema::{builder::SchemaBuilder, Schema, TIME_COLUMN_NAME},
    selection::Selection,
};
use object_store::{memory::InMemory, ObjectStore, ObjectStoreApi};
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::serialized_reader::{SerializedFileReader, SliceableCursor},
};
use tracker::MemRegistry;

use crate::{chunk::Chunk, storage::Storage};

/// Load parquet from store and return table name and parquet bytes.
pub async fn load_parquet_from_store(chunk: &Chunk, store: Arc<ObjectStore>) -> (String, Vec<u8>) {
    let table = chunk.table_names(None).next().unwrap();
    let path = chunk.table_path(&table).unwrap();
    let parquet_data = store
        .get(&path)
        .await
        .unwrap()
        .map_ok(|bytes| bytes.to_vec())
        .try_concat()
        .await
        .unwrap();
    (table, parquet_data)
}

/// Create a test chunk by writing data to object store.
///
/// See [`make_record_batch`] for the data content.
pub async fn make_chunk_given_record_batch(
    store: Arc<ObjectStore>,
    record_batches: Vec<RecordBatch>,
    schema: Schema,
    table: &str,
    column_summaries: Vec<ColumnSummary>,
    time_range: TimestampRange,
) -> Chunk {
    make_chunk_common(
        store,
        record_batches,
        schema,
        table,
        column_summaries,
        time_range,
    )
    .await
}

/// Same as [`make_chunk`] but parquet file does not contain any row group.
pub async fn make_chunk(store: Arc<ObjectStore>, column_prefix: &str) -> Chunk {
    let (record_batches, schema, column_summaries, time_range, _num_rows) =
        make_record_batch(column_prefix);
    make_chunk_common(
        store,
        record_batches,
        schema,
        "table1",
        column_summaries,
        time_range,
    )
    .await
}

/// Same as [`make_chunk`] but parquet file does not contain any row group.
pub async fn make_chunk_no_row_group(store: Arc<ObjectStore>, column_prefix: &str) -> Chunk {
    let (_, schema, column_summaries, time_range, _num_rows) = make_record_batch(column_prefix);
    make_chunk_common(
        store,
        vec![],
        schema,
        "table1",
        column_summaries,
        time_range,
    )
    .await
}

/// Common code for all [`make_chunk`] and [`make_chunk_no_row_group`].
async fn make_chunk_common(
    store: Arc<ObjectStore>,
    record_batches: Vec<RecordBatch>,
    schema: Schema,
    table: &str,
    column_summaries: Vec<ColumnSummary>,
    time_range: TimestampRange,
) -> Chunk {
    let memory_registry = MemRegistry::new();
    let server_id = ServerId::new(NonZeroU32::new(1).unwrap());
    let db_name = "db1";
    let part_key = "part1";
    let table_name = table;
    let chunk_id = 1;
    let mut chunk = Chunk::new(part_key.to_string(), chunk_id, &memory_registry);

    let storage = Storage::new(Arc::clone(&store), server_id, db_name.to_string());

    let mut table_summary = TableSummary::new(table_name.to_string());
    table_summary.columns = column_summaries;
    let stream: SendableRecordBatchStream = if record_batches.is_empty() {
        Box::pin(MemoryStream::new_with_schema(
            record_batches,
            Arc::clone(schema.inner()),
        ))
    } else {
        Box::pin(MemoryStream::new(record_batches))
    };
    let path = storage
        .write_to_object_store(
            part_key.to_string(),
            chunk_id,
            table_name.to_string(),
            stream,
        )
        .await
        .unwrap();
    chunk.add_table(
        table_summary,
        path,
        Arc::clone(&store),
        schema,
        Some(time_range),
    );

    chunk
}

fn create_column_tag(
    name: &str,
    data: Vec<Vec<&str>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
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
            min: data.iter().flatten().min().unwrap().to_string(),
            max: data.iter().flatten().max().unwrap().to_string(),
            count: data.iter().map(Vec::len).sum::<usize>() as u64,
        }),
    });

    schema_builder.tag(name)
}

fn create_column_field_string(
    name: &str,
    data: Vec<Vec<&str>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
    create_column_field_generic::<StringArray, _, _>(
        name,
        data,
        arrow_cols,
        summaries,
        schema_builder,
        |StatValues { min, max, count }| {
            Statistics::String(StatValues {
                min: min.to_string(),
                max: max.to_string(),
                count,
            })
        },
    )
}

fn create_column_field_i64(
    name: &str,
    data: Vec<Vec<i64>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
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
    data: Vec<Vec<u64>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
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
    data: Vec<Vec<f64>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
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
            min: *data
                .iter()
                .flatten()
                .filter(|x| !x.is_nan())
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap(),
            max: *data
                .iter()
                .flatten()
                .filter(|x| !x.is_nan())
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap(),
            count: data.iter().map(Vec::len).sum::<usize>() as u64,
        }),
    });

    schema_builder.field(name, array_data_type.unwrap())
}

fn create_column_field_bool(
    name: &str,
    data: Vec<Vec<bool>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
) -> SchemaBuilder {
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
    data: Vec<Vec<T>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    summaries: &mut Vec<ColumnSummary>,
    schema_builder: SchemaBuilder,
    f: F,
) -> SchemaBuilder
where
    A: 'static + Array,
    A: From<Vec<T>>,
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
            min: data.iter().flatten().min().unwrap().clone(),
            max: data.iter().flatten().max().unwrap().clone(),
            count: data.iter().map(Vec::len).sum::<usize>() as u64,
        }),
    });

    schema_builder.field(name, array_data_type.unwrap())
}

fn create_column_timestamp(
    data: Vec<Vec<i64>>,
    arrow_cols: &mut Vec<Vec<(String, ArrayRef, bool)>>,
    schema_builder: SchemaBuilder,
) -> (SchemaBuilder, TimestampRange) {
    assert_eq!(data.len(), arrow_cols.len());

    for (arrow_cols_sub, data_sub) in arrow_cols.iter_mut().zip(data.iter()) {
        let array: Arc<dyn Array> =
            Arc::new(TimestampNanosecondArray::from_vec(data_sub.clone(), None));
        arrow_cols_sub.push((TIME_COLUMN_NAME.to_string(), Arc::clone(&array), true));
    }

    let timestamp_range = TimestampRange::new(
        *data.iter().flatten().min().unwrap(),
        *data.iter().flatten().max().unwrap(),
    );

    let schema_builder = schema_builder.timestamp();
    (schema_builder, timestamp_range)
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
) -> (
    Vec<RecordBatch>,
    Schema,
    Vec<ColumnSummary>,
    TimestampRange,
    usize,
) {
    // (name, array, nullable)
    let mut arrow_cols: Vec<Vec<(String, ArrayRef, bool)>> = vec![vec![], vec![], vec![]];
    let mut summaries = vec![];
    let mut schema_builder = SchemaBuilder::new();

    // tag
    schema_builder = create_column_tag(
        &format!("{}_tag_nonempty", column_prefix),
        vec![vec!["foo"], vec!["bar"], vec!["baz", "foo"]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    schema_builder = create_column_tag(
        &format!("{}_tag_empty", column_prefix),
        vec![vec![""], vec![""], vec!["", ""]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );

    // field: string
    schema_builder = create_column_field_string(
        &format!("{}_field_string_nonempty", column_prefix),
        vec![vec!["foo"], vec!["bar"], vec!["baz", "foo"]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    schema_builder = create_column_field_string(
        &format!("{}_field_string_empty", column_prefix),
        vec![vec![""], vec![""], vec!["", ""]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );

    // field: i64
    schema_builder = create_column_field_i64(
        &format!("{}_field_i64_normal", column_prefix),
        vec![vec![-1], vec![2], vec![3, 4]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    schema_builder = create_column_field_i64(
        &format!("{}_field_i64_range", column_prefix),
        vec![vec![i64::MIN], vec![i64::MAX], vec![i64::MIN, i64::MAX]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );

    // field: u64
    schema_builder = create_column_field_u64(
        &format!("{}_field_u64_normal", column_prefix),
        vec![vec![1u64], vec![2], vec![3, 4]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    // TODO: broken due to https://github.com/apache/arrow-rs/issues/254
    // schema_builder = create_column_field_u64(
    //     "field_u64_range",
    //     vec![vec![u64::MIN, u64::MAX], vec![u64::MIN], vec![u64::MAX]],
    //     &mut arrow_cols,
    //     &mut summaries,
    //     schema_builder,
    // );

    // field: f64
    schema_builder = create_column_field_f64(
        &format!("{}_field_f64_normal", column_prefix),
        vec![vec![10.1], vec![20.1], vec![30.1, 40.1]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    schema_builder = create_column_field_f64(
        &format!("{}_field_f64_inf", column_prefix),
        vec![vec![0.0], vec![f64::INFINITY], vec![f64::NEG_INFINITY, 1.0]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );
    schema_builder = create_column_field_f64(
        &format!("{}_field_f64_zero", column_prefix),
        vec![vec![0.0], vec![-0.0], vec![0.0, -0.0]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );

    // TODO: NaNs are broken until https://github.com/apache/arrow-rs/issues/255 is fixed
    // let nan1 = f64::from_bits(0x7ff8000000000001);
    // let nan2 = f64::from_bits(0x7ff8000000000002);
    // assert!(nan1.is_nan());
    // assert!(nan2.is_nan());
    // schema_builder = create_column_field_f64(
    //     "field_f64_nan",
    //     vec![vec![nan1], vec![2.0], vec![1.0, nan2]],
    //     &mut arrow_cols,
    //     &mut summaries,
    //     schema_builder,
    // );

    // field: bool
    schema_builder = create_column_field_bool(
        &format!("{}_field_bool", column_prefix),
        vec![vec![true], vec![false], vec![true, false]],
        &mut arrow_cols,
        &mut summaries,
        schema_builder,
    );

    // time
    let (schema_builder, timestamp_range) = create_column_timestamp(
        vec![vec![1000], vec![2000], vec![3000, 4000]],
        &mut arrow_cols,
        schema_builder,
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

    (record_batches, schema, summaries, timestamp_range, num_rows)
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
