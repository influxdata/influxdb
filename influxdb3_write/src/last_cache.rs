use std::{any::Any, collections::VecDeque, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, GenericByteDictionaryBuilder, Int64Builder,
        RecordBatch, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
        UInt64Builder,
    },
    datatypes::{DataType, GenericStringType, Int32Type, SchemaRef, TimeUnit},
    error::ArrowError,
};
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use iox_time::Time;
use parking_lot::RwLock;
use schema::TIME_COLUMN_NAME;

use crate::{
    catalog::LastCacheSize,
    write_buffer::{buffer_segment::WriteBatch, FieldData, Row},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
    #[error("last cache already exists for database and table")]
    CacheAlreadyExists,
}

/// A three level hashmap storing Database -> Table
type CacheMap = RwLock<HashMap<String, HashMap<String, RwLock<LastCache>>>>;

pub struct LastCacheProvider {
    cache_map: CacheMap,
}

impl std::fmt::Debug for LastCacheProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LastCacheProvider")
    }
}

impl LastCacheProvider {
    pub(crate) fn new() -> Self {
        Self {
            cache_map: Default::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn get_cache_record_batches<D, T>(
        &self,
        db_name: D,
        tbl_name: T,
    ) -> Option<Result<RecordBatch, ArrowError>>
    where
        D: AsRef<str>,
        T: AsRef<str>,
    {
        self.cache_map
            .read()
            .get(db_name.as_ref())
            .and_then(|db| db.get(tbl_name.as_ref()))
            .map(|lc| lc.read().to_record_batch())
    }

    pub(crate) fn create_cache<D, T>(
        &self,
        db_name: D,
        tbl_name: T,
        count: usize,
        key_columns: impl IntoIterator<Item: Into<String>>,
        schema: SchemaRef,
    ) -> Result<(), Error>
    where
        D: Into<String>,
        T: Into<String>,
    {
        let db_name = db_name.into();
        let tbl_name = tbl_name.into();
        if self
            .cache_map
            .read()
            .get(&db_name)
            .is_some_and(|db| db.contains_key(&tbl_name))
        {
            return Err(Error::CacheAlreadyExists);
        }
        let last_cache = RwLock::new(LastCache::new(count, key_columns, schema)?);
        self.cache_map
            .write()
            .entry(db_name)
            .or_default()
            .insert(tbl_name, last_cache);
        Ok(())
    }

    pub(crate) fn write_batch_to_cache(&self, write_batch: &WriteBatch) {
        for (db_name, db_batch) in &write_batch.database_batches {
            for (tbl_name, tbl_batch) in &db_batch.table_batches {
                if let Some(db) = self.cache_map.read().get(db_name.as_str()) {
                    for row in &tbl_batch.rows {
                        if db
                            .get(tbl_name)
                            .is_some_and(|t| row.time > t.read().last_time.timestamp_nanos())
                        {
                            db.get(tbl_name).unwrap().write().push(row);
                        }
                    }
                }
            }
        }
    }
}

/// A ring buffer holding a set of [`Row`]s
pub(crate) struct LastCache {
    _count: LastCacheSize,
    // TODO: use for filter predicates
    _key_columns: HashSet<String>,
    schema: SchemaRef,
    cache: IndexMap<String, CacheColumn>,
    last_time: Time,
}

impl LastCache {
    /// Create a new [`LastCache`]
    pub(crate) fn new(
        count: usize,
        key_columns: impl IntoIterator<Item: Into<String>>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let cache = schema
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), CacheColumn::new(f.data_type(), count)))
            .collect();
        Ok(Self {
            _count: count.try_into().map_err(|_| Error::InvalidCacheSize)?,
            _key_columns: key_columns.into_iter().map(Into::into).collect(),
            schema,
            cache,
            last_time: Time::from_timestamp_nanos(0),
        })
    }

    pub(crate) fn push(&mut self, row: &Row) {
        let time_col = self
            .cache
            .get_mut(TIME_COLUMN_NAME)
            .expect("there should always be a time column");
        time_col.push(&FieldData::Timestamp(row.time));
        for field in &row.fields {
            if let Some(c) = self.cache.get_mut(&field.name) {
                c.push(&field.value);
            }
        }
        self.last_time = Time::from_timestamp_nanos(row.time);
    }

    fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            self.schema(),
            self.cache.iter().map(|(_, c)| c.data.as_array()).collect(),
        )
    }
}

#[async_trait]
impl TableProvider for LastCache {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        // TODO: need to handle filters on the key columns as predicates here
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let partitions = vec![vec![self.to_record_batch()?]];
        let mut exec = MemoryExec::try_new(&partitions, self.schema(), projection.cloned())?;

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

struct CacheColumn {
    size: usize,
    data: CacheColumnData,
}

impl CacheColumn {
    fn new(data_type: &DataType, size: usize) -> Self {
        Self {
            size,
            data: CacheColumnData::new(data_type, size),
        }
    }

    fn push(&mut self, field_data: &FieldData) {
        if self.data.len() == self.size {
            self.data.pop_back();
        }
        self.data.push_front(&field_data);
    }
}

#[derive(Debug)]
enum CacheColumnData {
    I64(VecDeque<i64>),
    U64(VecDeque<u64>),
    F64(VecDeque<f64>),
    String(VecDeque<String>),
    Bool(VecDeque<bool>),
    Tag(VecDeque<String>),
    Time(VecDeque<i64>),
}

impl CacheColumnData {
    fn new(data_type: &DataType, size: usize) -> Self {
        match data_type {
            DataType::Boolean => Self::Bool(VecDeque::with_capacity(size)),
            DataType::Int64 => Self::I64(VecDeque::with_capacity(size)),
            DataType::UInt64 => Self::U64(VecDeque::with_capacity(size)),
            DataType::Float64 => Self::F64(VecDeque::with_capacity(size)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::Time(VecDeque::with_capacity(size))
            }
            DataType::Utf8 => Self::String(VecDeque::with_capacity(size)),
            DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
                Self::Tag(VecDeque::with_capacity(size))
            }
            _ => panic!("unsupported data type for last cache: {data_type}"),
        }
    }

    fn len(&self) -> usize {
        match self {
            CacheColumnData::I64(v) => v.len(),
            CacheColumnData::U64(v) => v.len(),
            CacheColumnData::F64(v) => v.len(),
            CacheColumnData::String(v) => v.len(),
            CacheColumnData::Bool(v) => v.len(),
            CacheColumnData::Tag(v) => v.len(),
            CacheColumnData::Time(v) => v.len(),
        }
    }

    fn pop_back(&mut self) {
        match self {
            CacheColumnData::I64(v) => {
                v.pop_back();
            }
            CacheColumnData::U64(v) => {
                v.pop_back();
            }
            CacheColumnData::F64(v) => {
                v.pop_back();
            }
            CacheColumnData::String(v) => {
                v.pop_back();
            }
            CacheColumnData::Bool(v) => {
                v.pop_back();
            }
            CacheColumnData::Tag(v) => {
                v.pop_back();
            }
            CacheColumnData::Time(v) => {
                v.pop_back();
            }
        }
    }

    fn push_front(&mut self, field_data: &FieldData) {
        match (field_data, self) {
            (FieldData::Timestamp(d), CacheColumnData::Time(v)) => v.push_front(*d),
            (FieldData::Key(d), CacheColumnData::String(v)) => v.push_front(d.to_owned()),
            (FieldData::Tag(d), CacheColumnData::Tag(v)) => v.push_front(d.to_owned()),
            (FieldData::String(d), CacheColumnData::String(v)) => v.push_front(d.to_owned()),
            (FieldData::Integer(d), CacheColumnData::I64(v)) => v.push_front(*d),
            (FieldData::UInteger(d), CacheColumnData::U64(v)) => v.push_front(*d),
            (FieldData::Float(d), CacheColumnData::F64(v)) => v.push_front(*d),
            (FieldData::Boolean(d), CacheColumnData::Bool(v)) => v.push_front(*d),
            _ => panic!("invalid field data for cache column"),
        }
    }

    fn as_array(&self) -> ArrayRef {
        match self {
            CacheColumnData::I64(v) => {
                let mut b = Int64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::U64(v) => {
                let mut b = UInt64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::F64(v) => {
                let mut b = Float64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::String(v) => {
                let mut b = StringBuilder::new();
                v.iter().for_each(|val| b.append_value(val));
                Arc::new(b.finish())
            }
            CacheColumnData::Bool(v) => {
                let mut b = BooleanBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::Tag(v) => {
                let mut b: GenericByteDictionaryBuilder<Int32Type, GenericStringType<i32>> =
                    StringDictionaryBuilder::new();
                v.iter().for_each(|val| {
                    b.append(val)
                        .expect("should not overflow 32 bit dictionary");
                });
                Arc::new(b.finish())
            }
            CacheColumnData::Time(v) => {
                let mut b = TimestampNanosecondBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::object_store::{memory::InMemory, ObjectStore};
    use arrow_util::assert_batches_eq;
    use data_types::NamespaceName;
    use iox_time::{MockProvider, Time};

    use crate::{
        persister::PersisterImpl, wal::WalImpl, write_buffer::WriteBufferImpl, Bufferer, Precision,
        SegmentDuration,
    };

    #[tokio::test]
    async fn pick_up_latest_write() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(obj_store));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let wbuf = WriteBufferImpl::new(
            persister,
            Option::<Arc<WalImpl>>::None,
            time_provider,
            SegmentDuration::new_5m(),
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();

        let db_name = "foo";
        let tbl_name = "cpu";

        // Do a write to update the catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=120").as_str(),
            Time::from_timestamp_nanos(1_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Create the last cache:
        wbuf.create_last_cache(db_name, tbl_name, 1, ["host"])
            .expect("create the last cache");

        // Do a write to update the catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=99").as_str(),
            Time::from_timestamp_nanos(2_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let batch = wbuf
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000002Z | 99.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &[batch]
        );

        // Do another write and see that the cache only holds the latest value:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},host=a,region=us usage=88").as_str(),
            Time::from_timestamp_nanos(3_000),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        let batch = wbuf
            .last_cache()
            .get_cache_record_batches(db_name, tbl_name)
            .unwrap()
            .unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000003Z | 88.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &[batch]
        );
    }
}
