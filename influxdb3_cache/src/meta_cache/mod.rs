//! The Metadata Cache holds the distinct values for a column or set of columns on a table

use std::{
    cmp::Eq,
    collections::{BTreeMap, BinaryHeap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use arrow::{
    array::{ArrayRef, RecordBatch, StringViewBuilder},
    datatypes::{DataType, Field, SchemaBuilder, SchemaRef},
    error::ArrowError,
};
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::ColumnId;
use influxdb3_wal::{FieldData, Row};
use iox_time::TimeProvider;
use schema::{InfluxColumnType, InfluxFieldType};

/// A metadata cache for storing distinct values for a set of columns in a table
#[derive(Debug)]
pub struct MetaCache {
    time_provider: Arc<dyn TimeProvider>,
    /// The maximum number of unique value combinations in the cache
    max_cardinality: usize,
    /// The maximum age for entries in the cache
    max_age: Duration,
    /// The fixed Arrow schema used to produce record batches from the cache
    schema: SchemaRef,
    /// Holds current state of the cache
    state: MetaCacheState,
    /// The identifiers of the columns used in the cache
    column_ids: Vec<ColumnId>,
    /// The cache data, stored in a tree
    data: Node,
}

/// Type for tracking the current state of a [`MetaCache`]
#[derive(Debug, Default)]
struct MetaCacheState {
    /// The current number of unique value combinations in the cache
    cardinality: usize,
}

/// Arguments to create a new [`MetaCache`]
#[derive(Debug)]
pub struct CreateMetaCacheArgs {
    pub time_provider: Arc<dyn TimeProvider>,
    pub table_def: Arc<TableDefinition>,
    pub max_cardinality: MaxCardinality,
    pub max_age: MaxAge,
    pub column_ids: Vec<ColumnId>,
}

#[derive(Debug, Clone, Copy)]
pub struct MaxCardinality(NonZeroUsize);

#[cfg(test)]
impl MaxCardinality {
    fn try_new(size: usize) -> Result<Self, anyhow::Error> {
        Ok(Self(
            NonZeroUsize::try_from(size).context("invalid size provided")?,
        ))
    }
}

impl Default for MaxCardinality {
    fn default() -> Self {
        Self(NonZeroUsize::new(100_000).unwrap())
    }
}

impl From<MaxCardinality> for usize {
    fn from(value: MaxCardinality) -> Self {
        value.0.into()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaxAge(Duration);

impl Default for MaxAge {
    fn default() -> Self {
        Self(Duration::from_secs(24 * 60 * 60))
    }
}

impl From<MaxAge> for Duration {
    fn from(value: MaxAge) -> Self {
        value.0
    }
}

impl From<Duration> for MaxAge {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

impl MetaCache {
    /// Create a new [`MetaCache`]
    ///
    /// Must pass a non-empty set of [`ColumnId`]s which correspond to valid columns in the provided
    /// [`TableDefinition`].
    pub fn new(
        CreateMetaCacheArgs {
            time_provider,
            table_def,
            max_cardinality,
            max_age,
            column_ids,
        }: CreateMetaCacheArgs,
    ) -> Result<Self, anyhow::Error> {
        if column_ids.is_empty() {
            bail!("must pass a non-empty set of column ids");
        }
        let mut builder = SchemaBuilder::new();
        for id in &column_ids {
            let col = table_def
                .columns
                .get(id)
                .context("invalid column id encountered while creating metadata cache")?;
            let data_type = match col.data_type {
                InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String) => {
                    DataType::Utf8View
                }
                other => bail!(
                    "cannot use a column of type {other} in a metadata cache, only \
                    tags and string fields can be used"
                ),
            };

            builder.push(Arc::new(Field::new(col.name.as_ref(), data_type, false)));
        }
        Ok(Self {
            time_provider,
            max_cardinality: max_cardinality.into(),
            max_age: max_age.into(),
            state: MetaCacheState::default(),
            schema: Arc::new(builder.finish()),
            column_ids,
            data: Node::default(),
        })
    }

    /// Push a [`Row`] from the WAL into the cache, if the row contains all of the cached columns.
    pub fn push(&mut self, row: &Row) {
        let mut values = Vec::with_capacity(self.column_ids.len());
        for id in &self.column_ids {
            let Some(value) = row
                .fields
                .iter()
                .find(|f| &f.id == id)
                .map(|f| Value::from(&f.value))
            else {
                // ignore the row if it does not contain all columns in the cache:
                return;
            };
            values.push(value);
        }
        let mut target = &mut self.data;
        let mut val_iter = values.into_iter().peekable();
        let mut is_new = false;
        while let (Some(value), peek) = (val_iter.next(), val_iter.peek()) {
            let (last_seen, node) = target.0.entry(value).or_insert_with(|| {
                is_new = true;
                (row.time, peek.as_ref().map(|_| Node::default()))
            });
            *last_seen = row.time;
            if let Some(node) = node {
                target = node;
            } else {
                break;
            }
        }
        if is_new {
            self.state.cardinality += 1;
        }
    }

    /// Gather a record batch from a cache given the set of predicates
    ///
    /// This assumes the predicates are well behaved, and validated before being passed in. For example,
    /// there cannot be multiple predicates on a single column; the caller needs to validate/transform
    /// the incoming predicates from Datafusion before calling.
    ///
    /// Uses a [`StringViewBuilder`] to compose the set of [`RecordBatch`]es. This is convenient for
    /// the sake of nested caches, where a predicate on a higher branch in the cache will need to have
    /// its value in the outputted batches duplicated.
    // NOTE: this currently does not filter out records of entries that have exceeded the `max_age`
    // for the cache...
    pub fn to_record_batch(&self, predicates: &[Predicate]) -> Result<RecordBatch, ArrowError> {
        // predicates may not be provided for all columns in the cache, or not be provided in the
        // order of columns in the cache. This re-orders them to the correct order, and fills in any
        // gaps with None.
        let predicates: Vec<Option<&Predicate>> = self
            .column_ids
            .iter()
            .map(|id| predicates.iter().find(|p| &p.column_id == id))
            .collect();

        let mut builders: Vec<StringViewBuilder> = (0..self.column_ids.len())
            .map(|_| StringViewBuilder::new())
            .collect();

        let _ = self.data.evaluate_predicates(&predicates, &mut builders);

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            builders
                .into_iter()
                .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
                .collect(),
        )
    }

    /// Prune nodes from within the cache
    ///
    /// This first prunes entries that are older than the `max_age` of the cache. If the cardinality
    /// of the cache is still over its `max_cardinality`, it will do another pass to bring the cache
    /// size down.
    pub fn prune(&mut self) {
        let before_time_ns = self
            .time_provider
            .now()
            .checked_sub(self.max_age)
            .expect("max age on cache should not cause an overflow")
            .timestamp_nanos();
        let _ = self.data.remove_before(before_time_ns);
        self.state.cardinality = self.data.len();
        if self.state.cardinality > self.max_cardinality {
            let n_to_remove = self.state.cardinality - self.max_cardinality;
            self.data.remove_n_oldest(n_to_remove);
            self.state.cardinality = self.data.len();
        }
    }
}

/// A node in the `data` tree of a [`MetaCache`]
///
/// Recursive struct holding a [`BTreeMap`] whose keys are the values nested under this node, and
/// whose values hold the last seen time as an [`i64`] of each value, and an optional reference to
/// the node in the next level of the tree.
#[derive(Debug, Default)]
struct Node(BTreeMap<Value, (i64, Option<Node>)>);

impl Node {
    /// Remove all elements before the given nanosecond timestamp returning `true` if the resulting
    /// node is empty.
    fn remove_before(&mut self, time_ns: i64) -> bool {
        self.0.retain(|_, (last_seen, node)| {
            // Note that for a branch node, the `last_seen` time will be the greatest of
            // all its nodes, so an entire branch can be removed if its value has not been seen since
            // before `time_ns`, hence the short-curcuit here:
            *last_seen > time_ns
                || node
                    .as_mut()
                    .is_some_and(|node| node.remove_before(time_ns))
        });
        self.0.is_empty()
    }

    /// Remove the `n_to_remove` oldest entries from the cache
    fn remove_n_oldest(&mut self, n_to_remove: usize) {
        let mut times = BinaryHeap::with_capacity(n_to_remove);
        self.find_n_oldest(n_to_remove, &mut times);
        self.remove_before(*times.peek().unwrap());
    }

    /// Use a binary heap to find the time before which all nodes should be removed
    fn find_n_oldest(&self, n_to_remove: usize, times_heap: &mut BinaryHeap<i64>) {
        self.0
            .values()
            // do not need to add the last_seen time for a branch node to the
            // heap since it will be equal to the greatest of that of its leaves
            .for_each(|(last_seen, node)| {
                if let Some(node) = node {
                    node.find_n_oldest(n_to_remove, times_heap)
                } else if times_heap.len() < n_to_remove {
                    times_heap.push(*last_seen);
                } else if times_heap.peek().is_some_and(|newest| last_seen < newest) {
                    times_heap.pop();
                    times_heap.push(*last_seen);
                }
            });
    }

    /// Get the total count of elements nested under this node
    fn len(&self) -> usize {
        self.0
            .values()
            .map(|(_, node)| node.as_ref().map_or(1, |node| node.len()))
            .sum()
    }

    /// Evaluate the set of provided predicates against this node, adding values to the provided
    /// [`StringViewBuilder`]s. Predicates and builders are provided as slices, as this is called
    /// recursively down the cache tree.
    ///
    /// Returns the number of values that were added to the arrow builders.
    ///
    /// # Panics
    ///
    /// This will panic if invalid sized `predicates` and `builders` slices are passed in. When
    /// called from the root [`Node`], their size should be that of the depth of the cache, i.e.,
    /// the number of columns in the cache.
    fn evaluate_predicates(
        &self,
        predicates: &[Option<&Predicate>],
        builders: &mut [StringViewBuilder],
    ) -> usize {
        let mut total_count = 0;
        let (predicate, next_predicates) = predicates
            .split_first()
            .expect("predicates should not be empty");
        // if there is a predicate, evaluate it, otherwise, just grab everything from the node:
        let values_and_nodes = if let Some(predicate) = predicate {
            self.evaluate_predicate(predicate)
        } else {
            self.0
                .iter()
                .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                .collect()
        };
        // iterate over the resulting set of values and next nodes (if this is a branch), and add
        // the values to the arrow builders:
        for (value, node) in values_and_nodes {
            let (builder, next_builders) = builders
                .split_first_mut()
                .expect("builders should not be empty");
            if let Some(node) = node {
                let count = node.evaluate_predicates(next_predicates, next_builders);
                if count > 0 {
                    // we are not on a terminal node in the cache, so create a block, as this value
                    // repeated `count` times, i.e., depending on how many values come out of
                    // subsequent nodes:
                    let block = builder.append_block(value.0.as_bytes().into());
                    for _ in 0..count {
                        builder
                            .try_append_view(block, 0u32, value.0.as_bytes().len() as u32)
                            .expect("append view for known valid block, offset and length");
                    }
                    total_count += count;
                }
            } else {
                builder.append_value(value.0);
                total_count += 1;
            }
        }
        total_count
    }

    /// Evaluate a predicate against a [`Node`], producing a list of [`Value`]s and, if this is a
    /// branch node in the cache tree, a reference to the next [`Node`].
    fn evaluate_predicate(&self, predicate: &Predicate) -> Vec<(Value, Option<&Node>)> {
        match &predicate.kind {
            PredicateKind::Eq(rhs) => self
                .0
                .get_key_value(rhs)
                .map(|(v, (_, n))| vec![(v.clone(), n.as_ref())])
                .unwrap_or_default(),
            PredicateKind::NotEq(rhs) => self
                .0
                .iter()
                .filter(|(v, _)| *v != rhs)
                .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                .collect(),
            PredicateKind::In(in_list) => in_list
                .iter()
                .filter_map(|v| {
                    self.0
                        .get_key_value(v)
                        .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                })
                .collect(),
            PredicateKind::NotIn(not_in_set) => self
                .0
                .iter()
                .filter(|(v, (_, _))| !not_in_set.contains(v))
                .map(|(v, (_, n))| (v.clone(), n.as_ref()))
                .collect(),
        }
    }
}

/// A cache value, which for now, only holds strings
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
struct Value(Arc<str>);

impl From<&FieldData> for Value {
    fn from(field: &FieldData) -> Self {
        match field {
            FieldData::Key(s) => Self(Arc::from(s.as_str())),
            FieldData::Tag(s) => Self(Arc::from(s.as_str())),
            FieldData::String(s) => Self(Arc::from(s.as_str())),
            FieldData::Timestamp(_)
            | FieldData::Integer(_)
            | FieldData::UInteger(_)
            | FieldData::Float(_)
            | FieldData::Boolean(_) => panic!("unexpected field type for metadata cache"),
        }
    }
}

/// A predicate that can be applied when gathering [`RecordBatch`]es from a [`MetaCache`]
///
/// This is intended to be derived from a set of filter expressions in Datafusion
#[derive(Debug)]
pub struct Predicate {
    column_id: ColumnId,
    kind: PredicateKind,
}

#[cfg(test)]
impl Predicate {
    fn new_eq(column_id: ColumnId, rhs: impl Into<Arc<str>>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::Eq(Value(rhs.into())),
        }
    }

    fn new_not_eq(column_id: ColumnId, rhs: impl Into<Arc<str>>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::NotEq(Value(rhs.into())),
        }
    }

    fn new_in(column_id: ColumnId, in_vals: impl IntoIterator<Item: Into<Arc<str>>>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::In(in_vals.into_iter().map(Into::into).map(Value).collect()),
        }
    }

    fn new_not_in(column_id: ColumnId, in_vals: impl IntoIterator<Item: Into<Arc<str>>>) -> Self {
        Self {
            column_id,
            kind: PredicateKind::NotIn(in_vals.into_iter().map(Into::into).map(Value).collect()),
        }
    }
}

// TODO: remove this when fully implemented, for now just suppressing compiler warnings
#[allow(dead_code)]
#[derive(Debug)]
enum PredicateKind {
    Eq(Value),
    NotEq(Value),
    In(Vec<Value>),
    NotIn(HashSet<Value>),
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow_util::assert_batches_sorted_eq;
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
    use influxdb3_id::ColumnId;
    use influxdb3_wal::Row;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use iox_time::{MockProvider, Time, TimeProvider};

    use crate::meta_cache::Predicate;

    use super::{CreateMetaCacheArgs, MaxAge, MaxCardinality, MetaCache};

    struct TestWriter {
        catalog: Arc<Catalog>,
    }

    impl TestWriter {
        const DB_NAME: &str = "test_db";

        fn new() -> Self {
            Self {
                catalog: Arc::new(Catalog::new("test-host".into(), "test-instance".into())),
            }
        }
        fn write_lp(&self, lp: impl AsRef<str>, time_ns: i64) -> Vec<Row> {
            let lines_parsed = WriteValidator::initialize(
                Self::DB_NAME.try_into().unwrap(),
                Arc::clone(&self.catalog),
                time_ns,
            )
            .expect("initialize write validator")
            .v1_parse_lines_and_update_schema(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(time_ns),
                influxdb3_write::Precision::Nanosecond,
            )
            .expect("parse and validate v1 line protocol");

            lines_parsed.into_inner().to_rows()
        }

        fn db_schema(&self) -> Arc<DatabaseSchema> {
            self.catalog
                .db_schema(Self::DB_NAME)
                .expect("db schema should be initialized")
        }
    }

    #[test]
    fn evaluate_predicates() {
        let writer = TestWriter::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to get a set of rows destined for the WAL, and an updated catalog:
        let rows = writer.write_lp(
            "\
            cpu,region=us-east,host=a usage=100\n\
            cpu,region=us-east,host=b usage=100\n\
            cpu,region=us-west,host=c usage=100\n\
            cpu,region=us-west,host=d usage=100\n\
            cpu,region=ca-east,host=e usage=100\n\
            cpu,region=ca-east,host=f usage=100\n\
            cpu,region=ca-cent,host=g usage=100\n\
            cpu,region=ca-cent,host=h usage=100\n\
            cpu,region=eu-east,host=i usage=100\n\
            cpu,region=eu-east,host=j usage=100\n\
            cpu,region=eu-cent,host=k usage=100\n\
            cpu,region=eu-cent,host=l usage=100\n\
            ",
            0,
        );
        // grab the table definition for the table written to:
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        // use the two tags, in order, to create the cache:
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        let region_col_id = column_ids[0];
        let host_col_id = column_ids[1];
        // create the cache:
        let mut cache = MetaCache::new(CreateMetaCacheArgs {
            time_provider,
            table_def,
            max_cardinality: MaxCardinality::default(),
            max_age: MaxAge::default(),
            column_ids,
        })
        .expect("create cache");
        // push the row data into the cache:
        for row in rows {
            cache.push(&row);
        }

        // run a series of test cases with varying predicates:
        struct TestCase<'a> {
            desc: &'a str,
            predicates: &'a [Predicate],
            expected: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                desc: "no predicates",
                predicates: &[],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-cent | h    |",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "eq predicate on region",
                predicates: &[Predicate::new_eq(region_col_id, "us-east")],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "eq predicate on host",
                predicates: &[Predicate::new_eq(host_col_id, "h")],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | h    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "eq predicate on region and host",
                predicates: &[
                    Predicate::new_eq(region_col_id, "ca-cent"),
                    Predicate::new_eq(host_col_id, "h"),
                ],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | h    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not eq predicate on region",
                predicates: &[Predicate::new_not_eq(region_col_id, "ca-cent")],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not eq predicate on region and host",
                predicates: &[
                    Predicate::new_not_eq(region_col_id, "ca-cent"),
                    Predicate::new_not_eq(host_col_id, "a"),
                ],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "in predicate on region",
                predicates: &[Predicate::new_in(region_col_id, ["ca-cent", "ca-east"])],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-cent | h    |",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "in predicate on region and host",
                predicates: &[
                    Predicate::new_in(region_col_id, ["ca-cent", "ca-east"]),
                    Predicate::new_in(host_col_id, ["g", "e"]),
                ],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-east | e    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not in predicate on region",
                predicates: &[Predicate::new_not_in(region_col_id, ["ca-cent", "ca-east"])],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not in predicate on region and host",
                predicates: &[
                    Predicate::new_not_in(region_col_id, ["ca-cent", "ca-east"]),
                    Predicate::new_not_in(host_col_id, ["j", "k"]),
                ],
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
        ];

        for tc in test_cases {
            let records = cache
                .to_record_batch(tc.predicates)
                .expect("get record batches");
            println!("{}", tc.desc);
            assert_batches_sorted_eq!(tc.expected, &[records]);
        }
    }

    #[test]
    fn cache_pruning() {
        let writer = TestWriter::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to update the catalog:
        let _ = writer.write_lp(
            "\
            cpu,region=us-east,host=a usage=100\n\
            ",
            time_provider.now().timestamp_nanos(),
        );
        // grab the table definition for the table written to:
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        // use the two tags, in order, to create the cache:
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        // create a cache with some cardinality and age limits:
        let mut cache = MetaCache::new(CreateMetaCacheArgs {
            time_provider: Arc::clone(&time_provider) as _,
            table_def,
            max_cardinality: MaxCardinality::try_new(10).unwrap(),
            max_age: MaxAge::from(Duration::from_nanos(100)),
            column_ids,
        })
        .expect("create cache");
        // push a bunch of rows with incrementing times and varying tag values:
        (0..10).for_each(|mult| {
            time_provider.set(Time::from_timestamp_nanos(mult * 20));
            let rows = writer.write_lp(
                format!(
                    "\
                    cpu,region=us-east-{mult},host=a-{mult} usage=100\n\
                    cpu,region=us-west-{mult},host=b-{mult} usage=100\n\
                    cpu,region=us-cent-{mult},host=c-{mult} usage=100\n\
                    "
                ),
                time_provider.now().timestamp_nanos(),
            );
            // push the initial row data into the cache:
            for row in rows {
                cache.push(&row);
            }
        });
        // check the cache before prune:
        // NOTE: need to filter out elements that have exceeded the max age in query results...
        let records = cache.to_record_batch(&[]).unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+------+",
                "| region    | host |",
                "+-----------+------+",
                "| us-cent-0 | c-0  |",
                "| us-cent-1 | c-1  |",
                "| us-cent-2 | c-2  |",
                "| us-cent-3 | c-3  |",
                "| us-cent-4 | c-4  |",
                "| us-cent-5 | c-5  |",
                "| us-cent-6 | c-6  |",
                "| us-cent-7 | c-7  |",
                "| us-cent-8 | c-8  |",
                "| us-cent-9 | c-9  |",
                "| us-east-0 | a-0  |",
                "| us-east-1 | a-1  |",
                "| us-east-2 | a-2  |",
                "| us-east-3 | a-3  |",
                "| us-east-4 | a-4  |",
                "| us-east-5 | a-5  |",
                "| us-east-6 | a-6  |",
                "| us-east-7 | a-7  |",
                "| us-east-8 | a-8  |",
                "| us-east-9 | a-9  |",
                "| us-west-0 | b-0  |",
                "| us-west-1 | b-1  |",
                "| us-west-2 | b-2  |",
                "| us-west-3 | b-3  |",
                "| us-west-4 | b-4  |",
                "| us-west-5 | b-5  |",
                "| us-west-6 | b-6  |",
                "| us-west-7 | b-7  |",
                "| us-west-8 | b-8  |",
                "| us-west-9 | b-9  |",
                "+-----------+------+",
            ],
            &[records]
        );
        cache.prune();
        let records = cache.to_record_batch(&[]).unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+------+",
                "| region    | host |",
                "+-----------+------+",
                "| us-cent-7 | c-7  |",
                "| us-cent-8 | c-8  |",
                "| us-cent-9 | c-9  |",
                "| us-east-7 | a-7  |",
                "| us-east-8 | a-8  |",
                "| us-east-9 | a-9  |",
                "| us-west-7 | b-7  |",
                "| us-west-8 | b-8  |",
                "| us-west-9 | b-9  |",
                "+-----------+------+",
            ],
            &[records]
        );
    }
}
