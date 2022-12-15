use crate::{Schema, TIME_COLUMN_NAME};
use arrow::compute::SortOptions;
use arrow::{
    array::{Array, DictionaryArray, StringArray},
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use indexmap::{map::Iter, IndexMap};
use itertools::Itertools;
use observability_deps::tracing::debug;
use snafu::Snafu;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid column sort: {}", value))]
    InvalidColumnSort { value: String },

    #[snafu(display("invalid sort ordinal: {}", value))]
    InvalidSortOrdinal { value: String },

    #[snafu(display("invalid descending value: {}", value))]
    InvalidDescending { value: String },

    #[snafu(display("invalid nulls first value: {}", value))]
    InvalidNullsFirst { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ColumnSort {
    /// Position of this column in the sort key
    pub sort_ordinal: usize,
    pub options: SortOptions,
}

impl FromStr for ColumnSort {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (sort, descending, nulls) =
            s.split('/')
                .collect_tuple()
                .ok_or(Error::InvalidColumnSort {
                    value: s.to_string(),
                })?;

        Ok(Self {
            sort_ordinal: sort.parse().map_err(|_| Error::InvalidSortOrdinal {
                value: sort.to_string(),
            })?,
            options: SortOptions {
                descending: descending.parse().map_err(|_| Error::InvalidDescending {
                    value: descending.to_string(),
                })?,
                nulls_first: nulls.parse().map_err(|_| Error::InvalidNullsFirst {
                    value: nulls.to_string(),
                })?,
            },
        })
    }
}

impl std::fmt::Display for ColumnSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.sort_ordinal, self.options.descending, self.options.nulls_first
        )
    }
}

#[derive(Debug, Default)]
pub struct SortKeyBuilder {
    columns: IndexMap<Arc<str>, SortOptions>,
}

impl SortKeyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: IndexMap::with_capacity(capacity),
        }
    }

    pub fn with_col(self, column: impl Into<Arc<str>>) -> Self {
        self.with_col_sort_opts(column, Default::default())
    }

    /// Helper to insert col with specified sort options into sort key
    pub fn with_col_opts(
        self,
        col: impl Into<Arc<str>>,
        descending: bool,
        nulls_first: bool,
    ) -> Self {
        self.with_col_sort_opts(
            col,
            SortOptions {
                descending,
                nulls_first,
            },
        )
    }

    pub fn with_col_sort_opts(mut self, col: impl Into<Arc<str>>, options: SortOptions) -> Self {
        self.columns.insert(col.into(), options);
        self
    }

    pub fn build(self) -> SortKey {
        SortKey {
            columns: Arc::new(self.columns),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SortKey {
    columns: Arc<IndexMap<Arc<str>, SortOptions>>,
}

impl SortKey {
    /// Create a new empty sort key
    pub fn empty() -> Self {
        SortKey {
            columns: Default::default(),
        }
    }

    /// Create a new sort key from the provided columns
    pub fn from_columns<C, I>(columns: C) -> Self
    where
        C: IntoIterator<Item = I>,
        I: Into<Arc<str>>,
    {
        let iter = columns.into_iter();
        let mut builder = SortKeyBuilder::with_capacity(iter.size_hint().0);
        for c in iter {
            builder = builder.with_col(c);
        }
        builder.build()
    }

    pub fn to_columns(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|k| k.as_ref())
    }

    /// Gets the ColumnSort for a given column name
    pub fn get(&self, column: &str) -> Option<ColumnSort> {
        let (sort_ordinal, _, options) = self.columns.get_full(column)?;
        Some(ColumnSort {
            sort_ordinal,
            options: *options,
        })
    }

    /// Gets the column for a given index
    pub fn get_index(&self, idx: usize) -> Option<(&Arc<str>, &SortOptions)> {
        self.columns.get_index(idx)
    }

    /// Return the index of the given column and its sort option. Return None otherwise.
    pub fn find_index(&self, column: &str, sort_options: &SortOptions) -> Option<usize> {
        // Find the given column in this SortKey
        let (sort_ordinal, _, options) = self.columns.get_full(column)?;

        // See if it SortOptions the same
        if options == sort_options {
            return Some(sort_ordinal);
        }

        None
    }

    /// Return true if this column appears anywhere in the sort key.
    pub fn contains(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    /// Returns an iterator over the columns in this key
    pub fn iter(&self) -> Iter<'_, Arc<str>, SortOptions> {
        self.columns.iter()
    }

    /// Returns the length of the sort key
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns if this sort key is empty
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Filters this sort key to contain only the columns present in the primary key, in the order
    /// that the columns appear in this sort key.
    ///
    /// # Panics
    ///
    /// Panics if any columns in the primary key are NOT present in this sort key.
    pub fn filter_to(&self, primary_key: &[&str], partition_id: i64) -> SortKey {
        let missing_from_catalog_key: Vec<_> = primary_key
            .iter()
            .filter(|col| !self.contains(col))
            .collect();
        if !missing_from_catalog_key.is_empty() {
            panic!(
                "Primary key column(s) found that don't appear in the catalog sort key [{:?}] of partition: {}. Sort key: {:?}",
                missing_from_catalog_key, partition_id, self
            )
        }

        Self::from_columns(
            self.iter()
                .map(|(col, _opts)| col)
                .filter(|col| primary_key.contains(&col.as_ref()))
                .cloned(),
        )
    }

    /// Returns merge key of the 2 given keys if one covers the other. Returns None otherwise.
    /// Key1 is said to cover key2 if key2 is a subset and in the same order of key1.
    /// Examples:
    ///   . (a) covers empty and (a)  =>
    ///         . super key of (a) and empty is (a)
    ///         . super key of (a) and (a)) is (a)
    ///   . (a, b) covers empty, (a), (b), and (a, b) =>
    ///         . super key of (a, b) and empty is (a, b)
    ///         . super key of (a, b) and (a) is (a, b)
    ///         . super key of (a, b) and (b) is (a, b)
    ///         . super key of (a, b) and (a, b) is (a, b)
    ///   . (a, b) does not cover (b, a) => super key of (a, b) and (b, a) is None
    ///   . (a, b, c) covers (a, b), (a, c), (b, c), (a), (b), (c) and empty =>
    ///        super key of (a, b, c) and any of { (a, b), (a, c), (b, c), (a), (b), (c) and empty } is (a, b, c)
    ///   . (a, b, c) does not cover (b, a), (c, a), (c, b), (b, a, c), (b, c, a), (c, a, b), (c, b, a) =>
    ///        super key of (a, b, c) and any of { b, a), (c, a), (c, b), (b, a, c), (b, c, a), (c, a, b), (c, b, a) } is None
    ///
    ///  Note that the last column in the sort key must be time
    pub fn try_merge_key<'a>(key1: &'a SortKey, key2: &'a SortKey) -> Option<&'a SortKey> {
        if key1.is_empty() || key2.is_empty() {
            panic!("Sort key cannot be empty");
        }

        // Verify if time column in the sort key
        for key in [&key1, &key2] {
            match key.columns.get_index_of(TIME_COLUMN_NAME) {
                None => panic!("Time column is not included in the sort key {:#?}", key),
                Some(idx) => {
                    if idx < key.len() - 1 {
                        panic!("Time column is not last in the sort key {:#?}", key)
                    }
                }
            }
        }

        let (long_key, short_key) = if key1.len() > key2.len() {
            (key1, key2)
        } else {
            (key2, key1)
        };

        // Go over short key and check its right-order availability in the long key
        let mut prev_long_idx: Option<usize> = None;
        for (col, sort_options) in short_key.columns.iter() {
            if let Some(long_idx) = long_key.find_index(col, sort_options) {
                match prev_long_idx {
                    None => prev_long_idx = Some(long_idx),
                    Some(prev_idx) => {
                        if long_idx > prev_idx {
                            // In the right order, update the current idx
                            prev_long_idx = Some(long_idx);
                        } else {
                            // Not in the right order
                            return None;
                        }
                    }
                }
            } else {
                // Not found
                return None;
            }
        }

        // Reach here means the long key is the super key of the sort one
        Some(long_key)
    }

    /// Get size of `self.columns` EXCLUDING the type itself (i.e. `size_of_val(&self.columns)`).
    fn size_columns(&self) -> usize {
        // Size calculation for `self.columns`:
        //
        // - `self.columns` is an `IndexMap`, which is mostly backed by `IndexMapCore`.
        // - `IndexMapCore` is:
        //     struct IndexMapCore<K, V> {
        //         indices: RawTable<usize>,
        //         entries: Vec<Bucket<K, V>>,
        //     }
        // - `Bucket` is:
        //      struct Bucket<K, V> {
        //         hash: HashValue,
        //         key: K,
        //         value: V,
        //      }
        // - `HashValue` is just a newtype `usize`
        // - We assume that the hashbrown `RawTable` has 1 byte overhead per entry (as mentioned in their README) but
        //   allocates the whole capacity (which is very conservative).
        // - the size of `indices` and `entries` can sadly only be guessed, since `IndexMap::capacity` returns the
        //   minimum of the two capacities.
        type K = Arc<str>;
        type V = SortOptions;
        let capacity_indices = self.columns.capacity();
        let capacity_entries = capacity_indices;
        const SIZE_BUCKET: usize =
            std::mem::size_of::<usize>() + std::mem::size_of::<K>() + std::mem::size_of::<V>();
        let size_entries = SIZE_BUCKET * capacity_entries;
        const SIZE_HASHBROWN_ENTRY: usize = std::mem::size_of::<usize>() + 1;
        let size_indices = SIZE_HASHBROWN_ENTRY * capacity_indices;
        size_entries + size_indices
    }

    /// Memory size in bytes including `self`
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.size_columns()
            + self.columns.keys().map(|k| k.len()).sum::<usize>()
    }
}

// Produces a human-readable representation of a sort key that looks like:
//
//  "host, region DESC, env NULLS FIRST, time"
//
impl Display for SortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        for (i, (name, options)) in self.columns.iter().enumerate() {
            write!(f, "{}", name)?;
            if options.descending {
                write!(f, " DESC")?;
            }
            if !options.nulls_first {
                // less common case
                write!(f, " NULLS LAST")?;
            }
            write!(f, ",")?;

            if i < self.columns.len() - 1 {
                write!(f, " ")?;
            }
        }
        Ok(())
    }
}

/// Given a `Schema` and an iterator of `RecordBatch`es, compute a sort key based on:
///
/// - The columns that make up the primary key of the schema
/// - Order those columns from low cardinality to high cardinality based on the data
/// - Always have the time column last
pub fn compute_sort_key<'a>(
    schema: &Schema,
    batches: impl Iterator<Item = &'a RecordBatch>,
) -> SortKey {
    let primary_key = schema.primary_key();

    let cardinalities = distinct_counts(batches, &primary_key);

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities.sort_by_cached_key(|x| (x.1, x.0.clone()));

    let mut builder = SortKeyBuilder::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        builder = builder.with_col(col)
    }
    builder = builder.with_col(TIME_COLUMN_NAME);
    let sort_key = builder.build();

    debug!(?primary_key, ?sort_key, "computed sort key");
    sort_key
}

/// Takes batches of data and the columns that make up the primary key. Computes the number of
/// distinct values for each primary key column across all batches, also known as "cardinality".
/// Used to determine sort order.
fn distinct_counts<'a>(
    batches: impl Iterator<Item = &'a RecordBatch>,
    primary_key: &[&str],
) -> HashMap<String, u64> {
    let mut distinct_values_across_batches = HashMap::with_capacity(primary_key.len());

    for batch in batches {
        for (column, distinct_values) in distinct_values(batch, primary_key) {
            let set = distinct_values_across_batches
                .entry(column)
                .or_insert_with(HashSet::new);
            set.extend(distinct_values.into_iter());
        }
    }

    distinct_values_across_batches
        .into_iter()
        .map(|(column, distinct_values)| {
            let count = distinct_values
                .len()
                .try_into()
                .expect("usize -> u64 overflow");

            (column, count)
        })
        .collect()
}

/// Takes a `RecordBatch` and the column names that make up the primary key of the schema. Returns
/// a map of column names to the set of the distinct string values, for the specified columns. Used
/// to compute cardinality across multiple `RecordBatch`es.
fn distinct_values(batch: &RecordBatch, primary_key: &[&str]) -> HashMap<String, HashSet<String>> {
    let schema = batch.schema();
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .filter(|(_col, field)| primary_key.contains(&field.name().as_str()))
        .flat_map(|(col, field)| match field.data_type() {
            // Dictionaries of I32 => Utf8 are supported as tags in
            // `schema::InfluxColumnType::valid_arrow_type`
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
            {
                let col = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("unexpected datatype");

                let values = col.values();
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("unexpected datatype");

                Some((
                    field.name().into(),
                    values.iter().flatten().map(ToString::to_string).collect(),
                ))
            }
            // Utf8 types are supported as tags
            DataType::Utf8 => {
                let values = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("unexpected datatype");

                Some((
                    field.name().into(),
                    values.iter().flatten().map(ToString::to_string).collect(),
                ))
            }
            // No other data types are supported as tags; don't compute distinct values for them
            _ => None,
        })
        .collect()
}

/// Given a sort key from the catalog and the primary key (tags + time) from the data, return the
/// sort key that should be used for this parquet file and, if needed, the sort key that should
/// be updated in the catalog. These are computed as follows:
///
/// - Columns that appear in both the primary key and the catalog sort key should appear in the
///   same order as they appear in the catalog sort key.
/// - If there are new columns that appear in the primary key, add the new columns to the end of
///   the catalog sort key's tag list. Also return an updated catalog sort key to save the new
///   column in the catalog.
/// - If there are columns that appear in the catalog sort key but aren't present in this data's
///   primary key, don't include them in the sort key to be used for this data. Don't remove them
///   from the catalog sort key.
pub fn adjust_sort_key_columns(
    catalog_sort_key: &SortKey,
    primary_key: &[&str],
) -> (SortKey, Option<SortKey>) {
    let existing_columns_without_time = catalog_sort_key
        .iter()
        .map(|(col, _opts)| col)
        .cloned()
        .filter(|col| TIME_COLUMN_NAME != col.as_ref());
    let new_columns: Vec<_> = primary_key
        .iter()
        .filter(|col| !catalog_sort_key.contains(col))
        .collect();

    let metadata_sort_key = SortKey::from_columns(
        existing_columns_without_time
            .clone()
            .filter(|col| primary_key.contains(&col.as_ref()))
            .chain(new_columns.iter().map(|&&col| Arc::from(col)))
            .chain(std::iter::once(Arc::from(TIME_COLUMN_NAME))),
    );

    let catalog_update = if new_columns.is_empty() {
        None
    } else {
        Some(SortKey::from_columns(
            existing_columns_without_time
                .chain(new_columns.into_iter().map(|&col| Arc::from(col)))
                .chain(std::iter::once(Arc::from(TIME_COLUMN_NAME))),
        ))
    };

    debug!(?primary_key,
           input_catalog_sort_key=?catalog_sort_key,
           output_chunk_sort_key=?metadata_sort_key,
           output_catalog_sort_key=?catalog_update,
           "adjusted sort key");

    (metadata_sort_key, catalog_update)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SchemaBuilder;
    use arrow::array::ArrayRef;

    #[test]
    fn test_parse() {
        assert_eq!(
            ColumnSort::from_str("23/true/false").unwrap(),
            ColumnSort {
                sort_ordinal: 23,
                options: SortOptions {
                    descending: true,
                    nulls_first: false
                }
            }
        );

        assert!(matches!(
            ColumnSort::from_str("///").unwrap_err(),
            Error::InvalidColumnSort { value } if &value == "///"
        ));

        assert!(matches!(
            ColumnSort::from_str("-1/true/false").unwrap_err(),
            Error::InvalidSortOrdinal { value } if &value == "-1"
        ));

        assert!(matches!(
            ColumnSort::from_str("34/sdf d/false").unwrap_err(),
            Error::InvalidDescending {value } if &value == "sdf d"
        ));

        assert!(matches!(
            ColumnSort::from_str("34/true/s=,ds").unwrap_err(),
            Error::InvalidNullsFirst { value } if &value == "s=,ds"
        ));
    }

    #[test]
    fn test_basic() {
        let key = SortKey::from_columns(vec!["a", "c", "b"]);

        assert_eq!(key.len(), 3);
        assert!(!key.is_empty());

        assert_eq!(key.get("foo"), None);
        assert_eq!(
            key.get("a"),
            Some(ColumnSort {
                sort_ordinal: 0,
                options: Default::default()
            })
        );
        assert_eq!(
            key.get("b"),
            Some(ColumnSort {
                sort_ordinal: 2,
                options: Default::default()
            })
        );
        assert_eq!(
            key.get("c"),
            Some(ColumnSort {
                sort_ordinal: 1,
                options: Default::default()
            })
        );
    }

    #[test]
    fn test_sort_key_eq() {
        let key1 = SortKey::from_columns(vec!["a"]);

        let key1_2 = SortKeyBuilder::with_capacity(2)
            .with_col("a")
            .with_col_opts("b", true, false)
            .build();

        let key2 = SortKey::empty();

        // different keys
        assert_ne!(key1, key2);
        assert_ne!(key1_2, key2);
        assert_ne!(key1, key1_2);

        let key3 = SortKey::from_columns(vec!["a"]);

        let key3_2 = SortKeyBuilder::with_capacity(2)
            .with_col("a")
            .with_col_opts("b", true, false)
            .build();

        // same
        assert_eq!(key1, key3);
        assert_eq!(key1_2, key3_2);

        let key4 = SortKey::from_columns(vec!["aa"]);

        let key4_2 = SortKeyBuilder::with_capacity(2)
            .with_col("aa")
            .with_col_opts("bb", true, false)
            .build();

        // different key, same value
        assert_ne!(key1, key4);
        assert_ne!(key1_2, key4_2);

        let key5 = SortKeyBuilder::with_capacity(1)
            .with_col_opts("a", true, true)
            .build();

        let key5_2 = SortKeyBuilder::with_capacity(2)
            .with_col_opts("a", true, true)
            .with_col_opts("b", false, true)
            .build();

        // same key, different value
        assert_ne!(key1, key5);
        assert_ne!(key1_2, key5_2);
    }

    // Note that the last column must be TIME_COLUMN_NAME to avoid panicking
    #[test]
    fn test_super_sort_key() {
        let a = TIME_COLUMN_NAME;
        // key (a) with default sort options (false, true)
        let key_a = SortKey::from_columns(vec![a]);

        // key (a) with explicitly defined sort options
        let key_a_2 = SortKeyBuilder::with_capacity(1)
            .with_col_opts(TIME_COLUMN_NAME, true, false)
            .build();

        // super key of (a) and (a) is (a)
        let merge_key = SortKey::try_merge_key(&key_a, &key_a).unwrap();
        assert_eq!(merge_key, &key_a);
        let merge_key = SortKey::try_merge_key(&key_a_2, &key_a_2).unwrap();
        assert_eq!(merge_key, &key_a_2);

        // (a,b)
        let b = TIME_COLUMN_NAME;
        let key_ab = SortKey::from_columns(vec!["a", TIME_COLUMN_NAME]);
        let key_ab_2 = SortKeyBuilder::with_capacity(2)
            .with_col_opts("a", true, false)
            .with_col_opts(b, false, false)
            .build();

        //(b)
        let key_b = SortKey::from_columns(vec![b]);

        let key_b_2 = SortKeyBuilder::with_capacity(1)
            .with_col_opts(b, false, false)
            .build();

        // super key of (a, b) and (b) is (a, b)
        let merge_key = SortKey::try_merge_key(&key_ab, &key_b).unwrap();
        assert_eq!(merge_key, &key_ab);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_b_2).unwrap();
        assert_eq!(merge_key, &key_ab_2);
        // super key of (a, b) and (b') is None
        let merge_key = SortKey::try_merge_key(&key_ab, &key_b_2);
        assert_eq!(merge_key, None);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_b);
        assert_eq!(merge_key, None);

        // super key of (a, b) and (a, b) is (a, b)
        let merge_key = SortKey::try_merge_key(&key_ab, &key_ab).unwrap();
        assert_eq!(merge_key, &key_ab);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_ab_2).unwrap();
        assert_eq!(merge_key, &key_ab_2);
        // super key of (a, b) and (a',b') is None
        let merge_key = SortKey::try_merge_key(&key_ab, &key_ab_2);
        assert_eq!(merge_key, None);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_ab);
        assert_eq!(merge_key, None);

        // (a, b, c)
        let c = TIME_COLUMN_NAME;
        let key_abc_2 = SortKeyBuilder::with_capacity(3)
            .with_col_opts("a", true, false)
            .with_col_opts("b", false, false)
            .with_col_opts(c, true, true)
            .build();

        //  (c)
        let key_c_2 = SortKeyBuilder::with_capacity(1)
            .with_col_opts(c, true, true)
            .build();

        // (a, c)
        let key_ac_2 = SortKeyBuilder::with_capacity(2)
            .with_col_opts("a", true, false)
            .with_col_opts(c, true, true)
            .build();

        // (b,c)
        let key_bc_2 = SortKeyBuilder::with_capacity(2)
            .with_col_opts("b", false, false)
            .with_col_opts(c, true, true)
            .build();

        // (b,a,c)
        let key_bac_2 = SortKeyBuilder::with_capacity(3)
            .with_col_opts("b", false, false)
            .with_col_opts("a", true, false)
            .with_col_opts(c, true, true)
            .build();

        // super key of (a, b, c) and any of {  (a, c), (b, c), (a), (b), (c)  } is (a, b, c)
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_c_2).unwrap();
        assert_eq!(merge_key, &key_abc_2);
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_ac_2).unwrap();
        assert_eq!(merge_key, &key_abc_2);
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_bc_2).unwrap();
        assert_eq!(merge_key, &key_abc_2);

        // super key of (a, b, c) and any of (b, a, c) } is None
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_bac_2);
        assert_eq!(merge_key, None);
    }

    fn to_string_array(values: impl Into<StringArray>) -> ArrayRef {
        Arc::new(values.into()) as ArrayRef
    }

    #[test]
    fn test_distinct_values() {
        let rb = RecordBatch::try_from_iter(vec![
            ("host", to_string_array(vec!["a", "b", "c", "a"])),
            (
                "env",
                to_string_array(vec![None, Some("prod"), Some("stage"), Some("prod")]),
            ),
        ])
        .unwrap();

        // Pass the tag field names plus time as the primary key, this is what should happen
        let distinct = distinct_values(&rb, &["host", "env", "time"]);

        // The hashmap should contain the distinct values for "host" and "env" only
        assert_eq!(distinct.len(), 2);

        // Return unique values
        assert_eq!(
            *distinct.get("host").unwrap(),
            HashSet::from(["a".into(), "b".into(), "c".into()]),
        );
        // TODO: do nulls count as a value?
        assert_eq!(
            *distinct.get("env").unwrap(),
            HashSet::from(["prod".into(), "stage".into()]),
        );

        // Requesting a column not present returns None
        assert_eq!(distinct.get("foo"), None);

        // Distinct count isn't computed for the time column or fields
        assert_eq!(distinct.get("time"), None);
        assert_eq!(distinct.get("val"), None);

        // Specify a column in the primary key that doesn't appear in the data
        let distinct = distinct_values(&rb, &["host", "env", "foo", "time"]);
        // The hashmap should contain the distinct values for "host" and "env" only
        assert_eq!(distinct.len(), 2);

        // Don't specify one of the tag columns for the primary key
        let distinct = distinct_values(&rb, &["host", "foo", "time"]);
        // The hashmap should contain the distinct values for the specified columns only
        assert_eq!(distinct.len(), 1);
    }

    #[test]
    fn test_sort_key() {
        // Across these three record batches:
        // - `host` has 2 distinct values: "a", "b"
        // - 'env' has 3 distinct values: "prod", "stage", "dev"
        // host's 2 values appear in each record batch, so the distinct counts could be incorrectly
        // aggregated together as 2 + 2 + 2 = 6. env's 3 values each occur in their own record
        // batch, so they should always be aggregated as 3.
        // host has the lower cardinality, so it should appear first in the sort key.
        let rb1 = Arc::new(
            RecordBatch::try_from_iter(vec![
                ("host", to_string_array(vec!["a", "b"])),
                ("env", to_string_array(vec!["prod", "prod"])),
            ])
            .unwrap(),
        );
        let rb2 = Arc::new(
            RecordBatch::try_from_iter(vec![
                ("host", to_string_array(vec!["a", "b"])),
                ("env", to_string_array(vec!["stage", "stage"])),
            ])
            .unwrap(),
        );
        let rb3 = Arc::new(
            RecordBatch::try_from_iter(vec![
                ("host", to_string_array(vec!["a", "b"])),
                ("env", to_string_array(vec!["dev", "dev"])),
            ])
            .unwrap(),
        );
        let rbs = [rb1, rb2, rb3];
        let schema = SchemaBuilder::new()
            .tag("host")
            .tag("env")
            .timestamp()
            .build()
            .unwrap();

        let sort_key = compute_sort_key(&schema, rbs.iter().map(|rb| rb.as_ref()));

        assert_eq!(sort_key, SortKey::from_columns(["host", "env", "time"]));
    }

    #[test]
    fn test_sort_key_all_null() {
        let rb = Arc::new(
            RecordBatch::try_from_iter(vec![
                ("x", to_string_array(vec!["a", "b"])),
                ("y", to_string_array(vec![None, None])),
                ("z", to_string_array(vec!["c", "c"])),
            ])
            .unwrap(),
        );
        let rbs = [rb];
        let schema = SchemaBuilder::new()
            .tag("x")
            .tag("y")
            .tag("z")
            .timestamp()
            .build()
            .unwrap();

        let sort_key = compute_sort_key(&schema, rbs.iter().map(|rb| rb.as_ref()));

        assert_eq!(sort_key, SortKey::from_columns(["x", "z", "y", "time"]));
    }

    #[test]
    fn test_adjust_sort_key_columns() {
        // If the catalog sort key is the same as the primary key, no changes
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "env", "time"];

        let (metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &data_primary_key);

        assert_eq!(metadata, catalog_sort_key);
        assert!(update.is_none());

        // If the catalog sort key contains more columns than the primary key, the metadata key
        // should only contain the columns in the data and the catalog should not be updated
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "time"];

        let (metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &data_primary_key);

        assert_eq!(metadata, SortKey::from_columns(data_primary_key));
        assert!(update.is_none());

        // If the catalog sort key contains fewer columns than the primary key, add the new columns
        // just before the time column and update the catalog
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "temp", "env", "time"];

        let (metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &data_primary_key);

        let expected = SortKey::from_columns(["host", "env", "temp", "time"]);
        assert_eq!(metadata, expected);
        assert_eq!(update.unwrap(), expected);

        // If the sort key contains a column that doesn't exist in the data and is missing a column,
        // the metadata key should only contain the columns in the data and the catalog should be
        // updated to include the new column (but not remove the missing column)
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "temp", "time"];

        let (metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &data_primary_key);
        assert_eq!(metadata, SortKey::from_columns(data_primary_key));
        let expected = SortKey::from_columns(["host", "env", "temp", "time"]);
        assert_eq!(update.unwrap(), expected);
    }

    #[test]
    fn test_filter_to_primary_key() {
        // If the catalog sort key is the same as the primary key, no changes
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "env", "time"];

        let filtered = catalog_sort_key.filter_to(&data_primary_key, 1);
        assert_eq!(catalog_sort_key, filtered);

        // If the catalog sort key contains more columns than the primary key, the filtered key
        // should only contain the columns in the primary key
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "time"];

        let filtered = catalog_sort_key.filter_to(&data_primary_key, 1);
        let expected = SortKey::from_columns(["host", "time"]);
        assert_eq!(expected, filtered);

        // If the catalog sort key has columns in a different order than the primary key, the
        // filtered key should contain the columns in the same order as the catalog sort key.
        let catalog_sort_key = SortKey::from_columns(["host", "env", "zone", "time"]);
        let data_primary_key = ["env", "host", "time"];

        let filtered = catalog_sort_key.filter_to(&data_primary_key, 1);
        let expected = SortKey::from_columns(["host", "env", "time"]);
        assert_eq!(expected, filtered);
    }

    #[test]
    #[should_panic]
    fn test_filter_missing_columns() {
        // If the primary key has columns that are missing from the catalog sort key, panic.
        // This should never happen because the ingester should save the sort key as the union
        // of all primary key columns it has seen, and the compactor shouldn't get data that hasn't
        // been through the ingester.
        let catalog_sort_key = SortKey::from_columns(["host", "env", "time"]);
        let data_primary_key = ["host", "env", "zone", "time"];

        catalog_sort_key.filter_to(&data_primary_key, 1);
    }

    #[test]
    fn test_size() {
        let key_1 = SortKey::from_columns(vec![TIME_COLUMN_NAME]);
        let key_2 = SortKey::from_columns(vec!["a", TIME_COLUMN_NAME]);
        assert!(key_1.size() < key_2.size());
    }
}
