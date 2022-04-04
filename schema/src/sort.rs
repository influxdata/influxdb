use std::sync::Arc;
use std::{fmt::Display, str::FromStr};

use arrow::compute::SortOptions;
use indexmap::{map::Iter, IndexMap};
use itertools::Itertools;
use snafu::Snafu;

use super::TIME_COLUMN_NAME;

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

    pub fn to_columns(&self) -> String {
        self.columns.keys().join(",")
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
