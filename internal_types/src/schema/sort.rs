use std::{fmt::Display, str::FromStr};

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

/// Temporary - <https://github.com/apache/arrow-rs/pull/425>
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            nulls_first: true,
        }
    }
}

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

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct SortKey<'a> {
    columns: IndexMap<&'a str, SortOptions>,
}

impl<'a> SortKey<'a> {
    /// Create a new empty sort key that can store `capacity` columns without allocating
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: IndexMap::with_capacity(capacity),
        }
    }

    /// Adds a new column to the end of this sort key
    pub fn push(&mut self, column: &'a str, options: SortOptions) {
        self.columns.insert(column, options);
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
    pub fn get_index(&self, idx: usize) -> Option<(&'a str, SortOptions)> {
        self.columns
            .get_index(idx)
            .map(|(col, options)| (*col, *options))
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

    /// Returns an iterator over the columns in this key
    pub fn iter(&self) -> Iter<'_, &'a str, SortOptions> {
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

    /// Returns a subset of the sort key that includes only the given columns
    pub fn selected_sort_key(&self, select_keys: Vec<&str>) -> SortKey<'a> {
        let keys: IndexMap<&'a str, SortOptions> = self
            .columns
            .iter()
            .filter_map(|(col, options)| {
                if select_keys.iter().any(|key| key == col) {
                    Some((*col, *options))
                } else {
                    None
                }
            })
            .collect();

        SortKey { columns: keys }
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
    pub fn try_merge_key(key1: &SortKey<'a>, key2: &SortKey<'a>) -> Option<SortKey<'a>> {
        if key1.is_empty() || key2.is_empty() {
            panic!("Sort key cannot be empty");
        }

        let key1 = key1.clone();
        let key2 = key2.clone();

        // Verify if time column in the sort key
        match key1.columns.get_index_of(TIME_COLUMN_NAME) {
            None => panic!("Time column is not included in the sort key {:#?}", key1),
            Some(idx) => {
                if idx < key1.len() - 1 {
                    panic!("Time column is not last in the sort key {:#?}", key1)
                }
            }
        }
        match key2.columns.get_index_of(TIME_COLUMN_NAME) {
            None => panic!("Time column is not included in the sort key {:#?}", key2),
            Some(idx) => {
                if idx < key2.len() - 1 {
                    panic!("Time column is not last in the sort key {:#?}", key2)
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
        for (col, sort_options) in &short_key.columns {
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

    /// Helper to insert col with default sort options into sort key
    pub fn with_col(&mut self, col: &'a str) {
        self.push(col, Default::default());
    }

    /// Helper to insert col with specified sort options into sort key
    pub fn with_col_opts(&mut self, col: &'a str, descending: bool, nulls_first: bool) {
        self.push(
            col,
            SortOptions {
                descending,
                nulls_first,
            },
        );
    }
}

// Produces a human-readable representation of a sort key that looks like:
//
//  "host, region DESC, env NULLS FIRST, time"
//
impl<'a> Display for SortKey<'a> {
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
        let mut key = SortKey::with_capacity(3);
        key.push("a", Default::default());
        key.push("c", Default::default());
        key.push("b", Default::default());

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
        let mut key1 = SortKey::with_capacity(1);
        key1.with_col("a");

        let mut key1_2 = SortKey::with_capacity(2);
        key1_2.with_col("a");
        key1_2.with_col_opts("b", true, false);

        let key2 = SortKey::with_capacity(2);

        // different keys
        assert_ne!(key1, key2);
        assert_ne!(key1_2, key2);
        assert_ne!(key1, key1_2);

        let mut key3 = SortKey::with_capacity(1);
        key3.with_col("a");

        let mut key3_2 = SortKey::with_capacity(2);
        key3_2.with_col("a");
        key3_2.with_col_opts("b", true, false);

        // same
        assert_eq!(key1, key3);
        assert_eq!(key1_2, key3_2);

        let mut key4 = SortKey::with_capacity(1);
        key4.with_col("aa");

        let mut key4_2 = SortKey::with_capacity(2);
        key4_2.with_col("aa");
        key4_2.with_col_opts("bb", true, false);

        // different key, same value
        assert_ne!(key1, key4);
        assert_ne!(key1_2, key4_2);

        let mut key5 = SortKey::with_capacity(1);
        key5.with_col_opts("a", true, true);

        let mut key5_2 = SortKey::with_capacity(2);
        key5_2.with_col_opts("a", true, true);
        key5_2.with_col_opts("b", false, true);

        // same key, different value
        assert_ne!(key1, key5);
        assert_ne!(key1_2, key5_2);
    }

    // Note that the last column must be TIME_COLUMN_NAME to avoid panicking
    #[test]
    fn test_super_sort_key() {
        // key (a) with default sort options (false, true)
        let mut key_a = SortKey::with_capacity(1);
        let a = TIME_COLUMN_NAME;
        key_a.with_col(a);
        // key (a) with explicitly defined sort options
        let mut key_a_2 = SortKey::with_capacity(1);
        key_a_2.with_col_opts(a, true, false);

        // super key of (a) and (a) is (a)
        let merge_key = SortKey::try_merge_key(&key_a, &key_a).unwrap();
        assert_eq!(merge_key, key_a);
        let merge_key = SortKey::try_merge_key(&key_a_2, &key_a_2).unwrap();
        assert_eq!(merge_key, key_a_2);

        // (a,b)
        let b = TIME_COLUMN_NAME;
        let mut key_ab = SortKey::with_capacity(2);
        key_ab.with_col("a");
        key_ab.with_col(b);
        let mut key_ab_2 = SortKey::with_capacity(2);
        key_ab_2.with_col_opts("a", true, false);
        key_ab_2.with_col_opts(b, false, false);
        //(b)
        let mut key_b = SortKey::with_capacity(1);
        key_b.with_col(b);
        let mut key_b_2 = SortKey::with_capacity(1);
        key_b_2.with_col_opts(b, false, false);

        // super key of (a, b) and (b) is (a, b)
        let merge_key = SortKey::try_merge_key(&key_ab, &key_b).unwrap();
        assert_eq!(merge_key, key_ab);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_b_2).unwrap();
        assert_eq!(merge_key, key_ab_2);
        // super key of (a, b) and (b') is None
        let merge_key = SortKey::try_merge_key(&key_ab, &key_b_2);
        assert_eq!(merge_key, None);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_b);
        assert_eq!(merge_key, None);

        // super key of (a, b) and (a, b) is (a, b)
        let merge_key = SortKey::try_merge_key(&key_ab, &key_ab).unwrap();
        assert_eq!(merge_key, key_ab);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_ab_2).unwrap();
        assert_eq!(merge_key, key_ab_2);
        // super key of (a, b) and (a',b') is None
        let merge_key = SortKey::try_merge_key(&key_ab, &key_ab_2);
        assert_eq!(merge_key, None);
        let merge_key = SortKey::try_merge_key(&key_ab_2, &key_ab);
        assert_eq!(merge_key, None);

        // (a, b, c)
        let c = TIME_COLUMN_NAME;
        let mut key_abc_2 = SortKey::with_capacity(3);
        key_abc_2.with_col_opts("a", true, false);
        key_abc_2.with_col_opts("b", false, false);
        key_abc_2.with_col_opts(c, true, true);

        //  (c)
        let mut key_c_2 = SortKey::with_capacity(1);
        key_c_2.with_col_opts(c, true, true);

        // (a, c)
        let mut key_ac_2 = SortKey::with_capacity(2);
        key_ac_2.with_col_opts("a", true, false);
        key_ac_2.with_col_opts(c, true, true);

        // (b,c)
        let mut key_bc_2 = SortKey::with_capacity(2);
        key_bc_2.with_col_opts("b", false, false);
        key_bc_2.with_col_opts(c, true, true);

        // (b,a,c)
        let mut key_bac_2 = SortKey::with_capacity(3);
        key_bac_2.with_col_opts("b", false, false);
        key_bac_2.with_col_opts("a", true, false);
        key_bac_2.with_col_opts(c, true, true);

        // super key of (a, b, c) and any of {  (a, c), (b, c), (a), (b), (c)  } is (a, b, c)
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_c_2).unwrap();
        assert_eq!(merge_key, key_abc_2);
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_ac_2).unwrap();
        assert_eq!(merge_key, key_abc_2);
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_bc_2).unwrap();
        assert_eq!(merge_key, key_abc_2);

        // super key of (a, b, c) and any of (b, a, c) } is None
        let merge_key = SortKey::try_merge_key(&key_abc_2, &key_bac_2);
        assert_eq!(merge_key, None);
    }
}
