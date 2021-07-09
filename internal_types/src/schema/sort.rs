use std::{fmt::Display, str::FromStr};

use indexmap::{map::Iter, IndexMap};
use itertools::Itertools;
use snafu::Snafu;

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

/// Temporary - https://github.com/apache/arrow-rs/pull/425
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

    /// Returns the super key of the 2 given keys if one covers the other. Returns None otherwise.
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
    pub fn super_key(key1: &SortKey<'a>, key2: &SortKey<'a>) -> Option<SortKey<'a>> {
        let (long_key, short_key) = if key1.len() > key2.len() {
            (key1, key2)
        } else {
            (key2, key1)
        };

        if long_key.is_empty() && short_key.is_empty() {
            // They both empty
            return None;
        }

        if short_key.is_empty() {
            return Some(long_key.clone());
        }

        // Go over short key and check its right-order availability in the long key
        let mut prev_long_idx = 0;
        let mut first = true;
        for (col, sort_options) in &short_key.columns {
            if let Some(long_idx) = long_key.find_index(col, sort_options) {
                if first {
                    prev_long_idx = long_idx;
                    first = false;
                } else if long_idx > prev_long_idx {
                    // In the right order, update the current idx
                    prev_long_idx = long_idx;
                } else {
                    // Not in the right order
                    return None;
                }
            } else {
                // Not found
                return None;
            }
        }

        // Reach here means the long key is the super key of the sort one
        Some(long_key.clone())
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
        key1.push("a", Default::default());

        let mut key1_2 = SortKey::with_capacity(2);
        key1_2.push("a", Default::default());
        key1_2.push(
            "b",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );

        let key2 = SortKey::with_capacity(2);

        // different capacity
        assert_ne!(key1, key2);
        assert_ne!(key1_2, key2);
        assert_ne!(key1, key1_2);

        let mut key3 = SortKey::with_capacity(1);
        key3.push("a", Default::default());

        let mut key3_2 = SortKey::with_capacity(1);
        key3_2.push("a", Default::default());
        key3_2.push(
            "b",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );

        // same
        assert_eq!(key1, key3);
        assert_eq!(key1_2, key3_2);

        let mut key4 = SortKey::with_capacity(1);
        key4.push("aa", Default::default());

        let mut key4_2 = SortKey::with_capacity(1);
        key4_2.push("aa", Default::default());
        key4_2.push(
            "bb",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );

        // different key, same value
        assert_ne!(key1, key4);
        assert_ne!(key1_2, key4_2);

        let mut key5 = SortKey::with_capacity(1);
        key5.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );

        let mut key5_2 = SortKey::with_capacity(1);
        key5_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key5_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        );

        // same key, different value
        assert_ne!(key1, key5);
        assert_ne!(key1_2, key5_2);
    }

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
    #[test]
    fn test_super_sort_key() {
        // empty key
        let key_e = SortKey::with_capacity(0);
        // key (a) with default sort options (false, true)
        let mut key_a = SortKey::with_capacity(1);
        key_a.push("a", Default::default());
        // key (a) with explicitly defined sort options
        let mut key_a_2 = SortKey::with_capacity(1);
        key_a_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        // key (b) with default sort options (false, true)
        let mut key_b = SortKey::with_capacity(1);
        key_b.push("b", Default::default());
        // key (b) with explicitly defined sort options
        let mut key_b_2 = SortKey::with_capacity(1);
        key_b_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        // super key of (a) and empty is (a)
        let super_key = SortKey::super_key(&key_a, &key_e).unwrap();
        assert_eq!(super_key, key_a);
        let super_key = SortKey::super_key(&key_a_2, &key_e).unwrap();
        assert_eq!(super_key, key_a_2);

        // super key of (a) and (a) is (a)
        let super_key = SortKey::super_key(&key_a, &key_a).unwrap();
        assert_eq!(super_key, key_a);
        let super_key = SortKey::super_key(&key_a_2, &key_a_2).unwrap();
        assert_eq!(super_key, key_a_2);

        // super key of (a) and (b) is None
        let super_key = SortKey::super_key(&key_a, &key_b);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_a, &key_b_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_a_2, &key_b);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_a_2, &key_b_2);
        assert_eq!(super_key, None);

        // (a,b)
        let mut key_ab = SortKey::with_capacity(2);
        key_ab.push("a", Default::default());
        key_ab.push("b", Default::default());
        let mut key_ab_2 = SortKey::with_capacity(2);
        key_ab_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_ab_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        // (b,a)
        let mut key_ba = SortKey::with_capacity(2);
        key_ba.push("b", Default::default());
        key_ba.push("a", Default::default());
        let mut key_ba_2 = SortKey::with_capacity(2);
        key_ba_2.push(
            "b",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_ba_2.push(
            "a",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        // super key of (a, b) and empty is (a, b)
        let super_key = SortKey::super_key(&key_ab, &key_e).unwrap();
        assert_eq!(super_key, key_ab);
        let super_key = SortKey::super_key(&key_ab_2, &key_e).unwrap();
        assert_eq!(super_key, key_ab_2);

        // super key of (a, b) and (a) is (a, b)
        let super_key = SortKey::super_key(&key_ab, &key_a).unwrap();
        assert_eq!(super_key, key_ab);
        let super_key = SortKey::super_key(&key_ab_2, &key_a_2).unwrap();
        assert_eq!(super_key, key_ab_2);
        // super key of (a, b) and (a') is None
        let super_key = SortKey::super_key(&key_ab, &key_a_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab_2, &key_a);
        assert_eq!(super_key, None);

        // super key of (a, b) and (b) is (a, b)
        let super_key = SortKey::super_key(&key_ab, &key_b).unwrap();
        assert_eq!(super_key, key_ab);
        let super_key = SortKey::super_key(&key_ab_2, &key_b_2).unwrap();
        assert_eq!(super_key, key_ab_2);
        // super key of (a, b) and (b') is None
        let super_key = SortKey::super_key(&key_ab, &key_b_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab_2, &key_b);
        assert_eq!(super_key, None);

        // super key of (a, b) and (a, b) is (a, b)
        let super_key = SortKey::super_key(&key_ab, &key_ab).unwrap();
        assert_eq!(super_key, key_ab);
        let super_key = SortKey::super_key(&key_ab_2, &key_ab_2).unwrap();
        assert_eq!(super_key, key_ab_2);
        // super key of (a, b) and (a',b') is None
        let super_key = SortKey::super_key(&key_ab, &key_ab_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab_2, &key_ab);
        assert_eq!(super_key, None);
        // super key of (a, b) and (b,a) is None
        let super_key = SortKey::super_key(&key_ab, &key_ba);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab, &key_ba_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab_2, &key_ba);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_ab_2, &key_ba_2);
        assert_eq!(super_key, None);

        // (a, b, c)
        let mut key_abc_2 = SortKey::with_capacity(3);
        key_abc_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_abc_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        key_abc_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        //  (c)
        let mut key_c_2 = SortKey::with_capacity(1);
        key_c_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        // (a, c)
        let mut key_ac_2 = SortKey::with_capacity(2);
        key_ac_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_ac_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        // (b,c)
        let mut key_bc_2 = SortKey::with_capacity(2);
        key_bc_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        key_bc_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        // (c, a)
        let mut key_ca_2 = SortKey::with_capacity(2);
        key_ca_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key_ca_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        // (c, b)
        let mut key_cb_2 = SortKey::with_capacity(2);
        key_cb_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key_cb_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        // (b,a,c)
        let mut key_bac_2 = SortKey::with_capacity(3);
        key_bac_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        key_bac_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_bac_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        // (b,c,a)
        let mut key_bca_2 = SortKey::with_capacity(3);
        key_bca_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        key_bca_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key_bca_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        // (c,a,b)
        let mut key_cab_2 = SortKey::with_capacity(3);
        key_cab_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key_cab_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );
        key_cab_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        // (c,b,a)
        let mut key_cba_2 = SortKey::with_capacity(3);
        key_cba_2.push(
            "c",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        key_cba_2.push(
            "b",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );
        key_cba_2.push(
            "a",
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        );

        // super key of (a, b, c) and any of { (a, b), (a, c), (b, c), (a), (b), (c) and empty } is (a, b, c)
        let super_key = SortKey::super_key(&key_abc_2, &key_e).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_a_2).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_b_2).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_c_2).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_ab_2).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_ac_2).unwrap();
        assert_eq!(super_key, key_abc_2);
        let super_key = SortKey::super_key(&key_abc_2, &key_bc_2).unwrap();
        assert_eq!(super_key, key_abc_2);

        // super key of (a, b, c) and any of { b, a), (c, a), (c, b), (b, a, c), (b, c, a), (c, a, b), (c, b, a) } is None
        let super_key = SortKey::super_key(&key_abc_2, &key_ba_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_ba);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_ca_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_cb_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_bac_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_bca_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_cab_2);
        assert_eq!(super_key, None);
        let super_key = SortKey::super_key(&key_abc_2, &key_cba_2);
        assert_eq!(super_key, None);
    }
}
