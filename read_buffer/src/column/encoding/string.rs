pub mod dictionary;
pub mod rle;

use std::collections::BTreeSet;

use either::Either;

// This makes the encoding types available under the dictionary module.
pub use dictionary::Dictionary;
pub use rle::RLE;

use crate::column::{cmp, RowIDs};

/// The encoded id for a NULL value.
pub const NULL_ID: u32 = 0;

#[allow(clippy::upper_case_acronyms)] // this looks weird as `Rle`
pub enum Encoding {
    RLE(RLE),
    Plain(Dictionary),
}

impl Encoding {
    pub fn debug_name(&self) -> &'static str {
        match &self {
            Self::RLE(_) => "RLE encoder",
            Self::Plain(_) => "plain encoder",
        }
    }

    pub fn size(&self, buffers: bool) -> usize {
        match &self {
            Self::RLE(enc) => enc.size(buffers),
            Self::Plain(enc) => enc.size(buffers),
        }
    }

    pub fn num_rows(&self) -> u32 {
        match &self {
            Self::RLE(enc) => enc.num_rows(),
            Self::Plain(enc) => enc.num_rows(),
        }
    }

    pub fn push(&mut self, v: String) {
        match self {
            Self::RLE(ref mut enc) => enc.push(v),
            Self::Plain(ref mut enc) => enc.push(v),
        }
    }

    pub fn push_none(&mut self) {
        match self {
            Self::RLE(ref mut enc) => enc.push_none(),
            Self::Plain(ref mut enc) => enc.push_none(),
        }
    }

    /// Adds additional repetitions of the provided value to the encoded data.
    /// It is the caller's responsibility to ensure that the dictionary encoded
    /// remains sorted.
    pub fn push_additional(&mut self, v: Option<String>, additional: u32) {
        match self {
            Self::RLE(ref mut env) => env.push_additional(v, additional),
            Self::Plain(ref mut env) => env.push_additional(v, additional),
        }
    }

    /// Determine if NULL is encoded in the column.
    fn contains_null(&self) -> bool {
        match self {
            Self::RLE(enc) => enc.contains_null(),
            Self::Plain(enc) => enc.contains_null(),
        }
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Populates the provided destination container with the row ids satisfying
    /// the provided predicate.
    pub fn row_ids_filter(&self, value: &str, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        match self {
            Self::RLE(enc) => enc.row_ids_filter(value, op, dst),
            Self::Plain(enc) => enc.row_ids_filter(value, op, dst),
        }
    }

    /// Populates the provided destination container with the row ids for rows
    /// that null.
    fn row_ids_null(&self, dst: RowIDs) -> RowIDs {
        match self {
            Self::RLE(enc) => enc.row_ids_null(dst),
            Self::Plain(enc) => enc.row_ids_null(dst),
        }
    }

    /// Populates the provided destination container with the row ids for rows
    /// that are not null.
    fn row_ids_not_null(&self, dst: RowIDs) -> RowIDs {
        match self {
            Self::RLE(enc) => enc.row_ids_not_null(dst),
            Self::Plain(enc) => enc.row_ids_not_null(dst),
        }
    }

    // The set of row ids for each distinct value in the column.
    fn group_row_ids(&self) -> Either<Vec<&RowIDs>, Vec<RowIDs>> {
        match self {
            Self::RLE(enc) => Either::Left(enc.group_row_ids()),
            Self::Plain(enc) => Either::Right(enc.group_row_ids()),
        }
    }

    //
    //
    // ---- Methods for getting materialised values.
    //
    //

    pub fn dictionary(&self) -> Vec<&String> {
        match self {
            Self::RLE(enc) => enc.dictionary(),
            Self::Plain(enc) => enc.dictionary(),
        }
    }

    /// Returns the logical value present at the provided row id.
    ///
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    fn value(&self, row_id: u32) -> Option<&String> {
        match self {
            Self::RLE(enc) => enc.value(row_id),
            Self::Plain(enc) => enc.value(row_id),
        }
    }

    /// Materialises the decoded value belonging to the provided encoded id.
    ///
    /// Panics if there is no decoded value for the provided id
    fn decode_id(&self, encoded_id: u32) -> Option<&str> {
        match self {
            Self::RLE(enc) => enc.decode_id(encoded_id),
            Self::Plain(enc) => enc.decode_id(encoded_id),
        }
    }

    /// Materialises a vector of references to the decoded values in the
    /// provided row ids.
    ///
    /// NULL values are represented by None. It is the caller's responsibility
    /// to ensure row ids are a monotonically increasing set.
    pub fn values<'a>(
        &'a self,
        row_ids: &[u32],
        dst: Vec<Option<&'a str>>,
    ) -> Vec<Option<&'a str>> {
        match self {
            Self::RLE(enc) => enc.values(row_ids, dst),
            Self::Plain(enc) => enc.values(row_ids, dst),
        }
    }

    /// Returns the lexicographical minimum value for the provided set of row
    /// ids. NULL values are not considered the minimum value if any non-null
    /// value exists at any of the provided row ids.
    fn min<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        match self {
            Self::RLE(enc) => enc.min(row_ids),
            Self::Plain(enc) => enc.min(row_ids),
        }
    }

    /// Returns the lexicographical maximum value for the provided set of row
    /// ids. NULL values are not considered the maximum value if any non-null
    /// value exists at any of the provided row ids.
    fn max<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        match self {
            Self::RLE(enc) => enc.max(row_ids),
            Self::Plain(enc) => enc.max(row_ids),
        }
    }

    /// Returns the lexicographical minimum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_min(&self) -> Option<&'_ String> {
        match self {
            Self::RLE(enc) => enc.column_min(),
            Self::Plain(enc) => enc.column_min(),
        }
    }

    /// Returns the lexicographical maximum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_max(&self) -> Option<&'_ String> {
        match self {
            Self::RLE(enc) => enc.column_max(),
            Self::Plain(enc) => enc.column_max(),
        }
    }

    /// Returns the total number of non-null values found at the provided set of
    /// row ids.
    fn count(&self, row_ids: &[u32]) -> u32 {
        match self {
            Self::RLE(enc) => enc.count(row_ids),
            Self::Plain(enc) => enc.count(row_ids),
        }
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    fn all_values<'a>(&'a mut self, dst: Vec<Option<&'a str>>) -> Vec<Option<&'a str>> {
        match self {
            Self::RLE(enc) => enc.all_values(dst),
            Self::Plain(enc) => enc.all_values(dst),
        }
    }

    /// Returns references to the unique set of values encoded at each of the
    /// provided ids.
    ///
    /// It is the caller's responsibility to ensure row ids are a monotonically
    /// increasing set.
    fn distinct_values<'a>(
        &'a self,
        row_ids: impl Iterator<Item = u32>,
        dst: BTreeSet<Option<&'a str>>,
    ) -> BTreeSet<Option<&'a str>> {
        match self {
            Self::RLE(enc) => enc.distinct_values(row_ids, dst),
            Self::Plain(enc) => enc.distinct_values(row_ids, dst),
        }
    }

    //
    //
    // ---- Methods for getting encoded values directly, typically to be used
    //      as part of group keys.
    //
    //

    /// Return the raw encoded values for the provided logical row ids.
    /// Encoded values for NULL values are included.
    fn encoded_values(&self, row_ids: &[u32], dst: Vec<u32>) -> Vec<u32> {
        match self {
            Self::RLE(enc) => enc.encoded_values(row_ids, dst),
            Self::Plain(enc) => enc.encoded_values(row_ids, dst),
        }
    }

    /// Returns all encoded values for the column including the encoded value
    /// for any NULL values.
    fn all_encoded_values(&self, dst: Vec<u32>) -> Vec<u32> {
        match self {
            Self::RLE(enc) => enc.all_encoded_values(dst),
            Self::Plain(enc) => enc.all_encoded_values(dst),
        }
    }

    //
    //
    // ---- Methods for optimising schema exploration.
    //
    //

    /// Efficiently determines if this column contains non-null values that
    /// differ from the provided set of values.
    ///
    /// Informally, this method provides an efficient way of answering "is it
    /// worth spending time reading this column for distinct values that are not
    /// present in the provided set?".
    ///
    /// More formally, this method returns the relative complement of this
    /// column's dictionary in the provided set of values.
    fn has_other_non_null_values(&self, values: &BTreeSet<String>) -> bool {
        match self {
            Self::RLE(enc) => enc.has_other_non_null_values(values),
            Self::Plain(enc) => enc.has_other_non_null_values(values),
        }
    }

    /// Determines if the column contains at least one non-null value at
    /// any of the provided row ids.
    ///
    /// It is the caller's responsibility to ensure row ids are a monotonically
    /// increasing set.
    fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::RLE(enc) => enc.has_non_null_value(row_ids),
            Self::Plain(enc) => enc.has_non_null_value(row_ids),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::column::{cmp, RowIDs};

    use super::*;

    #[test]
    fn push() {
        let encodings = vec![
            Encoding::RLE(RLE::from(vec!["hello", "hello", "hello", "hello"])),
            Encoding::Plain(Dictionary::from(vec!["hello", "hello", "hello", "hello"])),
        ];

        for enc in encodings {
            _push(enc);
        }
    }

    fn _push(mut enc: Encoding) {
        enc.push_additional(Some("hello".to_string()), 1);
        enc.push_additional(None, 3);
        enc.push("world".to_string());

        let name = enc.debug_name();
        assert_eq!(
            enc.all_values(vec![]),
            [
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello"),
                None,
                None,
                None,
                Some("world"),
            ],
            "{}",
            name
        );

        enc.push_additional(Some("zoo".to_string()), 3);
        enc.push_none();
        assert_eq!(
            enc.all_values(vec![]),
            [
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("hello"),
                None,
                None,
                None,
                Some("world"),
                Some("zoo"),
                Some("zoo"),
                Some("zoo"),
                None,
            ],
            "{}",
            name
        );
    }

    // tests a defect I discovered.
    #[test]
    fn push_additional_first_run_length() {
        let dictionary = vec!["world".to_string(), "hello".to_string()]
            .into_iter()
            .collect::<BTreeSet<String>>();

        let encodings = vec![
            Encoding::RLE(RLE::with_dictionary(dictionary.clone())),
            Encoding::Plain(Dictionary::with_dictionary(dictionary)),
        ];

        for enc in encodings {
            _push_additional_first_run_length(enc);
        }
    }

    fn _push_additional_first_run_length(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("world".to_string()), 1);
        enc.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            enc.all_values(vec![]),
            vec![Some("world"), Some("hello")],
            "{}",
            name
        );
        assert_eq!(enc.all_encoded_values(vec![]), vec![2, 1], "{}", name);

        enc = Encoding::RLE(RLE::default());
        enc.push_additional(Some("hello".to_string()), 1);
        enc.push_additional(Some("world".to_string()), 1);

        assert_eq!(
            enc.all_values(vec![]),
            vec![Some("hello"), Some("world")],
            "{}",
            name
        );
        assert_eq!(enc.all_encoded_values(vec![]), vec![1, 2], "{}", name);
    }

    #[test]
    fn row_ids_filter_equal() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _row_ids_filter_equal(enc);
        }
    }

    fn _row_ids_filter_equal(mut enc: Encoding) {
        let name = enc.debug_name();
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_none(); // 9
        enc.push_additional(Some("south".to_string()), 2); // 10, 11

        let ids = enc.row_ids_filter("east", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("south", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![10, 11]), "{}", name);

        let ids = enc.row_ids_filter("foo", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        // != some value not in the column should exclude the NULL value.
        let ids = enc.row_ids_filter("foo", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("east", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 10, 11]), "{}", name);
    }

    #[test]
    fn row_ids_filter_equal_no_null() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _row_ids_filter_equal_no_null(enc);
        }
    }

    fn _row_ids_filter_equal_no_null(mut enc: Encoding) {
        let name = enc.debug_name();
        enc.push_additional(Some("east".to_string()), 2);
        enc.push_additional(Some("west".to_string()), 1);

        let ids = enc.row_ids_filter("abba", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2]), "{}", name);
    }

    #[test]
    fn row_ids_filter_cmp() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _row_ids_filter_cmp(enc);
        }
    }

    fn _row_ids_filter_cmp(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_additional(Some("west".to_string()), 1); // 11
        enc.push_additional(Some("north".to_string()), 1); // 12
        enc.push_none(); // 13
        enc.push_additional(Some("west".to_string()), 5); // 14, 15, 16, 17, 18

        let ids = enc.row_ids_filter("east", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("east", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        let ids = enc.row_ids_filter("abc", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);
        let ids = enc.row_ids_filter("abc", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        let ids = enc.row_ids_filter("zoo", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        let ids = enc.row_ids_filter("zoo", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        let ids = enc.row_ids_filter("west", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty(), "{}", name);

        let ids = enc.row_ids_filter("north", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![9, 10, 11, 14, 15, 16, 17, 18]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("north", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18]),
            "{}",
            name
        );

        // The encoding also supports comparisons on values that don't directly exist in
        // the column.
        let ids = enc.row_ids_filter("abba", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("east1", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("east1", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("east1", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("region", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 12]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("region", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 12]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("zoo", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ]),
            "{}",
            name
        );

        let ids = enc.row_ids_filter("zoo", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ]),
            "{}",
            name
        );
    }

    #[test]
    fn row_ids_filter_cmp_single() {
        let mut encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings.iter_mut() {
            let name = enc.debug_name();
            enc.push("east".to_string());

            let ids = enc.row_ids_filter("east", &cmp::Operator::GT, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("north", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("east", &cmp::Operator::LT, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::GT, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);

            let ids = enc.row_ids_filter("east", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);

            let ids = enc.row_ids_filter("east", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);

            let ids = enc.row_ids_filter("west", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);

            let ids = enc.row_ids_filter("west", &cmp::Operator::LT, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0]), "{}", name);
        }
    }

    #[test]
    fn row_ids_filter_cmp_with_null() {
        let dictionary = vec!["east".to_string(), "south".to_string(), "west".to_string()]
            .into_iter()
            .collect::<BTreeSet<String>>();

        let mut encodings = vec![
            Encoding::RLE(RLE::with_dictionary(dictionary.clone())),
            Encoding::Plain(Dictionary::with_dictionary(dictionary)),
        ];

        for enc in encodings.iter_mut() {
            let name = enc.debug_name();
            enc.push("south".to_string());
            enc.push("west".to_string());
            enc.push_none();
            enc.push("east".to_string());

            let ids = enc.row_ids_filter("west", &cmp::Operator::GT, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("zoo", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("east", &cmp::Operator::LT, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert!(ids.is_empty(), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::GT, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0, 1, 3]), "{}", name);

            let ids = enc.row_ids_filter("abc", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0, 1, 3]), "{}", name);

            let ids = enc.row_ids_filter("west", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![1]), "{}", name);

            let ids = enc.row_ids_filter("east", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![3]), "{}", name);

            let ids = enc.row_ids_filter("west", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0, 1, 3]), "{}", name);

            let ids = enc.row_ids_filter("west", &cmp::Operator::LT, RowIDs::Vector(vec![]));
            assert_eq!(ids, RowIDs::Vector(vec![0, 3]), "{}", name);
        }
    }

    #[test]
    fn row_ids_null() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _row_ids_null(enc);
        }
    }

    fn _row_ids_null(mut enc: Encoding) {
        let name = enc.debug_name();
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(None, 3); // 3, 4, 5
        enc.push_additional(Some("north".to_string()), 1); // 6
        enc.push_additional(None, 2); // 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10

        // essentially `WHERE value IS NULL`
        let ids = enc.row_ids_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 4, 5, 7, 8]), "{}", name);

        // essentially `WHERE value IS NOT NULL`
        let ids = enc.row_ids_not_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 6, 9, 10]), "{}", name);
    }

    #[test]
    fn group_row_ids() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _group_row_ids(enc);
        }
    }

    fn _group_row_ids(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 4); // 0, 1, 2, 3
        enc.push_additional(Some("west".to_string()), 2); // 4, 5
        enc.push_none(); // 6
        enc.push_additional(Some("zoo".to_string()), 1); // 7

        match enc {
            Encoding::RLE(_) => {
                assert_eq!(
                    enc.group_row_ids().unwrap_left(),
                    vec![
                        &RowIDs::bitmap_from_slice(&[6]),
                        &RowIDs::bitmap_from_slice(&[0, 1, 2, 3]),
                        &RowIDs::bitmap_from_slice(&[4, 5]),
                        &RowIDs::bitmap_from_slice(&[7]),
                    ],
                    "{}",
                    name
                );
            }
            Encoding::Plain(_) => {
                assert_eq!(
                    enc.group_row_ids().unwrap_right(),
                    vec![
                        RowIDs::bitmap_from_slice(&[6]),
                        RowIDs::bitmap_from_slice(&[0, 1, 2, 3]),
                        RowIDs::bitmap_from_slice(&[4, 5]),
                        RowIDs::bitmap_from_slice(&[7]),
                    ],
                    "{}",
                    name
                );
            }
        }
    }

    #[test]
    fn dictionary() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _dictionary(enc);
        }
    }

    fn _dictionary(mut enc: Encoding) {
        let name = enc.debug_name();
        assert!(enc.dictionary().is_empty());

        enc.push_additional(Some("east".to_string()), 23);
        enc.push_additional(Some("west".to_string()), 2);
        enc.push_none();
        enc.push_additional(Some("zoo".to_string()), 1);

        assert_eq!(enc.dictionary(), vec!["east", "west", "zoo"], "{}", name);
    }

    #[test]
    fn value() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _value(enc);
        }
    }

    fn _value(mut enc: Encoding) {
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_none(); // 11

        assert_eq!(enc.value(3), Some(&"north".to_string()));
        assert_eq!(enc.value(0), Some(&"east".to_string()));
        assert_eq!(enc.value(10), Some(&"south".to_string()));
        assert_eq!(enc.value(11), None);
    }

    #[test]
    #[should_panic]
    fn value_bounds() {
        let mut enc = RLE::default();
        enc.push("b".to_string());
        enc.value(100);

        let mut enc = Dictionary::default();
        enc.push("b".to_string());
        enc.value(100);
    }

    #[test]
    fn values() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _values(enc);
        }
    }

    fn _values(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_none(); // 11

        let mut dst = Vec::with_capacity(1000);
        dst = enc.values(&[0, 1, 3, 4], dst);
        assert_eq!(
            dst,
            vec![Some("east"), Some("east"), Some("north"), Some("east"),],
            "{}",
            name
        );

        dst = enc.values(&[8, 10, 11], dst);
        assert_eq!(dst, vec![Some("east"), Some("south"), None], "{}", name);

        assert_eq!(dst.capacity(), 1000, "{}", name);
    }

    #[test]
    fn all_values() {
        let encodings = vec![
            Encoding::RLE(RLE::from(vec!["hello", "zoo"])),
            Encoding::Plain(Dictionary::from(vec!["hello", "zoo"])),
        ];

        for enc in encodings {
            _all_values(enc);
        }
    }

    fn _all_values(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_none();

        let zoo = Some("zoo");
        let dst = vec![zoo; 4];
        let got = enc.all_values(dst);

        assert_eq!(got, [Some("hello"), zoo, None], "{}", name);
        assert_eq!(got.capacity(), 4, "{}", name);
    }

    #[test]
    fn encoded_values() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _encoded_values(enc);
        }
    }

    fn _encoded_values(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_none(); // 11

        let mut encoded = enc.encoded_values(&[0], vec![]);
        assert_eq!(encoded, vec![1], "{}", name);

        encoded = enc.encoded_values(&[1, 3, 5, 6], vec![]);
        assert_eq!(encoded, vec![1, 2, 1, 1], "{}", name);

        encoded = enc.encoded_values(&[9, 10, 11], vec![]);
        assert_eq!(encoded, vec![3, 3, 0], "{}", name);
    }

    #[test]
    fn all_encoded_values() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _all_encoded_values(enc);
        }
    }

    fn _all_encoded_values(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 3);
        enc.push_additional(None, 2);
        enc.push_additional(Some("north".to_string()), 2);

        let dst = Vec::with_capacity(100);
        let dst = enc.all_encoded_values(dst);
        assert_eq!(dst, vec![1, 1, 1, 0, 0, 2, 2], "{}", name);
        assert_eq!(dst.capacity(), 100, "{}", name);
    }

    #[test]
    fn min_max() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _min_max(enc);
        }
    }

    fn _min_max(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_none();
        enc.push_none();

        assert_eq!(enc.column_min(), None, "{}", name);
        assert_eq!(enc.column_max(), None, "{}", name);

        enc.push("Disintegration".to_owned());
        assert_eq!(
            enc.column_min(),
            Some(&"Disintegration".to_owned()),
            "{}",
            name
        );
        assert_eq!(
            enc.column_max(),
            Some(&"Disintegration".to_owned()),
            "{}",
            name
        );

        enc.push("Homesick".to_owned());
        assert_eq!(
            enc.column_min(),
            Some(&"Disintegration".to_owned()),
            "{}",
            name
        );
        assert_eq!(enc.column_max(), Some(&"Homesick".to_owned()), "{}", name);
    }

    #[test]
    fn distinct_values() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _distinct_values(enc);
        }
    }

    fn _distinct_values(mut enc: Encoding) {
        let name = enc.debug_name();

        enc.push_additional(Some("east".to_string()), 3);

        let values = enc.distinct_values((0..3).collect::<Vec<_>>().into_iter(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some("east")].into_iter().collect::<BTreeSet<_>>(),
            "{}",
            name,
        );

        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_none(); // 11

        let values = enc.distinct_values((0..12).collect::<Vec<_>>().into_iter(), BTreeSet::new());
        assert_eq!(
            values,
            vec![None, Some("east"), Some("north"), Some("south"),]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "{}",
            name,
        );

        let values = enc.distinct_values((0..4).collect::<Vec<_>>().into_iter(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some("east"), Some("north"),]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "{}",
            name,
        );

        let values = enc.distinct_values(vec![3, 10].into_iter(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some("north"), Some("south"),]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "{}",
            name,
        );
    }

    #[test]
    fn has_other_non_null_values() {
        let encodings = vec![
            Encoding::RLE(RLE::default()),
            Encoding::Plain(Dictionary::default()),
        ];

        for enc in encodings {
            _has_other_non_null_values(enc);
        }
    }

    fn _has_other_non_null_values(mut enc: Encoding) {
        enc.push_additional(Some("east".to_owned()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_owned()), 1); // 3
        enc.push_additional(Some("east".to_owned()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_owned()), 2); // 9, 10
        enc.push_none(); // 11

        let mut others = BTreeSet::new();
        others.insert("east".to_owned());
        others.insert("north".to_owned());

        assert!(enc.has_other_non_null_values(&others));

        others.insert("foo".to_owned());
        assert!(enc.has_other_non_null_values(&others));

        others.insert("south".to_owned());
        assert!(!enc.has_other_non_null_values(&others));

        others.insert("bar".to_owned());
        assert!(!enc.has_other_non_null_values(&others));

        assert!(enc.has_other_non_null_values(&BTreeSet::new()));
    }

    #[test]
    fn has_non_null_value() {
        let mut enc = Encoding::RLE(RLE::default());
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(Some("north".to_string()), 1); // 3
        enc.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        enc.push_additional(Some("south".to_string()), 2); // 9, 10
        enc.push_none(); // 11

        assert!(enc.has_non_null_value(&[0]));
        assert!(enc.has_non_null_value(&[0, 1, 2]));
        assert!(enc.has_non_null_value(&[10]));

        assert!(!enc.has_non_null_value(&[11]));
        assert!(!enc.has_non_null_value(&[11, 12, 100]));

        // Pure NULL column...
        enc = Encoding::RLE(RLE::default());
        enc.push_additional(None, 10);
        assert!(!enc.has_non_null_value(&[0]));
        assert!(!enc.has_non_null_value(&[4, 7]));
    }

    #[test]
    fn min() {
        let mut enc = Encoding::RLE(RLE::default());
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(None, 2); // 3, 4
        enc.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(enc.min(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(enc.min(&[0, 1, 2, 3, 4, 5, 6]), Some(&"east".to_string()));
        assert_eq!(enc.min(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(enc.min(&[3]), None);
        assert_eq!(enc.min(&[3, 4]), None);

        let mut drle = Encoding::RLE(RLE::default());
        drle.push_additional(None, 10);
        assert_eq!(drle.min(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn max() {
        let mut enc = Encoding::RLE(RLE::default());
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(None, 2); // 3, 4
        enc.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(enc.max(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(enc.max(&[0, 1, 2, 3, 4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(enc.max(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(enc.max(&[3]), None);
        assert_eq!(enc.max(&[3, 4]), None);

        let drle = Encoding::RLE(RLE::default());
        assert_eq!(drle.max(&[0]), None);

        let mut drle = Encoding::RLE(RLE::default());
        drle.push_additional(None, 10);
        assert_eq!(drle.max(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn count() {
        let mut enc = Encoding::RLE(RLE::default());
        enc.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        enc.push_additional(None, 2); // 3, 4
        enc.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(enc.count(&[0, 1, 2]), 3);
        assert_eq!(enc.count(&[0, 1, 2, 3, 4, 5, 6]), 5);
        assert_eq!(enc.count(&[4, 5, 6]), 2);
        assert_eq!(enc.count(&[3]), 0);
        assert_eq!(enc.count(&[3, 4]), 0);

        let mut drle = Encoding::RLE(RLE::default());
        drle.push_additional(None, 10);
        assert_eq!(drle.count(&[2, 3, 6, 8]), 0);
    }
}
