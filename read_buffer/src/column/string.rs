use std::collections::BTreeSet;

use arrow::{self, array::Array};
use either::Either;

use super::cmp;
use super::encoding::string::{dictionary, rle};
use super::encoding::string::{Dictionary, Encoding, NULL_ID, RLE};
use crate::column::{RowIDs, Statistics, Value, Values};

// Edd's totally made up magic constant. This determines whether we would use
// a run-length encoded dictionary encoding or just a plain dictionary encoding.
// I have ideas about how to build heuristics to do this in a much better way
// than an arbitrary constant but for now it's this...
//
// FWIW it's not the cardinality of the column that should drive the decision
// it's how many run-lengths would be produced in an RLE column and whether that
// compression is worth the memory and compute costs to work on it.
pub const TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT: usize = 100_000;

pub enum StringEncoding {
    RleDictionary(RLE),
    Dictionary(Dictionary),
    // TODO - simple array encoding, e.g., via Arrow String array.
}

/// This implementation is concerned with how to produce string columns with
/// different encodings.
impl StringEncoding {
    /// The estimated total size in bytes of the in-memory columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::RleDictionary(enc) => enc.size(),
            Self::Dictionary(enc) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying string values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as a pointer if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::RleDictionary(enc) => enc.size_raw(include_nulls),
            Self::Dictionary(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::RleDictionary(enc) => enc.num_rows(),
            Self::Dictionary(enc) => enc.num_rows(),
        }
    }

    /// The lexicographical min and max values in the column.
    pub fn column_range(&self) -> Option<(String, String)> {
        match self {
            Self::RleDictionary(enc) => match (enc.column_min(), enc.column_max()) {
                (None, None) => None,
                (Some(min), Some(max)) => Some((min.to_owned(), max.to_owned())),
                (min, max) => panic!("invalid column range: ({:?}, {:?})", min, max),
            },
            Self::Dictionary(enc) => match (enc.column_min(), enc.column_max()) {
                (None, None) => None,
                (Some(min), Some(max)) => Some((min.to_owned(), max.to_owned())),
                (min, max) => panic!("invalid column range: ({:?}, {:?})", min, max),
            },
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: match self {
                Self::RleDictionary(_) => rle::ENCODING_NAME.to_string(),
                Self::Dictionary(_) => dictionary::ENCODING_NAME.to_string(),
            },
            log_data_type: "string",
            values: self.num_rows(),
            nulls: self.null_count(),
            bytes: self.size(),
            raw_bytes: self.size_raw(true),
            raw_bytes_no_null: self.size_raw(false),
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            Self::RleDictionary(enc) => enc.contains_null(),
            Self::Dictionary(enc) => enc.contains_null(),
        }
    }

    /// Returns the number of null values in the column.
    ///
    /// TODO(edd): store this on encodings and then it's O(1) and `contain_null`
    /// can be replaced.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::RleDictionary(enc) => enc.null_count(),
            Self::Dictionary(enc) => enc.null_count(),
        }
    }

    /// Returns true if encoding can return row ID sets for logical values.
    pub fn has_pre_computed_row_id_sets(&self) -> bool {
        match &self {
            Self::RleDictionary(_) => true,
            Self::Dictionary(_) => false,
        }
    }

    /// Determines if the column contains a non-null value
    pub fn has_any_non_null_value(&self) -> bool {
        match &self {
            Self::RleDictionary(c) => c.has_any_non_null_value(),
            Self::Dictionary(c) => c.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match &self {
            Self::RleDictionary(c) => c.has_non_null_value(row_ids),
            Self::Dictionary(c) => c.has_non_null_value(row_ids),
        }
    }

    /// Determines if the column contains any values other than those provided.
    /// Short-circuits execution as soon as it finds a value not in `values`.
    pub fn has_other_non_null_values(&self, values: &BTreeSet<String>) -> bool {
        match &self {
            Self::RleDictionary(c) => c.has_other_non_null_values(values),
            Self::Dictionary(c) => c.has_other_non_null_values(values),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::RleDictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// All values present at the provided logical row IDs.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::RleDictionary(c) => Values::String(c.values(row_ids, vec![])),
            Self::Dictionary(c) => Values::String(c.values(row_ids, vec![])),
        }
    }

    /// Returns all values present at the provided logical row IDs as a
    /// dictionary encoded `Values` format.
    pub fn values_as_dictionary(&self, row_ids: &[u32]) -> Values<'_> {
        //
        // Example:
        //
        // Suppose you have column encoded like this:
        //
        // values: NULL, "alpha", "beta", "gamma"
        // encoded: 1, 1, 2, 0, 3 (alpha, alpha, beta, NULL, gamma)
        //
        // And only the rows: {0, 1, 3, 4} are required.
        //
        // The column encoding will return the following encoded values
        //
        // encoded: 1, 1, 0, 3 (alpha, alpha, NULL, gamma)
        //
        // Because the dictionary has likely changed, the encoded values need
        // to be transformed into a new domain `[0, encoded.len())` so that they
        // become:
        //
        // keys: [1, 1, 0, 2]
        // values: [None, Some("alpha"), Some("gamma")]
        let mut keys = self.encoded_values(row_ids, vec![]);

        // build a mapping from encoded value to new ordinal position.
        let mut ordinal_mapping = hashbrown::HashMap::new();
        for key in &keys {
            ordinal_mapping.insert(*key, u32::default()); // don't know final ordinal position yet
        }

        // create new ordinal offsets - the encoded values need to be shifted
        // into a new domain `[0, ordinal_mapping.len())` which is the length
        // of the new dictionary.
        let mut ordinal_mapping_keys = ordinal_mapping
            .keys()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        ordinal_mapping_keys.sort_unstable();

        for (i, key) in ordinal_mapping_keys.iter().enumerate() {
            // now we can insert the new ordinal position of the encoded in key
            // in the final values vector.
            ordinal_mapping.insert(*key, i as u32);
        }

        // Rewrite all the encoded values into the new domain.
        for key in keys.iter_mut() {
            *key = *ordinal_mapping.get(key).unwrap();
        }

        // now generate the values vector, which will contain the sorted set of
        // string values
        let mut values = match &self {
            Self::RleDictionary(c) => ordinal_mapping_keys
                .iter()
                .map(|id| c.decode_id(*id))
                .collect::<Vec<_>>(),
            Self::Dictionary(c) => ordinal_mapping_keys
                .iter()
                .map(|id| c.decode_id(*id))
                .collect::<Vec<_>>(),
        };
        values.sort_unstable();

        Values::Dictionary(keys, values)
    }

    /// All values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::RleDictionary(c) => Values::String(c.all_values(vec![])),
            Self::Dictionary(c) => Values::String(c.all_values(vec![])),
        }
    }

    /// Returns all values as a dictionary encoded `Values` format.
    pub fn all_values_as_dictionary(&self) -> Values<'_> {
        let mut keys = self.all_encoded_values(vec![]);

        let values = if self.contains_null() {
            // The column's ordered set of values including None because that is a
            // reserved encoded key (`0`).
            let mut values = vec![None];
            match &self {
                Self::RleDictionary(c) => {
                    values.extend(c.dictionary().into_iter().map(|s| Some(s.as_str())));
                }
                Self::Dictionary(c) => {
                    values.extend(c.dictionary().into_iter().map(|s| Some(s.as_str())));
                }
            };
            values
        } else {
            // since column doesn't contain null we need to shift all the encoded
            // values down
            assert_eq!(NULL_ID, 0);
            for key in keys.iter_mut() {
                *key -= 1;
            }

            match &self {
                Self::RleDictionary(c) => c
                    .dictionary()
                    .into_iter()
                    .map(|s| Some(s.as_str()))
                    .collect::<Vec<_>>(),
                Self::Dictionary(c) => c
                    .dictionary()
                    .into_iter()
                    .map(|s| Some(s.as_str()))
                    .collect::<Vec<_>>(),
            }
        };

        Values::Dictionary(keys, values)
    }

    /// Returns the logical value for the specified encoded representation.
    pub fn decode_id(&self, encoded_id: u32) -> Value<'_> {
        match &self {
            Self::RleDictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// Returns the distinct set of values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination sets.
    pub fn distinct_values(&self, row_ids: impl Iterator<Item = u32>) -> BTreeSet<Option<&'_ str>> {
        match &self {
            Self::RleDictionary(c) => c.distinct_values(row_ids, BTreeSet::new()),
            Self::Dictionary(c) => c.distinct_values(row_ids, BTreeSet::new()),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &str, dst: RowIDs) -> RowIDs {
        match &self {
            Self::RleDictionary(c) => c.row_ids_filter(value, op, dst),
            Self::Dictionary(c) => c.row_ids_filter(value, op, dst),
        }
    }

    /// The lexicographic minimum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RleDictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
        }
    }

    /// The lexicographic maximum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RleDictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
        }
    }

    /// The number of non-null values at the provided row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::RleDictionary(c) => c.count(row_ids),
            Self::Dictionary(c) => c.count(row_ids),
        }
    }

    /// Calculate all row ids for each distinct value in the column.
    pub fn group_row_ids(&self) -> Either<Vec<&RowIDs>, Vec<RowIDs>> {
        match self {
            Self::RleDictionary(enc) => Either::Left(enc.group_row_ids()),
            Self::Dictionary(enc) => Either::Right(enc.group_row_ids()),
        }
    }

    /// All encoded values for the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn encoded_values(&self, row_ids: &[u32], dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RleDictionary(c) => c.encoded_values(row_ids, dst),
            Self::Dictionary(c) => c.encoded_values(row_ids, dst),
        }
    }

    /// All encoded values for the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_encoded_values(&self, dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RleDictionary(c) => c.all_encoded_values(dst),
            Self::Dictionary(c) => c.all_encoded_values(dst),
        }
    }
}

impl std::fmt::Display for StringEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RleDictionary(data) => write!(f, "{}", data),
            Self::Dictionary(data) => write!(f, "{}", data),
        }
    }
}

/// Converts an Arrow `StringArray` into a `StringEncoding`.
///
/// Note: this currently runs through the array and builds the dictionary before
/// creating the encoding. There is room for performance improvement here but
/// ideally it's a "write once read many" scenario.
impl From<arrow::array::StringArray> for StringEncoding {
    fn from(arr: arrow::array::StringArray) -> Self {
        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                dictionary.insert(arr.value(i).to_owned());
            }
        }

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Dictionary::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = if !arr.is_null(0) {
            Some(arr.value(0))
        } else {
            None
        };

        let mut count = 1;
        for i in 1..arr.len() {
            let next = if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            };

            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        match data {
            Encoding::RLE(enc) => Self::RleDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

/// Converts an Arrow `StringDictionary` into a `StringEncoding`.
///
/// Note: This could be made more performant by constructing a mapping from
/// arrow dictionary key to RB dictionary key, instead of converting
/// to the string and back again
///
/// Note: Further to the above if the arrow dictionary is ordered we
/// could reuse its encoding directly
impl From<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>> for StringEncoding {
    fn from(arr: arrow::array::DictionaryArray<arrow::datatypes::Int32Type>) -> Self {
        let keys = arr.keys();
        let values = arr.values();
        let values = values
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let dictionary: BTreeSet<_> = values.iter().flatten().map(Into::into).collect();

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Dictionary::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = if !keys.is_null(0) {
            Some(keys.value(0))
        } else {
            None
        };

        let mut count = 1;
        for i in 1..keys.len() {
            let next = if keys.is_null(i) {
                None
            } else {
                Some(keys.value(i))
            };

            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(values.value(x as usize).to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(values.value(x as usize).to_string()), count),
            None => data.push_additional(None, count),
        };

        match data {
            Encoding::RLE(enc) => Self::RleDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

impl From<&[Option<&str>]> for StringEncoding {
    fn from(arr: &[Option<&str>]) -> Self {
        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for x in arr.iter().flatten() {
            dictionary.insert(x.to_string());
        }

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Dictionary::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = &arr[0];

        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        match data {
            Encoding::RLE(enc) => Self::RleDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

impl From<&[&str]> for StringEncoding {
    fn from(arr: &[&str]) -> Self {
        // build a sorted dictionary.
        let dictionary = arr.iter().map(|x| x.to_string()).collect::<BTreeSet<_>>();

        let mut data: Encoding = if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
            Encoding::Plain(Dictionary::with_dictionary(dictionary))
        } else {
            Encoding::RLE(RLE::with_dictionary(dictionary))
        };

        let mut prev = &arr[0];
        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            data.push_additional(Some(prev.to_string()), count);
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        data.push_additional(Some(prev.to_string()), count);

        match data {
            Encoding::RLE(enc) => Self::RleDictionary(enc),
            Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    // tests both `values_as_dictionary` and `all_values_as_dictionary`
    fn values_as_dictionary() {
        let set = vec!["apple", "beta", "orange", "pear"];
        let data = vec![
            Some("apple"),
            Some("apple"),
            Some("pear"),
            None,
            None,
            Some("orange"),
            Some("beta"),
        ];

        let mut rle = RLE::with_dictionary(
            set.iter()
                .cloned()
                .map(String::from)
                .collect::<BTreeSet<String>>(),
        );
        for v in data.iter().map(|x| x.map(String::from)) {
            rle.push_additional(v, 1);
        }

        let mut dict = Dictionary::with_dictionary(
            set.into_iter()
                .map(String::from)
                .collect::<BTreeSet<String>>(),
        );
        for v in data.iter().map(|x| x.map(String::from)) {
            dict.push_additional(v, 1);
        }

        let encodings = vec![
            StringEncoding::RleDictionary(rle),
            StringEncoding::Dictionary(dict),
        ];

        for enc in encodings {
            _values_as_dictionary(&enc);
            _all_values_as_dictionary(&enc);
        }

        // example without NULL values
        let data = vec![
            Some("apple"),
            Some("apple"),
            Some("beta"),
            Some("orange"),
            Some("pear"),
        ];

        let encodings = vec![
            StringEncoding::RleDictionary(RLE::from(data.clone())),
            StringEncoding::Dictionary(Dictionary::from(data)),
        ];

        for enc in encodings {
            let exp_keys = vec![0, 0, 1, 2, 3];
            let exp_values = vec![Some("apple"), Some("beta"), Some("orange"), Some("pear")];

            let values = enc.all_values_as_dictionary();
            if let Values::Dictionary(got_keys, got_values) = values {
                assert_eq!(got_keys, exp_keys, "key comparison for {} failed", enc);
                assert_eq!(
                    got_values, exp_values,
                    "values comparison for {} failed",
                    enc
                );
            } else {
                panic!("invalid Values format returned, got {:?}", values);
            }
        }
    }

    fn _values_as_dictionary(enc: &StringEncoding) {
        // column is: [apple, apple, pear, NULL, NULL, orange, beta]

        // Since the Read Buffer only accepts row IDs in order we only need to
        // cover ascending rows in these tests.
        let cases = vec![
            (
                &[0, 3, 4][..], // apple NULL, NULL
                (vec![1, 0, 0], vec![None, Some("apple")]),
            ),
            (
                &[6], // beta
                (vec![0], vec![Some("beta")]),
            ),
            (
                &[0, 3, 5][..], // apple NULL, orange
                (vec![1, 0, 2], vec![None, Some("apple"), Some("orange")]),
            ),
            (
                &[0, 1, 2, 3, 4, 5, 6], // apple, apple, pear, NULL, NULL, orange, beta
                (
                    vec![1, 1, 4, 0, 0, 3, 2],
                    vec![
                        None,
                        Some("apple"),
                        Some("beta"),
                        Some("orange"),
                        Some("pear"),
                    ],
                ),
            ),
        ];

        for (row_ids, (exp_keys, exp_values)) in cases {
            let values = enc.values_as_dictionary(row_ids);
            if let Values::Dictionary(got_keys, got_values) = values {
                assert_eq!(got_keys, exp_keys, "key comparison for {} failed", enc);
                assert_eq!(
                    got_values, exp_values,
                    "values comparison for {} failed",
                    enc
                );
            } else {
                panic!("invalid Values format returned, got {:?}", values);
            }
        }
    }

    fn _all_values_as_dictionary(enc: &StringEncoding) {
        // column is: [apple, apple, pear, NULL, NULL, orange, beta]

        let exp_keys = vec![1, 1, 4, 0, 0, 3, 2];
        let exp_values = vec![
            None,
            Some("apple"),
            Some("beta"),
            Some("orange"),
            Some("pear"),
        ];

        let values = enc.all_values_as_dictionary();
        if let Values::Dictionary(got_keys, got_values) = values {
            assert_eq!(got_keys, exp_keys, "key comparison for {} failed", enc);
            assert_eq!(
                got_values, exp_values,
                "values comparison for {} failed",
                enc
            );
        } else {
            panic!("invalid Values format returned, got {:?}", values);
        }
    }
}
