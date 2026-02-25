//! Implementations of pruning oracle for Server-side Bucketing.

use data_types::partition_template::{
    TablePartitionTemplateOverride, TemplatePart, bucket_for_tag_value,
};
use datafusion::scalar::ScalarValue;
use std::{collections::HashMap, sync::Arc};

/// A struct that is server-side bucketing aware, and can be used to prune
/// partitions based on the bucketing information.
///
/// This struct is used to prune partitions based on the bucketing information
/// that is stored in the partition template. It is used to determine if a
/// partition does not contain any data for a given set of tag values.
#[derive(Debug)]
pub struct BucketPartitionPruningOracle {
    /// A map of column names to the bucket info held for that column in this
    /// partition.
    tag_bucket_info: HashMap<Arc<str>, BucketInfo>,
}

impl BucketPartitionPruningOracle {
    /// An implementation must return an array that indicates, for each summary
    /// associated with this pruning oracle, if to its knowledge `column`
    /// contains ONLY the provided `values`.
    ///
    /// The implementation must adhere to the contract specified by [`PruningStatistics::contained()`]
    ///
    /// The returned array has one row for each summary, with the following meanings:
    ///
    /// - `true` if the values in `column` ONLY contain values from `values`
    /// - `false` if the values in `column` are NOT ANY of `values`
    /// - `null` if the neither of the above holds or is unknown.
    ///
    /// [`PruningStatistics::contained()`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/trait.PruningStatistics.html#tymethod.contained
    pub fn could_contain_values(
        &self,
        column: &datafusion::prelude::Column,
        values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<bool> {
        // If there are no values in the hash-set, the oracle should opt to not
        // prune.
        if values.is_empty() {
            return None;
        }

        // Grab the bucket information for the column, if any. If none exists
        // then this type can't provide any information.
        let column_bucket_info = self.tag_bucket_info.get(column.name.as_str())?;

        // Could the bucket contain any of the values?
        let may_contain_value = values.iter().any(|v| {
            // Is it a non-null string literal?
            match extract_tag_value(v).flatten() {
                Some(literal_value) => column_bucket_info.may_contain_value(literal_value),
                None => false,
            }
        });

        if may_contain_value {
            // Even though one of the `values` is contained in this bucket,
            // there can be other values not in `values` that are also contained
            // in this bucket. Therefore, we can't be sure that this bucket
            // contains ONLY the `values`, so return None
            None
        } else {
            // We are sure that the bucket ID for this column is NOT ANY
            // of the `values`, so return false
            Some(false)
        }
    }
}

/// Returns the underlying string for a tag column, if any, for the ScalarValue
///
/// * `Some(Some(str))` for non null string literals
/// * `Some(None)` for null string literals
/// * `None` for non-string literals
///
/// TODO: replace with `ScalarValue::try_as_str()` when it's available in the
/// Arrow crate.
///
/// See: <https://github.com/apache/datafusion/pull/14167>
fn extract_tag_value(scalar: &ScalarValue) -> Option<Option<&str>> {
    let v = match scalar {
        ScalarValue::Utf8(v) => v,
        ScalarValue::LargeUtf8(v) => v,
        ScalarValue::Utf8View(v) => v,
        ScalarValue::Dictionary(_index, v) => return extract_tag_value(v),
        _ => return None,
    };
    Some(v.as_ref().map(|v| v.as_str()))
}

/// A builder for constructing a [`BucketPartitionPruningOracle`].
#[derive(Default, Debug)]
pub struct BucketPartitionPruningOracleBuilder {
    /// A map of column names to the bucket info held for that column in this
    /// partition.
    tag_bucket_info: HashMap<Arc<str>, BucketInfo>,
}

impl BucketPartitionPruningOracleBuilder {
    /// Return `true` if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.tag_bucket_info.is_empty()
    }

    /// Insert a column name and its bucket info
    pub fn insert(&mut self, column: Arc<str>, bucket_info: BucketInfo) {
        self.tag_bucket_info.insert(column, bucket_info);
    }

    pub fn build(
        &mut self,
        table_partition_template: Option<&TablePartitionTemplateOverride>,
    ) -> BucketPartitionPruningOracle {
        // Insert missing entries if there are any. This step is necessary for
        // the cases when partition key is empty for a column, i.e. "!".
        if let Some(table_partition_template) = table_partition_template {
            for part in table_partition_template.parts() {
                if let TemplatePart::Bucket(column_name, num_buckets) = part {
                    self.tag_bucket_info
                        .entry(Arc::from(column_name))
                        .or_insert(BucketInfo {
                            id: None, // partition key is empty for this column
                            num_buckets,
                        });
                }
            }
        }

        BucketPartitionPruningOracle {
            tag_bucket_info: self.tag_bucket_info.clone(),
        }
    }
}

/// A container type to bundle a bucket ID with the number of
/// buckets for that column. This is used for pruning the server-side
/// bucketing later on.
///
/// If we have a partition with the following given information:
///
///   Partition template:
///   ```rs
///   TemplatePart::Bucket("banana", 50), TemplatePart::Bucket("uuid", 2000), TemplatePart::Bucket("apple", 20)
///   ```
///
///   Partition keys:
///   ```rs
///   "42|1010|!"
///   ```
///
///   Then the bucket info for each column would be:
///   ```rs
///   "banana" -> BucketInfo { id: Some(42), num_buckets: 50 }
///   "uuid" -> BucketInfo { id: Some(1010), num_buckets: 2000 }
///   "apple" -> BucketInfo { id: None, num_buckets: 20 }
///   ```
///
/// When do pruning, we can use `BucketInfo::may_contain_value` to check if a given
/// tag value belongs to the bucket:
///
/// # Example
///
/// ```rs
/// use data_types::partition_template::bucket_for_tag_value;
/// use iox_query::pruning_oracle::BucketInfo;
///
/// const NUM_BUCKETS: u32 = 50;
/// let bucket_id = bucket_for_tag_value("foo", NUM_BUCKETS);
/// let bucket_info = BucketInfo {
///   id: Some(bucket_id),
///   num_buckets: NUM_BUCKETS,
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketInfo {
    /// Bucket ID for this partition. If `None`, it means the
    /// partition key of this partition is empty, i.e. "!".
    pub id: Option<u32>,

    /// Number of buckets for the related column, specified in the partition template:
    /// [`TemplatePart::Bucket(tag_name, num_buckets)`](data_types::partition_template::TemplatePart::Bucket).
    pub num_buckets: u32,
}

impl BucketInfo {
    /// returns true if `tag_value` maps to this bucket id. Note that there are
    /// many tag values that map to the same bucket id.
    ///
    /// returns false if `tag_value` does not map to this bucket, or if this
    /// partition does not belong to a bucket (i.e. partition key is "!").
    pub fn may_contain_value(&self, tag_value: &str) -> bool {
        self.id
            .is_some_and(|id| id == bucket_for_tag_value(tag_value, self.num_buckets))
    }
}

#[cfg(test)]
mod tests {
    use crate::pruning_oracle::BucketInfo;
    use data_types::partition_template::bucket_for_tag_value;

    #[test]
    fn test_bucket_info() {
        const COLUMN_VALUE: &str = "foo";
        const NUM_BUCKETS: u32 = 50;

        let bucket_id = bucket_for_tag_value(COLUMN_VALUE, NUM_BUCKETS);
        let bucket_info = BucketInfo {
            id: Some(bucket_id),
            num_buckets: NUM_BUCKETS,
        };

        assert!(bucket_info.may_contain_value("foo"));
        assert!(!bucket_info.may_contain_value("bar"));
    }

    #[test]
    fn test_bucket_info_different_col_value_same_bucket_id() {
        const COLUMN_VALUE: &str = "foo";
        const NUM_BUCKETS: u32 = 2;

        let bucket_id_1 = bucket_for_tag_value(COLUMN_VALUE, NUM_BUCKETS);
        let bucket_info = BucketInfo {
            id: Some(bucket_id_1),
            num_buckets: NUM_BUCKETS,
        };
        assert!(bucket_info.may_contain_value("foo"));

        // "bar" maps to a different bucket than "foo"
        let bucket_id_2 = bucket_for_tag_value("bar", NUM_BUCKETS);
        assert_ne!(bucket_id_1, bucket_id_2);
        assert!(!bucket_info.may_contain_value("bar"));

        // "baz" maps to the same bucket as "foo"
        let bucket_id_3 = bucket_for_tag_value("baz", NUM_BUCKETS);
        assert_eq!(bucket_id_1, bucket_id_3);
        assert!(bucket_info.may_contain_value("baz"));
    }

    #[test]
    fn test_bucket_info_null_bucket() {
        const NUM_BUCKETS: u32 = 50;

        let bucket_info = BucketInfo {
            id: None,
            num_buckets: NUM_BUCKETS,
        };

        // Since no bucket id info, it should return false for any tag value
        assert!(!bucket_info.may_contain_value("foo"));
        assert!(!bucket_info.may_contain_value("bar"));
        assert!(!bucket_info.may_contain_value("baz"));
    }
}
