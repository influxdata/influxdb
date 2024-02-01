use data_types::partition_template;

#[derive(Debug)]
pub(super) struct BucketHasher {
    num_buckets: u32,
    last_assigned_bucket: Option<u32>,
}

impl BucketHasher {
    pub(super) fn new(num_buckets: u32) -> Self {
        Self {
            num_buckets,
            last_assigned_bucket: None,
        }
    }

    /// Assign a bucket for the provided `tag_value` using the [`BucketHasher`]s
    /// configuration.
    pub(super) fn assign_bucket(&mut self, tag_value: &str) -> u32 {
        let bucket = partition_template::bucket_for_tag_value(tag_value, self.num_buckets);
        self.last_assigned_bucket = Some(bucket);
        bucket
    }

    /// The last bucket assigned by the [`BucketHasher`].
    pub(super) fn last_assigned_bucket(&self) -> Option<u32> {
        self.last_assigned_bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_last_assigned_bucket() {
        let mut bucketer = BucketHasher::new(10);
        assert_eq!(bucketer.last_assigned_bucket, None);

        assert_eq!(bucketer.assign_bucket("foo"), 6);
        assert_eq!(bucketer.last_assigned_bucket, Some(6));

        assert_eq!(bucketer.assign_bucket("bat"), 5);
        assert_eq!(bucketer.last_assigned_bucket, Some(5));

        assert_eq!(bucketer.assign_bucket("qux"), 5);
        assert_eq!(bucketer.last_assigned_bucket, Some(5));
    }
}
