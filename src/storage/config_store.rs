use crate::delorean::Bucket;
use crate::storage::StorageError;
use std::sync::Arc;

pub trait ConfigStore: Sync + Send {
    fn create_bucket_if_not_exists(
        &self,
        org_id: u32,
        bucket: &Bucket,
    ) -> Result<u32, StorageError>;

    fn get_bucket_by_name(
        &self,
        org_id: u32,
        bucket_name: &str,
    ) -> Result<Option<Arc<Bucket>>, StorageError>;

    fn get_bucket_by_id(&self, bucket_id: u32) -> Result<Option<Arc<Bucket>>, StorageError>;
}
