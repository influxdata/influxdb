use crate::delorean::{Bucket, Predicate, TimestampRange};
use crate::id::Id;
use crate::line_parser::PointType;
use crate::storage::memdb::MemDB;
use crate::storage::partitioned_store::{Partition, ReadBatch};
use crate::storage::StorageError;

use futures::StreamExt;
use std::{collections::HashMap, convert::TryInto, sync::Arc};
use tokio::sync::RwLock;

pub struct Database {
    organizations: RwLock<HashMap<Id, RwLock<Organization>>>,
}

#[derive(Default)]
struct Organization {
    bucket_data: HashMap<Id, Arc<BucketData>>,
    bucket_name_to_id: HashMap<String, Id>,
}

impl Organization {
    // create_bucket_if_not_exists inserts the bucket into the map and returns its id
    fn create_bucket_if_not_exists(&mut self, mut bucket: Bucket) -> Id {
        match self.bucket_name_to_id.get(&bucket.name) {
            Some(id) => *id,
            None => {
                let id = (self.bucket_data.len() + 1) as u64;
                bucket.id = id;
                let id: Id = id.try_into().expect("usize plus 1 can't be zero");
                self.bucket_name_to_id.insert(bucket.name.clone(), id);
                self.bucket_data
                    .insert(id, Arc::new(BucketData::new(bucket)));
                id
            }
        }
    }
}

struct BucketData {
    config: Bucket,
    // TODO: wire up rules for partitioning data and storing and reading from multiple partitions
    partition: RwLock<Partition>,
}

impl BucketData {
    const BATCH_SIZE: usize = 100_000;

    fn new(bucket: Bucket) -> BucketData {
        let partition_id = bucket.name.clone();
        let partition = Partition::MemDB(Box::new(MemDB::new(partition_id)));

        BucketData {
            config: bucket,
            partition: RwLock::new(partition),
        }
    }

    async fn write_points(&self, points: &mut [PointType]) -> Result<(), StorageError> {
        self.partition.write().await.write_points(points).await
    }

    async fn read_points(
        &self,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<Vec<ReadBatch>, StorageError> {
        let p = self.partition.read().await;
        let stream = p
            .read_points(BucketData::BATCH_SIZE, predicate, range)
            .await?;
        Ok(stream.collect().await)
    }

    async fn get_tag_keys(
        &self,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>, StorageError> {
        let p = self.partition.read().await;
        let stream = p.get_tag_keys(predicate, range).await?;
        Ok(stream.collect().await)
    }

    async fn get_tag_values(
        &self,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>, StorageError> {
        let p = self.partition.read().await;
        let stream = p.get_tag_values(tag_key, predicate, range).await?;
        Ok(stream.collect().await)
    }
}

impl Database {
    pub fn new(_dir: &str) -> Database {
        Database {
            organizations: RwLock::new(HashMap::new()),
        }
    }

    pub async fn write_points(
        &self,
        org_id: Id,
        bucket_id: Id,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.write_points(points).await
    }

    pub async fn get_bucket_id_by_name(
        &self,
        org_id: Id,
        bucket_name: &str,
    ) -> Result<Option<Id>, StorageError> {
        let orgs = self.organizations.read().await;

        let org = match orgs.get(&org_id) {
            Some(org) => org,
            None => return Ok(None),
        };

        let id = match org.read().await.bucket_name_to_id.get(bucket_name) {
            Some(id) => Some(*id),
            None => None,
        };

        Ok(id)
    }

    pub async fn create_bucket_if_not_exists(
        &self,
        org_id: Id,
        bucket: Bucket,
    ) -> Result<Id, StorageError> {
        let mut orgs = self.organizations.write().await;
        let org = orgs
            .entry(org_id)
            .or_insert_with(|| RwLock::new(Organization::default()));
        let mut org = org.write().await;

        Ok(org.create_bucket_if_not_exists(bucket))
    }

    pub async fn read_points(
        &self,
        org_id: Id,
        bucket_id: Id,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<Vec<ReadBatch>, StorageError> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.read_points(predicate, range).await
    }

    pub async fn get_tag_keys(
        &self,
        org_id: Id,
        bucket_id: Id,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>, StorageError> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.get_tag_keys(predicate, range).await
    }

    pub async fn get_tag_values(
        &self,
        org_id: Id,
        bucket_id: Id,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>, StorageError> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.get_tag_values(tag_key, predicate, range).await
    }

    pub async fn buckets(&self, org_id: Id) -> Result<Vec<Bucket>, StorageError> {
        Ok(match self.organizations.read().await.get(&org_id) {
            None => vec![],
            Some(org) => org
                .read()
                .await
                .bucket_data
                .values()
                .map(|bd| bd.config.clone())
                .collect(),
        })
    }

    async fn bucket_data(
        &self,
        org_id: Id,
        bucket_id: Id,
    ) -> Result<Arc<BucketData>, StorageError> {
        let orgs = self.organizations.read().await;
        let org = orgs.get(&org_id).ok_or_else(|| StorageError {
            description: format!("org {} not found", org_id),
        })?;

        let org = org.read().await;

        match org.bucket_data.get(&bucket_id) {
            Some(b) => Ok(Arc::clone(b)),
            None => Err(StorageError {
                description: format!("bucket {} not found", bucket_id),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::line_parser::PointType;
    use crate::storage::database::Database;
    use crate::storage::partitioned_store::ReadValues;
    use crate::storage::predicate::parse_predicate;
    use crate::storage::ReadPoint;
    use std::convert::TryInto;

    #[tokio::test]
    async fn create_bucket() {
        let database = Database::new("");
        let org_id: Id = 2u64.try_into().unwrap();
        let bucket = Bucket {
            org_id: org_id.into(),
            id: 0,
            name: "first".to_string(),
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![],
        };
        let bucket_id = database
            .create_bucket_if_not_exists(org_id, bucket.clone())
            .await
            .unwrap();
        assert_eq!(bucket_id, 1u64.try_into().unwrap());

        let bucket_two = Bucket {
            org_id: org_id.into(),
            id: 0,
            name: "second".to_string(),
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![],
        };

        let bucket_id = database
            .create_bucket_if_not_exists(org_id, bucket_two)
            .await
            .unwrap();
        assert_eq!(bucket_id, 2u64.try_into().unwrap());

        let bucket_id = database
            .create_bucket_if_not_exists(org_id, bucket)
            .await
            .unwrap();
        assert_eq!(bucket_id, 1u64.try_into().unwrap());
    }

    #[tokio::test]
    async fn get_tag_keys() {
        let (db, org, bucket) = setup_db_and_bucket().await;
        db.write_points(
            org,
            bucket,
            &mut [
                PointType::new_i64("cpu,host=a,region=west\tfoo".to_string(), 1, 0),
                PointType::new_i64("mem,foo=bar\tasdf".to_string(), 1, 0),
            ],
        )
        .await
        .unwrap();

        let keys = db.get_tag_keys(org, bucket, None, None).await.unwrap();

        assert_eq!(keys, vec!["_f", "_m", "foo", "host", "region"]);
    }

    #[tokio::test]
    async fn get_tag_values() {
        let (db, org, bucket) = setup_db_and_bucket().await;
        db.write_points(
            org,
            bucket,
            &mut [
                PointType::new_i64("cpu,host=a,region=west\tfoo".to_string(), 1, 0),
                PointType::new_i64("mem,host=b\tasdf".to_string(), 1, 0),
            ],
        )
        .await
        .unwrap();

        let values = db
            .get_tag_values(org, bucket, "host", None, None)
            .await
            .unwrap();

        assert_eq!(values, vec!["a", "b"]);

        let values = db
            .get_tag_values(org, bucket, "region", None, None)
            .await
            .unwrap();

        assert_eq!(values, vec!["west"]);

        let values = db
            .get_tag_values(org, bucket, "_m", None, None)
            .await
            .unwrap();

        assert_eq!(values, vec!["cpu", "mem"]);
    }

    #[tokio::test]
    async fn read_points() {
        let (db, org, bucket) = setup_db_and_bucket().await;
        db.write_points(
            org,
            bucket,
            &mut [
                PointType::new_i64("cpu,host=a,region=west\tval".to_string(), 3, 1),
                PointType::new_i64("cpu,host=a,region=west\tval".to_string(), 2, 5),
                PointType::new_i64("cpu,host=a,region=west\tval".to_string(), 1, 10),
                PointType::new_i64("cpu,host=b,region=west\tval".to_string(), 5, 9),
            ],
        )
        .await
        .unwrap();

        let pred = parse_predicate(r#"host = "a""#).unwrap();
        let range = TimestampRange { start: 0, end: 11 };
        let batches = db.read_points(org, bucket, &pred, &range).await.unwrap();

        assert_eq!(
            batches,
            vec![ReadBatch {
                key: "cpu,host=a,region=west\tval".to_string(),
                values: ReadValues::I64(vec![
                    ReadPoint { value: 3, time: 1 },
                    ReadPoint { value: 2, time: 5 },
                    ReadPoint { value: 1, time: 10 },
                ])
            }]
        );
    }

    async fn setup_db_and_bucket() -> (Database, Id, Id) {
        let database = Database::new("");
        let org_id: Id = 1u64.try_into().unwrap();
        let bucket = Bucket {
            org_id: org_id.into(),
            id: 0,
            name: "foo".to_string(),
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![],
        };
        let bucket_id = database
            .create_bucket_if_not_exists(org_id, bucket)
            .await
            .unwrap();

        (database, org_id, bucket_id)
    }
}
