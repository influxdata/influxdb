use tracing::{debug, info};

use crate::generated_types::{Bucket, Predicate, TimestampRange};
use crate::id::Id;
use crate::line_parser::PointType;
use crate::storage::{
    memdb::MemDB,
    partitioned_store::{Partition, PartitionStore, ReadBatch},
    SeriesDataType, StorageError,
};

use futures::StreamExt;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::HashMap, convert::TryInto, fs, fs::DirBuilder, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Database error creating directory '{:?}': {}", path, source))]
    CreatingDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Database error reading path '{:?}': {}", path, source))]
    ReadingPath {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Organization {} not found", org_id))]
    OrgNotFound { org_id: Id },

    #[snafu(display("Bucket {} not found for organization {}", org_id, bucket_id))]
    BucketNotFound { org_id: String, bucket_id: String },

    // TODO remove when we have removed generic StorageError
    #[snafu(display("Database error due to underlying storyage error: {}'", source))]
    UnderlyingStorageError { source: StorageError },
}

// TODO: remove once we have made all modules have their own errors
impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Self::UnderlyingStorageError { source: e }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
struct Organization {
    id: Id,
    bucket_data: HashMap<Id, Arc<BucketData>>,
    bucket_name_to_id: HashMap<String, Id>,
}

impl Organization {
    // create_bucket_if_not_exists inserts the bucket into the map and returns its id
    async fn create_bucket_if_not_exists(
        &mut self,
        mut bucket: Bucket,
        wal_root_dir: Option<PathBuf>,
    ) -> Result<Id> {
        match self.bucket_name_to_id.get(&bucket.name) {
            Some(id) => Ok(*id),
            None => {
                let id = (self.bucket_data.len() + 1) as u64;
                bucket.id = id;
                let id: Id = id.try_into().expect("usize plus 1 can't be zero");

                let wal_dir = if let Some(root) = wal_root_dir {
                    let path = root.join(self.id.to_string()).join(bucket.name.clone());
                    DirBuilder::new()
                        .recursive(true)
                        .create(&path)
                        .context(CreatingDirectory { path: path.clone() })?;
                    Some(path)
                } else {
                    None
                };

                self.bucket_name_to_id.insert(bucket.name.clone(), id);
                self.bucket_data
                    .insert(id, Arc::new(BucketData::new(bucket, wal_dir).await?));
                Ok(id)
            }
        }
    }

    fn new(id: Id) -> Self {
        Self {
            id,
            bucket_data: HashMap::default(),
            bucket_name_to_id: HashMap::default(),
        }
    }

    async fn restore_from_wal(org_dir: &PathBuf) -> Result<Self> {
        let org_id: Id = org_dir
            .file_name()
            .expect("Path should not end in ..")
            .to_str()
            .expect("Organization WAL dir should have been UTF-8")
            .parse()
            .expect("Should have been able to parse Organization WAL dir into Organization Id");
        let mut org = Self::new(org_id);

        let dirs = fs::read_dir(org_dir).context(ReadingPath { path: org_dir })?;

        for dir in dirs {
            let bucket_dir = dir.context(ReadingPath { path: org_dir })?.path();

            info!("Restoring bucket from WAL path: {:?}", bucket_dir);

            let bucket_name = bucket_dir
                .file_name()
                .expect("Path should not end in ..")
                .to_str()
                .expect("Bucket WAL dir should have been UTF-8")
                .to_string();

            // TODO: Bucket IDs may be different on restore, that's probably not desired
            let id = (org.bucket_data.len() + 1) as u64;

            let bucket = Bucket {
                org_id: org_id.into(),
                id,
                name: bucket_name.clone(),
                retention: "0".to_string(),
                posting_list_rollover: 10_000,
                index_levels: vec![],
            };
            debug!("Restored bucket from WAL: {:?}", bucket);

            let bucket_data = BucketData::restore_from_wal(bucket, bucket_dir).await?;

            let id: Id = id.try_into().expect("usize plus 1 can't be zero");
            org.bucket_name_to_id.insert(bucket_name, id);
            org.bucket_data.insert(id, Arc::new(bucket_data));
        }

        Ok(org)
    }
}

#[derive(Debug)]
struct BucketData {
    config: Bucket,
    // TODO: wire up rules for partitioning data and storing and reading from multiple partitions
    partition: RwLock<Partition>,
}

impl BucketData {
    const BATCH_SIZE: usize = 100_000;

    async fn new(bucket: Bucket, wal_dir: Option<PathBuf>) -> Result<Self> {
        let partition_id = bucket.name.clone();
        let store = PartitionStore::MemDB(Box::new(MemDB::new(partition_id)));
        let partition = match wal_dir {
            Some(dir) => Partition::new_with_wal(store, dir).await?,
            None => Partition::new_without_wal(store),
        };

        Ok(Self {
            config: bucket,
            partition: RwLock::new(partition),
        })
    }

    async fn restore_from_wal(bucket: Bucket, bucket_dir: PathBuf) -> Result<Self> {
        let partition = Partition::restore_memdb_from_wal(&bucket.name, bucket_dir).await?;

        Ok(Self {
            config: bucket,
            partition: RwLock::new(partition),
        })
    }

    async fn write_points(&self, points: &mut [PointType]) -> Result<()> {
        self.partition
            .write()
            .await
            .write_points(points)
            .await
            .context(UnderlyingStorageError)
    }

    async fn read_points(
        &self,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<Vec<ReadBatch>> {
        let p = self.partition.read().await;
        let stream = p.read_points(Self::BATCH_SIZE, predicate, range).await?;
        Ok(stream.collect().await)
    }

    async fn get_tag_keys(
        &self,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let p = self.partition.read().await;
        let stream = p.get_tag_keys(predicate, range).await?;
        Ok(stream.collect().await)
    }

    async fn get_tag_values(
        &self,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let p = self.partition.read().await;
        let stream = p.get_tag_values(tag_key, predicate, range).await?;
        Ok(stream.collect().await)
    }

    async fn get_measurement_names(&self, range: Option<&TimestampRange>) -> Result<Vec<String>> {
        let p = self.partition.read().await;
        let stream = p.get_measurement_names(range).await?;
        Ok(stream.collect().await)
    }

    async fn get_measurement_tag_keys(
        &self,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let p = self.partition.read().await;
        let stream = p
            .get_measurement_tag_keys(measurement, predicate, range)
            .await?;
        Ok(stream.collect().await)
    }

    async fn get_measurement_tag_values(
        &self,
        measurement: &str,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let p = self.partition.read().await;
        let stream = p
            .get_measurement_tag_values(measurement, tag_key, predicate, range)
            .await?;
        Ok(stream.collect().await)
    }

    async fn get_measurement_fields(
        &self,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<(String, SeriesDataType, i64)>> {
        let p = self.partition.read().await;
        let stream = p
            .get_measurement_fields(measurement, predicate, range)
            .await?;
        Ok(stream.collect().await)
    }
}

#[derive(Debug)]
pub struct Database {
    dir: Option<PathBuf>,
    organizations: RwLock<HashMap<Id, RwLock<Organization>>>,
}

impl Database {
    /// Create a new database with a WAL for every bucket in the provided directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: Some(dir.into()),
            organizations: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new database without a WAL for any bucket.
    pub fn new_without_wal() -> Self {
        Self {
            dir: None,
            organizations: RwLock::new(HashMap::new()),
        }
    }

    pub async fn restore_from_wal(&self) -> Result<()> {
        // TODO: Instead of looking on disk, look in a Partition that holds org+bucket config
        if let Some(wal_dir) = &self.dir {
            let mut orgs = self.organizations.write().await;

            let dirs = fs::read_dir(wal_dir).context(ReadingPath { path: wal_dir })?;

            for org_dir in dirs {
                let org_dir = org_dir.context(ReadingPath { path: wal_dir })?;
                let org = Organization::restore_from_wal(&org_dir.path()).await?;
                orgs.insert(org.id, RwLock::new(org));
            }
        }

        Ok(())
    }

    pub async fn write_points(
        &self,
        org_id: Id,
        bucket_id: Id,
        points: &mut [PointType],
    ) -> Result<()> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.write_points(points).await
    }

    pub async fn get_bucket_id_by_name(&self, org_id: Id, bucket_name: &str) -> Result<Option<Id>> {
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

    pub async fn create_bucket_if_not_exists(&self, org_id: Id, bucket: Bucket) -> Result<Id> {
        let mut orgs = self.organizations.write().await;
        let org = orgs
            .entry(org_id)
            .or_insert_with(|| RwLock::new(Organization::new(org_id)));
        let mut org = org.write().await;

        // TODO: Add a way to configure whether a particular bucket has a WAL
        org.create_bucket_if_not_exists(bucket, self.dir.clone())
            .await
    }

    pub async fn read_points(
        &self,
        org_id: Id,
        bucket_id: Id,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<Vec<ReadBatch>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.read_points(predicate, range).await
    }

    pub async fn get_tag_keys(
        &self,
        org_id: Id,
        bucket_id: Id,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
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
    ) -> Result<Vec<String>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.get_tag_values(tag_key, predicate, range).await
    }

    pub async fn get_measurement_names(
        &self,
        org_id: Id,
        bucket_id: Id,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data.get_measurement_names(range).await
    }

    pub async fn get_measurement_tag_keys(
        &self,
        org_id: Id,
        bucket_id: Id,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data
            .get_measurement_tag_keys(measurement, predicate, range)
            .await
    }

    pub async fn get_measurement_tag_values(
        &self,
        org_id: Id,
        bucket_id: Id,
        measurement: &str,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<String>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data
            .get_measurement_tag_values(measurement, tag_key, predicate, range)
            .await
    }

    pub async fn get_measurement_fields(
        &self,
        org_id: Id,
        bucket_id: Id,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<Vec<(String, SeriesDataType, i64)>> {
        let bucket_data = self.bucket_data(org_id, bucket_id).await?;

        bucket_data
            .get_measurement_fields(measurement, predicate, range)
            .await
    }

    pub async fn buckets(&self, org_id: Id) -> Result<Vec<Bucket>> {
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

    async fn bucket_data(&self, org_id: Id, bucket_id: Id) -> Result<Arc<BucketData>> {
        let orgs = self.organizations.read().await;
        let org = orgs.get(&org_id).context(OrgNotFound { org_id })?;

        let org = org.read().await;

        match org.bucket_data.get(&bucket_id) {
            Some(b) => Ok(Arc::clone(b)),
            None => BucketNotFound { org_id, bucket_id }.fail(),
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
        let database = Database::new_without_wal();
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
        let database = Database::new_without_wal();
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
