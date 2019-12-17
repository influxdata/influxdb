use crate::line_parser::Point;
use crate::delorean::Bucket;

use rocksdb::{DB, IteratorMode, WriteBatch, Options, ColumnFamilyDescriptor, Direction};
use byteorder::{BigEndian, WriteBytesExt};
use prost::Message;

use bytes::BufMut;
use std::{error, fmt};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub struct Database {
    db: DB,
    bucket_name_to_id: Arc<RwLock<HashMap<Vec<u8>, u32>>>,
    next_bucket_id: u32,
}

const BUCKET_CF: &str = "buckets";

impl Database {
    pub fn new(dir: &str) -> Database {
        let mut opts = Options::default();

        // create the database and missing column families
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // ensure rocks uses more than one thread for compactions/etc
        let core_count = num_cpus::get();
        opts.increase_parallelism(core_count as i32);

        let buckets_cf = ColumnFamilyDescriptor::new(BUCKET_CF, Options::default());

        let db = DB::open_cf_descriptors(&opts, dir, vec![buckets_cf]).unwrap();

        let mut database = Database{
            db,
            bucket_name_to_id: Arc::new(RwLock::new(HashMap::new())),
            next_bucket_id: 0,
        };
        database.load_bucket_map();

        database
    }

    // TODO: wire up the org and bucket part of this
    // TODO: wire up series to ID
    // TODO: wire up inverted index creation
    pub fn write_points(&self, _org: &str, _bucket: &str, points: Vec<Point>) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();

        for point in points {
            let mut s = point.series.into_bytes();
            s.write_i64::<BigEndian>(point.time).unwrap();
            let mut val:Vec<u8> = Vec::with_capacity(4);
            val.write_i64::<BigEndian>(point.value).unwrap();

            batch.put(s, val).unwrap();
        }

        self.db.write(batch).unwrap(); // crash if there's some Rocks related error
        Ok(())
    }

    /// If the bucket name exists, this function returns the ID (ignoring whether the bucket options
    /// are different than the one that exists). If it doesn't exist, this function
    /// creates the bucket and returns its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `bucket` - The bucket to create along with all of its configuration options
    pub fn create_bucket_if_not_exists(&mut self, org_id: u32, bucket: &Bucket) -> Result<u32, StorageError> {
        validate_bucket_fields(bucket)?;

        let key = bucket_key(org_id, &bucket.name);
        if let Some(id) = {
            self.bucket_name_to_id.read().unwrap().get(&key)
        } {
            return Ok(*id);
        }

        let mut map = self.bucket_name_to_id.write().unwrap();
        if let Some(id) = map.get(&key) {
            return Ok(*id);
        }

        // assign the ID and insert the bucket
        let buckets = self.db.cf_handle(BUCKET_CF).unwrap();
        let mut buf: Vec<u8> = vec![];
        let mut store = bucket.clone();
        store.id = self.next_bucket_id.clone();
        store.encode(&mut buf).unwrap();
        self.db.put_cf(&buckets, &key, buf).unwrap();
        map.insert(key, store.id);
        self.next_bucket_id = self.next_bucket_id + 1;

        Ok(store.id)
    }

    /// Looks up the bucket object by org id and name and returns it.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `name` - The name of the bucket (which is unique under an organization)
    pub fn get_bucket_by_name(&self, org_id: u32, name: &str) -> Result<Option<Bucket>, rocksdb::Error> {
        let buckets = self.db.cf_handle(BUCKET_CF).unwrap();

        match self.db.get_cf(buckets, bucket_key(org_id, &name.to_string())) {
            Ok(b) => {
                match b {
                    Some(b) => {
                        let bucket = Bucket::decode(b).unwrap();
                        return Ok(Some(bucket));
                    }
                    None => return Ok(None),
                }
            }
            Err(e) => return Err(e),
        }
    }

    fn load_bucket_map(&mut self) {
        let buckets = self.db.cf_handle(BUCKET_CF).unwrap();
        let prefix = &[BUCKET_PREFIX];
        let mut iter = self.db.iterator_cf(&buckets, IteratorMode::From(prefix, Direction::Forward)).unwrap();

        for (key, value) in iter {
            match key.starts_with(prefix) {
                false => break,
                true => {
                    let bucket = Bucket::decode(value.into_vec()).unwrap();
                    let key = bucket_key(bucket.org_id, &bucket.name);
                    self.bucket_name_to_id.write().unwrap().insert(key, bucket.id);
                    if self.next_bucket_id < bucket.id {
                        self.next_bucket_id = bucket.id
                    }
                },
            }
        }
        self.next_bucket_id += 1;
    }
}

// TODO: ensure required fields are present and write tests
fn validate_bucket_fields(_bucket: &Bucket) -> Result<(), StorageError> {
    Ok(())
}

const BUCKET_PREFIX: u8 = 1;

fn bucket_key(org_id: u32, bucket_name: &String) -> Vec<u8> {
    let mut s = bucket_name.clone().into_bytes();
    let mut key = Vec::with_capacity(3 + s.len());
    key.push(BUCKET_PREFIX);
    key.write_u32::<BigEndian>(org_id).unwrap();
    key.append(&mut s);
    key
}

#[derive(Debug, Clone)]
pub struct StorageError {
    description: String,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for StorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dotenv::dotenv;
    use std::env;
    use serde_json::error::Category::Data;

    use rocksdb;

    #[test]
    fn create_and_get_buckets() {
        let mut bucket: Bucket;
        let org_id = 1;
        {
            let mut db = test_database("create_and_get_buckets", true);
            let mut b = Bucket{org_id, id: 0, name: "Foo".to_string(), retention: "7d".to_string()};

            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(b.id, 1);
            let stored_bucket = db.get_bucket_by_name(org_id, &b.name).unwrap().unwrap();
            assert_eq!(b, stored_bucket);
            bucket = stored_bucket;

            // ensure it doesn't insert again
            let id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(id, 1);

            // ensure second bucket gets new ID
            let mut b2 = Bucket{org_id, id: 0, name: "two".to_string(), retention: "24h".to_string()};
            b2.id = db.create_bucket_if_not_exists(org_id, &b2).unwrap();
            assert_eq!(b2.id, 2);
            let stored_bucket = db.get_bucket_by_name(org_id, &b2.name).unwrap().unwrap();
            assert_eq!(b2, stored_bucket);
        }

        // ensure it persists across database reload
        {
            let mut db = test_database("create_and_get_buckets", false);
            let stored_bucket = db.get_bucket_by_name(org_id, &bucket.name).unwrap().unwrap();
            assert_eq!(bucket, stored_bucket);

            // ensure a new bucket will get a new ID
            let mut b = Bucket{org_id, id:0, name: "asdf".to_string(), retention: "1d".to_string()};
            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(b.id, 3);
        }
    }

    // Test helpers
    fn get_test_storage_path() -> String {
        dotenv().ok();
        env::var("TEST_DELOREAN_DB_DIR").expect(
            "TEST_DELOREAN_DB_DIR must be set. Perhaps .env is missing?",
        )
    }

    fn test_database(name: &str, remove_old: bool) -> Database {
        let path = std::path::Path::new(&get_test_storage_path()).join(name);
        if remove_old {
            let _ = std::fs::remove_dir_all(path.to_str().unwrap());
        }
        Database::new(path.to_str().unwrap())
    }
}
