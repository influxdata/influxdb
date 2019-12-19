use crate::line_parser::Point;
use crate::delorean::{Bucket, IndexLevel};

use rocksdb::{DB, IteratorMode, WriteBatch, Options, ColumnFamilyDescriptor, Direction, ColumnFamily};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use prost::Message;

use bytes::BufMut;
use std::{error, fmt};
use std::sync::{Arc, RwLock, Mutex, MutexGuard};
use std::collections::HashMap;
use std::time::SystemTime;
use std::io::Cursor;
use rocksdb::MemtableFactory::{Vector, HashLinkList};
use crate::storage::IndexEntryType::SeriesKeyToID;
use futures::AsyncWriteExt;

/// Database wraps a RocksDB database for storing the raw series data, an inverted index of the
/// metadata and the metadata about what buckets exist in the system.
///
/// Each bucket gets a unique u32 ID assigned. This ID is unique across all orgs. Bucket names
/// are unique within an organization.
///
/// Series (measurement + tagset + field) are identified by a u64 ID that is unique within a bucket.
/// Each bucket keeps an incrementing counter for new series IDs.
pub struct Database {
    db: DB,
    // bucket_map is an in memory map of what buckets exist in the system. the key is the org id and bucket name together as bytes
    bucket_map: Arc<RwLock<HashMap<Vec<u8>, Bucket>>>,
    // series_insert_lock is a map of mutexes for creating new series in each bucket. Bucket ids are unique across all orgs
    series_insert_lock: Arc<RwLock<HashMap<u32, Mutex<u64>>>>,
}

#[derive(Debug, PartialEq)]
pub struct Series {
    id: Option<u64>,
    point: Point,
}

const BUCKET_CF: &str = "buckets";
const BUCKET_CF_WRITE_BUFFER_SIZE: usize = 1024 * 1024; // 1MB
const INDEX_CF: &str = "indexes";
const INDEX_CF_WRITE_BUFFER_SIZE: usize = 10 * 1024 * 1024; // 10MB

impl Database {
    pub fn new(dir: &str) -> Database {
        let mut opts = Options::default();

        // create the database and missing column families
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // ensure rocks uses more than one thread for compactions/etc
        let core_count = num_cpus::get();
        opts.increase_parallelism(core_count as i32);

        // ensure the buckets CF exists and open up any index CFs
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = match DB::list_cf(&opts, dir) {
            Ok(names) => {
                names.into_iter().map(|name| {
                    if &name == BUCKET_CF {
                        bucket_cf_descriptor()
                    } else {
                        ColumnFamilyDescriptor::new(&name, index_cf_options())
                    }
                }).collect()
            },
            Err(_) => vec![bucket_cf_descriptor()],
        };

        let db = DB::open_cf_descriptors(&opts, dir, cf_descriptors).unwrap();

        let mut database = Database{
            db,
            bucket_map: Arc::new(RwLock::new(HashMap::new())),
            series_insert_lock: Arc::new(RwLock::new(HashMap::new())),
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

    /// If the bucket name exists within an org, this function returns the ID (ignoring whether the
    /// bucket options are different than the one that exists). If it doesn't exist, this function
    /// creates the bucket and returns its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `bucket` - The bucket to create along with all of its configuration options. Ignores the ID.
    pub fn create_bucket_if_not_exists(&mut self, org_id: u32, bucket: &Bucket) -> Result<u32, StorageError> {
        validate_bucket_fields(bucket)?;

        let key = bucket_key(org_id, &bucket.name);
        if let Some(b) = self.bucket_map.read().unwrap().get(&key) {
            return Ok(b.id);
        }

        let mut map = self.bucket_map.write().unwrap();
        if let Some(b) = map.get(&key) {
            return Ok(b.id);
        }

        // assign the ID and insert the bucket
        let buckets = self.db.cf_handle(BUCKET_CF)
            .expect("unexpected rocksdb error while trying to get the buckets column family");

        let mut buf: Vec<u8> = vec![];
        let mut store = bucket.clone();

        // get the next bucket ID
        let mut next_id = match self.db.get_cf(buckets, next_bucket_id_key())
            .expect("unexpected rocksdb error while trying to get the next bucket id") {

            Some(val) => u32_from_bytes(&val),
            None => 1,
        };

        store.id = next_id;
        store.encode(&mut buf).expect("unexpected error encoding bucket");

        // write the bucket and the next ID counter atomically
        let mut batch = WriteBatch::default();
        batch.put_cf(&buckets, &key, buf).unwrap();
        batch.put_cf(&buckets, next_bucket_id_key(), u32_to_bytes(store.id + 1));
        self.db.write(batch).expect("unexpected rocksdb error writing to DB");

        let id = store.id;
        map.insert(key, store);

        Ok(id)
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

    // TODO: ensure that points with timestamps older than the first index level get matched against the appropriate index
    // TODO: maybe add an LRU for the series ID mappings?
    /// get_series_ids consumes the passed in points vector and returns a vector of series, one for
    /// each point (which is now in the series struct). The series struct has an option with the ID
    /// if the series already exists in the top level index.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `bucket` - The bucket these series are to be inserted into
    /// * `points` - The points that will be later written to the DB that need to have series IDs
    ///
    /// # Returns
    /// A vector of series where each point in the passed in vector is contained in a series
    pub fn get_series_ids(&self, org_id: u32, bucket: &Bucket, mut points: Vec<Point>) -> Vec<Series> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();

        let mut series = points.into_iter().map(|p| {
            let mut series = Series{id: None, point: p};
            let level = &bucket.index_levels[0];
            let cf_name = index_cf_name(org_id, bucket.id,level.duration_seconds, now);
            series.id = self.get_series_id(&cf_name, &series.point.series);
            series
        }).collect();

        series
    }

    // ensure_series_mutex_exists makes sure that the passed in bucket id has a mutex, which is used
    // when inserting new series into a bucket
    fn ensure_series_mutex_exists(&mut self, bucket_id: u32) {
        let map = self.series_insert_lock.read().expect("mutex poisoned");

        if let Some(next_id_mutex) = map.get(&bucket_id) {
            return;
        }

        // if we got this far we need to create a mutex for this bucket
        drop(map);
        let mut map = self.series_insert_lock.write().expect("mutex poisoned");

        // now only insert the new mutex if someone else hasn't done it between dropping read and obtaining write
        if let None = map.get(&bucket_id) {
            map.insert(bucket_id, Mutex::new(1));
        }
    }

    // TODO: ensure that points with timestamps older than the first index level get inserted only into the higher levels
    // TODO: build the index for levels other than the first
    // insert_series_without_ids will insert any series into the index and obtain an identifier for it.
    // the passed in series vector is modified so that the newly inserted series have their ids
    pub fn insert_series_without_ids(&mut self, org_id: u32, bucket: &Bucket, series: &mut Vec<Series>) {
        // We want to get a lock on new series only for this bucket
        self.ensure_series_mutex_exists(bucket.id);
        let map = self.series_insert_lock.read().expect("mutex poisoned");
        let mut next_id = map.get(&bucket.id).expect("should exist because of call to ensure_series_mutex_exists");
        let mut next_id = next_id.lock().expect("mutex poisoned");

        let mut batch = WriteBatch::default();

        // create the column family to store the index if it doesn't exist
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(org_id, bucket.id, bucket.index_levels[0].duration_seconds, now);
        let index_cf = match self.db.cf_handle(&cf_name) {
            Some(cf) => cf,
            None => {
                self.db.create_cf(&cf_name, &index_cf_options()).unwrap();
                self.db.cf_handle(&cf_name).unwrap()
            },
        };

        // now loop through the series and
        for series in series {
            // don't bother with series in the collection that already have IDs
            if let Some(_) = series.id {
                continue;
            }

            // now that we have the mutex on series, make sure these weren't inserted in some other thread
            if let None = self.get_series_id(&cf_name, &series.point.series) {
                series.id = Some(*next_id);
                let mut series_id = Vec::with_capacity(8);
                series_id.write_u64::<BigEndian>(*next_id).unwrap();
                batch.put_cf(index_cf, index_series_key_id(&series.point.series), series_id);
                *next_id += 1;
            }
        }

        // save the next series id
        let bucket_cf = self.db.cf_handle(BUCKET_CF).unwrap();
        let mut next_series_id_val = Vec::with_capacity(8);
        next_series_id_val.write_u64::<BigEndian>(*next_id).unwrap();
        batch.put_cf(bucket_cf, next_series_id_key(org_id, bucket.id), next_series_id_val);
        self.db.write(batch).expect("unexpected rocksdb error");
    }

    fn get_series_id(&self, cf_name: &str, series_key: &str) -> Option<u64> {
        // this column family might not exist if this index hasn't been created yet
        if let Some(cf) = self.db.cf_handle(cf_name) {
                if let Some(val) = self.db.get_cf(cf, index_series_key_id(series_key)).expect("unexpected rocksdb error") {
                        let mut c = Cursor::new(val);
                        Some(c.read_u64::<BigEndian>().unwrap())
                } else {
                    None
                }
        } else {
            None
        }
    }

    fn load_bucket_map(&mut self) {
        let buckets = self.db.cf_handle(BUCKET_CF).unwrap();
        let prefix = &[BucketEntryType::Bucket as u8];
        println!("prefix: {:?}", prefix);
        println!("series one: {:?}", BucketEntryType::NextSeriesID as u8);
        let mut iter = self.db.iterator_cf(&buckets, IteratorMode::From(prefix, Direction::Forward)).unwrap();

        let mut id_mutex_map = HashMap::new();
        let mut bucket_map = self.bucket_map.write().unwrap();

        for (key, value) in iter {
            match bucket_entry_type_from_byte(key[0]) {
                BucketEntryType::NextSeriesID=> {
                    // read the bucket id from the key
                    let mut c = Cursor::new(key[5..].to_vec());
                    let bucket_id = c.read_u32::<BigEndian>().expect(&format!("couldn't read the bucket id from the key {:?}", key));

                    // and the next series ID
                    let mut c= Cursor::new(value);
                    let next_id = c.read_u64::<BigEndian>().expect(&format!("couldn't read the next series id for bucket {}", bucket_id));
                    id_mutex_map.insert(bucket_id, Mutex::new(next_id));
                },
                BucketEntryType::Bucket => {
                    let bucket = Bucket::decode(value.into_vec()).expect("unexpected error decoding bucket");
                    let key = bucket_key(bucket.org_id, &bucket.name);
                    bucket_map.insert(key, bucket);
                },
                BucketEntryType::NextBucketID => (),
            }
        }
        self.series_insert_lock = Arc::new(RwLock::new(id_mutex_map));
    }
}

/*
Index entries all have the prefix:
<org id><bucket id><IndexEntryType>

Index keeps the following entries (entry type is the first part). So key:value

series key to ID: <SeriesKeyToID><key>:<id>
ID to series key: <IDToSeriesKey><BigEndian u64 ID>:<key>
key posting list: <KeyPostingList><tag key><big endian collection number>:<roaring bitmap>
key/value posting list: <KeyValuePostingList><tag key><0x0><tag value><big endian collection number>:<roaring bitmap>
tag value map: <TagValueMap><tag key><0x0><tag value>:<BigEndian created unix seconds epoch>
*/

// IndexEntryType is used as a u8 prefix for any key in rocks for these different index entries
enum IndexEntryType {
    SeriesKeyToID,
    IDToSeriesKey,
    KeyPostingList,
    KeyValuePostingList,
    TagValueMap,
}

fn index_cf_options() -> Options {
    let mut options = Options::default();
    options.set_write_buffer_size(INDEX_CF_WRITE_BUFFER_SIZE);
    options
}

// index_cf_name returns the name of the column family for the given index duration at a given epoch time (in seconds)
fn index_cf_name(org_id: u32, bucket_id: u32, duration: u32, epoch: u64) -> String {
    if duration == 0 {
        return format!("index_{}_{}_{}", org_id, bucket_id, "0");
    }

    let duration = duration as u64;

    format!("index_{}_{}_{}_{}", org_id, bucket_id, duration, epoch / duration * duration)
}

fn index_series_key_id(series_key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(series_key.len() + 1);
    v.push(IndexEntryType::SeriesKeyToID as u8);
    v.append(&mut series_key.as_bytes().to_vec());
    v
}

// next_series_id_key gives the key in the buckets CF in rocks that holds the value for the next series ID
fn next_series_id_key(org_id: u32, bucket_id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(9);
    v.push(BucketEntryType::NextSeriesID as u8);
    v.write_u32::<BigEndian>(org_id).unwrap();
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v
}

enum BucketEntryType {
    Bucket,
    NextSeriesID,
    NextBucketID,
}

// TODO: ensure required fields are present and write tests
fn validate_bucket_fields(_bucket: &Bucket) -> Result<(), StorageError> {
    Ok(())
}
// returns the byte key to find this bucket in the buckets CF in rocks
fn bucket_key(org_id: u32, bucket_name: &String) -> Vec<u8> {
    let mut s = bucket_name.clone().into_bytes();
    let mut key = Vec::with_capacity(3 + s.len());
    key.push(BucketEntryType::Bucket as u8);
    key.write_u32::<BigEndian>(org_id).unwrap();
    key.append(&mut s);
    key
}

fn next_bucket_id_key() -> Vec<u8> {
    vec![BucketEntryType::NextBucketID as u8]
}

fn bucket_entry_type_from_byte(b: u8) -> BucketEntryType {
    unsafe { ::std::mem::transmute(b) }
}

fn bucket_cf_descriptor() -> ColumnFamilyDescriptor {
    let mut buckets_options = Options::default();
    buckets_options.set_write_buffer_size(BUCKET_CF_WRITE_BUFFER_SIZE);
    ColumnFamilyDescriptor::new(BUCKET_CF, buckets_options)
}

fn u32_from_bytes(b: &[u8]) -> u32 {
    let mut c = Cursor::new(b);
    c.read_u32::<BigEndian>().unwrap()
}

fn u32_to_bytes(val: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(4);
    v.write_u32::<BigEndian>(val).unwrap();
    v
}

impl Bucket {
    pub fn new(org_id: u32, name: String) -> Bucket {
        Bucket{
            org_id,
            id: 0,
            name,
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![
                IndexLevel{duration_seconds: 0, timezone: "EDT".to_string()},
            ],
        }
    }
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
        let mut bucket2 = Bucket::new(2, "Foo".to_string());
        {
            let mut db = test_database("create_and_get_buckets", true);
            let mut b = Bucket::new(org_id, "Foo".to_string());

            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(b.id, 1);
            let stored_bucket = db.get_bucket_by_name(org_id, &b.name).unwrap().unwrap();
            assert_eq!(b, stored_bucket);
            bucket = stored_bucket;

            // ensure it doesn't insert again
            let id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(id, 1);

            // ensure second bucket in another org
            bucket2.id = db.create_bucket_if_not_exists(bucket2.org_id, &bucket2).unwrap();
            assert_eq!(bucket2.id, 2);
            let stored2 = db.get_bucket_by_name(bucket2.org_id, &bucket2.name).unwrap().unwrap();
            assert_eq!(bucket2, stored2);

            // ensure second bucket gets new ID
            let mut b2 = Bucket::new(org_id, "two".to_string());
            b2.id = db.create_bucket_if_not_exists(org_id, &b2).unwrap();
            assert_eq!(b2.id, 3);
            let stored_bucket = db.get_bucket_by_name(org_id, &b2.name).unwrap().unwrap();
            assert_eq!(b2, stored_bucket);

            // TODO: ensure that a bucket orders levels correctly
        }

        // ensure it persists across database reload
        {
            let mut db = test_database("create_and_get_buckets", false);
            let stored_bucket = db.get_bucket_by_name(org_id, &bucket.name).unwrap().unwrap();
            assert_eq!(bucket, stored_bucket);

            // ensure a new bucket will get a new ID
            let mut b = Bucket::new(org_id, "asdf".to_string());
            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(b.id, 4);
        }
    }

    #[test]
    fn series_id_indexing() {
        let org_id = 23;
        let mut b = Bucket::new(org_id, "series".to_string());
        let mut b2 = Bucket::new(1, "series".to_string());
        let p1 = Point{series: "one".to_string(), value: 1, time: 0};
        let p2 = Point{series: "two".to_string(), value: 23, time: 40};
        let p3 = Point{series: "three".to_string(), value: 33, time: 86};
        let p4 = Point{series: "four".to_string(), value: 234, time: 100};

        {
            let mut db = test_database("series_id_indexing", true);
            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            b2.id = db.create_bucket_if_not_exists(b2.org_id, &b2).unwrap();

            let mut series = db.get_series_ids(org_id, &b, vec![p1.clone(), p2.clone()]);
            assert_eq!(series, vec![
                Series{id: None, point: p1.clone()},
                Series{id: None, point: p2.clone()},
            ]);

            db.insert_series_without_ids(org_id, &b, &mut series);
            assert_eq!(series, vec![
                Series{id: Some(1), point: p1.clone()},
                Series{id: Some(2), point: p2.clone()},
            ]);

            // now insert a new series and make sure it shows up
            series = db.get_series_ids(org_id, &b, vec![p1.clone(), p3.clone()]);
            assert_eq!(series, vec![
                Series{id: Some(1), point: p1.clone()},
                Series{id: None, point: p3.clone()},
            ]);

            db.insert_series_without_ids(org_id, &b, &mut series);
            assert_eq!(series, vec![
                Series{id: Some(1), point: p1.clone()},
                Series{id: Some(3), point: p3.clone()},
            ]);

            series = db.get_series_ids(b2.org_id, &b2, vec![p1.clone()]);
            assert_eq!(series, vec![Series{id: None, point: p1.clone()}]);

            // insert a series into the other org bucket
            db.insert_series_without_ids(b2.org_id, &b2, &mut series);
            assert_eq!(series, vec![Series{id: Some(1), point: p1.clone()}]);
        }

        // now make sure that a new series gets inserted properly after restart
        {
            let mut db = test_database("series_id_indexing", false);

            // check the first org
            let mut series = vec![Series{id: None, point: p4.clone()}];
            db.insert_series_without_ids(org_id, &b, &mut series);
            assert_eq!(series, vec![Series{id: Some(4), point: p4.clone()}]);
            assert_eq!(
                db.get_series_ids(org_id, &b, vec![p1.clone(), p2.clone(), p3.clone(), p4.clone()]),
                vec![
                    Series{id: Some(1), point: p1.clone()},
                    Series{id: Some(2), point: p2.clone()},
                    Series{id: Some(3), point: p3.clone()},
                    Series{id: Some(4), point: p4.clone()},
                ],
            );

            // check the second org
            series = vec![Series{id: None, point: p2.clone()}];
            db.insert_series_without_ids(b2.org_id, &b2, &mut series);
            assert_eq!(series, vec![Series{id: Some(2), point: p2.clone()}]);
            assert_eq!(
                db.get_series_ids(b2.org_id, &b2, vec![p1.clone(), p2.clone(), p3.clone()]),
                vec![
                    Series{id: Some(1), point: p1},
                    Series{id: Some(2), point: p2},
                    Series{id: None, point: p3},
                ],
            );
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
