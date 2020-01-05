use crate::line_parser::Point;
use crate::storage::iterators::{ReadPoint, SeriesIterator};
use crate::delorean::{Bucket, IndexLevel, Predicate, Node};
use crate::delorean::node::{Value, Comparison, Logical};

use std::{error, fmt};
use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use std::time::SystemTime;
use std::io::Cursor;

use rocksdb::{DB, IteratorMode, WriteBatch, Options, ColumnFamilyDescriptor, Direction, DBIterator};
use byteorder::{ByteOrder, BigEndian, WriteBytesExt, ReadBytesExt};
use prost::Message;
use croaring::Treemap;
use croaring::treemap::NativeSerializer;
use actix_web::ResponseError;
use actix_web::http::StatusCode;

/// Database wraps a RocksDB database for storing the raw series data, an inverted index of the
/// metadata and the metadata about what buckets exist in the system.
///
/// Each bucket gets a unique u32 ID assigned. This ID is unique across all orgs. Bucket names
/// are unique within an organization.
///
/// Series (measurement + tagset + field) are identified by a u64 ID that is unique within a bucket.
/// Each bucket keeps an incrementing counter for new series IDs.
pub struct Database {
    db: Arc<RwLock<DB>>,
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
            db: Arc::new(RwLock::new(db)),
            bucket_map: Arc::new(RwLock::new(HashMap::new())),
            series_insert_lock: Arc::new(RwLock::new(HashMap::new())),
        };
        database.load_bucket_map();

        database
    }

    /// write_points will write values into the database under the given org_id and bucket_name. It
    /// also inserts the series and their metadata into the index if not already present.
    /// It does no validation against the org_id. It will create the bucket with the default
    /// single index level of all time if it hasn't been created yet.
    ///
    /// # Arguments
    /// * org_id - the organization this data resides under
    /// * bucket_name - the string identifier of the bucket
    /// * points - individual values with their timestamps and series keys
    pub fn write_points(&self, org_id: u32, bucket_name: &str, points: Vec<Point>) -> Result<(), StorageError> {
        let key = bucket_key(org_id, bucket_name);

        let _ = self.create_default_bucket_if_not_exists(org_id, bucket_name, &key)?;
        let bucket_map = self.bucket_map.read().unwrap();
        let bucket = bucket_map.get(&key).unwrap();

        let mut series = self.get_series_ids(org_id, &bucket, points);
        self.insert_series_without_ids(org_id, &bucket, &mut series);

        let mut batch = WriteBatch::default();

        for s in series {
            let key = key_for_series_and_time(bucket.id, s.id.unwrap(), s.point.time);
            let mut value = Vec::with_capacity(8);
            value.write_i64::<BigEndian>(s.point.value).unwrap();
            batch.put(key, value).unwrap();
        }

        self.db.read().unwrap().write(batch).expect("unexpected RocksDB error");
        Ok(())
    }

    fn create_default_bucket_if_not_exists(&self, org_id: u32, bucket_name: &str, bucket_key: &[u8]) -> Result<u32, StorageError> {
        match self.bucket_map.read().unwrap().get(bucket_key) {
            Some(b) => return Ok(b.id),
            None => (),
        }

        let bucket = Bucket::new(org_id, bucket_name.to_string());
        self.create_bucket_if_not_exists(org_id, &bucket)
    }

    pub fn read_range<'a>(&self, org_id: u32, bucket_name: &str, range: &'a Range, predicate: &'a Predicate, _batch_size: usize) -> Result<SeriesIterator, StorageError> {
        let bucket = match self.get_bucket_by_name(org_id, bucket_name).unwrap() {
            Some(b) => b,
            None => return Err(StorageError{description: format!("bucket {} not found", bucket_name)}),
        };

        let series_filters = self.get_series_filters(&bucket, Some(&predicate), range)?;

        Ok(SeriesIterator::new(org_id, bucket.id, series_filters))
    }

    fn get_db_points_iter(&self, _org_id: u32, bucket_id: u32, series_id: u64, range: &Range, batch_size: usize) -> Result<PointsIterator, StorageError> {
        let prefix = prefix_for_series(bucket_id, series_id, range.start);
        let mode = IteratorMode::From(&prefix, Direction::Forward);
        let iter = self.db.read().unwrap().iterator(mode);

        Ok(PointsIterator::new(batch_size, iter, range.stop, prefix[0..12].to_vec()))
    }

    /// If the bucket name exists within an org, this function returns the ID (ignoring whether the
    /// bucket options are different than the one that exists). If it doesn't exist, this function
    /// creates the bucket and returns its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `bucket` - The bucket to create along with all of its configuration options. Ignores the ID.
    pub fn create_bucket_if_not_exists(&self, org_id: u32, bucket: &Bucket) -> Result<u32, StorageError> {
        validate_bucket_fields(bucket)?;

        let key = bucket_key(org_id, &bucket.name);
        if let Some(b) = self.bucket_map.read().unwrap().get(&key) {
            return Ok(b.id);
        }

        let mut map = self.bucket_map.write().unwrap();
        if let Some(b) = map.get(&key) {
            return Ok(b.id);
        }

        let db = self.db.read().unwrap();

        // assign the ID and insert the bucket
        let buckets = db.cf_handle(BUCKET_CF)
            .expect("unexpected rocksdb error while trying to get the buckets column family");

        let mut buf: Vec<u8> = vec![];
        let mut store = bucket.clone();

        // get the next bucket ID
        let next_id = match db.get_cf(buckets, next_bucket_id_key())
            .expect("unexpected rocksdb error while trying to get the next bucket id") {

            Some(val) => u32_from_bytes(&val),
            None => 1,
        };

        store.id = next_id;
        store.encode(&mut buf).expect("unexpected error encoding bucket");

        // write the bucket and the next ID counter atomically
        let mut batch = WriteBatch::default();
        batch.put_cf(&buckets, &key, buf).unwrap();
        batch.put_cf(&buckets, next_bucket_id_key(), u32_to_bytes(store.id + 1)).unwrap();
        db.write(batch).expect("unexpected rocksdb error writing to DB");

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
        let db = self.db.read().unwrap();
        let buckets = db.cf_handle(BUCKET_CF).unwrap();

        match db.get_cf(buckets, bucket_key(org_id, &name.to_string())) {
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
    pub fn get_series_ids(&self, _org_id: u32, bucket: &Bucket, points: Vec<Point>) -> Vec<Series> {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();

        let series = points.into_iter().map(|p| {
            let mut series = Series{id: None, point: p};
            let level = &bucket.index_levels[0];
            let cf_name = index_cf_name(bucket.id,level.duration_seconds, now);
            series.id = self.get_series_id(&cf_name, &series.point.series);
            series
        }).collect();

        series
    }

    // TODO: create test with different data and predicates loaded to ensure it hits the index properly
    // TODO: refactor this to return an iterator so queries with many series don't materialize all at once
    // TODO: wire up the time range part of this
    /// get_series_filters returns a collection of series and associated value filters that can be used
    /// to iterate over raw tsm data. The predicate passed in is the same as that used in the Go based
    /// storage layer.
    pub fn get_series_filters(&self, bucket: &Bucket, predicate: Option<&Predicate>, range: &Range) -> Result<Vec<SeriesFilter>, StorageError> {
        if let Some(pred) = predicate {
            if let Some(root) = &pred.root {
                let map = self.evaluate_node(bucket, &root, range)?;
                let mut filters = Vec::with_capacity(map.cardinality() as usize);

                for id in map.iter() {
                    let key = self.get_series_key_by_id(&bucket, id, &range)?;
                    filters.push(SeriesFilter{id, key, value_predicate: None});
                }

                return Ok(filters);
            }
        }

        // TODO: return list of all series
        Err(StorageError{description: "get for all series ids not wired up yet".to_string()})
    }

    fn get_series_key_by_id(&self, bucket: &Bucket, id: u64, _range: &Range) -> Result<String, StorageError> {
        let index_level = bucket.index_levels.get(0).unwrap(); // TODO: find the right index based on range
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(bucket.id, index_level.duration_seconds, now);
        let db = self.db.read().unwrap();

        match db.cf_handle(&cf_name) {
            Some(cf) => {
                match db.get_cf(cf, index_series_id_from_id(id)).unwrap() {
                    Some(val) => {
                        Ok(std::str::from_utf8(&val).unwrap().to_owned())
                    },
                    None => Err(StorageError{description: "series id not found".to_string()})
                }
            },
            None => Err(StorageError{description: "unable to find index".to_string()}),
        }
    }

    fn evaluate_node(&self, bucket: &Bucket, n: &Node, range: &Range) -> Result<Treemap, StorageError> {
        if n.children.len() != 2 {
            return Err(StorageError{description: format!("expected only two children of node but found {}", n.children.len())})
        }

        match &n.value {
            Some(node_value) => {
                match node_value {
                    Value::Logical(l) => {
                        let l = Logical::from_i32(*l).unwrap();
                        self.evaluate_logical(bucket, &n.children[0], &n.children[1], l, range)
                    },
                    Value::Comparison(c) => {
                        let c = Comparison::from_i32(*c).unwrap();
                        self.evaluate_comparison(bucket, &n.children[0], &n.children[1], c, range)
                    },
                    val => Err(StorageError{description: format!("evaluate_node called on wrong type {:?}", val)}),
                }
            },
            None => Err(StorageError{description: "emtpy node value".to_string()}),
        }
    }

    fn evaluate_logical(&self, bucket: &Bucket, left: &Node, right: &Node, op: Logical, range: &Range) -> Result<Treemap, StorageError> {
        let mut left_result = self.evaluate_node(bucket, left, range)?;
        let right_result = self.evaluate_node(bucket, right, range)?;

        match op {
            Logical::And => left_result.and_inplace(&right_result),
            Logical::Or => left_result.or_inplace(&right_result),
        };

        Ok(left_result)
    }

    fn evaluate_comparison(&self, bucket: &Bucket, left: &Node, right: &Node, op: Comparison, range: &Range) -> Result<Treemap, StorageError> {
        let left = match &left.value {
            Some(Value::TagRefValue(s)) => s,
            _ => return Err(StorageError{description: "expected left operand to be a TagRefValue".to_string()}),
        };

        let right = match &right.value {
            Some(Value::StringValue(s)) => s,
            _ => return Err(StorageError{description: "unable to run comparison against anything other than a string".to_string()}),
        };

        match op {
            Comparison::Equal => {
                return self.get_posting_list_for_tag_key_value(bucket, &left, &right, range);
            },
            comp => return Err(StorageError{description: format!("unable to handle comparison {:?}", comp)}),
        }
    }

    fn get_posting_list_for_tag_key_value(&self, bucket: &Bucket, key: &str, value: &str, _range: &Range) -> Result<Treemap, StorageError> {
        // first get the cf for this index
        let index_level = bucket.index_levels.get(0).unwrap(); // TODO: find the right index based on range
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(bucket.id, index_level.duration_seconds, now);
        let db = self.db.read().unwrap();

        match db.cf_handle(&cf_name) {
            Some(cf) => {
                match db.get_cf(cf, index_key_value_posting_list(bucket.id, key, value)).unwrap() {
                    Some(val) => {
                        let map = Treemap::deserialize(&val).expect("unexpected error deserializing tree map");
                        Ok(map)
                    },
                    None => Ok(Treemap::create()),
                }
            },
            None => Err(StorageError{description: "unable to find index".to_string()}),
        }
    }

    // TODO: handle predicate
    pub fn get_tag_keys(&self, bucket: &Bucket, _predicate: Option<&Predicate>, _range: &Range) -> Vec<String> {
        let index_level = bucket.index_levels.get(0).unwrap(); // TODO: find the right index based on range
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(bucket.id, index_level.duration_seconds, now);

        let mut keys = vec![];

        let db = self.db.read().unwrap();

        match db.cf_handle(&cf_name) {
            Some(index) => {
                let prefix = index_tag_key_prefix(bucket.id);
                let mode = IteratorMode::From(&prefix, Direction::Forward);
                let iter = db.iterator_cf(index, mode)
                    .expect("unexpected rocksdb error getting iterator for index");

                for (key, _) in iter {
                    if !key.starts_with(&prefix) {
                        break;
                    }

                    let k = std::str::from_utf8(&key[prefix.len()..]).unwrap(); // TODO: determine what we want to do with errors
                    keys.push(k.to_string());
                }
            },
            None => (),
        }

        keys
    }

    pub fn get_tag_values(&self, bucket: &Bucket, tag: &str, _predicate: Option<&Predicate>, _range: &Range) -> Vec<String> {
        let index_level = bucket.index_levels.get(0).unwrap(); // TODO: find the right index based on range
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(bucket.id, index_level.duration_seconds, now);

        let db = self.db.read().unwrap();
        let mut values = vec![];

        match db.cf_handle(&cf_name) {
            Some(index) => {
                let prefix = index_tag_key_value_prefix(bucket.id, tag);
                let mode = IteratorMode::From(&prefix, Direction::Forward);
                let iter = db.iterator_cf(index, mode)
                    .expect("unexpected rocksdb error getting iterator for index");

                for (key, _) in iter {
                    if !key.starts_with(&prefix) {
                        break;
                    }

                    let v = std::str::from_utf8(&key[prefix.len()..]).unwrap(); // TODO: determine what to do with errors
                    values.push(v.to_string());
                }
            },
            None => (),
        }

        values
    }

    // ensure_series_mutex_exists makes sure that the passed in bucket id has a mutex, which is used
    // when inserting new series into a bucket
    fn ensure_series_mutex_exists(&self, bucket_id: u32) {
        let map = self.series_insert_lock.read().expect("mutex poisoned");

        if let Some(_next_id_mutex) = map.get(&bucket_id) {
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
    pub fn insert_series_without_ids(&self, org_id: u32, bucket: &Bucket, series: &mut Vec<Series>) {
        // We want to get a lock on new series only for this bucket
        self.ensure_series_mutex_exists(bucket.id);
        let map = self.series_insert_lock.read().expect("mutex poisoned");
        let next_id = map.get(&bucket.id).expect("should exist because of call to ensure_series_mutex_exists");
        let mut next_id = next_id.lock().expect("mutex poisoned");

        let mut batch = WriteBatch::default();

        // create the column family to store the index if it doesn't exist
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let cf_name = index_cf_name(bucket.id, bucket.index_levels[0].duration_seconds, now);
        let index_exists = match self.db.read().unwrap().cf_handle(&cf_name) {
            Some(_) => true,
            None => false,
        };

        if !index_exists {
            self.db.write().unwrap().create_cf(&cf_name, &index_cf_options()).unwrap();
        }

        let db = self.db.read().unwrap();
        let index_cf = db.cf_handle(&cf_name).expect("index column family should have already been inserted");

        // Keep an in memory map for updating multiple index entries at a time
        let mut index_map: HashMap<Vec<u8>, Treemap> = HashMap::new();
        let mut series_id_map: HashMap<String, u64> = HashMap::new();

        // now loop through the series and insert the index entries into the map
        for series in series {
            // don't bother with series in the collection that already have IDs
            if let Some(_) = series.id {
                continue;
            }

            // if we've already put this series in the map in this write, skip it
            if let Some(id) = series_id_map.get(&series.point.series) {
                series.id = Some(*id);
                continue;
            }

            // now that we have the mutex on series, make sure these weren't inserted in some other thread
            if let Some(_) = self.get_series_id(&cf_name, &series.point.series) {
                continue;
            }

            series.id = Some(*next_id);
            let id = *next_id;
            let mut series_id = Vec::with_capacity(8);
            series_id.write_u64::<BigEndian>(*next_id).unwrap();
            batch.put_cf(index_cf, index_series_key_id(&series.point.series), series_id.clone()).unwrap();
            batch.put_cf(index_cf, index_series_id(&series_id), &series.point.series.as_bytes()).unwrap();
            series_id_map.insert(series.point.series.clone(), *next_id);
            *next_id += 1;

            // insert the index entries
            // TODO: do the error handling bits, but how to handle? Should all series be validated before
            //       and fail the whole write if any one is bad, or insert the ones we can and ignore and log the bad?

            let pairs = series.point.index_pairs().unwrap();
            for pair in pairs {
                // insert the tag key index
                batch.put_cf(index_cf, index_tag_key(bucket.id, &pair.key), vec![0 as u8]).unwrap();

                // insert the tag value index
                batch.put_cf(index_cf, index_tag_key_value(bucket.id, &pair.key, &pair.value), vec![0 as u8]).unwrap();

                // update the key to id bitmap
                let index_key_posting_list_key = index_key_posting_list(bucket.id, &pair.key).to_vec();

                // put it in the temporary in memory map for a single write update later
                match index_map.get_mut(&index_key_posting_list_key) {
                    Some(tree) => {
                        tree.add(id);
                    },
                    None => {
                        let mut map = match self.db.read().unwrap().get_cf(index_cf, &index_key_posting_list_key).unwrap() {
                            Some(b) => {
                                Treemap::deserialize(&b).expect("unexpected error deserializing posting list")
                            },
                            None => Treemap::create(),
                        };
                        map.add(id);
                        index_map.insert(index_key_posting_list_key.clone(), map);
                    }
                };

                // update the key/value to id bitmap
                let index_key_value_posting_list_key = index_key_value_posting_list(bucket.id, &pair.key, &pair.value).to_vec();

                match index_map.get_mut(&index_key_value_posting_list_key) {
                    Some(tree) => {
                        tree.add(id);
                    },
                    None => {
                        let mut map = match self.db.read().unwrap().get_cf(index_cf, &index_key_value_posting_list_key).unwrap() {
                            Some(b) => {
                                Treemap::deserialize(&b).expect("unexpected error deserializing posting list")
                            },
                            None => Treemap::create(),
                        };
                        map.add(id);
                        index_map.insert(index_key_value_posting_list_key.clone(), map);
                    }
                }
            }
        }

        // do the index writes from the in temporary in memory map
        for (k, v) in index_map.iter() {
            let _ = batch.put_cf(index_cf, k, v.serialize().unwrap());
        }

        // save the next series id
        let bucket_cf = db.cf_handle(BUCKET_CF).unwrap();
        let mut next_series_id_val = Vec::with_capacity(8);
        next_series_id_val.write_u64::<BigEndian>(*next_id).unwrap();
        let _ = batch.put_cf(bucket_cf, next_series_id_key(org_id, bucket.id), next_series_id_val);

        db.write(batch).expect("unexpected rocksdb error");
    }

    fn get_series_id(&self, cf_name: &str, series_key: &str) -> Option<u64> {
        // this column family might not exist if this index hasn't been created yet
        if let Some(cf) = self.db.read().unwrap().cf_handle(cf_name) {
            if let Some(val) = self.db.read().unwrap().get_cf(cf, index_series_key_id(series_key)).expect("unexpected rocksdb error") {
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
        let db = self.db.read().unwrap();

        let buckets = db.cf_handle(BUCKET_CF).unwrap();
        let prefix = &[BucketEntryType::Bucket as u8];
        let iter = db.iterator_cf(&buckets, IteratorMode::From(prefix, Direction::Forward)).unwrap();

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

this one is for show keys or show values where key = value queries
tag value map: <TagValueMap><tag key><0x0><tag value><0x0><tag key 2><0x0><tag value 2>:<BigEndian created unix seconds epoch>
*/

/*

TODO: The index todo list
1. no predicate (return all series)
2. starts with
3. regex match
4. not equal
5. not regex
6. value matches
7. convert series/predicate matcher to return iterator over SeriesFilter
8. index levels

TODO: other pieces
  - API endpoint to delete old series data
  - API endpoint to delete old indexes
  - API endpoint to run tsm compaction
  - Write/read other data types
  - Buckets backed by alternate storage
  - Meta store abstracted from RocksDB
  - Index abstracted to Trait
  - Raw data iterator abstracted to Trait

*/

#[allow(dead_code)]
enum SeriesDataType {
    Int64,
    Float64,
    UInt64,
    String,
    Bool,
}

fn prefix_for_series(bucket_id: u32, series_id: u64, start_time: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.write_u64::<BigEndian>(series_id).unwrap();
    v.write_i64::<BigEndian>(start_time).unwrap();
    v
}

pub struct PointsIterator<'a> {
    batch_size: usize,
    iter: DBIterator<'a>,
    stop_time: i64,
    series_prefix: Vec<u8>,
    drained: bool,
}

impl PointsIterator<'_> {
    pub fn new(batch_size: usize, iter: DBIterator, stop_time: i64, series_prefix: Vec<u8>) -> PointsIterator {
        PointsIterator{
            batch_size,
            iter,
            stop_time,
            series_prefix,
            drained: false,
        }
    }

    pub fn new_from_series_filter<'a>(org_id: u32, bucket_id: u32, db: &'a Database, series_filter: &'a SeriesFilter, range: &Range, batch_size: usize) -> Result<PointsIterator<'a>, StorageError> {
        db.get_db_points_iter(org_id, bucket_id, series_filter.id, range, batch_size)
    }
}

impl Iterator for PointsIterator<'_> {
    type Item = Vec<ReadPoint<i64>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.drained {
            return None;
        }

        let mut v = Vec::with_capacity(self.batch_size);
        let mut n = 0;

        // we have to check if the iterator is still valid. There are some edge cases where
        // this function could get called with an invalid iterator because it has gone to
        // the end of th rocksdb keyspace. Calling next on it segfaults the program, so check it first.
        // Here's the issue: https://github.com/rust-rocksdb/rust-rocksdb/issues/361
        if !self.iter.valid() {
            self.drained = true;
            return None;
        }
        while let Some((key, value)) = self.iter.next() {
            if !key.starts_with(&self.series_prefix) {
                self.drained = true;
                break;
            }

            let time = BigEndian::read_i64(&key[12..]);
            if time > self.stop_time {
                self.drained = true;
                break;
            }

            let point = ReadPoint{
                value: BigEndian::read_i64(&value),
                time,
            };

            v.push(point);

            n += 1;
            if n >= self.batch_size {
                break;
            }
        }

        if v.is_empty() {
            self.drained = true;
            None
        } else {
            Some(v)
        }
    }
}

// IndexEntryType is used as a u8 prefix for any key in rocks for these different index entries
enum IndexEntryType {
    SeriesKeyToID,
    IDToSeriesKey,
    KeyPostingList,
    KeyValuePostingList,
    KeyList,
    KeyValueList,
}

fn index_cf_options() -> Options {
    let mut options = Options::default();
    options.set_write_buffer_size(INDEX_CF_WRITE_BUFFER_SIZE);
    options
}

// index_cf_name returns the name of the column family for the given index duration at a given epoch time (in seconds)
fn index_cf_name(bucket_id: u32, duration: u32, epoch: u64) -> String {
    if duration == 0 {
        return format!("index_{}_{}", bucket_id, "0");
    }

    let duration = duration as u64;

    format!("index_{}_{}_{}", bucket_id, duration, epoch / duration * duration)
}

fn index_series_key_id(series_key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(series_key.len() + 1);
    v.push(IndexEntryType::SeriesKeyToID as u8);
    v.append(&mut series_key.as_bytes().to_vec());
    v
}

fn index_series_id(id: &Vec<u8>) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 1);
    v.push(IndexEntryType::IDToSeriesKey as u8);
    v.append(&mut id.clone());
    v
}

fn index_series_id_from_id(id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 1);
    v.push(IndexEntryType::IDToSeriesKey as u8);
    v.write_u64::<BigEndian>(id).unwrap();
    v
}

fn index_tag_key(bucket_id: u32, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 5);
    v.push(IndexEntryType::KeyList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.append(&mut key.as_bytes().to_vec());
    v
}

fn index_tag_key_prefix(bucket_id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.push(IndexEntryType::KeyList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v
}

fn index_tag_key_value(bucket_id: u32, key: &str, value: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + value.len() + 6);
    v.push(IndexEntryType::KeyValueList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.append(&mut key.as_bytes().to_vec());
    v.push(0 as u8);
    v.append(&mut value.as_bytes().to_vec());
    v
}

fn index_tag_key_value_prefix(bucket_id: u32, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 6);
    v.push(IndexEntryType::KeyValueList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.append(&mut key.as_bytes().to_vec());
    v.push(0 as u8);
    v
}

fn index_key_posting_list(bucket_id: u32, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 6);
    v.push(IndexEntryType::KeyPostingList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.append(&mut key.as_bytes().to_vec());
    v
}

fn index_key_value_posting_list(bucket_id: u32, key: &str, value: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + value.len() + 6);
    v.push(IndexEntryType::KeyValuePostingList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.append(&mut key.as_bytes().to_vec());
    v.push(0 as u8);
    v.append(&mut value.as_bytes().to_vec());
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
fn bucket_key(org_id: u32, bucket_name: &str) -> Vec<u8> {
    let mut s = bucket_name.as_bytes().to_vec();
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

fn key_for_series_and_time(bucket_id: u32, series_id: u64, timestamp: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.write_u64::<BigEndian>(series_id).unwrap();
    v.write_i64::<BigEndian>(timestamp).unwrap();
    v
}

#[derive(Debug, PartialEq, Clone)]
pub struct SeriesFilter {
    pub id: u64,
    pub key: String,
    pub value_predicate: Option<Predicate>,
}

pub struct Range {
    pub start: i64,
    pub stop: i64,
}

#[derive(Debug, Clone)]
pub struct StorageError {
    pub description: String,
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

impl ResponseError for StorageError {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dotenv::dotenv;
    use std::env;

    use crate::storage::predicate::parse_predicate;

    #[test]
    fn create_and_get_buckets() {
        let bucket: Bucket;
        let org_id = 1;
        let mut bucket2 = Bucket::new(2, "Foo".to_string());
        {
            let db = test_database("create_and_get_buckets", true);
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
            let db = test_database("create_and_get_buckets", false);
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
            let db = test_database("series_id_indexing", true);
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
            let db = test_database("series_id_indexing", false);

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

    #[test]
    fn series_metadata_indexing() {
        let mut bucket = Bucket::new(1, "foo".to_string());
        let db = test_database("series_metadata_indexing", true);
        let p1 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 0};
        let p2 = Point{series: "cpu,host=a,region=west\tusage_system".to_string(), value: 1, time: 0};
        let p3 = Point{series: "cpu,host=a,region=west\tusage_user".to_string(), value: 1, time: 0};
        let p4 = Point{series: "mem,host=b,region=west\tfree".to_string(), value: 1, time: 0};

        bucket.id = db.create_bucket_if_not_exists(bucket.org_id, &bucket).unwrap();
        let mut series = db.get_series_ids(bucket.org_id, &bucket, vec![p1.clone(), p2.clone(), p3.clone(), p4.clone()]);
        db.insert_series_without_ids(bucket.org_id, &bucket, &mut series);

        let range = Range{start:0, stop: std::i64::MAX};
        let tag_keys = db.get_tag_keys(&bucket, None, &range);
        assert_eq!(tag_keys, vec!["_f", "_m", "host", "region"]);

        let tag_values = db.get_tag_values(&bucket, "host", None, &range);
        assert_eq!(tag_values, vec!["a", "b"]);

        // get all series

        // get series with measurement = mem
        let pred = parse_predicate("_m = \"cpu\"").unwrap();
        let series = db.get_series_filters(&bucket, Some(&pred), &range).unwrap();
        assert_eq!(series, vec![
            SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None},
            SeriesFilter{id: 2, key: "cpu,host=a,region=west\tusage_system".to_string(), value_predicate: None},
            SeriesFilter{id: 3, key: "cpu,host=a,region=west\tusage_user".to_string(), value_predicate: None},
        ]);

        // get series with host = a
        let pred = parse_predicate("host = \"a\"").unwrap();
        let series = db.get_series_filters(&bucket, Some(&pred), &range).unwrap();
        assert_eq!(series, vec![
            SeriesFilter{id: 2, key: "cpu,host=a,region=west\tusage_system".to_string(), value_predicate: None},
            SeriesFilter{id: 3, key: "cpu,host=a,region=west\tusage_user".to_string(), value_predicate: None},
        ]);

        // get series with measurement = cpu and host = b
        let pred = parse_predicate("_m = \"cpu\" and host = \"b\"").unwrap();
        let series = db.get_series_filters(&bucket, Some(&pred), &range).unwrap();
        assert_eq!(series, vec![
            SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None},
        ]);

        let pred = parse_predicate("host = \"a\" OR _m = \"mem\"").unwrap();
        let series = db.get_series_filters(&bucket, Some(&pred), &range).unwrap();
        assert_eq!(series, vec![
            SeriesFilter{id: 2, key: "cpu,host=a,region=west\tusage_system".to_string(), value_predicate: None},
            SeriesFilter{id: 3, key: "cpu,host=a,region=west\tusage_user".to_string(), value_predicate: None},
            SeriesFilter{id: 4, key: "mem,host=b,region=west\tfree".to_string(), value_predicate: None},
        ]);
    }

    #[test]
    fn write_creates_bucket() {
        let b1 = Bucket::new(1, "bucket1".to_string());
        let db = test_database("write_creates_bucket", true);

        let p1 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 1};
        let p2 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 2};

        db.write_points(b1.org_id, &b1.name, vec![p1, p2]).unwrap();
        assert_eq!(db.get_bucket_by_name(b1.org_id, &b1.name).unwrap().unwrap().id, 1);
    }

    #[test]
    fn catch_rocksdb_iterator_segfault() {
        let mut b1 = Bucket::new(1, "bucket1".to_string());
        let db = test_database("catch_rocksdb_iterator_segfault", true);

        let p1 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 1};

        b1.id = db.create_bucket_if_not_exists(b1.org_id, &b1).unwrap();

        db.write_points(b1.org_id, &b1.name, vec![p1.clone()]).unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = Range{start: 1, stop: 4};
        let pred = parse_predicate("_m = \"cpu\"").unwrap();
        let mut iter = db.read_range(b1.org_id, &b1.name, &range, &pred, 10).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None});
        assert_eq!(iter.next(), None);
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 1, value: 1},
        ]);
        assert_eq!(points_iter.next(), None);
    }

    #[test]
    fn write_and_read_points() {
        let mut b1 = Bucket::new(1, "bucket1".to_string());
        let mut b2 = Bucket::new(2, "bucket2".to_string());
        let db = test_database("write_and_read_points", true);

        let p1 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 1};
        let p2 = Point{series: "cpu,host=b,region=west\tusage_system".to_string(), value: 1, time: 2};
        let p3 = Point{series: "mem,host=b,region=west\tfree".to_string(), value: 1, time: 2};
        let p4 = Point{series: "mem,host=b,region=west\tfree".to_string(), value: 1, time: 4};

        b1.id = db.create_bucket_if_not_exists(b1.org_id, &b1).unwrap();
        b2.id = db.create_bucket_if_not_exists(b2.org_id, &b2).unwrap();

        db.write_points(b1.org_id, &b1.name, vec![p1.clone(), p2.clone()]).unwrap();
        db.write_points(b2.org_id, &b2.name, vec![p1.clone(), p2.clone(), p3.clone(), p4.clone()]).unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = Range{start: 1, stop: 4};
        let pred = parse_predicate("_m = \"cpu\" OR _m = \"mem\"").unwrap();
        let mut iter = db.read_range(b1.org_id, &b1.name, &range, &pred, 10).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None});
        assert_eq!(iter.next(), None);
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 1, value: 1},
            ReadPoint{time: 2, value: 1},
        ]);
        assert_eq!(points_iter.next(), None);

        // test that we'll read multiple series
        let pred = parse_predicate("_m = \"cpu\" OR _m = \"mem\"").unwrap();
        let mut iter = db.read_range(b2.org_id, &b2.name, &range, &pred, 10).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None});
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 1, value: 1},
            ReadPoint{time: 2, value: 1},
        ]);

        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 2, key: "mem,host=b,region=west\tfree".to_string(), value_predicate: None});
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 2, value: 1},
            ReadPoint{time: 4, value: 1},
        ]);

        // test that the batch size is honored
        let pred = parse_predicate("host = \"b\"").unwrap();
        let mut iter = db.read_range(b1.org_id, &b1.name, &range, &pred, 1).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None});
        assert_eq!(iter.next(), None);
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 1).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 1, value: 1},
        ]);
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 2, value: 1},
        ]);

        // test that the time range is properly limiting
        let range = Range{start: 2, stop: 3};
        let pred = parse_predicate("_m = \"cpu\" OR _m = \"mem\"").unwrap();
        let mut iter = db.read_range(b2.org_id, &b2.name, &range, &pred, 10).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 1, key: "cpu,host=b,region=west\tusage_system".to_string(), value_predicate: None});
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 2, value: 1},
        ]);

        let series_filter = iter.next().unwrap();
        assert_eq!(series_filter, SeriesFilter{id: 2, key: "mem,host=b,region=west\tfree".to_string(), value_predicate: None});
        let mut points_iter = PointsIterator::new_from_series_filter(iter.org_id, iter.bucket_id, &db, &series_filter, &range, 10).unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![
            ReadPoint{time: 2, value: 1},
        ]);
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
