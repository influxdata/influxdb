use crate::delorean::{Bucket, IndexLevel, Node, Predicate, TimestampRange};
use crate::line_parser::PointType;
use crate::storage::config_store::ConfigStore;
use crate::storage::inverted_index::{InvertedIndex, SeriesFilter};
use crate::storage::predicate::{Evaluate, EvaluateVisitor};
use crate::storage::series_store::{ReadPoint, SeriesStore};
use crate::storage::{SeriesDataType, StorageError};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::io::Cursor;
use std::sync::{Arc, Mutex, RwLock};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use croaring::treemap::NativeSerializer;
use croaring::Treemap;
use prost::Message;
use rocksdb::{
    ColumnFamilyDescriptor, DBIterator, Direction, IteratorMode, Options, WriteBatch, DB,
};

/// Database wraps a RocksDB database for storing the raw series data, an inverted index of the
/// metadata and the metadata about what buckets exist in the system.
///
/// Each bucket gets a unique u32 ID assigned. This ID is unique across all orgs. Bucket names
/// are unique within an organization.
///
/// Series (measurement + tagset + field) are identified by a u64 ID that is unique within a bucket.
/// Each bucket keeps an incrementing counter for new series IDs.
pub struct RocksDB {
    db: Arc<RwLock<DB>>,
    // bucket_map is an in memory map of what buckets exist in the system. the key is the org id and bucket name together as bytes
    bucket_map: Arc<RwLock<HashMap<Vec<u8>, Arc<Bucket>>>>,
    // `bucket_id_map` is an in-memory map of bucket IDs to buckets that exist in the system.
    bucket_id_map: Arc<RwLock<HashMap<u32, Arc<Bucket>>>>,
    // series_insert_lock is a map of mutexes for creating new series in each bucket. Bucket ids are unique across all orgs
    series_insert_lock: Arc<RwLock<HashMap<u32, Mutex<u64>>>>,
}

const BUCKET_CF: &str = "buckets";
const BUCKET_CF_WRITE_BUFFER_SIZE: usize = 1024 * 1024; // 1MB
const INDEX_CF_WRITE_BUFFER_SIZE: usize = 10 * 1024 * 1024; // 10MB

impl RocksDB {
    pub fn new(dir: &str) -> RocksDB {
        let mut opts = Options::default();

        // create the database and missing column families
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // ensure rocks uses more than one thread for compactions/etc
        let core_count = num_cpus::get();
        opts.increase_parallelism(core_count as i32);

        // ensure the buckets CF exists and open up any index CFs
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = match DB::list_cf(&opts, dir) {
            Ok(names) => names
                .into_iter()
                .map(|name| {
                    if name == BUCKET_CF {
                        bucket_cf_descriptor()
                    } else {
                        ColumnFamilyDescriptor::new(&name, index_cf_options())
                    }
                })
                .collect(),
            Err(_) => vec![bucket_cf_descriptor()],
        };

        let db = DB::open_cf_descriptors(&opts, dir, cf_descriptors).unwrap();

        let mut database = RocksDB {
            db: Arc::new(RwLock::new(db)),
            bucket_map: Arc::new(RwLock::new(HashMap::new())),
            bucket_id_map: Arc::new(RwLock::new(HashMap::new())),
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
    /// * bucket_id - the globally unique bucket id
    /// * points - individual values with their timestamps, series keys, and series IDs
    pub fn write_points(&self, bucket_id: u32, points: &[PointType]) -> Result<(), StorageError> {
        // TODO: validate bucket exists?

        let mut batch = WriteBatch::default();

        for p in points {
            let id = p.series_id().ok_or_else(|| StorageError {
                description: format!("point {:?} had no series id", p),
            })?;

            let key = key_for_series_and_time(bucket_id, id, p.time());
            let value = p.to_rocks_db_bytes();

            batch.put(key, value).unwrap();
        }

        self.db
            .read()
            .unwrap()
            .write(batch)
            .expect("unexpected RocksDB error");
        Ok(())
    }

    // TODO: update this so it decompresses at least the first point to verify the data type or return error
    fn read_range<T: 'static + FromBytes + Clone + Send>(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<T>>> + Send>, StorageError> {
        let (iter, series_prefix) = self.get_db_points_iter(bucket_id, series_id, range.start);

        Ok(Box::new(PointsIterator {
            batch_size,
            iter,
            stop_time: range.end,
            series_prefix,
            drained: false,
            read: FromBytes::from,
        }))
    }

    fn get_db_points_iter<'a>(
        &self,
        bucket_id: u32,
        series_id: u64,
        start: i64,
    ) -> (DBIterator<'a>, Vec<u8>) {
        let prefix = prefix_for_series(bucket_id, series_id, start);
        let mode = IteratorMode::From(&prefix, Direction::Forward);

        let iter = self.db.read().unwrap().iterator(mode);
        let prefix = prefix[0..12].to_vec();

        (iter, prefix)
    }

    /// If the bucket name exists within an org, this function returns the ID (ignoring whether the
    /// bucket options are different than the one that exists). If it doesn't exist, this function
    /// creates the bucket and returns its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `bucket` - The bucket to create along with all of its configuration options. Ignores the ID.
    pub fn create_bucket_if_not_exists(
        &self,
        org_id: u32,
        bucket: &Bucket,
    ) -> Result<u32, StorageError> {
        validate_bucket_fields(bucket)?;

        let key = bucket_key(org_id, &bucket.name);
        if let Some(b) = self.bucket_map.read().unwrap().get(&key) {
            return Ok(b.id);
        }

        let mut map = self.bucket_map.write().unwrap();
        let mut id_map = self.bucket_id_map.write().unwrap();
        if let Some(b) = map.get(&key) {
            return Ok(b.id);
        }

        let db = self.db.read().unwrap();

        // assign the ID and insert the bucket
        let buckets = db
            .cf_handle(BUCKET_CF)
            .expect("unexpected rocksdb error while trying to get the buckets column family");

        let mut buf: Vec<u8> = vec![];
        let mut store = bucket.clone();

        // get the next bucket ID
        let next_id = match db
            .get_cf(buckets, next_bucket_id_key())
            .expect("unexpected rocksdb error while trying to get the next bucket id")
        {
            Some(val) => u32_from_bytes(&val),
            None => 1,
        };

        store.id = next_id;
        store
            .encode(&mut buf)
            .expect("unexpected error encoding bucket");

        // write the bucket and the next ID counter atomically
        let mut batch = WriteBatch::default();
        batch.put_cf(&buckets, &key, buf).unwrap();
        batch
            .put_cf(&buckets, next_bucket_id_key(), (store.id + 1).to_be_bytes())
            .unwrap();
        db.write(batch)
            .expect("unexpected rocksdb error writing to DB");

        let id = store.id;
        let arc_bucket = Arc::new(store);
        map.insert(key, arc_bucket.clone());
        id_map.insert(id, arc_bucket);

        Ok(id)
    }

    /// Looks up the bucket object by org id and name and returns it.
    ///
    /// # Arguments
    ///
    /// * `org_id` - The organization this bucket is under
    /// * `name` - The name of the bucket (which is unique under an organization)
    pub fn get_bucket_by_name(
        &self,
        org_id: u32,
        name: &str,
    ) -> Result<Option<Arc<Bucket>>, StorageError> {
        let buckets = self.bucket_map.read().unwrap();
        let key = bucket_key(org_id, name);
        Ok(buckets.get(&key).map(Arc::clone))
    }

    /// Looks up the bucket object by bucket id and returns it.
    ///
    /// # Arguments
    ///
    /// * `bucket_id` - The ID of the bucket (which is globally unique)
    pub fn get_bucket_by_id(&self, bucket_id: u32) -> Result<Option<Arc<Bucket>>, StorageError> {
        let buckets = self.bucket_id_map.read().unwrap();
        Ok(buckets.get(&bucket_id).map(Arc::clone))
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
    pub fn get_series_ids(
        &self,
        bucket_id: u32,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        let cf_name = index_cf_name(bucket_id);

        for point in points {
            if let Some(id) = self.get_series_id(&cf_name, &point.series()) {
                point.set_series_id(id);
            }
        }

        Ok(())
    }

    // TODO: create test with different data and predicates loaded to ensure it hits the index properly
    // TODO: refactor this to return an iterator so queries with many series don't materialize all at once
    // TODO: wire up the time range part of this
    /// get_series_filters returns a collection of series and associated value filters that can be used
    /// to iterate over raw tsm data. The predicate passed in is the same as that used in the Go based
    /// storage layer.
    pub fn get_series_filters(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<SeriesFilter>, StorageError> {
        if let Some(pred) = predicate {
            if let Some(root) = &pred.root {
                let map = self.evaluate_node(bucket_id, &root)?;
                let mut filters = Vec::with_capacity(map.cardinality() as usize);

                for id in map.iter() {
                    let (key, series_type) = self.get_series_key_and_type_by_id(bucket_id, id)?;
                    filters.push(SeriesFilter {
                        id,
                        key,
                        value_predicate: None,
                        series_type,
                    });
                }

                return Ok(filters);
            }
        }

        // TODO: return list of all series
        Err(StorageError {
            description: "get for all series ids not wired up yet".to_string(),
        })
    }

    fn get_series_key_and_type_by_id(
        &self,
        bucket_id: u32,
        id: u64,
    ) -> Result<(String, SeriesDataType), StorageError> {
        let cf_name = index_cf_name(bucket_id);
        let db = self.db.read().unwrap();

        match db.cf_handle(&cf_name) {
            Some(cf) => match db.get_cf(cf, index_series_id_from_id(id)).unwrap() {
                Some(val) => {
                    let t = val[0].try_into().unwrap();
                    let key = std::str::from_utf8(&val[1..]).unwrap().to_owned();
                    Ok((key, t))
                }
                None => Err(StorageError {
                    description: "series id not found".to_string(),
                }),
            },
            None => Err(StorageError {
                description: "unable to find index".to_string(),
            }),
        }
    }

    fn evaluate_node(&self, bucket_id: u32, n: &Node) -> Result<Treemap, StorageError> {
        struct Visitor<'a> {
            db: &'a RocksDB,
            bucket_id: u32,
        };

        impl EvaluateVisitor for Visitor<'_> {
            fn equal(&mut self, left: &str, right: &str) -> Result<Treemap, StorageError> {
                self.db
                    .get_posting_list_for_tag_key_value(self.bucket_id, left, right)
            }
        }

        Evaluate::evaluate(
            Visitor {
                db: self,
                bucket_id,
            },
            n,
        )
    }

    fn get_posting_list_for_tag_key_value(
        &self,
        bucket_id: u32,
        key: &str,
        value: &str,
    ) -> Result<Treemap, StorageError> {
        // first get the cf for this index
        let cf_name = index_cf_name(bucket_id);
        let db = self.db.read().unwrap();

        match db.cf_handle(&cf_name) {
            Some(cf) => {
                match db
                    .get_cf(cf, index_key_value_posting_list(bucket_id, key, value))
                    .unwrap()
                {
                    Some(val) => {
                        let map = Treemap::deserialize(&val)
                            .expect("unexpected error deserializing tree map");
                        Ok(map)
                    }
                    None => Ok(Treemap::create()),
                }
            }
            None => Err(StorageError {
                description: "unable to find index".to_string(),
            }),
        }
    }

    // TODO: handle predicate
    pub fn get_tag_keys(&self, bucket_id: u32, _predicate: Option<&Predicate>) -> Vec<String> {
        let cf_name = index_cf_name(bucket_id);
        let mut keys = vec![];

        let db = self.db.read().unwrap();

        if let Some(index) = db.cf_handle(&cf_name) {
            let prefix = index_tag_key_prefix(bucket_id);
            let mode = IteratorMode::From(&prefix, Direction::Forward);
            let iter = db
                .iterator_cf(index, mode)
                .expect("unexpected rocksdb error getting iterator for index");

            for (key, _) in iter {
                if !key.starts_with(&prefix) {
                    break;
                }

                let k = std::str::from_utf8(&key[prefix.len()..]).unwrap(); // TODO: determine what we want to do with errors
                keys.push(k.to_string());
            }
        };

        keys
    }

    pub fn get_tag_values(
        &self,
        bucket_id: u32,
        tag: &str,
        _predicate: Option<&Predicate>,
    ) -> Vec<String> {
        let cf_name = index_cf_name(bucket_id);

        let db = self.db.read().unwrap();
        let mut values = vec![];

        if let Some(index) = db.cf_handle(&cf_name) {
            let prefix = index_tag_key_value_prefix(bucket_id, tag);
            let mode = IteratorMode::From(&prefix, Direction::Forward);
            let iter = db
                .iterator_cf(index, mode)
                .expect("unexpected rocksdb error getting iterator for index");

            for (key, _) in iter {
                if !key.starts_with(&prefix) {
                    break;
                }

                let v = std::str::from_utf8(&key[prefix.len()..]).unwrap(); // TODO: determine what to do with errors
                values.push(v.to_string());
            }
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
        if map.get(&bucket_id).is_none() {
            map.insert(bucket_id, Mutex::new(1));
        }
    }

    // TODO: ensure that points with timestamps older than the first index level get inserted only into the higher levels
    // TODO: build the index for levels other than the first
    // insert_series_without_ids will insert any series into the index and obtain an identifier for it.
    // the passed in series vector is modified so that the newly inserted series have their ids
    pub fn insert_series_without_ids(&self, bucket_id: u32, points: &mut [PointType]) {
        // We want to get a lock on new series only for this bucket
        self.ensure_series_mutex_exists(bucket_id);
        let map = self.series_insert_lock.read().expect("mutex poisoned");
        let next_id = map
            .get(&bucket_id)
            .expect("should exist because of call to ensure_series_mutex_exists");
        let mut next_id = next_id.lock().expect("mutex poisoned");

        let mut batch = WriteBatch::default();

        // create the column family to store the index if it doesn't exist
        let cf_name = index_cf_name(bucket_id);
        let index_exists = self.db.read().unwrap().cf_handle(&cf_name).is_some();

        if !index_exists {
            self.db
                .write()
                .unwrap()
                .create_cf(&cf_name, &index_cf_options())
                .unwrap();
        }

        let db = self.db.read().unwrap();
        let index_cf = db
            .cf_handle(&cf_name)
            .expect("index column family should have already been inserted");

        // Keep an in memory map for updating multiple index entries at a time
        let mut index_map: HashMap<Vec<u8>, Treemap> = HashMap::new();
        let mut series_id_map: HashMap<String, u64> = HashMap::new();

        // now loop through the series and insert the index entries into the map
        for point in points {
            // don't bother with series in the collection that already have IDs
            if point.series_id().is_some() {
                continue;
            }

            // if we've already put this series in the map in this write, skip it
            if let Some(id) = series_id_map.get(point.series()) {
                point.set_series_id(*id);
                continue;
            }

            // now that we have the mutex on series, make sure these weren't inserted in some other thread
            if let Some(id) = self.get_series_id(&cf_name, &point.series()) {
                point.set_series_id(id);
                continue;
            }

            point.set_series_id(*next_id);
            let id = *next_id;
            let series_id = next_id.to_be_bytes();

            batch
                .put_cf(index_cf, index_series_key_id(&point.series()), &series_id)
                .unwrap();
            batch
                .put_cf(
                    index_cf,
                    index_series_id(&series_id),
                    index_series_id_value(series_type_from_point_type(&point), &point.series()),
                )
                .unwrap();
            series_id_map.insert(point.series().clone(), *next_id);
            *next_id += 1;

            // insert the index entries
            // TODO: do the error handling bits, but how to handle? Should all series be validated before
            //       and fail the whole write if any one is bad, or insert the ones we can and ignore and log the bad?

            let pairs = point.index_pairs().unwrap();
            for pair in pairs {
                // insert the tag key index
                batch
                    .put_cf(index_cf, index_tag_key(bucket_id, &pair.key), vec![0 as u8])
                    .unwrap();

                // insert the tag value index
                batch
                    .put_cf(
                        index_cf,
                        index_tag_key_value(bucket_id, &pair.key, &pair.value),
                        vec![0 as u8],
                    )
                    .unwrap();

                // update the key to id bitmap
                let index_key_posting_list_key = index_key_posting_list(bucket_id, &pair.key);

                // put it in the temporary in memory map for a single write update later
                let tree = match index_map.entry(index_key_posting_list_key) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        let map = match self.db.read().unwrap().get_cf(index_cf, e.key()).unwrap() {
                            Some(b) => Treemap::deserialize(&b)
                                .expect("unexpected error deserializing posting list"),
                            None => Treemap::create(),
                        };
                        e.insert(map)
                    }
                };
                tree.add(id);

                // update the key/value to id bitmap
                let index_key_value_posting_list_key =
                    index_key_value_posting_list(bucket_id, &pair.key, &pair.value);

                let tree = match index_map.entry(index_key_value_posting_list_key) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        let map = match self.db.read().unwrap().get_cf(index_cf, e.key()).unwrap() {
                            Some(b) => Treemap::deserialize(&b)
                                .expect("unexpected error deserializing posting list"),
                            None => Treemap::create(),
                        };
                        e.insert(map)
                    }
                };
                tree.add(id);
            }
        }

        // do the index writes from the in temporary in memory map
        for (k, v) in &index_map {
            let _ = batch.put_cf(index_cf, k, v.serialize().unwrap());
        }

        // save the next series id
        let bucket_cf = db.cf_handle(BUCKET_CF).unwrap();
        let mut next_series_id_val = Vec::with_capacity(8);
        next_series_id_val.write_u64::<BigEndian>(*next_id).unwrap();
        let _ = batch.put_cf(bucket_cf, next_series_id_key(bucket_id), next_series_id_val);

        db.write(batch).expect("unexpected rocksdb error");
    }

    fn get_series_id(&self, cf_name: &str, series_key: &str) -> Option<u64> {
        // this column family might not exist if this index hasn't been created yet
        if let Some(cf) = self.db.read().unwrap().cf_handle(cf_name) {
            if let Some(val) = self
                .db
                .read()
                .unwrap()
                .get_cf(cf, index_series_key_id(series_key))
                .expect("unexpected rocksdb error")
            {
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
        let prefix = &[BucketEntryType::Bucket.into()];
        let iter = db
            .iterator_cf(&buckets, IteratorMode::From(prefix, Direction::Forward))
            .unwrap();

        let mut id_mutex_map = HashMap::new();
        let mut bucket_map = self.bucket_map.write().unwrap();
        let mut bucket_id_map = self.bucket_id_map.write().unwrap();

        for (key, value) in iter {
            match key[0].try_into().unwrap() {
                BucketEntryType::NextSeriesID => {
                    // read the bucket id from the key
                    let mut c = Cursor::new(&key[1..]);
                    let bucket_id = c.read_u32::<BigEndian>().unwrap_or_else(|_| {
                        panic!("couldn't read the bucket id from the key {:?}", key)
                    });

                    // and the next series ID
                    let mut c = Cursor::new(value);
                    let next_id = c.read_u64::<BigEndian>().unwrap_or_else(|_| {
                        panic!("couldn't read the next series id for bucket {}", bucket_id)
                    });
                    id_mutex_map.insert(bucket_id, Mutex::new(next_id));
                }
                BucketEntryType::Bucket => {
                    let bucket = Bucket::decode(&*value).expect("unexpected error decoding bucket");
                    let key = bucket_key(bucket.org_id, &bucket.name);
                    let arc_bucket = Arc::new(bucket);
                    bucket_map.insert(key, arc_bucket.clone());
                    bucket_id_map.insert(arc_bucket.id, arc_bucket);
                }
                BucketEntryType::NextBucketID => (),
            }
        }
        self.series_insert_lock = Arc::new(RwLock::new(id_mutex_map));
    }
}

pub trait ToRocksDBBytes {
    fn to_rocks_db_bytes(&self) -> Vec<u8>;
}

impl ToRocksDBBytes for PointType {
    fn to_rocks_db_bytes(&self) -> Vec<u8> {
        match self {
            PointType::I64(inner) => inner.value.to_rocks_db_bytes(),
            PointType::F64(inner) => inner.value.to_rocks_db_bytes(),
        }
    }
}

impl ToRocksDBBytes for i64 {
    fn to_rocks_db_bytes(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(8);
        value.write_i64::<BigEndian>(*self).unwrap();
        value
    }
}

impl ToRocksDBBytes for f64 {
    fn to_rocks_db_bytes(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(8);
        value.write_f64::<BigEndian>(*self).unwrap();
        value
    }
}

impl InvertedIndex for RocksDB {
    fn get_or_create_series_ids_for_points(
        &self,
        bucket_id: u32,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        self.get_series_ids(bucket_id, points)?;
        self.insert_series_without_ids(bucket_id, points);
        Ok(())
    }

    fn read_series_matching(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter> + Send>, StorageError> {
        let filters = self.get_series_filters(bucket_id, predicate)?;
        Ok(Box::new(filters.into_iter()))
    }

    fn get_tag_keys(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        let keys = self.get_tag_keys(bucket_id, predicate);
        Ok(Box::new(keys.into_iter()))
    }

    fn get_tag_values(
        &self,
        bucket_id: u32,
        tag_key: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        let values = self.get_tag_values(bucket_id, tag_key, predicate);
        Ok(Box::new(values.into_iter()))
    }
}

impl SeriesStore for RocksDB {
    fn write_points_with_series_ids(
        &self,
        bucket_id: u32,
        points: &[PointType],
    ) -> Result<(), StorageError> {
        self.write_points(bucket_id, &points)
    }

    fn read_i64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<i64>>> + Send>, StorageError> {
        self.read_range(bucket_id, series_id, range, batch_size)
    }

    fn read_f64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<f64>>> + Send>, StorageError> {
        self.read_range(bucket_id, series_id, range, batch_size)
    }
}

impl ConfigStore for RocksDB {
    fn create_bucket_if_not_exists(
        &self,
        org_id: u32,
        bucket: &Bucket,
    ) -> Result<u32, StorageError> {
        self.create_bucket_if_not_exists(org_id, bucket)
    }

    fn get_bucket_by_name(
        &self,
        org_id: u32,
        bucket_name: &str,
    ) -> Result<Option<Arc<Bucket>>, StorageError> {
        self.get_bucket_by_name(org_id, bucket_name)
    }

    fn get_bucket_by_id(&self, bucket_id: u32) -> Result<Option<Arc<Bucket>>, StorageError> {
        self.get_bucket_by_id(bucket_id)
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

fn prefix_for_series(bucket_id: u32, series_id: u64, start_time: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.write_u64::<BigEndian>(series_id).unwrap();
    v.write_i64::<BigEndian>(start_time).unwrap();
    v
}

pub struct PointsIterator<'a, T: Clone> {
    batch_size: usize,
    iter: DBIterator<'a>,
    stop_time: i64,
    series_prefix: Vec<u8>,
    drained: bool,
    read: fn(b: &[u8]) -> T,
}

impl<T: Clone> Iterator for PointsIterator<'_, T> {
    type Item = Vec<ReadPoint<T>>;

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

            let point = ReadPoint {
                value: (self.read)(&value),
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

fn index_cf_name(bucket_id: u32) -> String {
    format!("index_{}", bucket_id,)
}

fn index_series_key_id(series_key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(series_key.len() + 1);
    v.push(IndexEntryType::SeriesKeyToID as u8);
    v.extend_from_slice(series_key.as_bytes());
    v
}

fn index_series_id(id: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 1);
    v.push(IndexEntryType::IDToSeriesKey as u8);
    v.extend_from_slice(id);
    v
}

fn index_series_id_value(t: SeriesDataType, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + key.len());
    v.push(t.into());
    v.extend_from_slice(key.as_bytes());
    v
}

fn series_type_from_point_type(p: &PointType) -> SeriesDataType {
    match p {
        PointType::I64(_) => SeriesDataType::I64,
        PointType::F64(_) => SeriesDataType::F64,
    }
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
    v.extend_from_slice(key.as_bytes());
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
    v.extend_from_slice(key.as_bytes());
    v.push(0 as u8);
    v.extend_from_slice(value.as_bytes());
    v
}

fn index_tag_key_value_prefix(bucket_id: u32, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 6);
    v.push(IndexEntryType::KeyValueList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.extend_from_slice(key.as_bytes());
    v.push(0 as u8);
    v
}

fn index_key_posting_list(bucket_id: u32, key: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 6);
    v.push(IndexEntryType::KeyPostingList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.extend_from_slice(key.as_bytes());
    v
}

fn index_key_value_posting_list(bucket_id: u32, key: &str, value: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + value.len() + 6);
    v.push(IndexEntryType::KeyValuePostingList as u8);
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v.extend_from_slice(key.as_bytes());
    v.push(0 as u8);
    v.extend_from_slice(value.as_bytes());
    v
}

// next_series_id_key gives the key in the buckets CF in rocks that holds the value for the next series ID
fn next_series_id_key(bucket_id: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(5);
    v.push(BucketEntryType::NextSeriesID.into());
    v.write_u32::<BigEndian>(bucket_id).unwrap();
    v
}

// The values for these enum variants have no real meaning, but they
// are serialized to disk. Revisit these whenever it's time to decide
// on an on-disk format.
enum BucketEntryType {
    Bucket = 0,
    NextSeriesID = 1,
    NextBucketID = 2,
}

impl From<BucketEntryType> for u8 {
    fn from(other: BucketEntryType) -> Self {
        other as u8
    }
}

impl TryFrom<u8> for BucketEntryType {
    type Error = u8;

    fn try_from(other: u8) -> Result<Self, Self::Error> {
        use BucketEntryType::*;

        match other {
            v if v == Bucket as u8 => Ok(Bucket),
            v if v == NextSeriesID as u8 => Ok(NextSeriesID),
            v if v == NextBucketID as u8 => Ok(NextBucketID),
            _ => Err(other),
        }
    }
}

// TODO: ensure required fields are present and write tests
fn validate_bucket_fields(_bucket: &Bucket) -> Result<(), StorageError> {
    Ok(())
}
// returns the byte key to find this bucket in the buckets CF in rocks
fn bucket_key(org_id: u32, bucket_name: &str) -> Vec<u8> {
    let s = bucket_name.as_bytes();
    let mut key = Vec::with_capacity(3 + s.len());
    key.push(BucketEntryType::Bucket.into());
    key.write_u32::<BigEndian>(org_id).unwrap();
    key.extend_from_slice(s);
    key
}

fn next_bucket_id_key() -> Vec<u8> {
    vec![BucketEntryType::NextBucketID.into()]
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

trait FromBytes {
    fn from(b: &[u8]) -> Self;
}

impl FromBytes for i64 {
    fn from(b: &[u8]) -> i64 {
        let mut c = Cursor::new(b);
        c.read_i64::<BigEndian>().unwrap()
    }
}

impl FromBytes for f64 {
    fn from(b: &[u8]) -> f64 {
        let mut c = Cursor::new(b);
        c.read_f64::<BigEndian>().unwrap()
    }
}

impl Bucket {
    pub fn new(org_id: u32, name: String) -> Bucket {
        Bucket {
            org_id,
            id: 0,
            name,
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![IndexLevel {
                duration_seconds: 0,
                timezone: "EDT".to_string(),
            }],
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

#[cfg(test)]
mod tests {
    use super::*;

    use dotenv::dotenv;
    use std::{
        env,
        ops::{Deref, DerefMut},
    };
    use tempfile::TempDir;

    use crate::storage::predicate::parse_predicate;

    #[test]
    fn create_and_get_buckets() {
        let bucket: Arc<Bucket>;
        let org_id = 1;
        let mut bucket2 = Bucket::new(2, "Foo".to_string());

        let db_dir = {
            let db = TestDatabase::new().unwrap();
            let mut b = Bucket::new(org_id, "Foo".to_string());

            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(b.id, 1);

            let stored_bucket = db.get_bucket_by_name(org_id, &b.name).unwrap().unwrap();
            assert_eq!(Arc::new(b.clone()), stored_bucket);

            let bucket_by_id = db.get_bucket_by_id(b.id).unwrap().unwrap();
            assert_eq!(Arc::new(b.clone()), bucket_by_id);

            bucket = stored_bucket;

            // ensure it doesn't insert again
            let id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            assert_eq!(id, 1);

            // ensure second bucket in another org
            bucket2.id = db
                .create_bucket_if_not_exists(bucket2.org_id, &bucket2)
                .unwrap();
            assert_eq!(bucket2.id, 2);
            let stored2 = db
                .get_bucket_by_name(bucket2.org_id, &bucket2.name)
                .unwrap()
                .unwrap();
            assert_eq!(Arc::new(bucket2), stored2);

            // ensure second bucket gets new ID
            let mut b2 = Bucket::new(org_id, "two".to_string());
            b2.id = db.create_bucket_if_not_exists(org_id, &b2).unwrap();
            assert_eq!(b2.id, 3);
            let stored_bucket = db.get_bucket_by_name(org_id, &b2.name).unwrap().unwrap();
            assert_eq!(Arc::new(b2), stored_bucket);

            // TODO: ensure that a bucket orders levels correctly

            TestDatabase::close(db)
        };

        // ensure it persists across database reload
        {
            let db = TestDatabase::with_directory(db_dir);
            let stored_bucket = db
                .get_bucket_by_name(org_id, &bucket.name)
                .unwrap()
                .unwrap();
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
        let p1 = PointType::new_i64("one".to_string(), 1, 0);
        let p2 = PointType::new_i64("two".to_string(), 23, 40);
        let p3 = PointType::new_i64("three".to_string(), 33, 86);
        let p4 = PointType::new_i64("four".to_string(), 234, 100);

        let db_dir = {
            let db = TestDatabase::new().unwrap();
            b.id = db.create_bucket_if_not_exists(org_id, &b).unwrap();
            b2.id = db.create_bucket_if_not_exists(b2.org_id, &b2).unwrap();

            let mut points = vec![p1.clone(), p2.clone()];
            db.get_or_create_series_ids_for_points(b.id, &mut points)
                .unwrap();
            assert_eq!(points[0].series_id(), Some(1));
            assert_eq!(points[1].series_id(), Some(2));

            // now insert a new series and make sure it shows up
            let mut points = vec![p1.clone(), p3.clone()];
            db.get_series_ids(b.id, &mut points).unwrap();
            assert_eq!(points[0].series_id(), Some(1));
            assert_eq!(points[1].series_id(), None);

            db.get_or_create_series_ids_for_points(b.id, &mut points)
                .unwrap();
            assert_eq!(points[0].series_id(), Some(1));
            assert_eq!(points[1].series_id(), Some(3));

            let mut points = vec![p1.clone()];
            db.get_series_ids(b2.id, &mut points).unwrap();
            assert_eq!(points[0].series_id(), None);

            // insert a series into the other org bucket
            db.get_or_create_series_ids_for_points(b2.id, &mut points)
                .unwrap();
            assert_eq!(points[0].series_id(), Some(1));

            TestDatabase::close(db)
        };

        // now make sure that a new series gets inserted properly after restart
        {
            let db = TestDatabase::with_directory(db_dir);

            // check the first org
            let mut points = vec![p4.clone()];
            db.insert_series_without_ids(b.id, &mut points);
            assert_eq!(points[0].series_id(), Some(4));

            let mut points = vec![p1.clone(), p2.clone(), p3.clone(), p4];
            db.get_series_ids(b.id, &mut points).unwrap();
            assert_eq!(points[0].series_id(), Some(1));
            assert_eq!(points[1].series_id(), Some(2));
            assert_eq!(points[2].series_id(), Some(3));
            assert_eq!(points[3].series_id(), Some(4));

            // check the second org
            let mut points = vec![p2.clone()];
            db.insert_series_without_ids(b2.id, &mut points);
            assert_eq!(points[0].series_id(), Some(2));

            let mut points = vec![p1, p2, p3];
            db.get_series_ids(b2.id, &mut points).unwrap();
            assert_eq!(points[0].series_id(), Some(1));
            assert_eq!(points[1].series_id(), Some(2));
            assert_eq!(points[2].series_id(), None);
        }
    }

    #[test]
    fn series_metadata_indexing() {
        let mut bucket = Bucket::new(1, "foo".to_string());
        let db = TestDatabase::new().unwrap();
        let p1 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 0);
        let p2 = PointType::new_i64("cpu,host=a,region=west\tusage_system".to_string(), 1, 0);
        let p3 = PointType::new_i64("cpu,host=a,region=west\tusage_user".to_string(), 1, 0);
        let p4 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 0);

        bucket.id = db
            .create_bucket_if_not_exists(bucket.org_id, &bucket)
            .unwrap();
        let mut points = vec![p1, p2, p3, p4];
        db.get_or_create_series_ids_for_points(bucket.id, &mut points)
            .unwrap();

        let tag_keys = db.get_tag_keys(bucket.id, None);
        assert_eq!(tag_keys, vec!["_f", "_m", "host", "region"]);

        let tag_values = db.get_tag_values(bucket.id, "host", None);
        assert_eq!(tag_values, vec!["a", "b"]);

        // get all series

        // get series with measurement = mem
        let pred = parse_predicate(r#"_m = "cpu""#).unwrap();
        let series: Vec<SeriesFilter> = db
            .read_series_matching(bucket.id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 1,
                    key: "cpu,host=b,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );

        // get series with host = a
        let pred = parse_predicate(r#"host = "a""#).unwrap();
        let series: Vec<SeriesFilter> = db
            .read_series_matching(bucket.id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );

        // get series with measurement = cpu and host = b
        let pred = parse_predicate(r#"_m = "cpu" and host = "b""#).unwrap();
        let series: Vec<SeriesFilter> = db
            .read_series_matching(bucket.id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            },]
        );

        let pred = parse_predicate(r#"host = "a" OR _m = "mem""#).unwrap();
        let series: Vec<SeriesFilter> = db
            .read_series_matching(bucket.id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 4,
                    key: "mem,host=b,region=west\tfree".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );
    }

    #[test]
    fn catch_rocksdb_iterator_segfault() {
        let mut b1 = Bucket::new(1, "bucket1".to_string());
        let db = TestDatabase::new().unwrap();

        let p1 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 1);

        b1.id = db.create_bucket_if_not_exists(b1.org_id, &b1).unwrap();

        let mut points = vec![p1];
        db.get_or_create_series_ids_for_points(b1.id, &mut points)
            .unwrap();
        db.write_points(b1.id, &points).unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = TimestampRange { start: 1, end: 4 };
        let pred = parse_predicate(r#"_m = "cpu""#).unwrap();
        let mut iter = db.read_series_matching(b1.id, Some(&pred)).unwrap();

        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        assert_eq!(iter.next(), None);
        let mut points_iter = db
            .read_i64_range(b1.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 1, value: 1 },]);
        assert_eq!(points_iter.next(), None);
    }

    #[test]
    fn write_and_read_points() {
        let mut b1 = Bucket::new(1, "bucket1".to_string());
        let mut b2 = Bucket::new(2, "bucket2".to_string());
        let db = TestDatabase::new().unwrap();

        let p1 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 1);
        let p2 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 2);
        let p3 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 2);
        let p4 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 4);

        b1.id = db.create_bucket_if_not_exists(b1.org_id, &b1).unwrap();
        b2.id = db.create_bucket_if_not_exists(b2.org_id, &b2).unwrap();

        let mut b1_points = vec![p1.clone(), p2.clone()];
        db.get_or_create_series_ids_for_points(b1.id, &mut b1_points)
            .unwrap();
        db.write_points(b1.id, &b1_points).unwrap();

        let mut b2_points = vec![p1, p2, p3, p4];
        db.get_or_create_series_ids_for_points(b2.id, &mut b2_points)
            .unwrap();
        db.write_points(b2.id, &b2_points).unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = TimestampRange { start: 1, end: 4 };
        let pred = parse_predicate(r#"_m = "cpu" OR _m = "mem""#).unwrap();
        let mut iter = db.read_series_matching(b1.id, Some(&pred)).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        assert_eq!(iter.next(), None);
        let mut points_iter = db
            .read_i64_range(b1.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 1, value: 1 },
                ReadPoint { time: 2, value: 1 },
            ]
        );
        assert_eq!(points_iter.next(), None);

        // test that we'll read multiple series
        let pred = parse_predicate(r#"_m = "cpu" OR _m = "mem""#).unwrap();
        let mut iter = db.read_series_matching(b2.id, Some(&pred)).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        let mut points_iter = db
            .read_i64_range(b2.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 1, value: 1 },
                ReadPoint { time: 2, value: 1 },
            ]
        );

        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 2,
                key: "mem,host=b,region=west\tfree".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        let mut points_iter = db
            .read_i64_range(b2.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 2, value: 1 },
                ReadPoint { time: 4, value: 1 },
            ]
        );

        // test that the batch size is honored
        let pred = parse_predicate(r#"host = "b""#).unwrap();
        let mut iter = db.read_series_matching(b1.id, Some(&pred)).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        assert_eq!(iter.next(), None);
        let mut points_iter = db
            .read_i64_range(b1.id, series_filter.id, &range, 1)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 1, value: 1 },]);
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);

        // test that the time range is properly limiting
        let range = TimestampRange { start: 2, end: 3 };
        let pred = parse_predicate(r#"_m = "cpu" OR _m = "mem""#).unwrap();
        let mut iter = db.read_series_matching(b2.id, Some(&pred)).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        let mut points_iter = db
            .read_i64_range(b2.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);

        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 2,
                key: "mem,host=b,region=west\tfree".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            }
        );
        let mut points_iter = db
            .read_i64_range(b2.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);
    }

    #[test]
    fn write_and_read_float_values() {
        let mut b1 = Bucket::new(1, "bucket1".to_string());
        let db = TestDatabase::new().unwrap();

        let p1 = PointType::new_f64("cpu,host=b,region=west\tusage_system".to_string(), 1.0, 1);
        let p2 = PointType::new_f64("cpu,host=b,region=west\tusage_system".to_string(), 2.2, 2);

        b1.id = db.create_bucket_if_not_exists(b1.org_id, &b1).unwrap();

        let mut points = vec![p1, p2];
        db.get_or_create_series_ids_for_points(b1.id, &mut points)
            .unwrap();
        db.write_points_with_series_ids(b1.id, &points).unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = TimestampRange { start: 0, end: 4 };
        let pred = parse_predicate(r#"_m = "cpu""#).unwrap();
        let mut iter = db.read_series_matching(b1.id, Some(&pred)).unwrap();
        let series_filter = iter.next().unwrap();
        assert_eq!(
            series_filter,
            SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::F64
            }
        );
        assert_eq!(iter.next(), None);
        let mut points_iter = db
            .read_f64_range(b1.id, series_filter.id, &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint {
                    time: 1,
                    value: 1.0
                },
                ReadPoint {
                    time: 2,
                    value: 2.2
                },
            ]
        );
        assert_eq!(points_iter.next(), None);
    }

    struct TestDatabase {
        rocks_db: RocksDB,

        // The temporary directory **must** be last so that it is
        // dropped after the database closes.
        dir: TempDir,
    }

    impl TestDatabase {
        fn new() -> Result<Self, std::io::Error> {
            let _ = dotenv(); // load .env file if present

            let root =
                env::var_os("TEST_DELOREAN_DB_DIR").unwrap_or_else(|| env::temp_dir().into());
            let dir = tempfile::Builder::new()
                .prefix("delorean")
                .tempdir_in(root)?;

            Ok(Self::with_directory(dir))
        }

        fn with_directory(dir: TempDir) -> Self {
            let rocks_db = RocksDB::new(dir.path().to_str().unwrap());

            Self { dir, rocks_db }
        }

        fn close(this: Self) -> TempDir {
            this.dir
        }
    }

    impl Deref for TestDatabase {
        type Target = RocksDB;

        fn deref(&self) -> &Self::Target {
            &self.rocks_db
        }
    }

    impl DerefMut for TestDatabase {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.rocks_db
        }
    }
}
