//! partitioned_store is an enum and set of helper functions and structs to define Partitions
//! that store data. The helper funcs and structs merge results from multiple partitions together.
use crate::generated_types::{wal, Predicate, TimestampRange};
use crate::line_parser::{self, PointType};
use crate::storage::{
    memdb::MemDB, remote_partition::RemotePartition, s3_partition::S3Partition, ReadPoint,
    SeriesDataType, StorageError,
};

use delorean_wal::{Wal, WalBuilder};
use futures::{
    channel::mpsc,
    stream::{BoxStream, Stream},
    FutureExt, SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    io::Write,
    mem,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::task;

#[derive(Clone)]
pub enum PartitionStore {
    MemDB(Box<MemDB>),
    S3(Box<S3Partition>),
    Remote(Box<RemotePartition>),
}

/// A Partition is a block of data. It has methods for reading the metadata like which measurements,
/// tags, tag values, and fields exist, along with the raw time series data. It is designed to work
/// as a stream so that it can be used safely in an asynchronous context. A partition is the
/// lowest level organization scheme. Above it, you will have a database which keeps track of
/// what organizations and buckets exist. A bucket will have 1 to many partitions and a partition
/// will only ever contain data for a single bucket.
///
/// A Partition is backed by some Partition Store mechanism, such as in memory, on S3, or in a
/// remote partition.
///
/// A Partition may optionally have a write-ahead log.
pub struct Partition {
    store: PartitionStore,
    wal_details: Option<WalDetails>,
}

struct WalDetails {
    wal: Wal<mpsc::Sender<delorean_wal::Result<()>>>,
    metadata: WalMetadata,
    // There is no mechanism available to the HTTP API to trigger an immediate sync yet
    #[allow(dead_code)]
    start_sync_tx: mpsc::Sender<()>,
}

impl WalDetails {
    async fn write_metadata(&self) -> Result<(), StorageError> {
        let metadata_path = self.wal.metadata_path();
        Ok(tokio::fs::write(metadata_path, serde_json::to_string(&self.metadata)?).await?)
    }
}

impl Partition {
    pub fn new_without_wal(store: PartitionStore) -> Partition {
        Partition {
            store,
            wal_details: None,
        }
    }

    pub async fn new_with_wal(
        store: PartitionStore,
        wal_dir: PathBuf,
    ) -> Result<Partition, StorageError> {
        let wal_builder = WalBuilder::new(wal_dir);
        let wal_details = start_wal_sync_task(wal_builder).await?;
        wal_details.write_metadata().await?;

        Ok(Partition {
            store,
            wal_details: Some(wal_details),
        })
    }

    pub async fn restore_memdb_from_wal(
        bucket_name: &str,
        bucket_dir: PathBuf,
    ) -> Result<Partition, StorageError> {
        let partition_id = bucket_name.to_string();
        let mut db = MemDB::new(partition_id);
        let wal_builder = WalBuilder::new(bucket_dir);
        let wal_details = start_wal_sync_task(wal_builder.clone()).await?;

        match wal_details.metadata.format {
            WalFormat::Unknown => {
                return Err(StorageError {
                    description: "Cannot restore from WAL; unknown format".into(),
                })
            }
            WalFormat::FlatBuffers => {
                let mut points = Vec::new();

                for entry in wal_builder.entries()? {
                    let entry = entry?;
                    let bytes = entry.as_data();

                    let entry = flatbuffers::get_root::<wal::Entry<'_>>(&bytes);

                    if let Some(entry_type) = entry.entry_type() {
                        if let Some(write) = entry_type.write() {
                            if let Some(wal_points) = write.points() {
                                for wal_point in wal_points {
                                    points.push(wal_point.into());
                                }
                            }
                        }
                    }
                }

                db.write_points(&mut points)?;
            }
        }

        let store = PartitionStore::MemDB(Box::new(db));
        wal_details.write_metadata().await?;

        Ok(Partition {
            store,
            wal_details: Some(wal_details),
        })
    }

    pub async fn write_points(&mut self, points: &mut [PointType]) -> Result<(), StorageError> {
        // TODO: Allow each kind of PartitionWithWal to configure the guarantees around when this
        // function returns and the state of data in regards to the WAL

        if let Some(WalDetails { wal, .. }) = &self.wal_details {
            let (ingest_done_tx, mut ingest_done_rx) = mpsc::channel(1);

            let mut w = wal.append();
            let flatbuffer = points_to_flatbuffer(&points);
            w.write_all(flatbuffer.finished_data())?;
            w.finalize(ingest_done_tx).expect("TODO handle errors");

            ingest_done_rx
                .next()
                .await
                .expect("TODO handle errors")
                .expect("TODO handle errors");
        }

        match &mut self.store {
            PartitionStore::MemDB(db) => db.write_points(points),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub fn id(&self) -> &str {
        match &self.store {
            PartitionStore::MemDB(db) => &db.id,
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub fn size(&self) -> usize {
        match &self.store {
            PartitionStore::MemDB(db) => db.size(),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_tag_keys(
        &self,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, String>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.get_tag_keys(predicate, range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_tag_values(
        &self,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, String>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.get_tag_values(tag_key, predicate, range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn read_points(
        &self,
        batch_size: usize,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<BoxStream<'_, ReadBatch>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.read_points(batch_size, predicate, range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_measurement_names(
        &self,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, String>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.get_measurement_names(range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_measurement_tag_keys(
        &self,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, String>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.get_measurement_tag_keys(measurement, predicate, range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_measurement_tag_values(
        &self,
        measurement: &str,
        tag_key: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, String>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => {
                db.get_measurement_tag_values(measurement, tag_key, predicate, range)
            }
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }

    pub async fn get_measurement_fields(
        &self,
        measurement: &str,
        predicate: Option<&Predicate>,
        range: Option<&TimestampRange>,
    ) -> Result<BoxStream<'_, (String, SeriesDataType, i64)>, StorageError> {
        match &self.store {
            PartitionStore::MemDB(db) => db.get_measurement_fields(measurement, predicate, range),
            PartitionStore::S3(_) => panic!("s3 partition not implemented!"),
            PartitionStore::Remote(_) => panic!("remote partition not implemented!"),
        }
    }
}

async fn start_wal_sync_task(wal_builder: WalBuilder) -> Result<WalDetails, StorageError> {
    let wal = wal_builder.wal::<mpsc::Sender<_>>()?;

    let metadata = tokio::fs::read_to_string(wal.metadata_path())
        .await
        .and_then(|raw_metadata| {
            serde_json::from_str::<WalMetadata>(&raw_metadata).map_err(Into::into)
        })
        .unwrap_or_default();

    let (start_sync_tx, mut start_sync_rx) = mpsc::channel(1);

    tokio::spawn({
        let wal = wal.clone();
        // TODO: Make delay configurable
        let mut sync_frequency = tokio::time::interval(Duration::from_millis(50));

        async move {
            loop {
                // TODO: What if syncing takes longer than the delay?
                futures::select! {
                    _ = start_sync_rx.next() => {},
                    _ = sync_frequency.tick().fuse() => {},
                };

                let (to_notify, outcome) = task::block_in_place(|| wal.sync());

                for mut notify in to_notify {
                    notify
                        .send(outcome.clone())
                        .await
                        .expect("TODO handle failures");
                }
            }
        }
    });

    Ok(WalDetails {
        wal,
        start_sync_tx,
        metadata,
    })
}

/// Metadata about this particular WAL
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WalMetadata {
    format: WalFormat,
}

impl Default for WalMetadata {
    fn default() -> Self {
        WalMetadata {
            format: WalFormat::FlatBuffers,
        }
    }
}

/// Supported WAL formats that can be restored
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
enum WalFormat {
    FlatBuffers,
    #[serde(other)]
    Unknown,
}

fn points_to_flatbuffer(points: &[PointType]) -> flatbuffers::FlatBufferBuilder<'_> {
    let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

    let point_offsets: Vec<_> = points
        .iter()
        .map(|p| {
            let key = builder.create_string(p.series());

            match p {
                PointType::I64(inner_point) => {
                    let value = wal::I64Value::create(
                        &mut builder,
                        &wal::I64ValueArgs {
                            value: inner_point.value,
                        },
                    );
                    wal::Point::create(
                        &mut builder,
                        &wal::PointArgs {
                            key: Some(key),
                            time: p.time(),
                            value_type: wal::PointValue::I64Value,
                            value: Some(value.as_union_value()),
                        },
                    )
                }
                PointType::F64(inner_point) => {
                    let value = wal::F64Value::create(
                        &mut builder,
                        &wal::F64ValueArgs {
                            value: inner_point.value,
                        },
                    );
                    wal::Point::create(
                        &mut builder,
                        &wal::PointArgs {
                            key: Some(key),
                            time: p.time(),
                            value_type: wal::PointValue::F64Value,
                            value: Some(value.as_union_value()),
                        },
                    )
                }
            }
        })
        .collect();
    let point_offsets = builder.create_vector(&point_offsets);

    let write_offset = wal::Write::create(
        &mut builder,
        &wal::WriteArgs {
            points: Some(point_offsets),
        },
    );

    let entry_type = wal::EntryType::create(
        &mut builder,
        &wal::EntryTypeArgs {
            write: Some(write_offset),
            ..Default::default()
        },
    );

    let entry_offset = wal::Entry::create(
        &mut builder,
        &wal::EntryArgs {
            entry_type: Some(entry_type),
        },
    );

    builder.finish(entry_offset, None);

    builder
}

impl From<wal::Point<'_>> for PointType {
    fn from(other: wal::Point<'_>) -> Self {
        let key = other
            .key()
            .expect("Key should have been deserialized from flatbuffer")
            .to_string();
        let time = other.time();

        match other.value_type() {
            wal::PointValue::I64Value => {
                let value = other
                    .value_as_i64value()
                    .expect("Value should match value type")
                    .value();
                PointType::new_i64(key, value, time)
            }
            wal::PointValue::F64Value => {
                let value = other
                    .value_as_f64value()
                    .expect("Value should match value type")
                    .value();
                PointType::new_f64(key, value, time)
            }
            _ => unimplemented!(),
        }
    }
}

/// StringMergeStream will do a merge sort with deduplication of multiple streams of Strings. This
/// is used for combining results from multiple partitions for calls to get measurements, tag keys,
/// tag values, or field keys. It assumes the incoming streams are in sorted order with no duplicates.
pub struct StringMergeStream<'a> {
    states: Vec<StreamState<'a, String>>,
    drained: bool,
}

struct StreamState<'a, T> {
    stream: BoxStream<'a, T>,
    next: Poll<Option<T>>,
}

impl StringMergeStream<'_> {
    #[allow(dead_code)]
    fn new(streams: Vec<BoxStream<'_, String>>) -> StringMergeStream<'_> {
        let states = streams
            .into_iter()
            .map(|s| StreamState {
                stream: s,
                next: Poll::Pending,
            })
            .collect();

        StringMergeStream {
            states,
            drained: false,
        }
    }
}

impl Stream for StringMergeStream<'_> {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.drained {
            return Poll::Ready(None);
        }

        let mut one_pending = false;

        for state in &mut self.states {
            if state.next.is_pending() {
                state.next = state.stream.as_mut().poll_next(cx);
                one_pending = one_pending || state.next.is_pending();
            }
        }

        if one_pending {
            return Poll::Pending;
        }

        let mut next_val: Option<String> = None;
        let mut next_pos = 0;

        for (pos, state) in self.states.iter_mut().enumerate() {
            match (&next_val, &state.next) {
                (None, Poll::Ready(Some(ref val))) => {
                    next_val = Some(val.clone());
                    next_pos = pos;
                }
                (Some(next), Poll::Ready(Some(ref val))) => match next.cmp(val) {
                    Ordering::Greater => {
                        next_val = Some(val.clone());
                        next_pos = pos;
                    }
                    Ordering::Equal => {
                        state.next = state.stream.as_mut().poll_next(cx);
                    }
                    _ => (),
                },
                (Some(_), Poll::Ready(None)) => (),
                (None, Poll::Ready(None)) => (),
                _ => unreachable!(),
            }
        }

        if next_val.is_none() {
            self.drained = true;
            return Poll::Ready(None);
        }

        let next_state: &mut StreamState<'_, String> = &mut self.states[next_pos];

        mem::replace(
            &mut next_state.next,
            next_state.stream.as_mut().poll_next(cx),
        )
    }
}

/// ReadMergeStream will do a merge sort of the ReadBatches from multiple partitions. When merging
/// it will ensure that batches are sent through in lexographical order by key. In situations
/// where multiple partitions have batches with the same key, they are merged together in time
/// ascending order. For any given key, multiple read batches can come through.
///
/// It assume that the input streams send batches in key lexographical order and that values are
/// always of the same type for a given key, and that those values are in time sorted order. A
/// stream can have multiple batches with the same key, as long as the values across those batches
/// are in time sorted order (ascending).
pub struct ReadMergeStream<'a> {
    states: Vec<StreamState<'a, ReadBatch>>,
    drained: bool,
}

impl ReadMergeStream<'_> {
    #[allow(dead_code)]
    fn new(streams: Vec<BoxStream<'_, ReadBatch>>) -> ReadMergeStream<'_> {
        let states = streams
            .into_iter()
            .map(|s| StreamState {
                stream: s,
                next: Poll::Pending,
            })
            .collect();

        ReadMergeStream {
            states,
            drained: false,
        }
    }
}

impl Stream for ReadMergeStream<'_> {
    type Item = ReadBatch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.drained {
            return Poll::Ready(None);
        }

        // ensure that every stream in pending state is called next and return if any are still pending
        let mut one_pending = false;

        for state in &mut self.states {
            if state.next.is_pending() {
                state.next = state.stream.as_mut().poll_next(cx);
                one_pending = one_pending || state.next.is_pending();
            }
        }

        if one_pending {
            return Poll::Pending;
        }

        // find the minimum key for the next batch and keep track of the other batches that have
        // the same key
        let mut next_min_key: Option<String> = None;
        let mut min_time = std::i64::MAX;
        let mut min_pos = 0;
        let mut positions = Vec::with_capacity(self.states.len());

        for (pos, state) in self.states.iter().enumerate() {
            match (&next_min_key, &state.next) {
                (None, Poll::Ready(Some(batch))) => {
                    next_min_key = Some(batch.key.clone());
                    min_pos = pos;
                    let (_, t) = batch.start_stop_times();
                    min_time = t;
                }
                (Some(min_key), Poll::Ready(Some(batch))) => {
                    match min_key.cmp(&batch.key) {
                        Ordering::Greater => {
                            next_min_key = Some(batch.key.clone());
                            min_pos = pos;
                            positions = Vec::with_capacity(self.states.len());
                            let (_, t) = batch.start_stop_times();
                            min_time = t;
                        }
                        Ordering::Equal => {
                            // if this batch has an end time less than the existing min time, make this
                            // the batch that we want to pull out first
                            let (_, t) = batch.start_stop_times();
                            if t < min_time {
                                min_time = t;
                                positions.push(min_pos);
                                min_pos = pos;
                            } else {
                                positions.push(pos);
                            }
                        }
                        _ => (),
                    }
                }
                (Some(_), Poll::Ready(None)) => (),
                (None, Poll::Ready(None)) => (),
                _ => unreachable!(),
            }
        }

        if next_min_key.is_none() {
            self.drained = true;
            return Poll::Ready(None);
        }

        let mut val = mem::replace(&mut self.states[min_pos].next, Poll::Pending);

        if positions.is_empty() {
            return val;
        }

        // pull out all the values with times less than the end time from the val batch
        match &mut val {
            Poll::Ready(Some(batch)) => {
                for pos in positions {
                    if let Poll::Ready(Some(b)) = &mut self.states[pos].next {
                        if batch.append_below_time(b, min_time) {
                            self.states[pos].next = Poll::Pending;
                        }
                    }
                }

                batch.sort_by_time();
            }
            _ => unreachable!(),
        }

        val
    }
}

// TODO: Make a constructor function that fails if given an empty `Vec` of `ReadPoint`s.
#[derive(Debug, PartialEq, Clone)]
pub enum ReadValues {
    I64(Vec<ReadPoint<i64>>),
    F64(Vec<ReadPoint<f64>>),
}

impl ReadValues {
    pub fn is_empty(&self) -> bool {
        match self {
            ReadValues::I64(vals) => vals.is_empty(),
            ReadValues::F64(vals) => vals.is_empty(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReadBatch {
    pub key: String,
    pub values: ReadValues,
}

impl ReadBatch {
    /// Returns the first time and the last time in the batch.
    ///
    /// # Panics
    ///
    /// Will panic if there are no values in the `ReadValues`.
    fn start_stop_times(&self) -> (i64, i64) {
        match &self.values {
            ReadValues::I64(vals) => (vals.first().unwrap().time, vals.last().unwrap().time),
            ReadValues::F64(vals) => (vals.first().unwrap().time, vals.last().unwrap().time),
        }
    }

    fn sort_by_time(&mut self) {
        match &mut self.values {
            ReadValues::I64(vals) => vals.sort_by_key(|v| v.time),
            ReadValues::F64(vals) => vals.sort_by_key(|v| v.time),
        }
    }

    // append_below_time will append all values from other that have a time < than the one passed in.
    // it returns true if other has been cleared of all values
    fn append_below_time(&mut self, other: &mut ReadBatch, t: i64) -> bool {
        match (&mut self.values, &mut other.values) {
            (ReadValues::I64(vals), ReadValues::I64(other_vals)) => {
                let pos = other_vals.iter().position(|val| val.time > t);
                match pos {
                    None => vals.append(other_vals),
                    Some(pos) => vals.extend(other_vals.drain(..pos)),
                }
                other_vals.is_empty()
            }
            (ReadValues::F64(vals), ReadValues::F64(other_vals)) => {
                let pos = other_vals.iter().position(|val| val.time > t);
                match pos {
                    None => vals.append(other_vals),
                    Some(pos) => vals.extend(other_vals.drain(..pos)),
                }
                other_vals.is_empty()
            }
            (_, _) => true, // do nothing here
        }
    }

    /// Returns the tag keys and values for this batch, sorted by key.
    pub fn tags(&self) -> Vec<(String, String)> {
        self.tag_string_slices().into_iter().collect()
    }

    /// Returns all tag keys.
    pub fn tag_keys(&self) -> Vec<String> {
        self.tag_string_slices().keys().cloned().collect()
    }

    fn tag_string_slices(&self) -> BTreeMap<String, String> {
        let mut tags = BTreeMap::new();

        for pair in line_parser::index_pairs(&self.key) {
            tags.insert(pair.key, pair.value);
        }

        tags
    }

    /// Returns the `Tag` value associated with the provided key.
    pub fn tag_with_key(&self, key: &str) -> Option<String> {
        self.tag_string_slices().get(key).cloned()
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct PartitionKeyValues {
    pub values: Vec<Option<String>>,
}

impl PartitionKeyValues {
    pub fn new(group_keys: &[String], batch: &ReadBatch) -> Self {
        PartitionKeyValues {
            values: group_keys
                .iter()
                .map(|group_key| batch.tag_with_key(group_key).map(String::from))
                .collect(),
        }
    }
}

impl Ord for PartitionKeyValues {
    fn cmp(&self, other: &Self) -> Ordering {
        self.values
            .iter()
            .zip(other.values.iter())
            .fold(Ordering::Equal, |acc, (a, b)| {
                acc.then_with(|| match (a, b) {
                    (Some(a), Some(b)) => a.partial_cmp(b).unwrap(),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                })
            })
    }
}

impl PartialOrd for PartitionKeyValues {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt};
    use std::fs;

    #[test]
    fn string_merge_stream() {
        let one = stream::iter(vec!["a".to_string(), "c".to_string()].into_iter());
        let two = stream::iter(vec!["b".to_string(), "c".to_string(), "d".to_string()].into_iter());
        let three =
            stream::iter(vec!["c".to_string(), "e".to_string(), "f".to_string()].into_iter());
        let four = stream::iter(vec![].into_iter());

        let merger =
            StringMergeStream::new(vec![one.boxed(), two.boxed(), three.boxed(), four.boxed()]);

        let stream = futures::executor::block_on_stream(merger);
        let vals: Vec<_> = stream.collect();

        assert_eq!(
            vals,
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string()
            ],
        );
    }

    #[test]
    fn read_merge_stream() {
        let one = stream::iter(
            vec![
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 3, value: 30 },
                        ReadPoint { time: 4, value: 40 },
                    ]),
                },
                ReadBatch {
                    key: "test".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 1,
                            value: 1.1,
                        },
                        ReadPoint {
                            time: 2,
                            value: 2.2,
                        },
                    ]),
                },
            ]
            .into_iter(),
        );

        let two = stream::iter(
            vec![
                ReadBatch {
                    key: "bar".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 5,
                            value: 5.5,
                        },
                        ReadPoint {
                            time: 6,
                            value: 6.6,
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 1, value: 10 },
                        ReadPoint { time: 2, value: 20 },
                        ReadPoint { time: 6, value: 60 },
                        ReadPoint {
                            time: 11,
                            value: 110,
                        },
                    ]),
                },
            ]
            .into_iter(),
        );

        let three = stream::iter(
            vec![ReadBatch {
                key: "foo".to_string(),
                values: ReadValues::I64(vec![
                    ReadPoint { time: 5, value: 50 },
                    ReadPoint {
                        time: 10,
                        value: 100,
                    },
                ]),
            }]
            .into_iter(),
        );

        let four = stream::iter(vec![].into_iter());

        let merger =
            ReadMergeStream::new(vec![one.boxed(), two.boxed(), three.boxed(), four.boxed()]);
        let stream = futures::executor::block_on_stream(merger);
        let vals: Vec<_> = stream.collect();

        assert_eq!(
            vals,
            vec![
                ReadBatch {
                    key: "bar".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 5,
                            value: 5.5
                        },
                        ReadPoint {
                            time: 6,
                            value: 6.6
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 1, value: 10 },
                        ReadPoint { time: 2, value: 20 },
                        ReadPoint { time: 3, value: 30 },
                        ReadPoint { time: 4, value: 40 },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 5, value: 50 },
                        ReadPoint { time: 6, value: 60 },
                        ReadPoint {
                            time: 10,
                            value: 100
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![ReadPoint {
                        time: 11,
                        value: 110
                    },]),
                },
                ReadBatch {
                    key: "test".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 1,
                            value: 1.1
                        },
                        ReadPoint {
                            time: 2,
                            value: 2.2
                        }
                    ]),
                },
            ],
        )
    }

    #[test]
    fn read_batch_tag_parsing() {
        let batch = ReadBatch {
            key: "cpu,host=b,region=west\tusage_system".to_string(),
            values: ReadValues::I64(vec![]),
        };

        assert_eq!(
            batch
                .tags()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("_f", "usage_system"),
                ("_m", "cpu"),
                ("host", "b"),
                ("region", "west"),
            ]
        );
    }

    #[test]
    fn partition_key_values_creation() {
        let batch = ReadBatch {
            key: "cpu,host=b,region=west\tusage_system".to_string(),
            values: ReadValues::I64(vec![]),
        };

        let group_keys = vec![
            String::from("region"),
            String::from("not_present"),
            String::from("host"),
        ];

        let partition_key_values = PartitionKeyValues::new(&group_keys, &batch);

        assert_eq!(
            partition_key_values.values,
            vec![Some(String::from("west")), None, Some(String::from("b"))]
        );
    }

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test(threaded_scheduler)]
    async fn partition_writes_wal_metadata() -> Result<()> {
        let store = PartitionStore::MemDB(Box::new(MemDB::new("wal metadata write".into())));
        let dir = delorean_test_helpers::tmp_dir()?.into_path();
        let partition = Partition::new_with_wal(store, dir).await?;
        let wal_metadata_path = partition.wal_details.unwrap().wal.metadata_path();

        let metadata_file_contents = fs::read_to_string(wal_metadata_path)?;

        assert_eq!(metadata_file_contents, r#"{"format":"FlatBuffers"}"#);
        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn partition_checks_metadata_for_supported_format() -> Result<()> {
        let bucket_name = "wal metadata read";
        let store = PartitionStore::MemDB(Box::new(MemDB::new(bucket_name.into())));
        let dir = delorean_test_helpers::tmp_dir()?.into_path();

        let wal_metadata_path = {
            // Create a new Partition to get the WAL metadata path, then drop it
            let partition = Partition::new_with_wal(store.clone(), dir.clone()).await?;
            partition.wal_details.unwrap().wal.metadata_path()
        };

        // Change the metadata to say the WAL is in some format other than what we know about
        let unsupported_format_metadata = r#"{"format":"NotAnythingSupported"}"#;
        fs::write(wal_metadata_path, unsupported_format_metadata)?;

        let partition_error = Partition::restore_memdb_from_wal(bucket_name, dir).await;

        assert!(partition_error.is_err());
        assert_eq!(
            partition_error.err().unwrap().to_string(),
            "Cannot restore from WAL; unknown format"
        );
        Ok(())
    }
}
