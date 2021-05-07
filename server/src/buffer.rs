//! This module contains code for managing the Write Buffer

use data_types::{database_rules::WriteBufferRollover, server_id::ServerId, DatabaseName};
use entry::{ClockValue, Segment as EntrySegment, SequencedEntry};
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};

use std::{convert::TryInto, mem, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use crc32fast::Hasher;
use data_types::database_rules::WriteBufferConfig;
use data_types::write_buffer::SegmentPersistence;
use observability_deps::tracing::{error, info, warn};
use parking_lot::Mutex;
use snafu::{ensure, ResultExt, Snafu};
use tracker::{TaskRegistration, TrackedFutureExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Max size limit hit {}", size))]
    MaxSizeLimit { size: u64 },

    #[snafu(display(
        "Unable to drop segment to reduce size below max. Current size {}, segment count {}",
        size,
        segment_count
    ))]
    UnableToDropSegment { size: usize, segment_count: usize },

    #[snafu(display(
        "Sequence from server {} out of order. Current: {}, incomming {}",
        server,
        current_sequence,
        incoming_sequence,
    ))]
    SequenceOutOfOrder {
        server: ServerId,
        current_sequence: u64,
        incoming_sequence: u64,
    },

    #[snafu(display("segment id must be between [1, 1,000,000,000)"))]
    SegmentIdOutOfBounds,

    #[snafu(display("unable to compress segment id {}: {}", segment_id, source))]
    UnableToCompressData {
        segment_id: u64,
        source: snap::Error,
    },

    #[snafu(display("unable to decompress segment data: {}", source))]
    UnableToDecompressData { source: snap::Error },

    #[snafu(display("unable to read checksum: {}", source))]
    UnableToReadChecksum {
        source: std::array::TryFromSliceError,
    },

    #[snafu(display("checksum mismatch for segment"))]
    ChecksumMismatch,

    #[snafu(display("the flatbuffers Segment is invalid: {}", source))]
    InvalidFlatbuffersSegment {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("the flatbuffers size is too small; only found {} bytes", bytes))]
    FlatbuffersSegmentTooSmall { bytes: usize },

    #[snafu(display("flatbuffers for segment invalid: {}", source))]
    FlatbuffersInvalid { source: entry::SequencedEntryError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An in-memory buffer for the Write Buffer. It is split up into segments,
/// which can be persisted to object storage.
#[derive(Debug)]
pub struct Buffer {
    max_size: usize,
    current_size: usize,
    segment_size: usize,
    pub persist: bool,
    open_segment: Segment,
    closed_segments: Vec<Arc<Segment>>,
    rollover_behavior: WriteBufferRollover,
    highwater_clock: Option<ClockValue>,
    server_id: ServerId,
}

impl Buffer {
    pub fn new(
        max_size: usize,
        segment_size: usize,
        rollover_behavior: WriteBufferRollover,
        persist: bool,
        server_id: ServerId,
    ) -> Self {
        Self {
            max_size,
            segment_size,
            persist,
            rollover_behavior,
            open_segment: Segment::new(1, server_id),
            current_size: 0,
            closed_segments: vec![],
            highwater_clock: None,
            server_id,
        }
    }

    pub fn new_from_config(config: &WriteBufferConfig, server_id: ServerId) -> Self {
        Self::new(
            config.buffer_size,
            config.segment_size,
            config.buffer_rollover,
            config.store_segments,
            server_id,
        )
    }

    /// Appends a `SequencedEntry` onto the buffer, returning the segment if it
    /// has been closed out. If the max size of the buffer would be exceeded
    /// by accepting the write, the oldest (first) of the closed segments
    /// will be dropped, if it is persisted. Otherwise, an error is returned.
    pub fn append(&mut self, write: Arc<dyn SequencedEntry>) -> Result<Option<Arc<Segment>>> {
        let write_size = write.size();

        while self.current_size + write_size > self.max_size {
            let oldest_is_persisted = match self.closed_segments.get(0) {
                Some(s) => s.persisted().is_some(),
                None => false,
            };

            if oldest_is_persisted {
                self.remove_oldest_segment();
                continue;
            }

            match self.rollover_behavior {
                WriteBufferRollover::DropIncoming => {
                    warn!(
                        "Write Buffer is full, dropping incoming write \
                        for current segment (segment id: {:?})",
                        self.open_segment.id,
                    );
                    return Ok(None);
                }
                WriteBufferRollover::DropOldSegment => {
                    let oldest_segment_id = self.remove_oldest_segment();
                    warn!(
                        "Write Buffer is full, dropping oldest segment (segment id: {:?})",
                        oldest_segment_id
                    );
                }
                WriteBufferRollover::ReturnError => {
                    return UnableToDropSegment {
                        size: self.current_size,
                        segment_count: self.closed_segments.len(),
                    }
                    .fail();
                }
            }
        }

        let mut closed_segment = None;

        self.current_size += write_size;
        self.open_segment.append(write);
        if self.open_segment.size > self.segment_size {
            let next_id = self.open_segment.id + 1;
            let segment = mem::replace(
                &mut self.open_segment,
                Segment::new(next_id, self.server_id),
            );
            let segment = Arc::new(segment);

            self.closed_segments.push(Arc::clone(&segment));
            closed_segment = Some(segment);
        }

        Ok(closed_segment)
    }

    /// Returns the current size of the buffer.
    pub fn size(&self) -> usize {
        self.current_size
    }

    /// Returns any writes after the passed in `ClockValue`
    pub fn writes_since(&self, since: ClockValue) -> Vec<Arc<dyn SequencedEntry>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for w in self.open_segment.writes.iter().rev() {
            if w.clock_value() <= since {
                writes.reverse();
                return writes;
            }
            writes.push(Arc::clone(w));
        }

        for s in self.closed_segments.iter().rev() {
            for w in s.writes.iter().rev() {
                if w.clock_value() == since {
                    writes.reverse();
                    return writes;
                }
                writes.push(Arc::clone(w));
            }
        }

        writes.reverse();
        writes
    }

    /// Removes the oldest segment present in the buffer, returning its id
    fn remove_oldest_segment(&mut self) -> u64 {
        let removed_segment = self.closed_segments.remove(0);
        self.current_size -= removed_segment.size;
        removed_segment.id
    }
}

/// Segment is a collection of sequenced entries that can be persisted to
/// object store.
#[derive(Debug)]
pub struct Segment {
    pub(crate) id: u64,
    size: usize,
    pub writes: Vec<Arc<dyn SequencedEntry>>,
    // Time this segment was initialized
    created_at: DateTime<Utc>,
    // Persistence metadata if segment is persisted
    persisted: Mutex<Option<SegmentPersistence>>,
    server_id: ServerId,
    highwater_clock: Option<ClockValue>,
}

impl Segment {
    fn new(id: u64, server_id: ServerId) -> Self {
        Self {
            id,
            size: 0,
            writes: vec![],
            created_at: Utc::now(),
            persisted: Mutex::new(None),
            server_id,
            highwater_clock: None,
        }
    }

    // appends the write to the segment
    fn append(&mut self, write: Arc<dyn SequencedEntry>) {
        self.size += write.size();
        self.writes.push(write);
    }

    /// sets the persistence metadata for this segment
    pub fn set_persisted(&self, persisted: SegmentPersistence) {
        let mut self_persisted = self.persisted.lock();
        *self_persisted = Some(persisted);
    }

    /// returns persistence metadata for this segment if persisted
    pub fn persisted(&self) -> Option<SegmentPersistence> {
        self.persisted.lock().clone()
    }

    /// Spawns a tokio task that will continuously try to persist the bytes to
    /// the given object store location.
    pub fn persist_bytes_in_background(
        &self,
        tracker: TaskRegistration,
        db_name: &DatabaseName<'_>,
        store: Arc<ObjectStore>,
    ) -> Result<()> {
        let data = self.to_file_bytes()?;
        let location = database_object_store_path(self.server_id, db_name, &store);
        let location = object_store_path_for_segment(&location, self.id)?;

        let len = data.len();
        let mut stream_data = std::io::Result::Ok(data.clone());

        tokio::task::spawn(
            async move {
                while let Err(err) = store
                    .put(
                        &location,
                        futures::stream::once(async move { stream_data }),
                        Some(len),
                    )
                    .await
                {
                    error!("error writing bytes to store: {}", err);
                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        super::STORE_ERROR_PAUSE_SECONDS,
                    ))
                    .await;
                    stream_data = std::io::Result::Ok(data.clone());
                }

                // TODO: Mark segment as persisted
                info!("persisted data to {}", location.display());
            }
            .track(tracker),
        );

        Ok(())
    }

    /// serialize the segment to the bytes to represent it in a file. This
    /// compresses the flatbuffers payload and writes a crc32 checksum at
    /// the end. The Segment will get the server ID this Buffer is on and
    /// the highwater clock value for the last consistency check of this Buffer.
    pub fn to_file_bytes(&self) -> Result<Bytes> {
        let segment = EntrySegment::new_from_entries(
            self.id,
            self.server_id,
            self.highwater_clock,
            &self.writes,
        );

        let mut encoder = snap::raw::Encoder::new();
        let mut compressed_data =
            encoder
                .compress_vec(segment.data())
                .context(UnableToCompressData {
                    segment_id: self.id,
                })?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let checksum = hasher.finalize();

        compressed_data.extend_from_slice(&checksum.to_le_bytes());

        Ok(Bytes::from(compressed_data))
    }

    /// checks the crc32 for the compressed data, decompresses it and
    /// deserializes it into a Segment from internal_types::entry.
    pub fn entry_segment_from_file_bytes(data: &[u8]) -> Result<EntrySegment> {
        if data.len() < std::mem::size_of::<u32>() {
            return FlatbuffersSegmentTooSmall { bytes: data.len() }.fail();
        }

        let (data, checksum) = data.split_at(data.len() - std::mem::size_of::<u32>());
        let checksum = u32::from_le_bytes(checksum.try_into().context(UnableToReadChecksum)?);

        let mut hasher = Hasher::new();
        hasher.update(&data);

        if checksum != hasher.finalize() {
            return Err(Error::ChecksumMismatch);
        }

        let mut decoder = snap::raw::Decoder::new();
        let data = decoder
            .decompress_vec(data)
            .context(UnableToDecompressData)?;

        data.try_into().context(FlatbuffersInvalid)
    }
}

const WRITE_BUFFER_DIR: &str = "wb";
const MAX_SEGMENT_ID: u64 = 999_999_999;
const SEGMENT_FILE_EXTENSION: &str = ".segment";

/// Builds the path for a given segment id, given the root object store path.
/// The path should be where the root of the database is (e.g. 1/my_db/).
fn object_store_path_for_segment<P: ObjectStorePath>(root_path: &P, segment_id: u64) -> Result<P> {
    ensure!(
        segment_id < MAX_SEGMENT_ID && segment_id > 0,
        SegmentIdOutOfBounds
    );

    let millions_place = segment_id / 1_000_000;
    let millions = millions_place * 1_000_000;
    let thousands_place = (segment_id - millions) / 1_000;
    let thousands = thousands_place * 1_000;
    let hundreds_place = segment_id - millions - thousands;

    let mut path = root_path.clone();
    path.push_all_dirs(&[
        WRITE_BUFFER_DIR,
        &format!("{:03}", millions_place),
        &format!("{:03}", thousands_place),
    ]);
    path.set_file_name(format!("{:03}{}", hundreds_place, SEGMENT_FILE_EXTENSION));

    Ok(path)
}

// base location in object store for a given database name
fn database_object_store_path(
    server_id: ServerId,
    database_name: &DatabaseName<'_>,
    store: &ObjectStore,
) -> object_store::path::Path {
    let mut path = store.new_path();
    path.push_dir(format!("{}", server_id));
    path.push_dir(database_name.to_string());
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use entry::{test_helpers::lp_to_sequenced_entry as lp_2_se, SequencedEntry};
    use object_store::memory::InMemory;
    use std::{convert::TryFrom, ops::Deref};

    #[test]
    fn append_increments_current_size_and_uses_existing_segment() {
        let max = 1 << 32;
        let segment = 1 << 16;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::ReturnError,
            false,
            server_id,
        );
        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");

        let size = write.size();
        assert_eq!(0, buf.size());
        let segment = buf.append(write).unwrap();
        assert_eq!(size, buf.size());
        assert!(segment.is_none());

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert_eq!(size * 2, buf.size());
        assert!(segment.is_none());
    }

    #[test]
    fn append_rolls_over_segment() {
        let max = 1 << 16;
        let segment = 1;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::ReturnError,
            false,
            server_id,
        );
        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 1);

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");

        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 2);
    }

    #[test]
    fn drops_persisted_segment_when_over_size() {
        let max = 600;
        let segment = 1;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::ReturnError,
            false,
            server_id,
        );

        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);
        assert!(segment.persisted().is_none());
        segment.set_persisted(SegmentPersistence {
            location: "PLACEHOLDER".to_string(),
            time: Utc::now(),
        });

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_sequenced_entry(server_id, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);
        assert!(segment.persisted().is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_old_segment_even_if_not_persisted() {
        let max = 600;
        let segment = 1;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::DropOldSegment,
            false,
            server_id,
        );

        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_sequenced_entry(server_id, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[test]
    fn drops_incoming_write_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::DropIncoming,
            false,
            server_id,
        );

        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_sequenced_entry(server_id, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[test]
    fn returns_error_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let server_id = ServerId::try_from(1).unwrap();
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::ReturnError,
            false,
            server_id,
        );

        let write = lp_to_sequenced_entry(server_id, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_sequenced_entry(server_id, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_sequenced_entry(server_id, 3, "cpu val=1 10");
        assert!(buf.append(write).is_err());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    fn equal_to_server_id_and_clock_value(
        sequenced_entry: &dyn SequencedEntry,
        expected_server_id: ServerId,
        expected_clock_value: u64,
    ) {
        assert_eq!(sequenced_entry.server_id(), expected_server_id);
        assert_eq!(
            sequenced_entry.clock_value(),
            ClockValue::try_from(expected_clock_value).unwrap()
        );
    }

    #[test]
    fn writes_since() {
        let max = 1 << 63;
        let server_id1 = ServerId::try_from(1).unwrap();
        let server_id2 = ServerId::try_from(2).unwrap();
        let write = lp_to_sequenced_entry(server_id1, 1, "cpu val=1 10");
        let segment = write.size() + 1;
        let mut buf = Buffer::new(
            max,
            segment,
            WriteBufferRollover::ReturnError,
            false,
            server_id1,
        );

        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_sequenced_entry(server_id2, 1, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_sequenced_entry(server_id1, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let write = lp_to_sequenced_entry(server_id1, 3, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_sequenced_entry(server_id2, 2, "cpu val=1 10");
        let segment = buf.append(write).unwrap();
        assert!(segment.is_none());

        let writes = buf.writes_since(ClockValue::try_from(1).unwrap());
        assert_eq!(3, writes.len());
        equal_to_server_id_and_clock_value(writes[0].deref(), server_id1, 2);
        equal_to_server_id_and_clock_value(writes[1].deref(), server_id1, 3);
        equal_to_server_id_and_clock_value(writes[2].deref(), server_id2, 2);
    }

    #[test]
    fn valid_object_store_path_for_segment() {
        let storage = ObjectStore::new_in_memory(InMemory::new());
        let mut base_path = storage.new_path();
        base_path.push_all_dirs(&["1", "mydb"]);

        let segment_path = object_store_path_for_segment(&base_path, 23).unwrap();
        let mut expected_segment_path = base_path.clone();
        expected_segment_path.push_all_dirs(&["wb", "000", "000"]);
        expected_segment_path.set_file_name("023.segment");
        assert_eq!(segment_path, expected_segment_path);

        let segment_path = object_store_path_for_segment(&base_path, 20_003).unwrap();
        let mut expected_segment_path = base_path.clone();
        expected_segment_path.push_all_dirs(&["wb", "000", "020"]);
        expected_segment_path.set_file_name("003.segment");
        assert_eq!(segment_path, expected_segment_path);

        let segment_path = object_store_path_for_segment(&base_path, 45_010_105).unwrap();
        let mut expected_segment_path = base_path;
        expected_segment_path.push_all_dirs(&["wb", "045", "010"]);
        expected_segment_path.set_file_name("105.segment");
        assert_eq!(segment_path, expected_segment_path);
    }

    #[test]
    fn object_store_path_for_segment_out_of_bounds() {
        let storage = ObjectStore::new_in_memory(InMemory::new());
        let mut base_path = storage.new_path();
        base_path.push_all_dirs(&["1", "mydb"]);

        let segment_path = object_store_path_for_segment(&base_path, 0).err().unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);

        let segment_path = object_store_path_for_segment(&base_path, 23_000_000_000)
            .err()
            .unwrap();
        matches!(segment_path, Error::SegmentIdOutOfBounds);
    }

    #[test]
    fn segment_serialize_deserialize() {
        let server_id = ServerId::try_from(1).unwrap();
        let id = 1;
        let mut segment = Segment::new(id, server_id);
        let entry1 = lp_to_sequenced_entry(server_id, 1, "foo val=1 123");
        segment.append(Arc::clone(&entry1));
        let entry2 = lp_to_sequenced_entry(server_id, 2, "foo val=2 124");
        segment.append(Arc::clone(&entry2));

        let data = segment.to_file_bytes().unwrap();
        let recovered_segment = Segment::entry_segment_from_file_bytes(&data).unwrap();
        let recovered_entries: Vec<_> = recovered_segment.fb().entries().unwrap().iter().collect();

        assert_eq!(recovered_entries.len(), 2);
        assert_eq!(recovered_entries[0].server_id(), 1);
        assert_eq!(recovered_entries[0].clock_value(), 1);
        assert_eq!(
            recovered_entries[0].entry_bytes().unwrap(),
            entry1.fb().entry_bytes().unwrap()
        );

        assert_eq!(recovered_entries[1].server_id(), 1);
        assert_eq!(recovered_entries[1].clock_value(), 2);
        assert_eq!(
            recovered_entries[1].entry_bytes().unwrap(),
            entry2.fb().entry_bytes().unwrap()
        );
    }

    fn lp_to_sequenced_entry(
        server_id: ServerId,
        clock_value: u64,
        line_protocol: &str,
    ) -> Arc<dyn SequencedEntry> {
        Arc::new(lp_2_se(line_protocol, server_id.get_u32(), clock_value))
    }
}
