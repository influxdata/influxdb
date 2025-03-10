//! This crate provides the data layout for the InfluxDB 3.0 compacted data. This includes the
//! persisted files that Enterprise has compacted into later generations, the snapshots of those
//! generations, and file indexes.

pub mod persist;

use chrono::{DateTime, NaiveDateTime, Utc};
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_enterprise_index::FileIndex;
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::ParquetFile;
use object_store::path::Path as ObjPath;
use serde::{Deserialize, Serialize};
use std::cmp::PartialOrd;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time;

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum Error {
    #[error("invalid generation time string. Expected format %Y-%m-%d/%H-%M")]
    InvalidGenTimeString,

    #[error("error parsing time: {0}")]
    TimeParseError(#[from] chrono::ParseError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System table query result
#[derive(Debug)]
pub struct CompactedDataSystemTableQueryResult {
    pub generation_id: u64,
    pub generation_level: u8,
    pub generation_time: String,
    pub parquet_files: Vec<Arc<ParquetFile>>,
}

/// Which version of the compaction summary we serialize/deserialize with
/// to/from object storage so that we can handle breaking changes and multiple
/// different versions of a file when transitioning from one version to the next
/// when upgrading InfluxDB 3 Enterprise
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum CompactionSummaryVersion {
    #[serde(rename = "1")]
    V1(CompactionSummary),
}

impl CompactionSummaryVersion {
    /// Consume the `CompactionSummaryVersion` and turn into a v1 CompactionSummary
    /// without checking if if is or not
    pub fn v1(self) -> CompactionSummary {
        let Self::V1(cs) = self;
        cs
    }
}

/// The `CompactionSummary` keeps track of the last snapshot from each writer that has been compacted.
/// Every table will have its own `CompactionDetail` and the summary contains a pointer to
/// whatever the latest compaction detail is for each table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionSummary {
    /// The compaction sequence number that created this summary.
    pub compaction_sequence_number: CompactionSequenceNumber,
    /// The compaction catalog's sequence number
    pub catalog_sequence_number: CatalogSequenceNumber,
    /// The last `ParquetFileId` that was used. This will be used to initialize the
    /// `ParquetFileId` on startup to ensure that we don't reuse file ids.
    pub last_file_id: ParquetFileId,
    /// The last `GenerationId` that was used. This will be used to initialize the `GenerationId`
    /// on startup to ensure that we don't reuse generation ids.
    pub last_generation_id: GenerationId,
    /// The last `SnapshotSequenceNumber` for each writer that is getting compacted.
    pub snapshot_markers: Vec<Arc<NodeSnapshotMarker>>,
    /// The compactions sequence number that each table last had a compaction run. This can be used
    /// to construct a path to read the `CompactionDetail`
    pub compaction_details: SerdeVecMap<(DbId, TableId), CompactionSequenceNumber>,
}

/// The last snapshot sequence number for each writer that is getting compacted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshotMarker {
    /// The node identifier prefix this snapshot tracker is for
    pub node_id: String,
    /// The last snapshot sequence number we compacted for this writer. All < than this will have
    /// been compacted.
    pub snapshot_sequence_number: SnapshotSequenceNumber,
    /// The next file id this writer would use after this last snapshot. Any file ids < than this
    /// will have been compacted.
    pub next_file_id: ParquetFileId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum CompactionDetailVersion {
    #[serde(rename = "1")]
    V1(CompactionDetail),
}

/// The `CompactionDetail` contains all the information for the current state of compaction
/// for the given table. A new detail will be generated each time a compaction is run for a table,
/// but only the most recent one is needed. The detail contains the information for any
/// gen1 files that have yet to be compacted into older generation blocks, the young generations
/// and their files, and information about the older generations which can be used to organize
/// compaction and retrieve their files when they are needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionDetail {
    /// The database name for this compaction detail.
    pub db_name: Arc<str>,
    /// The database ID for this compaction detail.
    pub db_id: DbId,
    /// The table name for this compaction detail.
    pub table_name: Arc<str>,
    /// The table ID for this compaction.
    pub table_id: TableId,
    /// The sequence number of the compaction that produced this detail.
    pub sequence_number: CompactionSequenceNumber,
    /// The snapshot markers for this run. Since the compaction details get created as each
    /// table gets compacted, they can run ahead of the global compaction summary. This information
    /// will allow downstream writers using the compacted data to know which gen1 files they
    /// have that should be used vs. what is already compacted in.
    pub snapshot_markers: Vec<Arc<NodeSnapshotMarker>>,
    /// This is the list of all generations of compacted data. The ids of the generations are
    /// unique and can be used to lookup the `GenerationDetail` which contains the list of
    /// `ParquetFile`s that are in that generation and the `FileIndex` for the generation.
    pub compacted_generations: Vec<Generation>,
    /// We keep leftover gen1 files separate from the main generations so that we can advance the
    /// snapshot tracker on the writers. These leftovers will either be stuff that has yet to be
    /// compacted or is historical backfill. We will want to wait to do those compactions with
    /// later generations. Keeping this record lets us have the files referenced but compact them
    /// at a later time. The location of these files will all be from the writers that did the
    /// original persistence.
    pub leftover_gen1_files: Vec<Gen1File>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generation {
    pub id: GenerationId,
    pub level: GenerationLevel,
    pub start_time_secs: i64,
    /// The max nanosecond timestamp of data in this generation
    pub max_time: i64,
}

impl Generation {
    pub fn new(level: GenerationLevel) -> Self {
        Self {
            id: GenerationId::new(),
            level,
            start_time_secs: 0,
            max_time: 0,
        }
    }

    pub fn new_with_start(
        level: GenerationLevel,
        start_time_secs: i64,
        duration: time::Duration,
    ) -> Self {
        Self {
            id: GenerationId::new(),
            level,
            start_time_secs,
            max_time: (start_time_secs + duration.as_secs() as i64) * 1_000_000_000,
        }
    }

    pub fn to_u8_level(&self) -> u8 {
        self.level.as_u8()
    }

    pub fn to_vec_levels(gens: &[Generation]) -> Vec<u8> {
        gens.iter().map(|genr| genr.to_u8_level()).collect()
    }
}

// sort for generation where newer generations are greater than older generations
impl PartialOrd for Generation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Generation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.start_time_secs.cmp(&self.start_time_secs)
    }
}

/// Configuration for compaction
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompactionConfig {
    /// Starting with the gen2 duration, which is the first compacted generation. Its duration
    /// should be a multiple of the gen1 files that are compacted into it. However, the writing
    /// writers creating gen1 files can have different configurations for different gen1 durations.
    /// This will ensure that they get compacted into a gen2 time and then later generations will
    /// be multiples of the previous ones.
    generation_durations: Vec<time::Duration>,
    /// The row limit per compacted file. After this number of rows have been written in, the
    /// remaining rows in the series that is currently being written will be written and the
    /// file will be closed.
    pub per_file_row_limit: usize,
    /// The maximum number of files that will be included in a compaction plan
    pub max_num_files_per_compaction: usize,
}

impl CompactionConfig {
    /// Creates a new `CompactionConfig` with the given multipliers based on the gen2 duration.
    pub fn new(generation_multipliers: &[u8], gen2_duration: time::Duration) -> Self {
        let mut generation_durations = Vec::with_capacity(generation_multipliers.len() + 1);
        generation_durations.push(gen2_duration);
        let mut current_duration = gen2_duration;
        for multiplier in generation_multipliers {
            current_duration *= *multiplier as u32;
            generation_durations.push(current_duration);
        }

        Self {
            generation_durations,
            per_file_row_limit: DEFAULT_PER_FILE_ROW_LIMIT,
            max_num_files_per_compaction: DEFAULT_MAX_NUM_FILES_PER_COMPACTION,
        }
    }

    pub fn with_per_file_row_limit(mut self, per_file_row_limit: usize) -> Self {
        self.per_file_row_limit = per_file_row_limit;
        self
    }

    pub fn with_max_num_files_per_compaction(
        mut self,
        max_num_files_per_compaction: usize,
    ) -> Self {
        self.max_num_files_per_compaction = max_num_files_per_compaction;
        self
    }

    /// Returns the time for the start of the generation based on the level and the time
    pub fn generation_start_time(&self, level: GenerationLevel, time_secs: i64) -> i64 {
        // we don't know the start time for generations lower than gen2, so just return the time back
        if level.is_under_two() {
            return time_secs;
        }

        if let Some(gen_duration) = self.generation_durations.get(level.0 as usize - 2) {
            time_secs - (time_secs % gen_duration.as_secs() as i64)
        } else {
            // if we don't have a duration for this level, just return the time
            time_secs
        }
    }

    /// For a given generation level, return the number of the previous levels it takes to compact
    /// to this generation. Levels 1 or fully compacted generations will return 0.
    pub fn number_of_previous_generations_to_compact(&self, level: GenerationLevel) -> u8 {
        if level.is_under_two() {
            return 0;
        } else if level.is_two() {
            return 2; // gen2 is the first compacted generation, requiring two gen1 blocks
        }

        let previous_duration = self.generation_duration(GenerationLevel::new(level.0 - 1));
        let current_duration = self.generation_duration(level);

        match (previous_duration, current_duration) {
            (Some(prev), Some(curr)) => (curr.as_secs() / prev.as_secs()) as u8,
            _ => 0,
        }
    }

    /// Returns the duration of the level based on the config
    pub fn generation_duration(&self, level: GenerationLevel) -> Option<time::Duration> {
        if level.is_under_two() {
            return None;
        }

        self.generation_durations.get(level.0 as usize - 2).copied()
    }

    /// Returns the start time of the previous generation for the given chunk time
    pub fn previous_generation_start_time(
        &self,
        start_time_secs: i64,
        level: GenerationLevel,
    ) -> i64 {
        let prev_level = GenerationLevel::new(level.0 - 1);
        self.generation_start_time(prev_level, start_time_secs)
    }

    /// Returns the start time of the next generation the passed in generation would be compacted
    /// into, or returns None if the gen has no next generation.
    pub fn next_generation_start_time(&self, genr: &Generation) -> Option<i64> {
        let next_level = GenerationLevel::new(genr.level.0 + 1);
        self.generation_duration(next_level)
            .map(|_d| self.generation_start_time(next_level, genr.start_time_secs))
    }

    /// Returns the compaction levels greater than 2 based on the configuration
    pub fn compaction_levels(&self) -> Vec<GenerationLevel> {
        (3..=self.generation_durations.len() as u8 + 1)
            .map(GenerationLevel::new)
            .collect()
    }
}

const DEFAULT_GENERATION_MULTIPLIERS: &[u8] = &[3, 4, 6, 5];
const DEFAULT_GEN_2_DURATION: std::time::Duration = std::time::Duration::from_secs(1_200);
const DEFAULT_PER_FILE_ROW_LIMIT: usize = 1_000_000;
const DEFAULT_MAX_NUM_FILES_PER_COMPACTION: usize = 500;

/// Default compaction configuration. This is a 20 minute gen2 duration and then 1 hour for gen3,
/// 4 hours for gen4, 1 day for gen5, and 5 days for gen6.
impl Default for CompactionConfig {
    fn default() -> Self {
        Self::new(DEFAULT_GENERATION_MULTIPLIERS, DEFAULT_GEN_2_DURATION)
            .with_per_file_row_limit(DEFAULT_PER_FILE_ROW_LIMIT)
            .with_max_num_files_per_compaction(DEFAULT_MAX_NUM_FILES_PER_COMPACTION)
    }
}

/// The different generation levels. Gen1 represents the parquet files first persisted from
/// the write buffer. Gen2 and above are larger blocks of time compacted together. Thus, gen2
/// is the first generation that is the result of a compaction.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
pub struct GenerationLevel(u8);

impl GenerationLevel {
    pub fn new(value: u8) -> Self {
        Self(value)
    }

    pub fn one() -> Self {
        Self(1)
    }

    pub fn two() -> Self {
        Self(2)
    }

    pub fn is_one(&self) -> bool {
        self.0 == 1
    }

    pub fn is_two(&self) -> bool {
        self.0 == 2
    }

    pub fn is_under_two(&self) -> bool {
        self.0 < 2
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl Display for GenerationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A wrapper for a single gen1 file from the original persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gen1File {
    pub id: GenerationId,
    pub file: Arc<ParquetFile>,
}

impl Gen1File {
    pub fn new(file: Arc<ParquetFile>) -> Self {
        Self {
            id: GenerationId::new(),
            file,
        }
    }

    pub fn generation(&self) -> Generation {
        Generation {
            id: self.id,
            level: GenerationLevel::one(),
            start_time_secs: self.file.chunk_time / 1_000_000_000,
            max_time: self.file.max_time,
        }
    }
}

impl PartialOrd for Gen1File {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Gen1File {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file.chunk_time.cmp(&other.file.chunk_time)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct GenerationId(u64);

pub static NEXT_GENERATION_ID: AtomicU64 = AtomicU64::new(0);

impl GenerationId {
    pub fn new() -> Self {
        Self(NEXT_GENERATION_ID.fetch_add(1, Ordering::SeqCst))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn current() -> Self {
        GenerationId(NEXT_GENERATION_ID.load(Ordering::SeqCst))
    }

    pub fn initialize(last_generation_id: GenerationId) {
        NEXT_GENERATION_ID.store(last_generation_id.0 + 1, Ordering::SeqCst);
    }
}

impl Default for GenerationId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<u64> for GenerationId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionSequenceNumber(u64);

impl CompactionSequenceNumber {
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionSummaryPath(ObjPath);

impl CompactionSummaryPath {
    pub fn new(
        compactor_id: Arc<str>,
        compaction_sequence_number: CompactionSequenceNumber,
    ) -> Self {
        Self(ObjPath::from(format!(
            "{}/cs/{:020}.json",
            compactor_id,
            object_store_number_order(compaction_sequence_number.0)
        )))
    }

    pub fn into_inner(self) -> ObjPath {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionDetailPath(ObjPath);

impl CompactionDetailPath {
    pub fn into_inner(self) -> ObjPath {
        self.0
    }
}

/// Serde serialization for `CompactionDetailPath` that serializes it to a string.
impl Serialize for CompactionDetailPath {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.to_string().serialize(serializer)
    }
}

/// Serde deserialization for `CompactionDetailPath` that deserializes it from a string.
impl<'de> Deserialize<'de> for CompactionDetailPath {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self(ObjPath::from(s)))
    }
}

impl CompactionDetailPath {
    pub fn new(
        compactor_id: Arc<str>,
        db_id: DbId,
        table_id: TableId,
        compaction_sequence_number: CompactionSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{compactor_id}/cd/{db_id}/{table_id}/{:020}.json",
            object_store_number_order(compaction_sequence_number.0),
        ));
        Self(path)
    }

    pub fn db_id(&self) -> DbId {
        self.0
            .parts()
            .nth(2)
            .map(|part| part.as_ref().parse::<u32>())
            .expect("valid u32 when extracting db id from compaction path")
            .expect("valid compaction detail path when extracting db id")
            .into()
    }

    pub fn table_id(&self) -> TableId {
        self.0
            .parts()
            .nth(3)
            .map(|part| part.as_ref().parse::<u32>())
            .expect("valid compaction detail path when extracting table id")
            .expect("valid u32 when extracting table id from compaction path")
            .into()
    }

    pub fn as_path(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenerationDetail {
    pub id: GenerationId,
    pub level: GenerationLevel,
    pub start_time_s: i64,
    pub max_time_ns: i64,
    pub files: Vec<Arc<ParquetFile>>,
    pub file_index: FileIndex,
}

/// Generation detail files contain the `Vec<ParquetFile>` in the generation and the `FileIndex`.
/// The path to these files can be determined with only the compactor id and the generation id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationDetailPath(ObjPath);

impl GenerationDetailPath {
    pub fn new(compactor_id: Arc<str>, generation_id: GenerationId) -> Self {
        // we use a hash of the id so that we don't create hotspots in the S3 keyspace from
        // using sequential ids
        let hash = xxhash_rust::xxh32::xxh32(&generation_id.0.to_be_bytes(), 0);
        let hash = format!("{:x}", hash);

        // we break it up into directories so that we don't have too many subdirectories of files
        // in a single directory
        Self(ObjPath::from(format!(
            "{}/c/{}/{}/{}/{}.json",
            compactor_id,
            &hash[0..=1],
            &hash[2..=4],
            &hash[5..],
            generation_id.0,
        )))
    }

    pub fn into_inner(self) -> ObjPath {
        self.0
    }

    pub fn as_path(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedFilePath(ObjPath);

impl CompactedFilePath {
    pub fn new(compactor_id: &str, generation_id: GenerationId, file_id: ParquetFileId) -> Self {
        let hash = xxhash_rust::xxh32::xxh32(&generation_id.0.to_be_bytes(), 0);
        let hash = format!("{:x}", hash);

        Self(ObjPath::from(format!(
            "{}/c/{}/{}/{}/{}.parquet",
            compactor_id,
            &hash[0..=1],
            &hash[2..=4],
            &hash[5..],
            file_id.as_u64(),
        )))
    }

    pub fn as_object_store_path(&self) -> &ObjPath {
        &self.0
    }
}

fn object_store_number_order(n: u64) -> u64 {
    u64::MAX - n
}

const GEN_TIME_FORMAT: &str = "%Y-%m-%d/%H-%M";

pub fn gen_time_string(start_time_secs: i64) -> String {
    let date_time = DateTime::<Utc>::from_timestamp(start_time_secs, 0);
    date_time
        .map(|dt| dt.format(GEN_TIME_FORMAT).to_string())
        .unwrap_or_else(|| "INVALID_TIME".to_string())
}

pub fn gen_time_string_to_start_time_secs(s: &str) -> Result<i64> {
    let t = NaiveDateTime::parse_from_str(s, GEN_TIME_FORMAT)?;
    Ok(t.and_utc().timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn gen_time_strings() {
        let test_time: DateTime<Utc> = Utc.with_ymd_and_hms(2017, 4, 2, 12, 50, 0).unwrap();
        let genstr = gen_time_string(test_time.timestamp());
        assert_eq!(genstr, "2017-04-02/12-50");
        let from_str = gen_time_string_to_start_time_secs(&genstr).unwrap();
        assert_eq!(test_time.timestamp(), from_str);

        let test_time: DateTime<Utc> = Utc.with_ymd_and_hms(2017, 4, 2, 23, 0, 0).unwrap();
        let genstr = gen_time_string(test_time.timestamp());
        assert_eq!(genstr, "2017-04-02/23-00");
        let from_str = gen_time_string_to_start_time_secs(&genstr).unwrap();
        assert_eq!(test_time.timestamp(), from_str);
    }

    #[test]
    fn compaction_config() {
        // gen2 duration is 20 minutes
        // gen3 is 3 of gen 2 (1hour), 4 is 4 hours, 5 is 1d, and 6 is 5d
        let config = CompactionConfig::new(&[3, 4, 6, 5], time::Duration::from_secs(1200))
            .with_per_file_row_limit(1_000_000);

        let middle_hour = gen_time_string_to_start_time_secs("2024-09-08/15-30").unwrap();
        assert_eq!(gen_time_string(middle_hour), "2024-09-08/15-30");
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(2), middle_hour)),
            "2024-09-08/15-20"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(3), middle_hour)),
            "2024-09-08/15-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(4), middle_hour)),
            "2024-09-08/12-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(5), middle_hour)),
            "2024-09-08/00-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(6), middle_hour)),
            "2024-09-04/00-00"
        );

        let start_hour = gen_time_string_to_start_time_secs("2024-09-08/00-00").unwrap();
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(2), start_hour)),
            "2024-09-08/00-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(3), start_hour)),
            "2024-09-08/00-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(4), start_hour)),
            "2024-09-08/00-00"
        );
        assert_eq!(
            gen_time_string(config.generation_start_time(GenerationLevel::new(5), start_hour)),
            "2024-09-08/00-00"
        );
    }

    #[test]
    fn generations_should_sort_newest_to_oldest() {
        let g1 = Generation {
            id: GenerationId::from(30),
            level: GenerationLevel::one(),
            start_time_secs: 100,
            max_time: 0,
        };
        let g2 = Generation {
            id: GenerationId::from(26),
            level: GenerationLevel::two(),
            start_time_secs: 50,
            max_time: 0,
        };
        let g3 = Generation {
            id: GenerationId::from(11),
            level: GenerationLevel::new(3),
            start_time_secs: 0,
            max_time: 0,
        };

        let mut gens = vec![g1, g2, g3];
        gens.sort();
        assert_eq!(gens, vec![g1, g2, g3]);

        let mut gens = vec![g3, g2, g1];
        gens.sort();
        assert_eq!(gens, vec![g1, g2, g3]);
    }

    #[test]
    fn compaction_levels() {
        let config = CompactionConfig::new(&[2, 3, 4, 5], time::Duration::from_secs(60))
            .with_per_file_row_limit(1_000_000);
        let levels = config.compaction_levels();
        assert_eq!(
            levels,
            vec![
                GenerationLevel::new(3),
                GenerationLevel::new(4),
                GenerationLevel::new(5),
                GenerationLevel::new(6)
            ]
        );
    }

    #[test]
    fn test_number_of_previous_generations_to_compact() {
        let config = CompactionConfig::new(&[2, 3, 4, 5], time::Duration::from_secs(60))
            .with_per_file_row_limit(1_000_000);
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(1)),
            0
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(2)),
            2
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(3)),
            2
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(4)),
            3
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(5)),
            4
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(6)),
            5
        );
        assert_eq!(
            config.number_of_previous_generations_to_compact(GenerationLevel::new(7)),
            0
        );
    }

    #[test]
    fn compaction_detail_v1_serializes_and_deserializes_properly() {
        let cdv = CompactionDetailVersion::V1(CompactionDetail {
            db_name: "foo".into(),
            db_id: 0.into(),
            table_name: "bar".into(),
            table_id: 0.into(),
            sequence_number: CompactionSequenceNumber(0),
            snapshot_markers: Vec::new(),
            compacted_generations: Vec::new(),
            leftover_gen1_files: Vec::new(),
        });

        let serialized = serde_json::to_string(&cdv).unwrap();
        insta::assert_json_snapshot!(serialized);
        let deserialized: CompactionDetailVersion = serde_json::from_str(&serialized).unwrap();

        assert_eq!(cdv, deserialized);
    }

    #[test]
    fn compaction_summary_v1_serializes_and_deserializes_properly() {
        let cdv = CompactionSummaryVersion::V1(CompactionSummary {
            compaction_sequence_number: CompactionSequenceNumber(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            last_file_id: ParquetFileId::from(0),
            last_generation_id: GenerationId(0),
            snapshot_markers: Vec::new(),
            compaction_details: SerdeVecMap::new(),
        });

        let serialized = serde_json::to_string(&cdv).unwrap();
        insta::assert_json_snapshot!(serialized);
        let deserialized: CompactionSummaryVersion = serde_json::from_str(&serialized).unwrap();

        assert_eq!(cdv, deserialized);
    }
}
