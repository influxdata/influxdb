//! This crate provides the data layout for the InfluxDB 3.0 compacted data. This includes the
//! persisted files that Pro has compacted into later generations, the snapshots of those
//! generations, and file indexes.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use hashbrown::HashMap;
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::{ParquetFile, ParquetFileId};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::cmp::PartialOrd;
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time;

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum Error {
    #[error("invalid generation time string. Expected format %Y-%m-%d/%H-%M")]
    InvalidGenTimeString,

    #[error("error parsing time: {0}")]
    TimeParseError(#[from] chrono::ParseError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The `CompactionSummary` keeps track of the last snapshot from each host that has been compacted.
/// Every table will have its own `CompactionDetail` and the summary contains a pointer to
/// whatever the latest compaction detail is for each table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionSummary {
    /// The compaction sequence number that created this summary.
    pub compaction_sequence_number: CompactionSequenceNumber,
    /// The next `ParquetFileId` that that should be used if this server is restarted and
    /// reads state off object store to then start doing compactions.
    pub next_file_id: ParquetFileId,
    /// The last `SnapshotSequenceNumber` for each host that is getting compacted.
    pub snapshot_markers: Vec<HostSnapshotMarker>,
    /// The paths to the compaction details for each table.
    pub compaction_details: Vec<CompactionDetailPath>,
}

/// The last snapshot sequence number for each host that is getting compacted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSnapshotMarker {
    pub host_id: Arc<str>,
    pub snapshot_sequence_number: SnapshotSequenceNumber,
}

/// The `CompactionDetail` contains all the information for the current state of compaction
/// for the given table. A new detail will be generated each time a compaction is run for a table,
/// but only the most recent one is needed. The detail contains the information for any
/// gen1 files that have yet to be compacted into older generation blocks, the young generations
/// and their files, and information about the older generations which can be used to organize
/// compaction and retrieve their files when they are needed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionDetail {
    /// The sequence number of the compaction that produced this detail.
    pub sequence_number: CompactionSequenceNumber,
    /// The snapshot markers for this run. Since the compaction details get created as each
    /// table gets compacted, they can run ahead of the global compaction summary. This information
    /// will allow downstream hosts using the compacted data to know which gen1 files they
    /// have that should be used vs. what is already compacted in.
    pub snapshot_markers: Vec<HostSnapshotMarker>,
    /// We keep the young generations separate from the old generations so that the young
    /// generations can include the file lists.
    pub young_generations: Vec<Arc<YoungGeneration>>,
    /// Old generations don't include the file list. They load them dynamically from the
    /// index when requested.
    pub old_generations: Vec<Arc<OldGeneration>>,
    /// We keep leftover gen1 files separate from the main generations so that we can advance the
    /// snapshot tracker on the hosts. These leftovers will either be stuff that has yet to be
    /// compacted or is historical backfill. We will want to wait do do those compactions with
    /// later generations. Keeping this record lets us have the files referenced but compact them
    /// at a later time. The location of these files will all be from the hosts that did the
    /// original persistence.
    pub leftover_gen1_files: Vec<Arc<ParquetFile>>,
}

impl CompactionDetail {
    /// Returns all generations in a single collection in time descending order, including young,
    /// old, and leftover gen1.
    fn generations(
        &self,
        compactor_id: &str,
        db_name: &str,
        table_name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Vec<Arc<dyn Generation>> {
        let mut generations = Vec::new();
        for young_gen in &self.young_generations {
            generations.push(Arc::clone(young_gen) as Arc<dyn Generation>);
        }

        for f in &self.leftover_gen1_files {
            generations.push(Arc::new(Gen1::new(Arc::clone(f))) as Arc<dyn Generation>);
        }

        for old_gen in &self.old_generations {
            let index_file_path = IndexFilePath::new(
                compactor_id,
                db_name,
                table_name,
                old_gen.level,
                &gen_time_string(old_gen.start_time),
                old_gen.compaction_sequence_number,
            );
            generations.push(Arc::new(OldGenWrapper {
                _object_store: Arc::clone(&object_store),
                _index_file_path: index_file_path,
                old_gen: Arc::clone(old_gen),
            }) as Arc<dyn Generation>);
        }

        generations.sort_by_key(|a| a.start_time_secs());

        generations
    }
}

/// Trait for generation to hide the implementation of young, old, and leftover gen1. Used by the
/// planner to determine which generations to compact together to form a new generation.
#[async_trait]
pub trait Generation {
    fn id(&self) -> GenerationId;

    /// The start time as a second epoch for the generation
    fn start_time_secs(&self) -> i64;

    /// The path to files for this generation. Files are under:
    /// `<compactor_id>/c/<db_name>/<table_name>/<generation_level>/<YYYY-MM-DD>/<HH-MM>/...`
    /// This function returns the `<generation_level>/<YYYY-MM-DD>/<HH-MM>` part.
    fn gen_path(&self) -> String;

    fn level(&self) -> GenerationLevel;

    /// All the `ParquetFile`s contained in this generation.
    async fn files(&self) -> Vec<Arc<ParquetFile>>;
}

impl std::fmt::Debug for dyn Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Generation")
            .field("id", &self.id())
            .field("start_time_secs", &self.start_time_secs())
            .field("gen_path", &self.gen_path())
            .field("level", &self.level())
            .finish()
    }
}

/// Configuration for compaction
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Starting with the gen2 duration, which is the first compacted generation. Its duration
    /// should be a multiple of the gen1 files that are compacted into it. However, the writing
    /// hosts creating gen1 files can have different configurations for different gen1 durations.
    /// This will ensure that they get compacted into a gen2 time and then later generations will
    /// be multiples of the previous ones.
    generation_durations: Vec<time::Duration>,
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
        }
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
}

/// Default compaction configuration. This is a 20 minute gen2 duration and then 1 hour for gen3,
/// 4 hours for gen4, 1 day for gen5, and 5 days for gen6.
impl Default for CompactionConfig {
    fn default() -> Self {
        Self::new(&[3, 4, 6, 5], time::Duration::from_secs(1200))
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

    pub fn is_two(&self) -> bool {
        self.0 == 2
    }

    pub fn is_under_two(&self) -> bool {
        self.0 < 2
    }
}

impl Display for GenerationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The details of a young generation, which will be 2-5. Gen1 has its own implementation since
/// those are always only a single file. The young generations will have the full file information
/// included, which will include the details in the `CompactionDetail`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct YoungGeneration {
    id: GenerationId,
    compaction_sequence_number: CompactionSequenceNumber,
    level: GenerationLevel,
    start_time: i64,
    files: Vec<Arc<ParquetFile>>,
}

#[async_trait]
impl Generation for YoungGeneration {
    fn id(&self) -> GenerationId {
        self.id
    }

    fn start_time_secs(&self) -> i64 {
        self.start_time / 1_000_000_000
    }

    fn gen_path(&self) -> String {
        format!("{}/{}", self.level().0, gen_time_string(self.start_time))
    }

    fn level(&self) -> GenerationLevel {
        self.level
    }

    async fn files(&self) -> Vec<Arc<ParquetFile>> {
        self.files.clone()
    }
}

/// The details of an old generation, which will be >= 6. The old generations will not have the
/// file information included, as they will be loaded dynamically from the index when needed.
/// We do this because older generations could potentially have thousands of individual files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OldGeneration {
    id: GenerationId,
    compaction_sequence_number: CompactionSequenceNumber,
    level: GenerationLevel,
    start_time: i64,
}

/// A wrapper for a single gen1 file from the original persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gen1 {
    id: GenerationId,
    file: Arc<ParquetFile>,
}

impl Gen1 {
    pub fn new(file: Arc<ParquetFile>) -> Self {
        Self {
            id: GenerationId::new(),
            file,
        }
    }
}

#[async_trait]
impl Generation for Gen1 {
    fn id(&self) -> GenerationId {
        self.id
    }

    fn start_time_secs(&self) -> i64 {
        self.file.chunk_time / 1_000_000_000
    }

    fn gen_path(&self) -> String {
        format!(
            "{}/{}",
            self.level().0,
            gen_time_string(self.start_time_secs())
        )
    }

    fn level(&self) -> GenerationLevel {
        GenerationLevel::one()
    }

    async fn files(&self) -> Vec<Arc<ParquetFile>> {
        vec![Arc::clone(&self.file)]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionSequenceNumber(u64);

/// Wrapper for an old generation that includes the getter for the files.
struct OldGenWrapper {
    _object_store: Arc<dyn ObjectStore>,
    _index_file_path: IndexFilePath,
    old_gen: Arc<OldGeneration>,
}

#[async_trait]
impl Generation for OldGenWrapper {
    fn id(&self) -> GenerationId {
        self.old_gen.id
    }

    fn start_time_secs(&self) -> i64 {
        self.old_gen.start_time / 1_000_000_000
    }

    fn gen_path(&self) -> String {
        format!(
            "{}/{}",
            self.level().0,
            gen_time_string(self.start_time_secs())
        )
    }

    fn level(&self) -> GenerationLevel {
        self.old_gen.level
    }

    async fn files(&self) -> Vec<Arc<ParquetFile>> {
        todo!("Load files from index")
    }
}

/// The in-memory mapping of the compacted state of all databases and files. This will be
/// updated as compactions run.
#[derive(Debug)]
pub struct CompactedData {
    pub compactor_id: Arc<str>,
    pub host_id: Arc<str>,
    pub databases: HashMap<Arc<str>, CompactedDatabase>,
}

impl CompactedData {
    pub fn get_generations(
        &self,
        db_name: &str,
        table_name: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Vec<Arc<dyn Generation>>> {
        self.databases.get(db_name).and_then(|db| {
            db.tables.get(table_name).map(|table| {
                table.compacton_detail.generations(
                    self.compactor_id.as_ref(),
                    db_name,
                    table_name,
                    object_store,
                )
            })
        })
    }
}

#[derive(Debug)]
pub struct CompactedDatabase {
    tables: HashMap<Arc<str>, CompactedTable>,
}

#[derive(Debug)]
pub struct CompactedTable {
    compacton_detail: Arc<CompactionDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionDetailPath(ObjPath);

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
        compactor_id: &str,
        db_name: &str,
        table_name: &str,
        compaction_sequence_number: CompactionSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{compactor_id}/cd/{}/{}/{:020}.json",
            db_name,
            table_name,
            object_store_number_order(compaction_sequence_number.0),
        ));
        Self(path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexFilePath(ObjPath);

impl IndexFilePath {
    pub fn new(
        compactor_id: &str,
        db_name: &str,
        table_name: &str,
        generation_level: GenerationLevel,
        gen_time_str: &str,
        compaction_sequence_number: CompactionSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{compactor_id}/c/{}/{}/{}/{}/i/{}.json",
            db_name, table_name, generation_level.0, gen_time_str, compaction_sequence_number.0,
        ));
        Self(path)
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
        let config = CompactionConfig::new(&[3, 4, 6, 5], time::Duration::from_secs(1200));

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
}
