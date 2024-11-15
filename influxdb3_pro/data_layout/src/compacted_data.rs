//! Module that keeps track of comapcted data and provides methods for querying and loading it.

use crate::{
    gen_time_string,
    persist::{
        get_compaction_detail, get_generation_detail, load_compaction_summary,
        load_compaction_summary_for_sequence, CompactedDataPersistenceError,
    },
    CompactedDataSystemTableQueryResult, CompactedDataSystemTableView,
};
use crate::{
    CompactionConfig, CompactionDetail, CompactionSequenceNumber, CompactionSummary, Gen1File,
    Generation, GenerationDetail, GenerationDetailPath, GenerationId, GenerationLevel,
    HostSnapshotMarker,
};
use datafusion::logical_expr::Expr;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId, NEXT_FILE_ID};
use influxdb3_pro_index::memory::FileIndex;
use influxdb3_write::{ParquetFile, PersistedSnapshot};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("couldn't find compaction detail {0} from object store")]
    CompactionDetailReadError(String),

    #[error("couldn't find generation detail {0} from object store")]
    GenerationDetailReadError(String),

    #[error("from compacted data persistence: {0}")]
    CompactedDataPersistenceError(#[from] CompactedDataPersistenceError),
}

/// Result type for functions in this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct CompactedData {
    pub compactor_id: Arc<str>,
    pub catalog: Arc<Catalog>,
    pub object_store: Arc<dyn ObjectStore>,
    pub compaction_config: CompactionConfig,
    data: RwLock<InnerCompactedData>,
    snapshot_added_tx: tokio::sync::broadcast::Sender<()>,
}

impl Debug for CompactedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.read();

        f.debug_struct("CompactedData")
            .field("compactor_id", &self.compactor_id)
            .field("data", &data)
            .finish()
    }
}

impl PartialEq for CompactedData {
    fn eq(&self, other: &Self) -> bool {
        self.compactor_id == other.compactor_id
            && self.compaction_config.eq(&other.compaction_config)
            && self.data.read().eq(&other.data.read())
    }
}

impl Eq for CompactedData {}

impl CompactedData {
    pub fn new(
        compactor_id: Arc<str>,
        compaction_config: CompactionConfig,
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
    ) -> Self {
        let (snapshot_added_tx, _) = tokio::sync::broadcast::channel(1);
        Self {
            compactor_id,
            catalog,
            object_store,
            compaction_config,
            snapshot_added_tx,
            data: RwLock::new(InnerCompactedData {
                snapshots: Vec::new(),
                databases: HashMap::new(),
                last_compaction_summary: None,
                last_compaction_sequence_number: CompactionSequenceNumber::new(0),
            }),
        }
    }

    pub fn add_snapshot(&self, snapshot: PersistedSnapshot) {
        let mut data = self.data.write();

        // save the basic snapshot info for the host
        let marker = Arc::new(HostSnapshotMarker {
            host_id: snapshot.host_id.clone(),
            next_file_id: snapshot.next_file_id,
            snapshot_sequence_number: snapshot.snapshot_sequence_number,
        });
        data.snapshots.push(marker);

        // add the parquet files to the compacted table as gen1 files that are in "compacting" state
        for (db_name, dbtables) in snapshot.databases {
            let db = data.databases.entry(db_name).or_default();
            for (table_name, parquet_files) in dbtables.tables {
                let table = db.tables.entry(table_name).or_default();
                table.add_compacting_gen1_files(parquet_files);
            }
        }

        // notify any watchers if there are any (mostly the compactor loop)
        if let Err(e) = self.snapshot_added_tx.send(()) {
            error!(error = %e, "error sending snapshot added notification");
        }
    }

    pub fn last_snapshot_host(&self) -> Option<String> {
        self.data.read().snapshots.last().map(|s| s.host_id.clone())
    }

    pub fn snapshots_awaiting_compaction(&self) -> Vec<Arc<HostSnapshotMarker>> {
        self.data.read().snapshots.clone()
    }

    pub fn should_compact_and_advance_snapshots(&self) -> bool {
        let snapshots = self.data.read().snapshots.clone();

        // if any host has 3 or more snapshots, we must compact. If all hosts have at least 2 snapshots
        // we will compact. Otherwise, we'll only compact if there is a NextCompactionPlan (i.e. not all just leftover plans)
        let mut per_host_count = HashMap::new();
        for s in &snapshots {
            let count = per_host_count.entry(&s.host_id).or_insert(0);
            *count += 1;
        }

        for count in per_host_count.values() {
            if *count >= 3 {
                return true;
            } else if *count < 2 {
                return false;
            }
        }

        // all hosts have at least 2 snapshots waiting to be processed
        true
    }

    pub fn snapshot_notification_receiver(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.snapshot_added_tx.subscribe()
    }

    pub async fn load_compacted_data(
        compactor_id: &str,
        compaction_config: CompactionConfig,
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
    ) -> Result<Arc<Self>> {
        let compaction_summary =
            load_compaction_summary(compactor_id, Arc::clone(&object_store)).await?;

        let Some(compaction_summary) = compaction_summary else {
            return Ok(Arc::new(Self::new(
                Arc::from(compactor_id),
                compaction_config,
                object_store,
                catalog,
            )));
        };

        // set parquet file id and generation id so that they'll start off from where they left off
        NEXT_FILE_ID.store(
            compaction_summary.last_file_id.as_u64() + 1,
            std::sync::atomic::Ordering::SeqCst,
        );
        GenerationId::initialize(compaction_summary.last_generation_id);

        let mut data = InnerCompactedData {
            snapshots: Vec::new(),
            databases: HashMap::new(),
            last_compaction_summary: Some(compaction_summary.clone()),
            last_compaction_sequence_number: compaction_summary.compaction_sequence_number,
        };

        info!(
            "loaded compaction_sequence_number from summary: {:?}",
            compaction_summary.compaction_sequence_number
        );

        // load all the compaction details
        for path in &compaction_summary.compaction_details {
            let Some(compaction_detail) =
                get_compaction_detail(path, Arc::clone(&object_store)).await
            else {
                return Err(Error::CompactionDetailReadError(path.0.to_string()));
            };

            let db = data.databases.entry(compaction_detail.db_id).or_default();
            let table = db.tables.entry(compaction_detail.table_id).or_default();

            // load all the generation details
            for gen in &compaction_detail.compacted_generations {
                let gen_path = GenerationDetailPath::new(compactor_id, gen.id);
                let Some(generation_detail) =
                    get_generation_detail(&gen_path, Arc::clone(&object_store)).await
                else {
                    return Err(Error::GenerationDetailReadError(gen_path.0.to_string()));
                };

                table.add_generation_detail(generation_detail);
            }

            // and now add the compaction detail to the table
            table.compaction_detail = Some(Arc::new(compaction_detail));
        }

        let (snapshot_added_tx, _) = tokio::sync::broadcast::channel(1);
        Ok(Arc::new(Self {
            compactor_id: Arc::from(compactor_id),
            object_store,
            compaction_config,
            data: RwLock::new(data),
            catalog,
            snapshot_added_tx,
        }))
    }

    pub fn databases(&self) -> Vec<DbId> {
        self.data.read().databases.keys().copied().collect()
    }

    pub fn tables(&self, db_id: DbId) -> Vec<TableId> {
        self.data
            .read()
            .databases
            .get(&db_id)
            .map_or_else(Vec::new, |db| db.tables.keys().copied().collect())
    }

    pub fn get_generations(&self, db_id: DbId, table_id: TableId) -> Vec<Generation> {
        if let Some(detail) = self.get_last_compaction_detail(db_id, table_id) {
            let mut gens: Vec<_> = detail.compacted_generations.clone();
            gens.extend(detail.leftover_gen1_files.iter().map(|f| f.generation()));
            gens.sort();
            gens
        } else {
            Vec::new()
        }
    }

    pub fn tables_with_gen1_awaiting_compaction_and_markers(
        &self,
    ) -> (DatabaseTableGenerations, Vec<Arc<HostSnapshotMarker>>) {
        let data = self.data.read();
        let mut db_tables: HashMap<DbId, HashMap<TableId, Vec<Generation>>> = HashMap::new();

        for (db_id, compacted_db) in data.databases.iter() {
            for (table_id, compacted_table) in compacted_db.tables.iter() {
                if let Some(gens) = compacted_table.compacting_gen1s_and_generations() {
                    db_tables.entry(*db_id).or_default().insert(*table_id, gens);
                }
            }
        }

        // get the last host snapshot marker from each host
        let mut markers = HashMap::new();
        for s in &data.snapshots {
            let e = markers.entry(&s.host_id).or_insert_with(|| s);
            if s.snapshot_sequence_number > e.snapshot_sequence_number {
                *e = s;
            }
        }
        let markers: Vec<_> = markers.into_values().cloned().collect();

        (db_tables, markers)
    }

    pub fn last_snapshot_marker_per_host(&self) -> Vec<Arc<HostSnapshotMarker>> {
        let data = self.data.read();
        let mut markers = HashMap::new();
        for s in &data.snapshots {
            let e = markers.entry(&s.host_id).or_insert_with(|| s);
            if s.snapshot_sequence_number > e.snapshot_sequence_number {
                *e = s;
            }
        }
        markers.into_values().cloned().collect()
    }

    pub fn add_compacting_gen1_files(
        &self,
        db_id: DbId,
        table_id: TableId,
        gen1_files: Vec<ParquetFile>,
    ) -> Vec<Generation> {
        let mut data = self.data.write();

        let db = data.databases.entry(db_id).or_default();
        let table = db.tables.entry(table_id).or_default();

        table.add_compacting_gen1_files(gen1_files)
    }

    pub fn remove_compacting_gen1_files(
        &self,
        db_id: DbId,
        table_id: TableId,
        gen_ids: &[GenerationId],
    ) -> Vec<Gen1File> {
        let mut data = self.data.write();

        let db = data.databases.entry(db_id).or_default();
        let table = db.tables.entry(table_id).or_default();
        table.remove_compacting_gen1_files(gen_ids)
    }

    pub fn update_compaction_detail_with_generation(
        &self,
        compacted_ids: &[GenerationId],
        compaction_detail: CompactionDetail,
        generation_detail: GenerationDetail,
    ) {
        let mut data = self.data.write();

        let db = data.databases.entry(compaction_detail.db_id).or_default();
        let compacted_table = db.tables.entry(compaction_detail.table_id).or_default();

        compacted_table.update_detail_with_generation(
            compacted_ids,
            compaction_detail,
            generation_detail,
        );
    }

    // used when the new compaction detail only has new leftover gen1 files
    pub fn update_compaction_detail_without_generation(&self, compaction_detail: CompactionDetail) {
        let mut data = self.data.write();

        let db = data.databases.entry(compaction_detail.db_id).or_default();

        let table_id = compaction_detail.table_id;
        db.tables.entry(table_id).or_default().compaction_detail =
            Some(Arc::new(compaction_detail));
    }

    pub fn next_compaction_sequence_number(&self) -> CompactionSequenceNumber {
        let mut data = self.data.write();
        let next = data.last_compaction_sequence_number.next();
        data.last_compaction_sequence_number = next;
        next
    }

    pub fn get_last_compaction_detail(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> Option<Arc<CompactionDetail>> {
        self.data.read().databases.get(&db_id).and_then(|db| {
            db.tables
                .get(&table_id)
                .and_then(|t| t.compaction_detail.clone())
        })
    }

    pub fn get_last_summary(&self) -> Option<CompactionSummary> {
        self.data.read().last_compaction_summary.clone()
    }

    /// Sets the last compaction summary and removes all the matching markers from the compacted data.
    pub fn set_last_summary_and_remove_markers(&self, summary: CompactionSummary) {
        let mut data = self.data.write();
        data.snapshots.retain(|s| {
            summary
                .snapshot_markers
                .iter()
                .find(|m| m.host_id == s.host_id)
                .map(|m| s.snapshot_sequence_number > m.snapshot_sequence_number)
                .unwrap_or(true)
        });
        data.last_compaction_summary = Some(summary);
    }

    pub fn paths_for_files_in_generations(
        &self,
        db_id: DbId,
        table_id: TableId,
        generation_ids: &[GenerationId],
    ) -> Vec<ObjPath> {
        let data = self.data.read();
        data.databases
            .get(&db_id)
            .and_then(|db| {
                db.tables.get(&table_id).map(|compacted_table| {
                    compacted_table.paths_for_files_in_generations(generation_ids)
                })
            })
            .unwrap_or_default()
    }

    /// Looks up the compaction detail and returns all the parquet files and host markers for the given table.
    pub fn get_parquet_files_and_host_markers(
        &self,
        db_id: DbId,
        table_id: TableId,
        filters: &[Expr],
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<HostSnapshotMarker>>) {
        let data = self.data.read();
        data.databases
            .get(&db_id)
            .and_then(|db| db.tables.get(&table_id))
            .map(|t| t.parquet_files_and_host_markers(filters))
            .unwrap_or_default()
    }

    /// Continuously polls object storage for the next compaction summary written by the compactor.
    pub async fn poll_for_updates(&self) {
        loop {
            // sleep for 10 seconds before looking for a new snapshot
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;

            if let Some(compaction_summary) = self.update_from_next_summary().await {
                info!(compaction_summary = ?compaction_summary, "updated from compaction summary");
            }
        }
    }

    /// Looks for the next compaction summary based on the next sequence number. If present,
    /// compaction details are loaded along with generation details. The `CompactedData` structure
    /// is updated and the new summary is returned. Or None if no new summary is found.
    async fn update_from_next_summary(&self) -> Option<CompactionSummary> {
        let (last_summary, next_compaction_sequence_number) = {
            let data = self.data.read();
            (
                data.last_compaction_summary.clone(),
                data.last_compaction_sequence_number.next(),
            )
        };

        // get next summary or ignore error and return none
        let next_summary = match load_compaction_summary_for_sequence(
            &self.compactor_id,
            next_compaction_sequence_number,
            Arc::clone(&self.object_store),
        )
        .await
        {
            Ok(Some(summary)) => summary,
            Ok(None) => return None,
            Err(e) => {
                warn!(error = ?e, "error loading compaction summary");
                return None;
            }
        };

        // load each new compaction detail, updating based on every generation and detail within
        for compaction_detail_path in &next_summary.compaction_details {
            // only update if this is a new detail from the last summary
            if last_summary.as_ref().map_or(true, |last| {
                !last.compaction_details.contains(compaction_detail_path)
            }) {
                let detail = match get_compaction_detail(
                    compaction_detail_path,
                    Arc::clone(&self.object_store),
                )
                .await
                {
                    Some(detail) => detail,
                    None => {
                        error!(compaction_detail = ?compaction_detail_path, "compaction detail not found");
                        continue;
                    }
                };

                // figure out which generation details we have to load
                let new_gens = self.generations_to_load(
                    detail.db_id,
                    detail.table_id,
                    &detail.compacted_generations,
                );

                // load those generations and inject them into the compacted table, removing all others
                let mut gen_details = Vec::with_capacity(new_gens.len());
                for genid in new_gens {
                    let gen_path = GenerationDetailPath::new(&self.compactor_id, genid);
                    let detail = match get_generation_detail(
                        &gen_path,
                        Arc::clone(&self.object_store),
                    )
                    .await
                    {
                        Some(detail) => detail,
                        None => {
                            error!(generation_detail = ?gen_path, "generation detail not found");
                            continue;
                        }
                    };

                    gen_details.push(detail);
                }

                self.update_compaction_detail_and_add_generations(detail, gen_details);
            }
        }

        // now update the summary
        let mut data = self.data.write();
        data.last_compaction_summary = Some(next_summary.clone());
        data.last_compaction_sequence_number = next_summary.compaction_sequence_number;

        Some(next_summary)
    }

    /// Returns a vec of ids for generations that don't exist in the table
    fn generations_to_load(
        &self,
        db_id: DbId,
        table_id: TableId,
        generations: &[Generation],
    ) -> Vec<GenerationId> {
        let data = self.data.read();
        let table = data
            .databases
            .get(&db_id)
            .and_then(|db| db.tables.get(&table_id));

        match table {
            None => generations.iter().map(|g| g.id).collect::<Vec<_>>(),
            Some(t) => generations
                .iter()
                .filter(|g| !t.compacted_generations.contains_key(&g.id))
                .map(|g| g.id)
                .collect::<Vec<_>>(),
        }
    }

    /// Updates the compaction detail, adds the new generations and removes all generations from
    /// the map that aren't in the compaction detail.
    fn update_compaction_detail_and_add_generations(
        &self,
        compaction_detail: CompactionDetail,
        generations: Vec<GenerationDetail>,
    ) {
        let mut data = self.data.write();

        let db = data.databases.entry(compaction_detail.db_id).or_default();
        let table = db.tables.entry(compaction_detail.table_id).or_default();

        // add the new generations
        for gen in generations {
            table.add_generation_detail(gen);
        }

        // get the list of generations that are no longer in the new detail
        let mut generatons_to_keep =
            HashMap::with_capacity(compaction_detail.compacted_generations.len());
        for gen in &compaction_detail.compacted_generations {
            if let Some(compacted_files) = table.compacted_generations.remove(&gen.id) {
                generatons_to_keep.insert(gen.id, compacted_files);
            }
        }

        table.compacted_generations = generatons_to_keep;

        table.compaction_detail = Some(Arc::new(compaction_detail));
    }
}

impl CompactedDataSystemTableView for CompactedData {
    fn query(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> Option<Vec<CompactedDataSystemTableQueryResult>> {
        let inner_data = self.data.read();
        let compacted_table = inner_data
            .get_databases()
            .get(&db_id)?
            .tables
            .get(&table_id)?;
        let compaction_detail = &compacted_table.compaction_detail.clone()?;
        let all_parquet_files = &compacted_table.compacted_generations;
        let results = compaction_detail
            .compacted_generations
            .iter()
            .map(|gen| {
                let parquet_files: Vec<Arc<ParquetFile>> = all_parquet_files
                    .get(&gen.id)
                    .expect("generation to have parquet files")
                    // is this clone cheap? does it matter?
                    .clone();
                CompactedDataSystemTableQueryResult {
                    generation_id: gen.id.as_u64(),
                    generation_level: gen.level.as_u8(),
                    generation_time: gen_time_string(gen.start_time_secs),
                    parquet_files,
                }
            })
            .collect();
        Some(results)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct InnerCompactedData {
    // snapshots from different hosts that have yet to be compacted
    snapshots: Vec<Arc<HostSnapshotMarker>>,
    databases: HashMap<DbId, CompactedDatabase>,
    last_compaction_summary: Option<CompactionSummary>,
    last_compaction_sequence_number: CompactionSequenceNumber,
}

impl InnerCompactedData {
    fn get_databases(&self) -> &HashMap<DbId, CompactedDatabase> {
        &self.databases
    }
}

pub type DatabaseTableGenerations = HashMap<DbId, HashMap<TableId, Vec<Generation>>>;

#[derive(Debug, Default, Eq, PartialEq)]
struct CompactedDatabase {
    tables: HashMap<TableId, CompactedTable>,
}

#[derive(Debug, Default, Eq, PartialEq)]
struct CompactedTable {
    compaction_detail: Option<Arc<CompactionDetail>>,
    compacted_generations: HashMap<GenerationId, Vec<Arc<ParquetFile>>>,
    // gen1 files that are either part of a compaction that is running or will be leftover
    // after a compaction is done. This is a temporary holder for the compaction process
    compacting_gen1_files: HashMap<GenerationId, Arc<ParquetFile>>,
    file_index: FileIndex,
}

impl CompactedTable {
    fn paths_for_files_in_generations(&self, generation_ids: &[GenerationId]) -> Vec<ObjPath> {
        let mut paths = Vec::new();
        for id in generation_ids {
            if let Some(files) = self.compacted_generations.get(id) {
                for file in files {
                    paths.push(ObjPath::from(file.path.as_ref()));
                }
            } else if let Some(file) = self.compacting_gen1_files.get(id) {
                paths.push(ObjPath::from(file.path.as_ref()));
            } else if let Some(detail) = &self.compaction_detail {
                for gen1_file in &detail.leftover_gen1_files {
                    if gen1_file.id == *id {
                        paths.push(ObjPath::from(gen1_file.file.path.as_ref()));
                    }
                }
            }
        }

        paths
    }

    fn add_generation_detail(&mut self, generation_detail: GenerationDetail) {
        self.file_index.add_files(&generation_detail.files);

        for (col, valfiles) in generation_detail.file_index.index {
            for (val, file_ids) in valfiles {
                self.file_index.append_with_hashed_values(
                    col,
                    val,
                    generation_detail.start_time_s * 1_000_000_000,
                    generation_detail.max_time_ns,
                    &file_ids,
                );
            }
        }

        self.compacted_generations
            .insert(generation_detail.id, generation_detail.files);
    }

    fn update_detail_with_generation(
        &mut self,
        compacted_ids: &[GenerationId],
        compaction_detail: CompactionDetail,
        generation_detail: GenerationDetail,
    ) {
        self.compaction_detail = Some(Arc::new(compaction_detail));

        for id in compacted_ids {
            if let Some(gen) = self.compacted_generations.remove(id) {
                self.file_index.remove_files(&gen);
            }
        }

        self.add_generation_detail(generation_detail);
    }

    fn parquet_files_and_host_markers(
        &self,
        filters: &[Expr],
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<HostSnapshotMarker>>) {
        let mut files = self.file_index.parquet_files_for_filter(filters);
        let mut markers = None;

        if let Some(detail) = &self.compaction_detail {
            for gen1_file in &detail.leftover_gen1_files {
                files.push(Arc::clone(&gen1_file.file));
            }

            markers = Some(detail.snapshot_markers.clone());
        }

        (files, markers.unwrap_or_default())
    }

    fn add_compacting_gen1_files(&mut self, gen1_files: Vec<ParquetFile>) -> Vec<Generation> {
        let mut gens = Vec::new();
        for f in gen1_files {
            let g = Gen1File::new(Arc::new(f));
            gens.push(g.generation());
            self.compacting_gen1_files.insert(g.id, g.file);
        }
        gens
    }

    fn remove_compacting_gen1_files(&mut self, gen_ids: &[GenerationId]) -> Vec<Gen1File> {
        let mut removed = Vec::new();
        for id in gen_ids {
            if let Some(file) = self.compacting_gen1_files.remove(id) {
                removed.push(Gen1File { id: *id, file });
            }
        }
        removed
    }

    /// If there are any gen1 files awaiting compaction, this returns a vec of all those generations
    /// along with the generations that have been compacted and the leftover gen1 files. This can be
    /// used by the planner to create a plan.
    fn compacting_gen1s_and_generations(&self) -> Option<Vec<Generation>> {
        if self.compacting_gen1_files.is_empty() {
            None
        } else {
            let mut gens: Vec<_> = self
                .compacting_gen1_files
                .iter()
                .map(|(gid, f)| Generation {
                    id: *gid,
                    level: GenerationLevel::one(),
                    start_time_secs: f.chunk_time / 1_000_000_000,
                    max_time: f.max_time,
                })
                .collect();

            if let Some(detail) = &self.compaction_detail {
                for g in &detail.compacted_generations {
                    gens.push(*g);
                }

                for g in detail.leftover_gen1_files.iter().map(|f| f.generation()) {
                    gens.push(g);
                }
            }

            gens.sort();

            Some(gens)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::{
        persist_compaction_detail, persist_compaction_summary, persist_generation_detail,
    };
    use crate::CompactionDetailPath;
    use influxdb3_id::ParquetFileId;
    use influxdb3_wal::SnapshotSequenceNumber;

    #[tokio::test]
    async fn updates_next_summary() {
        let catalog = Arc::new(Catalog::new("test-host".into(), "test".into()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let compactor_id = "com";
        let compacted_data = CompactedData::load_compacted_data(
            compactor_id,
            CompactionConfig::default(),
            Arc::clone(&object_store),
            catalog,
        )
        .await
        .unwrap();

        // we should get back none when we try to get the next compaction summary
        assert!(compacted_data.update_from_next_summary().await.is_none());

        // write a generation and compaction detail for a db and table
        let db_name: Arc<str> = "testdb".into();
        let db_id = DbId::new();
        let table_name: Arc<str> = "table1".into();
        let table_id = TableId::new();
        let sequence_number = CompactionSequenceNumber::new(1);
        let gen2 = Generation {
            id: GenerationId::new(),
            level: GenerationLevel::two(),
            start_time_secs: 0,
            max_time: 10000,
        };
        let gen2_file_id = ParquetFileId::new();
        let gen2_detail = GenerationDetail {
            id: gen2.id,
            level: gen2.level,
            start_time_s: gen2.start_time_secs,
            max_time_ns: gen2.max_time,
            files: vec![Arc::from(ParquetFile {
                id: gen2_file_id,
                path: "2".to_string(),
                size_bytes: 0,
                row_count: 0,
                chunk_time: 0,
                min_time: 0,
                max_time: 0,
            })],
            file_index: Default::default(),
        };
        persist_generation_detail(
            compactor_id,
            gen2.id,
            &gen2_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let gen3 = Generation {
            id: GenerationId::new(),
            level: GenerationLevel::new(3),
            start_time_secs: 0,
            max_time: 0,
        };
        let gen3_file_id = ParquetFileId::new();
        let gen3_detail = GenerationDetail {
            id: gen3.id,
            level: gen3.level,
            start_time_s: gen3.start_time_secs,
            max_time_ns: gen3.max_time,
            files: vec![Arc::from(ParquetFile {
                id: gen3_file_id,
                path: "3".to_string(),
                size_bytes: 0,
                row_count: 0,
                chunk_time: 0,
                min_time: 0,
                max_time: 0,
            })],
            file_index: Default::default(),
        };
        persist_generation_detail(
            compactor_id,
            gen3.id,
            &gen3_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let snapshot_markers = vec![Arc::new(HostSnapshotMarker {
            host_id: "A".into(),
            next_file_id: ParquetFileId::from(25),
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        })];

        let compaction_detail_path =
            CompactionDetailPath::new("com", "testdb", db_id, "table1", table_id, sequence_number);
        let compaction_detail = CompactionDetail {
            db_name: Arc::clone(&db_name),
            db_id,
            table_name: Arc::clone(&table_name),
            table_id,
            sequence_number,
            snapshot_markers: snapshot_markers.clone(),
            compacted_generations: vec![gen2, gen3],
            leftover_gen1_files: vec![],
        };
        persist_compaction_detail(
            compactor_id,
            Arc::clone(&db_name),
            db_id,
            Arc::clone(&table_name),
            table_id,
            &compaction_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        // now write a summary
        let summary = CompactionSummary {
            compaction_sequence_number: CompactionSequenceNumber::new(1),
            last_file_id: ParquetFileId::from(25),
            last_generation_id: GenerationId::from(3),
            compaction_details: vec![compaction_detail_path],
            snapshot_markers,
        };
        persist_compaction_summary(compactor_id, &summary, Arc::clone(&object_store))
            .await
            .unwrap();

        let next_summary = compacted_data.update_from_next_summary().await.unwrap();
        assert_eq!(next_summary, summary);
        // ensure we have the new generations in the compacted data
        let detail = compacted_data
            .get_last_compaction_detail(db_id, table_id)
            .unwrap();
        assert_eq!(detail.as_ref(), &compaction_detail);

        let paths =
            compacted_data.paths_for_files_in_generations(db_id, table_id, &[gen2.id, gen3.id]);
        let expected_paths = vec![ObjPath::from("2"), ObjPath::from("3")];
        assert_eq!(paths, expected_paths);

        // now ensure that the next update is none
        assert!(compacted_data.update_from_next_summary().await.is_none());

        // now write a new summary with a new compaction detail and a new generation that replaces one of the older ones
        let next_sequence_number = sequence_number.next();
        let gen4 = Generation {
            id: GenerationId::new(),
            level: GenerationLevel::new(4),
            start_time_secs: 0,
            max_time: 0,
        };
        let gen4_file_id = ParquetFileId::new();
        let gen4_detail = GenerationDetail {
            id: gen4.id,
            level: gen4.level,
            start_time_s: gen4.start_time_secs,
            max_time_ns: gen4.max_time,
            files: vec![Arc::from(ParquetFile {
                id: gen4_file_id,
                path: "4".to_string(),
                size_bytes: 0,
                row_count: 0,
                chunk_time: 0,
                min_time: 0,
                max_time: 0,
            })],
            file_index: Default::default(),
        };
        persist_generation_detail(
            compactor_id,
            gen4.id,
            &gen4_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let next_snapshot_markers = vec![Arc::new(HostSnapshotMarker {
            host_id: "A".into(),
            next_file_id: ParquetFileId::from(30),
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
        })];
        let new_detail = CompactionDetail {
            db_name: Arc::clone(&db_name),
            db_id,
            table_name: Arc::clone(&table_name),
            table_id,
            sequence_number: next_sequence_number,
            snapshot_markers: next_snapshot_markers.clone(),
            compacted_generations: vec![gen3, gen4],
            leftover_gen1_files: vec![],
        };
        let new_detail_path = CompactionDetailPath::new(
            "com",
            "testdb",
            db_id,
            "table1",
            table_id,
            next_sequence_number,
        );
        persist_compaction_detail(
            compactor_id,
            Arc::clone(&db_name),
            db_id,
            Arc::clone(&table_name),
            table_id,
            &new_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let next_summary = CompactionSummary {
            compaction_sequence_number: next_sequence_number,
            last_file_id: ParquetFileId::from(30),
            last_generation_id: GenerationId::from(4),
            compaction_details: vec![new_detail_path],
            snapshot_markers: next_snapshot_markers,
        };
        persist_compaction_summary(compactor_id, &next_summary, Arc::clone(&object_store))
            .await
            .unwrap();

        let next_summary = compacted_data.update_from_next_summary().await.unwrap();
        assert_eq!(next_summary, next_summary);
        // ensure we have the new generations in the compacted data
        let detail = compacted_data
            .get_last_compaction_detail(db_id, table_id)
            .unwrap();
        assert_eq!(detail.as_ref(), &new_detail);

        let paths =
            compacted_data.paths_for_files_in_generations(db_id, table_id, &[gen3.id, gen4.id]);
        let expected_paths = vec![ObjPath::from("3"), ObjPath::from("4")];
        assert_eq!(paths, expected_paths);

        // make sure gen2 is purged
        let paths = compacted_data.paths_for_files_in_generations(db_id, table_id, &[gen2.id]);
        assert!(paths.is_empty());

        // now ensure that the next update is none
        assert!(compacted_data.update_from_next_summary().await.is_none());
    }
}
