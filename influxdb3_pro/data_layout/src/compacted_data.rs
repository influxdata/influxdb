//! Module that keeps track of comapcted data and provides methods for querying and loading it.

use crate::persist::{
    get_compaction_detail, get_generation_detail, load_compaction_summary,
    CompactedDataPersistenceError,
};
use crate::{
    CompactionConfig, CompactionDetail, CompactionSequenceNumber, CompactionSummary, Gen1File,
    Generation, GenerationDetail, GenerationDetailPath, GenerationId, HostSnapshotMarker,
};
use datafusion::logical_expr::Expr;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId, NEXT_FILE_ID};
use influxdb3_pro_index::memory::FileIndex;
use influxdb3_write::ParquetFile;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::info;
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

#[derive(Debug, Eq, PartialEq)]
struct InnerCompactedData {
    databases: HashMap<DbId, CompactedDatabase>,
    last_compaction_summary: Option<CompactionSummary>,
    last_compaction_sequence_number: CompactionSequenceNumber,
}

impl CompactedData {
    pub fn new(
        compactor_id: Arc<str>,
        compaction_config: CompactionConfig,
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            compactor_id,
            catalog,
            object_store,
            compaction_config,
            data: RwLock::new(InnerCompactedData {
                databases: HashMap::new(),
                last_compaction_summary: None,
                last_compaction_sequence_number: CompactionSequenceNumber::new(0),
            }),
        }
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

        Ok(Arc::new(Self {
            compactor_id: Arc::from(compactor_id),
            object_store,
            compaction_config,
            data: RwLock::new(data),
            catalog,
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

    pub fn set_last_summary(&self, summary: CompactionSummary) {
        let mut data = self.data.write();
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
    ) -> (Vec<Arc<ParquetFile>>, Vec<HostSnapshotMarker>) {
        let data = self.data.read();
        data.databases
            .get(&db_id)
            .and_then(|db| db.tables.get(&table_id))
            .map(|t| t.parquet_files_and_host_markers(filters))
            .unwrap_or_default()
    }
}

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
                self.file_index.append(
                    &col,
                    &val,
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
    ) -> (Vec<Arc<ParquetFile>>, Vec<HostSnapshotMarker>) {
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
}
