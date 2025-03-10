//! Module has a view of all compacted data from a given compactor id.

use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_enterprise_data_layout::persist::{get_compaction_detail, get_generation_detail};
use influxdb3_enterprise_data_layout::{
    CompactedDataSystemTableQueryResult, CompactionDetail, CompactionDetailPath,
    CompactionDetailVersion, CompactionSequenceNumber, CompactionSummary, CompactionSummaryVersion,
    GenerationDetail, GenerationDetailPath, GenerationId, NodeSnapshotMarker, gen_time_string,
};
use influxdb3_enterprise_index::memory::{InMemoryFileIndex, ParquetFileMeta, ParquetMetaIndex};
use influxdb3_id::{DbId, TableId};
use influxdb3_write::{ChunkFilter, ParquetFile};
use object_store::ObjectStore;
use observability_deps::tracing::trace;
use parking_lot::RwLock;
use std::{fmt::Debug, time::Instant};
use std::{mem, sync::Arc};
use thiserror::Error;
use tokio::task::JoinSet;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error loading compaction detail: {0}")]
    CompactionDetailReadError(String),

    #[error("Error loading generation detail: {0}")]
    GenerationDetailReadError(String),

    #[error("Error while joining: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
}

/// This trait is for a view of `CompactedData` that can be queried as a system table
pub trait CompactedDataSystemTableView: Send + Sync + 'static + std::fmt::Debug {
    // TODO: rationalise Option<Vec<_>> vs Vec<_>
    fn query(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<Vec<CompactedDataSystemTableQueryResult>>;

    fn catalog(&self) -> Arc<Catalog>;
}

/// Result type for functions in this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct CompactedData {
    pub compactor_id: Arc<str>,
    pub catalog: Arc<Catalog>,
    inner_compacted_data: RwLock<InnerCompactedData>,
}

struct InnerCompactedData {
    compaction_summary: Arc<CompactionSummary>,
    databases: HashMap<DbId, CompactedDatabase>,
}

impl CompactedData {
    pub fn compaction_detail(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> Option<Arc<CompactionDetail>> {
        self.inner_compacted_data
            .read()
            .databases
            .get(&db_id)
            .and_then(|t| {
                t.tables
                    .get(&table_id)
                    .map(|t| Arc::clone(&t.compaction_detail))
            })
    }

    pub(crate) fn update_compaction_summary(&self, compaction_summary: CompactionSummaryVersion) {
        match compaction_summary {
            CompactionSummaryVersion::V1(cs) => {
                self.inner_compacted_data.write().compaction_summary = Arc::new(cs)
            }
        }
    }

    pub(crate) fn current_compaction_sequence_number(&self) -> CompactionSequenceNumber {
        self.inner_compacted_data
            .read()
            .compaction_summary
            .compaction_sequence_number
    }

    pub(crate) fn next_compaction_sequence_number(&self) -> CompactionSequenceNumber {
        self.inner_compacted_data
            .read()
            .compaction_summary
            .compaction_sequence_number
            .next()
    }

    pub(crate) fn db_ids(&self) -> Vec<DbId> {
        self.inner_compacted_data
            .read()
            .databases
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn table_ids(&self, db_id: DbId) -> Vec<TableId> {
        self.inner_compacted_data
            .read()
            .databases
            .get(&db_id)
            .map(|t| t.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn parquet_files(
        &self,
        db_id: DbId,
        table_id: TableId,
        generation_id: GenerationId,
    ) -> Vec<Arc<ParquetFile>> {
        self.inner_compacted_data
            .read()
            .databases
            .get(&db_id)
            .and_then(|t| {
                t.tables
                    .get(&table_id)
                    .and_then(|t| t.compacted_generations.get(&generation_id).cloned())
            })
            .unwrap_or_default()
    }

    pub fn compaction_summary(&self) -> Arc<CompactionSummary> {
        Arc::clone(&self.inner_compacted_data.read().compaction_summary)
    }

    /// Use the provided filters to get parquet files from the compactor as well as the writer
    /// snapshot markers that mark the most recent parquet file ID that has been compacted.
    ///
    /// It is important to use the database and table names in order to access the mapped entities
    /// in the compacted catalog, vs. passing in the IDs from the writer; which is why this method
    /// accepts the names and not IDs.
    pub fn get_parquet_files_and_writer_markers(
        &self,
        database_name: &str,
        table_name: &str,
        filter: &ChunkFilter<'_>,
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<NodeSnapshotMarker>>) {
        let Some(db) = self.catalog.db_schema(database_name) else {
            return Default::default();
        };

        let Some(table_id) = db.table_name_to_id(table_name) else {
            return Default::default();
        };

        let d = self.inner_compacted_data.read();
        let Some(table) = d
            .databases
            .get(&db.id)
            .and_then(|t| t.tables.get(&table_id))
        else {
            return Default::default();
        };

        table.get_parquet_files_and_host_markers(filter)
    }

    pub(crate) fn clone_in_memory_file_index(
        &self,
        db_id: &DbId,
        table_id: &TableId,
    ) -> Option<InMemoryFileIndex> {
        let in_mem_file_index = self
            .inner_compacted_data
            .read()
            .databases
            .get(db_id)?
            .tables
            .get(table_id)?
            .file_index
            .clone();
        Some(in_mem_file_index)
    }

    pub(crate) fn update_detail_with_generations(
        &self,
        compaction_detail: CompactionDetail,
        file_index_with_parquet_files_in_gen: (InMemoryFileIndex, ParquetFilesInGeneration),
    ) -> (ParquetFilesInGeneration, InMemoryFileIndex) {
        let mut d = self.inner_compacted_data.write();
        let db = d.databases.entry(compaction_detail.db_id).or_default();
        let compaction_detail = Arc::new(compaction_detail);
        let table = db
            .tables
            .entry(compaction_detail.table_id)
            .or_insert_with(|| CompactedTable {
                compaction_detail: Arc::clone(&compaction_detail),
                compacted_generations: HashMap::new(),
                file_index: InMemoryFileIndex::default(),
            });

        let (in_mem_file_index, parquet_files_in_gens) = file_index_with_parquet_files_in_gen;
        table.compaction_detail = compaction_detail;
        let file_index_timer = Instant::now();
        let old_values =
            table.update_compacted_generations(parquet_files_in_gens, in_mem_file_index);
        trace!(time_taken = ?file_index_timer.elapsed(), ">>> total time taken to remove compacted generations");
        // This function returns old values so that they can be deallocated at the call site, as
        // deallocation leads to >10ms times when holding this lock
        old_values
    }

    pub(crate) fn update_compaction_detail(&self, compaction_detail: CompactionDetailVersion) {
        let mut d = self.inner_compacted_data.write();
        let CompactionDetailVersion::V1(compaction_detail) = compaction_detail;
        let db = d.databases.entry(compaction_detail.db_id).or_default();
        let compaction_detail = Arc::new(compaction_detail);
        let table = db
            .tables
            .entry(compaction_detail.table_id)
            .or_insert_with(|| CompactedTable {
                compaction_detail: Arc::clone(&compaction_detail),
                compacted_generations: HashMap::new(),
                file_index: InMemoryFileIndex::default(),
            });
        table.compaction_detail = compaction_detail;
    }

    pub(crate) async fn load_compacted_data(
        compactor_id: Arc<str>,
        compaction_summary: CompactionSummary,
        catalog: Arc<Catalog>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let mut databases = HashMap::new();
        let mut join_set = JoinSet::new();

        // load all the compaction details
        for ((db_id, table_id), compaction_sequence_number) in
            compaction_summary.compaction_details.clone()
        {
            let compactor_id = Arc::clone(&compactor_id);
            let object_store = Arc::clone(&object_store);
            join_set.spawn(async move {
                let path = CompactionDetailPath::new(
                    Arc::clone(&compactor_id),
                    db_id,
                    table_id,
                    compaction_sequence_number,
                );
                let Some(CompactionDetailVersion::V1(compaction_detail)) =
                    get_compaction_detail(&path, Arc::clone(&object_store)).await
                else {
                    return Err(Error::CompactionDetailReadError(path.as_path().to_string()));
                };

                let compaction_detail_db_id = compaction_detail.db_id;

                // initialize the table and load all its generations and set up the file index
                let mut table = CompactedTable {
                    compaction_detail: Arc::new(compaction_detail),
                    compacted_generations: HashMap::new(),
                    file_index: InMemoryFileIndex::default(),
                };

                // load all the generation details
                let mut gen_details = JoinSet::new();
                for genr in &table.compaction_detail.compacted_generations {
                    let gen_path = GenerationDetailPath::new(Arc::clone(&compactor_id), genr.id);

                    let obj_store = Arc::clone(&object_store);
                    gen_details.spawn(async move {
                        match get_generation_detail(&gen_path, obj_store).await {
                            Some(genr) => Ok(genr),
                            None => Err(Error::GenerationDetailReadError(
                                gen_path.as_path().to_string(),
                            )),
                        }
                    });
                }

                while let Some(gen_detail) = gen_details.join_next().await {
                    table.add_generation_detail(gen_detail??);
                }

                Ok((compaction_detail_db_id, table))
            });
        }

        while let Some(data) = join_set.join_next().await {
            let (db_id, table) = data??;
            let db: &mut CompactedDatabase = databases.entry(db_id).or_default();
            db.tables.insert(table.compaction_detail.table_id, table);
        }

        Ok(Self {
            compactor_id,
            catalog,
            inner_compacted_data: RwLock::new(InnerCompactedData {
                databases,
                compaction_summary: Arc::new(compaction_summary),
            }),
        })
    }

    pub fn build_new_meta_index(
        &self,
        generation_details: &Vec<GenerationDetail>,
        removed_gen_details: &Vec<GenerationDetail>,
        meta_index: &mut ParquetMetaIndex,
    ) {
        for generation_detail in generation_details {
            let min_time = generation_detail.start_time_s * 1_000_000_000;
            let max_time = generation_detail.max_time_ns;
            for (col, valfiles) in &generation_detail.file_index.index {
                for (val, file_ids) in valfiles {
                    let parquet_file_metas = meta_index.entry((*col, *val)).or_default();
                    for id in file_ids {
                        parquet_file_metas.push(ParquetFileMeta::new(*id, min_time, max_time));
                    }
                    parquet_file_metas.sort();
                }
            }
        }

        for generation_detail in removed_gen_details {
            for (col, valfiles) in &generation_detail.file_index.index {
                for (val, file_ids) in valfiles {
                    let metas = meta_index
                        .entry((*col, *val))
                        .or_insert_with(Default::default);
                    metas.retain(move |meta| {
                        for parquet_file in file_ids {
                            // matches old file id, remove this
                            if meta.eq_id(*parquet_file) {
                                return false;
                            }
                        }
                        true
                    });
                }
            }
        }
    }

    pub fn build_parquet_file_index(
        &self,
        generation_details: Vec<GenerationDetail>,
        removed_gen_details: &Vec<GenerationDetail>,
        parquet_files_in_generations: &mut ParquetFilesInGeneration,
        parquet_file_index: &mut InMemoryFileIndex,
    ) {
        for gen_detail in generation_details {
            parquet_file_index.add_files(&gen_detail.files);
            parquet_files_in_generations.insert(gen_detail.id, gen_detail.files);
        }

        for gen_details in removed_gen_details {
            if let Some(files) = parquet_files_in_generations.remove(&gen_details.id) {
                for f in files {
                    parquet_file_index.remove_file(f.id);
                }
            }
        }
    }

    pub fn clone_parquet_file_generations(
        &self,
        db_id: &DbId,
        table_id: &TableId,
    ) -> Option<ParquetFilesInGeneration> {
        let gen_files = self
            .inner_compacted_data
            .read()
            .databases
            .get(db_id)?
            .tables
            .get(table_id)?
            .compacted_generations
            .clone();
        Some(gen_files)
    }

    pub fn build_new_parquet_files_in_gen_and_file_index(
        &self,
        db_id: &influxdb3_id::DbId,
        table_id: &influxdb3_id::TableId,
        generation_details: Vec<GenerationDetail>,
        removed_gen_details: &Vec<GenerationDetail>,
    ) -> (InMemoryFileIndex, ParquetFilesInGeneration) {
        // both the clones below are expensive, but we're trading
        // mem for cpu time to avoid taking the write lock for too
        // long
        let mut in_mem_file_index = self
            .clone_in_memory_file_index(db_id, table_id)
            .unwrap_or_default();

        let mut parquet_files_in_generations = self
            .clone_parquet_file_generations(db_id, table_id)
            .unwrap_or_default();

        self.build_new_meta_index(
            &generation_details,
            removed_gen_details,
            &mut in_mem_file_index.index,
        );
        self.build_parquet_file_index(
            generation_details,
            removed_gen_details,
            &mut parquet_files_in_generations,
            &mut in_mem_file_index,
        );

        (in_mem_file_index, parquet_files_in_generations)
    }
}

impl Debug for CompactedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let d = self.inner_compacted_data.read();

        f.debug_struct("CompactedData")
            .field("compactor_id", &self.compactor_id)
            .field("compaction_summary", &d.compaction_summary)
            .field("databases", &d.databases)
            .finish()
    }
}

impl CompactedDataSystemTableView for CompactedData {
    fn query(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<Vec<CompactedDataSystemTableQueryResult>> {
        let db = self.catalog.db_schema(db_name)?;
        let table_id = db.table_name_to_id(table_name)?;

        let inner_data = self.inner_compacted_data.read();
        let compacted_table = inner_data.databases.get(&db.id)?.tables.get(&table_id)?;
        let all_parquet_files = &compacted_table.compacted_generations;
        let results = compacted_table
            .compaction_detail
            .compacted_generations
            .iter()
            .map(|genr| {
                let parquet_files: Vec<Arc<ParquetFile>> = all_parquet_files
                    .get(&genr.id)
                    .expect("generation to have parquet files")
                    // is this clone cheap? does it matter?
                    .clone();
                CompactedDataSystemTableQueryResult {
                    generation_id: genr.id.as_u64(),
                    generation_level: genr.level.as_u8(),
                    generation_time: gen_time_string(genr.start_time_secs),
                    parquet_files,
                }
            })
            .collect();
        Some(results)
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct CompactedDatabase {
    tables: HashMap<TableId, CompactedTable>,
}

type ParquetFilesInGeneration = HashMap<GenerationId, Vec<Arc<ParquetFile>>>;

#[derive(Debug, Eq, PartialEq)]
struct CompactedTable {
    compaction_detail: Arc<CompactionDetail>,
    compacted_generations: ParquetFilesInGeneration,
    file_index: InMemoryFileIndex,
}

impl CompactedTable {
    fn add_generation_detail(&mut self, generation_detail: GenerationDetail) {
        let add_file_index_timer = Instant::now();
        self.file_index.add_files(&generation_detail.files);
        self.compacted_generations
            .insert(generation_detail.id, generation_detail.files);
        trace!(time_taken = ?add_file_index_timer.elapsed(), ">>> time taken for adding file index");
    }

    #[inline]
    fn update_compacted_generations(
        &mut self,
        parquet_files_in_gens: ParquetFilesInGeneration,
        new_file_index: InMemoryFileIndex,
    ) -> (ParquetFilesInGeneration, InMemoryFileIndex) {
        let start = Instant::now();
        // we do the switcheroo but not drop the old values here
        let old_files_in_gen = mem::replace(&mut self.compacted_generations, parquet_files_in_gens);
        let old_file_index = mem::replace(&mut self.file_index, new_file_index);
        trace!(time_taken = ?start.elapsed(), ">>> time taken to update compacted generations");
        (old_files_in_gen, old_file_index)
    }

    fn get_parquet_files_and_host_markers(
        &self,
        filter: &ChunkFilter<'_>,
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<NodeSnapshotMarker>>) {
        let mut parquet_files = self.file_index.parquet_files_for_filter(filter);

        // add the gen1 files, filtered based on their timestamps
        for f in self
            .compaction_detail
            .leftover_gen1_files
            .iter()
            .filter(|gen1| filter.test_time_stamp_min_max(gen1.file.min_time, gen1.file.max_time))
        {
            parquet_files.push(Arc::clone(&f.file));
        }

        (
            parquet_files,
            self.compaction_detail.snapshot_markers.clone(),
        )
    }
}
