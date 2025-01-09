//! Module has a view of all compacted data from a given compactor id.

use crate::catalog::CompactedCatalog;
use datafusion::prelude::Expr;
use hashbrown::HashMap;
use influxdb3_enterprise_data_layout::persist::{get_compaction_detail, get_generation_detail};
use influxdb3_enterprise_data_layout::{
    gen_time_string, CompactedDataSystemTableQueryResult, CompactionDetail, CompactionDetailPath,
    CompactionSequenceNumber, CompactionSummary, Generation, GenerationDetail,
    GenerationDetailPath, GenerationId, HostSnapshotMarker,
};
use influxdb3_enterprise_index::memory::FileIndex;
use influxdb3_id::{DbId, TableId};
use influxdb3_write::ParquetFile;
use object_store::ObjectStore;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::Arc;
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

    fn catalog(&self) -> &CompactedCatalog;
}

/// Result type for functions in this module.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct CompactedData {
    pub compactor_id: Arc<str>,
    pub compacted_catalog: CompactedCatalog,
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

    pub(crate) fn update_compaction_summary(&self, compaction_summary: CompactionSummary) {
        self.inner_compacted_data.write().compaction_summary = Arc::new(compaction_summary);
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

    pub fn get_parquet_files_and_host_markers(
        &self,
        db_name: &str,
        table_name: &str,
        filters: &[Expr],
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<HostSnapshotMarker>>) {
        let Some(db) = self.compacted_catalog.db_schema(db_name) else {
            return (vec![], vec![]);
        };
        let Some(table_id) = db.table_name_to_id(table_name) else {
            return (vec![], vec![]);
        };

        let d = self.inner_compacted_data.read();
        let Some(table) = d
            .databases
            .get(&db.id)
            .and_then(|t| t.tables.get(&table_id))
        else {
            return (vec![], vec![]);
        };

        table.get_parquet_files_and_host_markers(filters)
    }

    pub(crate) fn update_detail_with_generations(
        &self,
        compaction_detail: CompactionDetail,
        generation_details: Vec<GenerationDetail>,
        removed_generations: Vec<Generation>,
    ) {
        let mut d = self.inner_compacted_data.write();
        let db = d.databases.entry(compaction_detail.db_id).or_default();
        let compaction_detail = Arc::new(compaction_detail);
        let table = db
            .tables
            .entry(compaction_detail.table_id)
            .or_insert_with(|| CompactedTable {
                compaction_detail: Arc::clone(&compaction_detail),
                compacted_generations: HashMap::new(),
                file_index: FileIndex::default(),
            });

        table.compaction_detail = compaction_detail;
        for g in generation_details {
            table.add_generation_detail(g);
        }
        table.remove_compacted_generations(removed_generations);
    }

    pub(crate) fn update_compaction_detail(&self, compaction_detail: CompactionDetail) {
        let mut d = self.inner_compacted_data.write();
        let db = d.databases.entry(compaction_detail.db_id).or_default();
        let compaction_detail = Arc::new(compaction_detail);
        let table = db
            .tables
            .entry(compaction_detail.table_id)
            .or_insert_with(|| CompactedTable {
                compaction_detail: Arc::clone(&compaction_detail),
                compacted_generations: HashMap::new(),
                file_index: FileIndex::default(),
            });
        table.compaction_detail = compaction_detail;
    }

    pub(crate) async fn load_compacted_data(
        compactor_id: Arc<str>,
        compaction_summary: CompactionSummary,
        compacted_catalog: CompactedCatalog,
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
                let Some(compaction_detail) =
                    get_compaction_detail(&path, Arc::clone(&object_store)).await
                else {
                    return Err(Error::CompactionDetailReadError(path.as_path().to_string()));
                };

                let compaction_detail_db_id = compaction_detail.db_id;

                // initialize the table and load all its generations and set up the file index
                let mut table = CompactedTable {
                    compaction_detail: Arc::new(compaction_detail),
                    compacted_generations: HashMap::new(),
                    file_index: FileIndex::default(),
                };

                // load all the generation details
                let mut gen_details = JoinSet::new();
                for gen in &table.compaction_detail.compacted_generations {
                    let gen_path = GenerationDetailPath::new(Arc::clone(&compactor_id), gen.id);

                    let obj_store = Arc::clone(&object_store);
                    gen_details.spawn(async move {
                        match get_generation_detail(&gen_path, obj_store).await {
                            Some(gen) => Ok(gen),
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
            compacted_catalog,
            inner_compacted_data: RwLock::new(InnerCompactedData {
                databases,
                compaction_summary: Arc::new(compaction_summary),
            }),
        })
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
        let db = self.compacted_catalog.db_schema(db_name)?;
        let table_id = db.table_name_to_id(table_name)?;

        let inner_data = self.inner_compacted_data.read();
        let compacted_table = inner_data.databases.get(&db.id)?.tables.get(&table_id)?;
        let all_parquet_files = &compacted_table.compacted_generations;
        let results = compacted_table
            .compaction_detail
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

    fn catalog(&self) -> &CompactedCatalog {
        &self.compacted_catalog
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct CompactedDatabase {
    tables: HashMap<TableId, CompactedTable>,
}

#[derive(Debug, Eq, PartialEq)]
struct CompactedTable {
    compaction_detail: Arc<CompactionDetail>,
    compacted_generations: HashMap<GenerationId, Vec<Arc<ParquetFile>>>,
    file_index: FileIndex,
}

impl CompactedTable {
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

    fn remove_compacted_generations(&mut self, generations: Vec<Generation>) {
        for generation in generations {
            if let Some(files) = self.compacted_generations.remove(&generation.id) {
                for f in files {
                    self.file_index.remove_file(f.id);
                }
            }
        }
    }

    fn get_parquet_files_and_host_markers(
        &self,
        filters: &[Expr],
    ) -> (Vec<Arc<ParquetFile>>, Vec<Arc<HostSnapshotMarker>>) {
        let mut parquet_files = self.file_index.parquet_files_for_filter(filters);

        // add the gen1 files
        for f in &self.compaction_detail.leftover_gen1_files {
            parquet_files.push(Arc::clone(&f.file));
        }

        (
            parquet_files,
            self.compaction_detail.snapshot_markers.clone(),
        )
    }
}
