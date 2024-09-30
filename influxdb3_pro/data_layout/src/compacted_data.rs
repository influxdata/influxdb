//! Module that keeps track of comapcted data and provides methods for querying and loading it.

use crate::persist::{
    get_compaction_detail, get_generation_detail, load_compaction_summary,
    CompactedDataPersistenceError,
};
use crate::{
    CompactionConfig, CompactionDetail, CompactionSequenceNumber, CompactionSummary, Gen1File,
    Generation, GenerationDetail, GenerationDetailPath, GenerationId,
};
use hashbrown::HashMap;
use influxdb3_write::{ParquetFile, NEXT_FILE_ID};
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
    databases: HashMap<Arc<str>, CompactedDatabase>,
    generation_details: HashMap<GenerationId, Arc<GenerationDetail>>,
    gen1_files: HashMap<GenerationId, Gen1File>,
    last_compaction_summary: Option<CompactionSummary>,
    last_compaction_sequence_number: CompactionSequenceNumber,
}

impl CompactedData {
    pub fn new(
        compactor_id: Arc<str>,
        compaction_config: CompactionConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            compactor_id,
            object_store,
            compaction_config,
            data: RwLock::new(InnerCompactedData {
                databases: HashMap::new(),
                generation_details: HashMap::new(),
                gen1_files: HashMap::new(),
                last_compaction_summary: None,
                last_compaction_sequence_number: CompactionSequenceNumber::new(0),
            }),
        }
    }

    pub async fn load_compacted_data(
        compactor_id: &str,
        compaction_config: CompactionConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<Self>> {
        let compaction_summary =
            load_compaction_summary(compactor_id, Arc::clone(&object_store)).await?;

        let Some(compaction_summary) = compaction_summary else {
            return Ok(Arc::new(Self::new(
                Arc::from(compactor_id),
                compaction_config,
                object_store,
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
            generation_details: HashMap::new(),
            gen1_files: HashMap::new(),
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

            // load all the generation details
            for gen in &compaction_detail.compacted_generations {
                let gen_path = GenerationDetailPath::new(compactor_id, gen.id);
                let Some(generation_detail) =
                    get_generation_detail(&gen_path, Arc::clone(&object_store)).await
                else {
                    return Err(Error::GenerationDetailReadError(gen_path.0.to_string()));
                };

                data.generation_details
                    .insert(generation_detail.id, Arc::new(generation_detail));
            }

            // inject the gen1 files into the data
            for gen1_file in &compaction_detail.leftover_gen1_files {
                data.gen1_files.insert(gen1_file.id, gen1_file.clone());
            }

            // and now add the compaction detail to the data
            data.databases
                .entry(Arc::clone(&compaction_detail.db_name))
                .or_insert_with(|| CompactedDatabase {
                    tables: HashMap::new(),
                })
                .tables
                .insert(
                    Arc::clone(&compaction_detail.table_name),
                    Arc::new(compaction_detail),
                );
        }

        Ok(Arc::new(Self {
            compactor_id: Arc::from(compactor_id),
            object_store,
            compaction_config,
            data: RwLock::new(data),
        }))
    }

    pub fn get_generations_newer_than(
        &self,
        db_name: &str,
        table_name: &str,
        min_time_secs: i64,
    ) -> Vec<Generation> {
        self.data
            .read()
            .databases
            .get(db_name)
            .and_then(|db| {
                db.tables.get(table_name).map(|compaction_detail| {
                    compaction_detail
                        .compacted_generations
                        .iter()
                        .filter(|g| g.start_time_secs >= min_time_secs)
                        .cloned()
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    pub fn add_gen1_file_to_map(&self, file: Arc<ParquetFile>) -> Gen1File {
        let gen1_file = Gen1File::new(file);

        let mut data = self.data.write();
        data.gen1_files.insert(gen1_file.id, gen1_file.clone());

        gen1_file
    }

    pub fn update_compaction_detail_with_generation(
        &self,
        compacted_ids: &[GenerationId],
        compaction_detail: CompactionDetail,
        generation_detail: GenerationDetail,
    ) {
        let mut data = self.data.write();

        // ensure that the generation is removed from the maps
        for id in compacted_ids {
            data.gen1_files.remove(id);
            data.generation_details.remove(id);
        }

        // update the compaction detail
        let db = data
            .databases
            .entry(Arc::clone(&compaction_detail.db_name))
            .or_insert_with(|| CompactedDatabase {
                tables: HashMap::new(),
            });

        db.tables.insert(
            Arc::clone(&compaction_detail.table_name),
            Arc::new(compaction_detail),
        );

        data.generation_details
            .insert(generation_detail.id, Arc::new(generation_detail));
    }

    pub fn update_compaction_detail(&self, compaction_detail: CompactionDetail) {
        let mut data = self.data.write();

        let db = data
            .databases
            .entry(Arc::clone(&compaction_detail.db_name))
            .or_insert_with(|| CompactedDatabase {
                tables: HashMap::new(),
            });

        db.tables.insert(
            Arc::clone(&compaction_detail.table_name),
            Arc::new(compaction_detail),
        );
    }

    pub fn get_generation_detail(
        &self,
        generation_id: GenerationId,
    ) -> Option<Arc<GenerationDetail>> {
        self.data
            .read()
            .generation_details
            .get(&generation_id)
            .cloned()
    }

    pub fn next_compaction_sequence_number(&self) -> CompactionSequenceNumber {
        let mut data = self.data.write();
        let next = data.last_compaction_sequence_number.next();
        data.last_compaction_sequence_number = next;
        next
    }

    pub fn get_last_compaction_detail(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<Arc<CompactionDetail>> {
        self.data
            .read()
            .databases
            .get(db_name)
            .and_then(|db| db.tables.get(table_name).map(Arc::clone))
    }

    pub fn get_last_summary(&self) -> Option<CompactionSummary> {
        self.data.read().last_compaction_summary.clone()
    }

    pub fn set_last_summary(&self, summary: CompactionSummary) {
        let mut data = self.data.write();
        data.last_compaction_summary = Some(summary);
    }

    pub fn files_for_generation(&self, generation_id: GenerationId) -> Vec<Arc<ParquetFile>> {
        let data = self.data.read();
        data.generation_details
            .get(&generation_id)
            .map(|detail| detail.files.clone())
            .unwrap_or_else(|| {
                data.gen1_files
                    .get(&generation_id)
                    .map(|f| vec![Arc::clone(&f.file)])
                    .unwrap_or_default()
            })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct CompactedDatabase {
    tables: HashMap<Arc<str>, Arc<CompactionDetail>>,
}
