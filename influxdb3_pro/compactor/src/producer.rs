//! Produces the compacted data view from the snapshots of 1 or more hosts. Also keeps a unified
//! `Catalog` with id mappings for each host to the compacted view.

use crate::catalog::{CatalogSnapshotMarker, CompactedCatalog};
use crate::compacted_data::CompactedData;
use crate::planner::{CompactionPlanGroup, NextCompactionPlan};
use crate::{compact_files, CompactFilesArgs, ParquetCachePreFetcher};
use datafusion::datasource::object_store::ObjectStoreUrl;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_config::ProConfig;
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_pro_data_layout::persist::{
    get_bytes_at_path, load_compaction_summary, persist_compaction_detail,
    persist_compaction_summary, persist_generation_detail, CompactedDataPersistenceError,
};
use influxdb3_pro_data_layout::{
    CompactionConfig, CompactionDetail, CompactionSequenceNumber, CompactionSummary, Gen1File,
    GenerationDetail, GenerationId, GenerationLevel, HostSnapshotMarker,
};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::paths::SnapshotInfoFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::PersistedSnapshot;
use iox_query::exec::Executor;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, trace, warn};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompactedDataProducerError {
    #[error("Error loading compacted catalog: {0}")]
    LoadError(#[from] crate::catalog::Error),
    #[error("Error loading compacted data from object store: {0}")]
    CompactedDataLoadError(#[from] crate::compacted_data::Error),
    #[error("Error loading compaction summary: {0}")]
    CompactionSummaryLoadError(#[from] CompactedDataPersistenceError),
    #[error("Error loading snapshots: {0}")]
    SnapshotLoadError(#[from] influxdb3_write::persister::Error),
    #[error("Error deserializeing snapshot: {0}")]
    SnapshotDeserializeError(#[from] serde_json::Error),
}

/// Result type for functions in this module.
pub type Result<T, E = CompactedDataProducerError> = std::result::Result<T, E>;

pub struct CompactedDataProducer {
    pub compactor_id: Arc<str>,
    pub object_store: Arc<dyn ObjectStore>,
    pub compaction_config: CompactionConfig,
    pub object_store_url: ObjectStoreUrl,
    pub executor: Arc<Executor>,
    pub pro_config: Arc<tokio::sync::RwLock<ProConfig>>,
    pub compacted_data: Arc<CompactedData>,
    parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,

    /// The gen1 data to compact should only ever be accessed by the compaction loop, so we use a
    /// tokio Mutex so we can hold it across await points to object storage.
    compaction_state: tokio::sync::Mutex<CompactionState>,
}

impl Debug for CompactedDataProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactedDataProducer")
            .field("compactor_id", &self.compactor_id)
            .field("compaction_config", &self.compaction_config)
            .field(
                "prefetcher_configured",
                &self.parquet_cache_prefetcher.is_some(),
            )
            .finish()
    }
}

impl CompactedDataProducer {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        compactor_id: &str,
        hosts: Vec<String>,
        compaction_config: CompactionConfig,
        pro_config: Arc<tokio::sync::RwLock<ProConfig>>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
        executor: Arc<Executor>,
        parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
    ) -> Result<Self> {
        let (mut compaction_state, compacted_data) =
            CompactionState::load_or_initialize(compactor_id, hosts, Arc::clone(&object_store))
                .await?;
        compaction_state
            .load_snapshots(&compacted_data, Arc::clone(&object_store))
            .await?;

        Ok(Self {
            compactor_id: Arc::from(compactor_id),
            pro_config,
            object_store,
            compaction_config,
            object_store_url,
            executor,
            parquet_cache_prefetcher,
            compacted_data: Arc::new(compacted_data),
            compaction_state: tokio::sync::Mutex::new(compaction_state),
        })
    }

    /// The background loop that periodically checks for new snapshots and run compaction plans
    pub async fn compact(&self, check_interval: Duration) {
        let generation_levels = self.compaction_config.compaction_levels();
        let mut ticker = tokio::time::interval(check_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let mut compaction_state = self.compaction_state.lock().await;

            // load any new snapshots and catalog changes since the last time we checked
            if let Err(e) = compaction_state
                .load_snapshots(&self.compacted_data, Arc::clone(&self.object_store))
                .await
            {
                warn!(error = %e, "error loading snapshots");
                continue;
            };

            // handle the first level of compaction to move files from the host snapshots to the
            // compacted data
            if let Some(plan) = CompactionPlanGroup::plans_for_level(
                &self.compaction_config,
                &self.compacted_data,
                &compaction_state.files_to_compact,
                GenerationLevel::two(),
            ) {
                if let Err(e) = self
                    .run_compaction_plan_group(plan, &mut compaction_state)
                    .await
                {
                    warn!(error = %e, "error running compaction plan group");
                    continue;
                }
            }

            for level in &generation_levels {
                // TODO: wire up later generation compactions
                if *level > GenerationLevel::new(4) {
                    break;
                }

                if let Some(plan) = CompactionPlanGroup::plans_for_level(
                    &self.compaction_config,
                    &self.compacted_data,
                    &compaction_state.files_to_compact,
                    *level,
                ) {
                    if let Err(e) = self
                        .run_compaction_plan_group(plan, &mut compaction_state)
                        .await
                    {
                        warn!(error = %e, "error running compaction plan group");
                        continue;
                    }
                }
            }

            drop(compaction_state);

            ticker.tick().await;
        }
    }

    async fn run_compaction_plan_group(
        &self,
        compaction_plan_group: CompactionPlanGroup,
        compaction_state: &mut CompactionState,
    ) -> Result<()> {
        let sequence_number = self.compacted_data.next_compaction_sequence_number();
        let snapshot_markers = compaction_state.last_markers();

        let mut compaction_details = SerdeVecMap::new();

        // run all the compaction plans
        for plan in compaction_plan_group.next_compaction_plans {
            let key = (plan.db_schema.id, plan.table_definition.table_id);

            if let Err(e) = self
                .run_plan(plan, sequence_number, snapshot_markers.clone())
                .await
            {
                warn!(error = %e, "error running compaction plan");
                continue;
            }
            compaction_details.insert(key, sequence_number);
        }

        // write new compaction details for plans that only contain leftover gen1 files
        for plan in compaction_plan_group.leftover_plans {
            let key = (plan.db_schema.id, plan.table_definition.table_id);

            let mut detail = CompactionDetail {
                db_name: Arc::clone(&plan.db_schema.name),
                db_id: plan.db_schema.id,
                table_name: Arc::clone(&plan.table_definition.table_name),
                table_id: plan.table_definition.table_id,
                sequence_number,
                snapshot_markers: snapshot_markers.clone(),
                compacted_generations: vec![],
                leftover_gen1_files: plan.leftover_gen1_files,
            };

            if let Some(existing) = self
                .compacted_data
                .compaction_detail(plan.db_schema.id, plan.table_definition.table_id)
            {
                detail.compacted_generations = existing.compacted_generations.clone();
            }

            // TODO: handle this failure, which will only occur if we can't serialize the JSON
            persist_compaction_detail(
                self.compactor_id.as_ref(),
                plan.db_schema.id,
                plan.table_definition.table_id,
                &detail,
                Arc::clone(&self.object_store),
            )
            .await
            .expect("error persisting compaction detail");

            compaction_details.insert(key, sequence_number);
            self.compacted_data.update_compaction_detail(detail);
        }

        // carry over any tables that didn't get compacted
        for ((db_id, table_id), sequence_number) in
            &self.compacted_data.compaction_summary().compaction_details
        {
            if !compaction_details.contains_key(&(*db_id, *table_id)) {
                compaction_details.insert((*db_id, *table_id), *sequence_number);
            }
        }

        let compaction_summary = CompactionSummary {
            compaction_sequence_number: sequence_number,
            catalog_sequence_number: self
                .compacted_data
                .compacted_catalog
                .catalog
                .sequence_number(),
            last_file_id: ParquetFileId::next_id(),
            last_generation_id: GenerationId::current(),
            snapshot_markers,
            compaction_details,
        };

        // TODO: handle this failure, which will only occur if we can't serialize the JSON
        persist_compaction_summary(
            self.compactor_id.as_ref(),
            &compaction_summary,
            Arc::clone(&self.object_store),
        )
        .await
        .expect("error persisting compaction summary");

        self.compacted_data
            .update_compaction_summary(compaction_summary);

        compaction_state.clear();

        Ok(())
    }

    async fn run_plan(
        &self,
        plan: NextCompactionPlan,
        sequence_number: CompactionSequenceNumber,
        snapshot_markers: Vec<Arc<HostSnapshotMarker>>,
    ) -> Result<()> {
        let index_columns = self
            .pro_config
            .read()
            .await
            .index_columns(plan.db_schema.id, &plan.table_definition)
            .unwrap_or_else(|| plan.table_definition.index_column_ids());

        let args = CompactFilesArgs {
            table_def: Arc::clone(&plan.table_definition),
            compactor_id: Arc::clone(&self.compactor_id),
            paths: plan.input_paths,
            limit: self.compaction_config.per_file_row_limit,
            generation: plan.output_generation,
            index_columns,
            object_store: Arc::clone(&self.object_store),
            object_store_url: self.object_store_url.clone(),
            exec: Arc::clone(&self.executor),
            parquet_cache_prefetcher: self.parquet_cache_prefetcher.clone(),
        };

        // TODO: if this fails, instead write a compaction detail with all the L1 files added to it
        let compactor_output = compact_files(args).await.expect("compaction failed");

        // get the max time of the files in the output generation
        let max_time_ns = compactor_output
            .file_metadata
            .iter()
            .map(|f| f.max_time)
            .max()
            .unwrap_or(0);

        // write the generation detail to object store
        let generation_detail = GenerationDetail {
            id: plan.output_generation.id,
            level: plan.output_generation.level,
            start_time_s: plan.output_generation.start_time_secs,
            max_time_ns,
            files: compactor_output
                .file_metadata
                .into_iter()
                .map(Arc::new)
                .collect(),
            file_index: compactor_output.file_index,
        };

        // TODO: handle this failure, which will only occur if we can't serialize the JSON
        persist_generation_detail(
            self.compactor_id.as_ref(),
            plan.output_generation.id,
            &generation_detail,
            Arc::clone(&self.object_store),
        )
        .await
        .expect("error persisting generation detail");

        trace!(generaton_detail = ?generation_detail, "Generation detail written");

        let mut compacted_generations = self
            .compacted_data
            .compaction_detail(plan.db_schema.id, plan.table_definition.table_id)
            .map(|d| {
                d.compacted_generations
                    .iter()
                    .filter(|g| !plan.input_generations.contains(g))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        compacted_generations.push(plan.output_generation);
        compacted_generations.sort();

        let compaction_detail = CompactionDetail {
            db_name: Arc::clone(&plan.db_schema.name),
            db_id: plan.db_schema.id,
            table_name: Arc::clone(&plan.table_definition.table_name),
            table_id: plan.table_definition.table_id,
            sequence_number,
            snapshot_markers,
            compacted_generations,
            leftover_gen1_files: plan.leftover_gen1_files,
        };

        // TODO: handle this failure, which will only occur if we can't serialize the JSON
        persist_compaction_detail(
            self.compactor_id.as_ref(),
            plan.db_schema.id,
            plan.table_definition.table_id,
            &compaction_detail,
            Arc::clone(&self.object_store),
        )
        .await
        .expect("error persisting compaction detail");

        debug!(compaction_detail = ?compaction_detail, "Compaction detail written");

        self.compacted_data.update_detail_with_generations(
            compaction_detail,
            vec![generation_detail],
            plan.input_generations,
        );

        Ok(())
    }
}

/// CompactionState tracks the snapshot data and associated files from hosts that have yet to be
/// compacted or tracked in the comapcted data structure.
pub(crate) struct CompactionState {
    // the most recent snapshot marker for each host
    host_to_last_marker: HashMap<String, Arc<HostSnapshotMarker>>,
    // snapshots that have yet to be compacted (could be multiple per host)
    host_markers: Vec<Arc<HostSnapshotMarker>>,
    // map of files in all snapshots waiting to be compacted
    files_to_compact: Gen1FileMap,
}

pub(crate) type Gen1FileMap = HashMap<DbId, HashMap<TableId, Vec<Gen1File>>>;

impl CompactionState {
    /// Loads or initializes the compacted catalog, the last compaction summary, and the snapshots
    /// from
    pub(crate) async fn load_or_initialize(
        compactor_id: &str,
        hosts: Vec<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(Self, CompactedData)> {
        // load or initialize and persist a compacted catalog
        let compacted_catalog =
            match CompactedCatalog::load(compactor_id, Arc::clone(&object_store)).await? {
                Some(catalog) => catalog,
                None => {
                    let catalog = CompactedCatalog::load_merged_from_hosts(
                        compactor_id,
                        hosts.clone(),
                        Arc::clone(&object_store),
                    )
                    .await?;

                    catalog.persist(Arc::clone(&object_store)).await?;

                    catalog
                }
            };

        // load or initialize and persist the first compaction summary
        let compaction_summary =
            match load_compaction_summary(compactor_id, Arc::clone(&object_store)).await? {
                Some(summary) => summary,
                None => {
                    // if there's no compaction summary, we initialize a new one with hosts that have
                    // snapshot sequence numbers of zero, so that compaction can pick up all snapshots
                    // from there
                    let snapshot_markers = hosts
                        .iter()
                        .map(|host| {
                            Arc::new(HostSnapshotMarker {
                                host_id: host.clone(),
                                snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                                next_file_id: ParquetFileId::from(0),
                            })
                        })
                        .collect::<Vec<_>>();

                    let summary = CompactionSummary {
                        compaction_sequence_number: CompactionSequenceNumber::new(0),
                        catalog_sequence_number: CatalogSequenceNumber::new(0),
                        last_file_id: ParquetFileId::from(0),
                        last_generation_id: GenerationId::from(0),
                        snapshot_markers,
                        compaction_details: SerdeVecMap::new(),
                    };

                    persist_compaction_summary(compactor_id, &summary, Arc::clone(&object_store))
                        .await?;

                    summary
                }
            };

        // ensure that every host is in the last marker map
        let mut host_to_last_marker = compaction_summary
            .snapshot_markers
            .iter()
            .map(|marker| (marker.host_id.clone(), Arc::clone(marker)))
            .collect::<HashMap<_, _>>();
        for host in hosts {
            if !host_to_last_marker.contains_key(&host) {
                host_to_last_marker.insert(
                    host.clone(),
                    Arc::new(HostSnapshotMarker {
                        host_id: host,
                        snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                        next_file_id: ParquetFileId::from(0),
                    }),
                );
            }
        }

        // now load the compacted data
        let compacted_data = CompactedData::load_compacted_data(
            compactor_id,
            compaction_summary,
            compacted_catalog,
            Arc::clone(&object_store),
        )
        .await?;

        Ok((
            Self {
                host_to_last_marker,
                host_markers: vec![],
                files_to_compact: HashMap::new(),
            },
            compacted_data,
        ))
    }

    /// Loads the snapshots for each host up to the most recent 1,000 snapshots, or the next
    /// snapshot if the host has a snapshot sequence number greater than zero. Adds them to the
    /// state.
    async fn load_snapshots(
        &mut self,
        compacted_data: &CompactedData,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let mut snapshots = vec![];

        for (host, marker) in &self.host_to_last_marker {
            if marker.snapshot_sequence_number == SnapshotSequenceNumber::new(0) {
                // if the snapshot sequence number is zero, we need to load all snapshots for this
                // host up to the most recent 1,000
                let persister = Persister::new(Arc::clone(&object_store), host);
                let host_snapshots = persister.load_snapshots(1_000).await?;
                info!(host = %host, snapshot_count = host_snapshots.len(), "loaded snapshots");
                snapshots.extend(host_snapshots);
            } else {
                let next_snapshot_path =
                    SnapshotInfoFilePath::new(host, marker.snapshot_sequence_number.next());
                if let Some(data) =
                    get_bytes_at_path(next_snapshot_path.as_ref(), Arc::clone(&object_store)).await
                {
                    let snapshot: PersistedSnapshot = serde_json::from_slice(&data)?;
                    info!(host = %host, snapshot = %snapshot.snapshot_sequence_number, "loaded snapshot");
                    snapshots.push(snapshot);
                }
            }
        }

        // create host catalog markers with the max catalog_sequence_number from each host
        let mut host_catalog_markers: HashMap<&String, CatalogSnapshotMarker> = HashMap::new();
        for snapshot in &snapshots {
            let marker = host_catalog_markers
                .entry(&snapshot.host_id)
                .or_insert_with(|| CatalogSnapshotMarker {
                    sequence_number: snapshot.catalog_sequence_number,
                    host: snapshot.host_id.clone(),
                });

            if snapshot.catalog_sequence_number > marker.sequence_number {
                marker.sequence_number = snapshot.catalog_sequence_number;
            }
        }
        let host_catalog_markers = host_catalog_markers.into_values().collect::<Vec<_>>();

        // if we have any catalog markers, we need to see if we need to update the catalog
        if !host_catalog_markers.is_empty() {
            if let Err(e) = compacted_data
                .compacted_catalog
                .update_from_markers(host_catalog_markers, Arc::clone(&object_store))
                .await
            {
                warn!(error = %e, "error updating compacted catalog from markers");
            };
        }

        // now map all the snapshot ids before adding them in
        let mut mapped_snapshots = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let s = compacted_data
                .compacted_catalog
                .map_persisted_snapshot_contents(snapshot)?;
            mapped_snapshots.push(s);
        }

        self.add_snapshots(mapped_snapshots);

        Ok(())
    }

    /// Adds snapshots to the state, updating the host markers and the files to compact
    fn add_snapshots(&mut self, snapshots: Vec<PersistedSnapshot>) {
        if snapshots.is_empty() {
            return;
        }

        for snapshot in snapshots {
            let Some(marker) = self.host_to_last_marker.get_mut(&snapshot.host_id) else {
                warn!(host = %snapshot.host_id, "snapshot host not in marker map");
                continue;
            };

            let new_marker = Arc::new(HostSnapshotMarker {
                host_id: snapshot.host_id.clone(),
                snapshot_sequence_number: snapshot.snapshot_sequence_number,
                next_file_id: snapshot.next_file_id,
            });

            if snapshot.snapshot_sequence_number > marker.snapshot_sequence_number {
                self.host_to_last_marker
                    .insert(snapshot.host_id.clone(), Arc::clone(&new_marker));
            }

            self.host_markers.push(new_marker);

            for (db_id, tables) in snapshot.databases {
                let db_map = self.files_to_compact.entry(db_id).or_default();

                for (table_id, parquet_files) in tables.tables {
                    let files = db_map.entry(table_id).or_default();
                    for f in parquet_files {
                        files.push(Gen1File::new(Arc::new(f)))
                    }
                }
            }
        }
    }

    /// Returns the last snapshot marker for each host that we're compacting data for
    fn last_markers(&self) -> Vec<Arc<HostSnapshotMarker>> {
        self.host_to_last_marker.values().cloned().collect()
    }

    /// Clears the state of all snapshots and files to compact
    fn clear(&mut self) {
        self.host_markers.clear();
        self.files_to_compact.clear();
    }
}
