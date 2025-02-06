//! Produces the compacted data view from the snapshots of 1 or more writers. Also keeps a unified
//! `Catalog` with id mappings for each writer to the compacted view.

use crate::compacted_data::CompactedData;
use crate::planner::{CompactionPlanGroup, NextCompactionPlan};
use crate::{
    catalog::{CatalogSnapshotMarker, CompactedCatalog},
    sys_events::{
        compaction_completed::{self, PlanIdentifier},
        compaction_planned,
        snapshot_fetched::{FailedInfo, SuccessInfo},
        CompactionEventStore,
    },
};
use crate::{compact_files, CompactFilesArgs, ParquetCachePreFetcher};
use datafusion::{common::instant::Instant, datasource::object_store::ObjectStoreUrl};
use hashbrown::HashMap;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_config::EnterpriseConfig;
use influxdb3_enterprise_data_layout::persist::get_generation_detail;
use influxdb3_enterprise_data_layout::CompactionSummaryPath;
use influxdb3_enterprise_data_layout::{
    persist::{
        get_bytes_at_path, load_compaction_summary, persist_compaction_detail,
        persist_compaction_summary, persist_generation_detail, CompactedDataPersistenceError,
    },
    Generation,
};
use influxdb3_enterprise_data_layout::{
    CompactionConfig, CompactionDetail, CompactionSequenceNumber, CompactionSummary, Gen1File,
    GenerationDetail, GenerationId, GenerationLevel, NodeSnapshotMarker,
};
use influxdb3_enterprise_data_layout::{CompactionDetailPath, GenerationDetailPath};
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::paths::SnapshotInfoFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::PersistedSnapshot;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use object_store::path::Path;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, trace, warn};
use parking_lot::Mutex;
use std::collections::HashMap as StdHashMap;
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
    #[error("Error joining spawned task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
}

/// Result type for functions in this module.
pub type Result<T, E = CompactedDataProducerError> = std::result::Result<T, E>;

pub struct CompactedDataProducer {
    pub compactor_id: Arc<str>,
    pub object_store: Arc<dyn ObjectStore>,
    pub compaction_config: CompactionConfig,
    pub object_store_url: ObjectStoreUrl,
    pub executor: Arc<Executor>,
    pub enterprise_config: Arc<EnterpriseConfig>,
    pub datafusion_config: Arc<StdHashMap<String, String>>,
    pub compacted_data: Arc<CompactedData>,
    parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,

    /// The gen1 data to compact should only ever be accessed by the compaction loop, so we use a
    /// tokio Mutex so we can hold it across await points to object storage.
    compaction_state: tokio::sync::Mutex<CompactionState>,
    sys_events_store: Arc<dyn CompactionEventStore>,
    _time_provider: Arc<dyn TimeProvider>,
    // Public only for tests
    #[doc(hidden)]
    pub to_delete: Mutex<Vec<Path>>,
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

#[derive(Debug)]
pub struct CompactedDataProducerArgs {
    pub compactor_id: Arc<str>,
    pub node_ids: Vec<String>,
    pub compaction_config: CompactionConfig,
    pub enterprise_config: Arc<EnterpriseConfig>,
    pub datafusion_config: Arc<StdHashMap<String, String>>,
    pub object_store: Arc<dyn ObjectStore>,
    pub object_store_url: ObjectStoreUrl,
    pub executor: Arc<Executor>,
    pub parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
    pub sys_events_store: Arc<dyn CompactionEventStore>,
    pub time_provider: Arc<dyn TimeProvider>,
}

impl CompactedDataProducer {
    pub async fn new(
        CompactedDataProducerArgs {
            compactor_id,
            node_ids,
            compaction_config,
            enterprise_config,
            datafusion_config,
            object_store,
            object_store_url,
            executor,
            parquet_cache_prefetcher,
            sys_events_store,
            time_provider,
        }: CompactedDataProducerArgs,
    ) -> Result<Self> {
        let (mut compaction_state, compacted_data) = CompactionState::load_or_initialize(
            Arc::clone(&compactor_id),
            node_ids,
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await?;
        compaction_state
            .load_snapshots(
                &compacted_data,
                Arc::clone(&object_store),
                Arc::clone(&sys_events_store),
            )
            .await?;

        Ok(Self {
            compactor_id,
            enterprise_config,
            object_store,
            compaction_config,
            object_store_url,
            executor,
            parquet_cache_prefetcher,
            compacted_data: Arc::new(compacted_data),
            compaction_state: tokio::sync::Mutex::new(compaction_state),
            sys_events_store,
            datafusion_config,
            to_delete: Mutex::new(Vec::new()),
            _time_provider: time_provider,
        })
    }

    /// The background loop that periodically checks for new snapshots and run compaction plans
    pub async fn run_compaction_loop(
        mut self,
        check_interval: Duration,
        wait_time_to_delete: Duration,
    ) {
        let generation_levels = self.compaction_config.compaction_levels();
        let mut ticker = tokio::time::interval(check_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if let Err(e) = self
                .plan_and_run_compaction(&generation_levels, Arc::clone(&self.sys_events_store))
                .await
            {
                // Clear out the vec for files to delete as compaction has failed
                *self.to_delete.lock() = Vec::new();
                warn!(error = %e, "error running compaction");
            } else {
                // In the case where compaction was successful spawn a CompactionCleaner to clean up
                // the `to_delete` files
                self.spawn_compaction_deletion(wait_time_to_delete);
            };
            ticker.tick().await;
        }
    }

    fn spawn_compaction_deletion(&mut self, wait_time: Duration) {
        let mut locked = self.to_delete.lock();
        let to_delete = std::mem::take(&mut *locked);
        CompactionCleaner::spawn_new(Arc::clone(&self.object_store), to_delete, wait_time);
    }

    pub async fn plan_and_run_compaction(
        &self,
        generation_levels: &[GenerationLevel],
        sys_events_store: Arc<dyn CompactionEventStore>,
    ) -> Result<()> {
        let mut compaction_state = self.compaction_state.lock().await;

        // load any new snapshots and catalog changes since the last time we checked
        compaction_state
            .load_snapshots(
                &self.compacted_data,
                Arc::clone(&self.object_store),
                Arc::clone(&sys_events_store),
            )
            .await?;

        // handle the first level of compaction to move files from the writer snapshots to the
        // compacted data
        let planning_timer = Instant::now();
        match CompactionPlanGroup::plans_for_level(
            &self.compaction_config,
            &self.compacted_data,
            &compaction_state.files_to_compact,
            GenerationLevel::two(),
        ) {
            Ok(maybe_plan) => {
                if let Some(plan) = maybe_plan {
                    record_compaction_plans(
                        &plan,
                        planning_timer.elapsed(),
                        Arc::clone(&sys_events_store),
                    );
                    let run_timer = Instant::now();
                    let identifiers: Vec<PlanIdentifier> = plan
                        .next_compaction_plans
                        .iter()
                        .map(|plan| PlanIdentifier {
                            db_name: Arc::clone(&plan.db_schema.name),
                            table_name: Arc::clone(&plan.table_definition.table_name),
                            output_generation: plan.output_generation.level.as_u8(),
                        })
                        .collect();
                    self.run_compaction_plan_group(plan, &mut compaction_state, &sys_events_store)
                        .await
                        .inspect_err(|err| {
                            let event = compaction_completed::PlanGroupRunFailedInfo {
                                duration: run_timer.elapsed(),
                                error: err.to_string(),
                                plans_ran: identifiers.clone(),
                            };
                            sys_events_store.record_compaction_plan_group_run_failed(event);
                        })?;
                    let event = compaction_completed::PlanGroupRunSuccessInfo {
                        duration: run_timer.elapsed(),
                        plans_ran: identifiers,
                    };
                    sys_events_store.record_compaction_plan_group_run_success(event);
                }
            }
            Err(err_string) => {
                record_error_compaction_plan(
                    planning_timer.elapsed(),
                    err_string,
                    Arc::clone(&sys_events_store),
                );
            }
        }

        let planning_timer = Instant::now();
        for level in generation_levels {
            match CompactionPlanGroup::plans_for_level(
                &self.compaction_config,
                &self.compacted_data,
                &compaction_state.files_to_compact,
                *level,
            ) {
                Ok(maybe_plan) => {
                    if let Some(plan) = maybe_plan {
                        record_compaction_plans(
                            &plan,
                            planning_timer.elapsed(),
                            Arc::clone(&sys_events_store),
                        );
                        let run_timer = Instant::now();
                        let identifiers: Vec<PlanIdentifier> = plan
                            .next_compaction_plans
                            .iter()
                            .map(|plan| PlanIdentifier {
                                db_name: Arc::clone(&plan.db_schema.name),
                                table_name: Arc::clone(&plan.table_definition.table_name),
                                output_generation: plan.output_generation.level.as_u8(),
                            })
                            .collect();
                        self.run_compaction_plan_group(
                            plan,
                            &mut compaction_state,
                            &sys_events_store,
                        )
                        .await
                        .inspect_err(|err| {
                            let event = compaction_completed::PlanGroupRunFailedInfo {
                                duration: run_timer.elapsed(),
                                error: err.to_string(),
                                plans_ran: identifiers.clone(),
                            };
                            sys_events_store.record_compaction_plan_group_run_failed(event);
                        })?;
                        let event = compaction_completed::PlanGroupRunSuccessInfo {
                            duration: run_timer.elapsed(),
                            plans_ran: identifiers,
                        };
                        sys_events_store.record_compaction_plan_group_run_success(event);
                    }
                }
                Err(err_string) => {
                    record_error_compaction_plan(
                        planning_timer.elapsed(),
                        err_string,
                        Arc::clone(&sys_events_store),
                    );
                }
            }
        }

        Ok(())
    }

    async fn run_compaction_plan_group(
        &self,
        compaction_plan_group: CompactionPlanGroup,
        compaction_state: &mut CompactionState,
        sys_events_store: &Arc<dyn CompactionEventStore>,
    ) -> Result<()> {
        let sequence_number = self.compacted_data.current_compaction_sequence_number();
        let next_sequence_number = self.compacted_data.next_compaction_sequence_number();
        let snapshot_markers = compaction_state.last_markers();

        let mut compaction_details = SerdeVecMap::new();

        // run all the compaction plans
        for plan in compaction_plan_group.next_compaction_plans {
            let start = Instant::now();
            let key = (plan.db_schema.id, plan.table_definition.table_id);

            let input_generations = Generation::to_vec_levels(&plan.input_generations);
            let input_paths = plan
                .input_paths
                .iter()
                .map(|path| Arc::from(path.as_ref()))
                .collect();

            let db_name = Arc::clone(&plan.db_schema.name);
            let table_name = Arc::clone(&plan.table_definition.table_name);
            let output_generation = plan.output_generation.level.as_u8();
            let left_over_gen1_files = plan.leftover_gen1_files.len() as u64;
            if let Err(e) = self
                .run_plan(plan, next_sequence_number, snapshot_markers.clone())
                .await
            {
                warn!(error = %e, "error running compaction plan");
                let identifier = PlanIdentifier {
                    db_name,
                    table_name,
                    output_generation,
                };
                record_plan_run_failed(e.to_string(), start, identifier, sys_events_store);
                continue;
            }

            let event = compaction_completed::PlanRunSuccessInfo {
                input_generations,
                input_paths,
                output_level: output_generation,
                db_name,
                table_name,
                duration: start.elapsed(),
                left_over_gen1_files,
            };
            sys_events_store.record_compaction_plan_run_success(event);
            compaction_details.insert(key, next_sequence_number);
        }

        // write new compaction details for plans that only contain leftover gen1 files
        for plan in compaction_plan_group.leftover_plans {
            let key = (plan.db_schema.id, plan.table_definition.table_id);

            let mut detail = CompactionDetail {
                db_name: Arc::clone(&plan.db_schema.name),
                db_id: plan.db_schema.id,
                table_name: Arc::clone(&plan.table_definition.table_name),
                table_id: plan.table_definition.table_id,
                sequence_number: next_sequence_number,
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
                Arc::clone(&self.compactor_id),
                plan.db_schema.id,
                plan.table_definition.table_id,
                &detail,
                Arc::clone(&self.object_store),
            )
            .await
            .expect("error persisting compaction detail");

            compaction_details.insert(key, next_sequence_number);
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
            compaction_sequence_number: next_sequence_number,
            catalog_sequence_number: self.compacted_data.compacted_catalog.sequence_number(),
            last_file_id: ParquetFileId::next_id(),
            last_generation_id: GenerationId::current(),
            snapshot_markers,
            compaction_details,
        };

        // TODO: handle this failure, which will only occur if we can't serialize the JSON
        persist_compaction_summary(
            Arc::clone(&self.compactor_id),
            &compaction_summary,
            Arc::clone(&self.object_store),
        )
        .await
        .expect("error persisting compaction summary");

        // Push the path to the compaction summary we wish to delete
        self.to_delete.lock().push(
            CompactionSummaryPath::new(Arc::clone(&self.compactor_id), sequence_number)
                .into_inner(),
        );

        self.compacted_data
            .update_compaction_summary(compaction_summary);

        compaction_state.clear();

        Ok(())
    }

    async fn run_plan(
        &self,
        plan: NextCompactionPlan,
        sequence_number: CompactionSequenceNumber,
        snapshot_markers: Vec<Arc<NodeSnapshotMarker>>,
    ) -> Result<()> {
        let index_columns = self
            .enterprise_config
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
            datafusion_config: Arc::clone(&self.datafusion_config),
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
            Arc::clone(&self.compactor_id),
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
            Arc::clone(&self.compactor_id),
            plan.db_schema.id,
            plan.table_definition.table_id,
            &compaction_detail,
            Arc::clone(&self.object_store),
        )
        .await
        .expect("error persisting compaction detail");

        debug!(compaction_detail = ?compaction_detail, "Compaction detail written");

        // Remove all the generation details and parquet files used as inputs for this generation
        for generation in plan.input_generations.iter() {
            if generation.level == GenerationLevel::one() {
                // Skip every input that is Gen 1
                continue;
            }
            let path = GenerationDetailPath::new(Arc::clone(&self.compactor_id), generation.id);
            let Some(gen_detail) =
                get_generation_detail(&path, Arc::clone(&self.object_store)).await
            else {
                continue;
            };
            let mut lock = self.to_delete.lock();
            for file in gen_detail.files {
                lock.push(Path::from(file.path.clone()));
            }
            lock.push(path.into_inner());
        }

        // Push the old compaction detail path into our deletion vec which we
        // will delete after compaction is completed
        self.to_delete.lock().push(
            CompactionDetailPath::new(
                Arc::clone(&self.compactor_id),
                plan.db_schema.id,
                plan.table_definition.table_id,
                self.compacted_data.current_compaction_sequence_number(),
            )
            .into_inner(),
        );

        self.compacted_data.update_detail_with_generations(
            compaction_detail,
            vec![generation_detail],
            plan.input_generations,
        );

        Ok(())
    }
}

fn record_plan_run_failed(
    error: String,
    start: std::time::Instant,
    identifier: PlanIdentifier,
    sys_events_store: &Arc<dyn CompactionEventStore>,
) {
    let event = compaction_completed::PlanRunFailedInfo {
        duration: start.elapsed(),
        error,
        identifier,
    };
    sys_events_store.record_compaction_plan_run_failed(event);
}

fn record_error_compaction_plan(
    duration: Duration,
    error: String,
    sys_events_store: Arc<dyn CompactionEventStore>,
) {
    let event = compaction_planned::FailedInfo { duration, error };
    sys_events_store.record_compaction_plan_failed(event);
}

fn record_compaction_plans(
    plan_group: &CompactionPlanGroup,
    duration: Duration,
    sys_events_store: Arc<dyn CompactionEventStore>,
) {
    let next_compaction_plans = &plan_group.next_compaction_plans;
    for plan in next_compaction_plans {
        record_compaction_plan(plan, duration, &sys_events_store);
    }
}

fn record_compaction_plan(
    plan: &NextCompactionPlan,
    duration: Duration,
    sys_events_store: &Arc<dyn CompactionEventStore>,
) {
    let event = compaction_planned::SuccessInfo {
        input_generations: Generation::to_vec_levels(&plan.input_generations),
        input_paths: plan
            .input_paths
            .iter()
            .map(|path| Arc::from(path.as_ref()))
            .collect(),
        output_level: GenerationLevel::two().as_u8(),
        db_name: Arc::clone(&plan.db_schema.name),
        table_name: Arc::clone(&plan.table_definition.table_name),
        duration,
        left_over_gen1_files: plan.leftover_gen1_files.len() as u64,
    };
    sys_events_store.record_compaction_plan_success(event);
}

/// CompactionState tracks the snapshot data and associated files from writers that have yet to be
/// compacted or tracked in the comapcted data structure.
pub(crate) struct CompactionState {
    // The most recent snapshot marker for each writer
    node_id_to_last_marker: HashMap<String, Arc<NodeSnapshotMarker>>,
    // Snapshots that have yet to be compacted (could be multiple per writer)
    writer_markers: Vec<Arc<NodeSnapshotMarker>>,
    // Map of files in all snapshots waiting to be compacted
    files_to_compact: Gen1FileMap,
    /// time provider
    time_provider: Arc<dyn TimeProvider>,
}

pub(crate) type Gen1FileMap = HashMap<DbId, HashMap<TableId, Vec<Gen1File>>>;

impl CompactionState {
    /// Loads or initializes the compacted catalog, the last compaction summary, and the snapshots
    /// from the target writers.
    pub(crate) async fn load_or_initialize(
        compactor_id: Arc<str>,
        node_ids: Vec<String>,
        object_store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<(Self, CompactedData)> {
        // load or initialize and persist a compacted catalog
        async fn compacted_catalog(
            compactor_id: Arc<str>,
            node_ids: Vec<String>,
            obj_store: Arc<dyn ObjectStore>,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Result<CompactedCatalog> {
            match CompactedCatalog::load(Arc::clone(&compactor_id), Arc::clone(&obj_store)).await? {
                Some(catalog) => Ok(catalog),
                None => {
                    let catalog = CompactedCatalog::load_merged_from_node_ids(
                        compactor_id,
                        node_ids.clone(),
                        Arc::clone(&obj_store),
                        time_provider,
                    )
                    .await?;

                    catalog.persist(obj_store).await?;

                    Ok(catalog)
                }
            }
        }

        // load or initialize and persist the first compaction summary
        async fn summary(
            compactor_id: Arc<str>,
            node_ids: Vec<String>,
            obj_store: Arc<dyn ObjectStore>,
        ) -> Result<(CompactionSummary, HashMap<String, Arc<NodeSnapshotMarker>>)> {
            let compaction_summary =
                match load_compaction_summary(Arc::clone(&compactor_id), Arc::clone(&obj_store))
                    .await?
                {
                    Some(summary) => summary,
                    None => {
                        // if there's no compaction summary, we initialize a new one with writers that have
                        // snapshot sequence numbers of zero, so that compaction can pick up all snapshots
                        // from there
                        let snapshot_markers = node_ids
                            .iter()
                            .map(|node_id| {
                                Arc::new(NodeSnapshotMarker {
                                    node_id: node_id.clone(),
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

                        persist_compaction_summary(compactor_id, &summary, Arc::clone(&obj_store))
                            .await?;

                        summary
                    }
                };

            // ensure that every writer is in the last marker map
            let mut node_id_to_last_marker = compaction_summary
                .snapshot_markers
                .iter()
                .map(|marker| (marker.node_id.clone(), Arc::clone(marker)))
                .collect::<HashMap<_, _>>();
            for node_id in node_ids {
                if !node_id_to_last_marker.contains_key(&node_id) {
                    node_id_to_last_marker.insert(
                        node_id.clone(),
                        Arc::new(NodeSnapshotMarker {
                            node_id,
                            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                            next_file_id: ParquetFileId::from(0),
                        }),
                    );
                }
            }
            Ok((compaction_summary, node_id_to_last_marker))
        }

        // Spawn tasks so they can run in parallel
        let task_1 = tokio::spawn(compacted_catalog(
            Arc::clone(&compactor_id),
            node_ids.clone(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        ));
        let task_2 = tokio::spawn(summary(
            Arc::clone(&compactor_id),
            node_ids,
            Arc::clone(&object_store),
        ));

        // Await both futures concurrently
        let (compacted_catalog, summary_tuple) = tokio::try_join!(task_1, task_2)?;
        let compacted_catalog = compacted_catalog?;
        let (compaction_summary, node_id_to_last_marker) = summary_tuple?;

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
                node_id_to_last_marker,
                writer_markers: vec![],
                files_to_compact: HashMap::new(),
                time_provider,
            },
            compacted_data,
        ))
    }

    /// Loads the snapshots for each writer up to the most recent 1,000 snapshots, or the next
    /// snapshot if the writer has a snapshot sequence number greater than zero. Adds them to the
    /// state.
    async fn load_snapshots(
        &mut self,
        compacted_data: &CompactedData,
        object_store: Arc<dyn ObjectStore>,
        sys_events_store: Arc<dyn CompactionEventStore>,
    ) -> Result<()> {
        let mut snapshots = vec![];
        for (node_id, marker) in &self.node_id_to_last_marker {
            let start = Instant::now();
            if marker.snapshot_sequence_number == SnapshotSequenceNumber::new(0) {
                // if the snapshot sequence number is zero, we need to load all snapshots for this
                // writer up to the most recent 1,000
                let persister = Persister::new(
                    Arc::clone(&object_store),
                    node_id,
                    Arc::clone(&self.time_provider),
                );
                let writer_snapshots =
                    load_all_snapshots(persister, node_id, marker, &sys_events_store)
                        .await
                        .inspect_err(|err| {
                            let failed_event = FailedInfo {
                                node_id: Arc::from(node_id.as_str()),
                                duration: start.elapsed(),
                                sequence_number: marker.snapshot_sequence_number.as_u64(),
                                error: err.to_string(),
                            };
                            sys_events_store.record_snapshot_failed(failed_event);
                        })?;
                snapshots.extend(writer_snapshots);
            } else {
                load_next_snapshot(
                    marker,
                    node_id,
                    &object_store,
                    &sys_events_store,
                    &mut snapshots,
                )
                .await
                .inspect_err(|err| {
                    let failed_event = FailedInfo {
                        node_id: Arc::from(node_id.as_str()),
                        duration: start.elapsed(),
                        sequence_number: marker.snapshot_sequence_number.as_u64(),
                        error: err.to_string(),
                    };
                    sys_events_store.record_snapshot_failed(failed_event);
                })?;
            }
        }

        // create writer catalog markers with the max catalog_sequence_number from each writer
        let mut writer_catalog_markers: HashMap<&String, CatalogSnapshotMarker> = HashMap::new();
        for snapshot in &snapshots {
            let marker = writer_catalog_markers
                .entry(&snapshot.node_id)
                .or_insert_with(|| CatalogSnapshotMarker {
                    snapshot_sequence_number: snapshot.snapshot_sequence_number,
                    catalog_sequence_number: snapshot.catalog_sequence_number,
                    node_id: snapshot.node_id.clone(),
                });

            if snapshot.catalog_sequence_number > marker.catalog_sequence_number {
                marker.catalog_sequence_number = snapshot.catalog_sequence_number;
            }
        }
        let writer_catalog_markers = writer_catalog_markers.into_values().collect::<Vec<_>>();

        // if we have any catalog markers, we need to see if we need to update the catalog
        if !writer_catalog_markers.is_empty() {
            if let Err(e) = compacted_data
                .compacted_catalog
                .update_from_markers(
                    writer_catalog_markers,
                    Arc::clone(&object_store),
                    Arc::clone(&sys_events_store),
                )
                .await
            {
                warn!(error = %e, "error updating compacted catalog from markers");
            };
        }

        // now map all the snapshot ids before adding them in
        let mut mapped_snapshots = Vec::with_capacity(snapshots.len());
        let start = Instant::now();
        for snapshot in snapshots {
            let node_id = snapshot.node_id.to_owned();
            let sequence_num = snapshot.snapshot_sequence_number;
            let s = compacted_data
                .compacted_catalog
                .map_persisted_snapshot_contents(snapshot)
                .inspect_err(|err| {
                    let event = FailedInfo {
                        node_id: Arc::from(node_id.as_str()),
                        duration: start.elapsed(),
                        sequence_number: sequence_num.as_u64(),
                        error: err.to_string(),
                    };
                    sys_events_store.record_snapshot_failed(event);
                })?;
            mapped_snapshots.push(s);
        }

        self.add_snapshots(mapped_snapshots);

        Ok(())
    }

    /// Adds snapshots to the state, updating the writer markers and the files to compact
    fn add_snapshots(&mut self, snapshots: Vec<PersistedSnapshot>) {
        if snapshots.is_empty() {
            return;
        }

        for snapshot in snapshots {
            let Some(marker) = self.node_id_to_last_marker.get_mut(&snapshot.node_id) else {
                warn!(node_id = %snapshot.node_id, "snapshot writer not in marker map");
                continue;
            };

            let new_marker = Arc::new(NodeSnapshotMarker {
                node_id: snapshot.node_id.clone(),
                snapshot_sequence_number: snapshot.snapshot_sequence_number,
                next_file_id: snapshot.next_file_id,
            });

            if snapshot.snapshot_sequence_number > marker.snapshot_sequence_number {
                self.node_id_to_last_marker
                    .insert(snapshot.node_id.clone(), Arc::clone(&new_marker));
            }

            self.writer_markers.push(new_marker);

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

    /// Returns the last snapshot marker for each writer that we're compacting data for
    fn last_markers(&self) -> Vec<Arc<NodeSnapshotMarker>> {
        self.node_id_to_last_marker.values().cloned().collect()
    }

    /// Clears the state of all snapshots and files to compact
    fn clear(&mut self) {
        self.writer_markers.clear();
        self.files_to_compact.clear();
    }
}

async fn load_next_snapshot(
    marker: &Arc<NodeSnapshotMarker>,
    node_id: &str,
    object_store: &Arc<dyn ObjectStore>,
    sys_events_store: &Arc<dyn CompactionEventStore>,
    snapshots: &mut Vec<PersistedSnapshot>,
) -> Result<(), CompactedDataProducerError> {
    let next_snapshot_sequence_number = marker.snapshot_sequence_number.next();
    let next_snapshot_path = SnapshotInfoFilePath::new(node_id, next_snapshot_sequence_number);
    let start = Instant::now();
    if let Some(data) =
        get_bytes_at_path(next_snapshot_path.as_ref(), Arc::clone(object_store)).await
    {
        let snapshot: PersistedSnapshot = serde_json::from_slice(&data)?;
        let time_taken = start.elapsed();
        info!(node_id, snapshot = %snapshot.snapshot_sequence_number, "loaded snapshot");
        let overall_counts = snapshot.db_table_and_file_count();
        let success_event = SuccessInfo::new(
            node_id,
            next_snapshot_sequence_number.as_u64(),
            time_taken,
            overall_counts,
        );
        sys_events_store.record_snapshot_success(success_event);
        snapshots.push(snapshot);
    };
    Ok(())
}

async fn load_all_snapshots(
    persister: Persister,
    node_id: &str,
    marker: &Arc<NodeSnapshotMarker>,
    sys_events_store: &Arc<dyn CompactionEventStore>,
) -> Result<Vec<PersistedSnapshot>> {
    let start = Instant::now();
    let writer_snapshots = persister.load_snapshots(1_000).await?;
    let time_taken = start.elapsed();
    info!(
        node_id,
        snapshot_count = writer_snapshots.len(),
        "loaded snapshots"
    );
    let overall_counts = PersistedSnapshot::overall_db_table_file_counts(&writer_snapshots);
    let success_event = SuccessInfo::new(
        node_id,
        marker.snapshot_sequence_number.as_u64(),
        time_taken,
        overall_counts,
    );
    sys_events_store.record_snapshot_success(success_event);
    Ok(writer_snapshots)
}

#[derive(Debug)]
pub struct CompactionCleaner {
    object_store: Arc<dyn ObjectStore>,
    to_delete: Vec<Path>,
    wait_time: Duration,
}

impl CompactionCleaner {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        to_delete: Vec<Path>,
        wait_time: Duration,
    ) -> Self {
        Self {
            object_store,
            to_delete,
            wait_time,
        }
    }
    pub async fn data_deletion(mut self) {
        tokio::time::sleep(self.wait_time).await;
        'outer: for location in self.to_delete.drain(0..) {
            let mut retry_count = 0;
            while let Err(e) = self.object_store.delete(&location).await {
                if let object_store::Error::NotFound { path, .. } = e {
                    debug!("{path} does not exist, continuing to next one in compaction cleanup");
                    continue 'outer;
                }
                crate::error!("{e}");
                retry_count += 1;
                if retry_count > 10 {
                    crate::error!(
                            "Unable to delete {location}. Will attempt deletion during next compaction."
                        );
                    break;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    pub fn spawn_new(
        object_store: Arc<dyn ObjectStore>,
        to_delete: Vec<Path>,
        wait_time: Duration,
    ) {
        tokio::spawn(Self::new(object_store, to_delete, wait_time).data_deletion());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_enterprise_data_layout::NodeSnapshotMarker;
    use influxdb3_id::{ColumnId, DbId, ParquetFileId, SerdeVecMap, TableId};
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use influxdb3_write::{persister::Persister, PersistedSnapshot};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use observability_deps::tracing::debug;
    use pretty_assertions::assert_eq;

    use crate::producer::{load_all_snapshots, load_next_snapshot};
    use crate::test_helpers::TestWriter;
    use crate::{
        consumer::CompactedDataConsumer,
        sys_events::{snapshot_fetched::SnapshotFetched, CompactionEvent},
    };

    use super::*;
    use influxdb3_enterprise_data_layout::persist::{get_compaction_detail, get_generation_detail};
    use influxdb3_enterprise_data_layout::{
        CompactionDetailPath, Generation, GenerationDetailPath,
    };
    use object_store::path::Path;
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_run_compaction() {
        let node_id = "test_host";
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer = TestWriter::new(node_id, Arc::clone(&object_store));

        let lp = r#"
            cpu,host=foo usage=1 1
            cpu,host=foo usage=2 2
            cpu,host=foo usage=3 3
        "#;
        let snapshot = writer.persist_lp_and_snapshot(lp, 0).await;
        assert_eq!(
            snapshot.snapshot_sequence_number,
            SnapshotSequenceNumber::new(1)
        );
        assert_eq!(writer.get_files("cpu").len(), 1);

        let snapshot = writer
            .persist_lp_and_snapshot("cpu,host=foo usage=4 60000000000", 0)
            .await;
        assert_eq!(
            snapshot.snapshot_sequence_number,
            SnapshotSequenceNumber::new(2)
        );
        assert_eq!(writer.get_files("cpu").len(), 2);

        let snapshot = writer
            .persist_lp_and_snapshot("cpu,host=foo usage=5 120000000000", 0)
            .await;
        assert_eq!(
            snapshot.snapshot_sequence_number,
            SnapshotSequenceNumber::new(3)
        );
        assert_eq!(writer.get_files("cpu").len(), 3);
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store = Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
            &time_provider,
        )));

        let compactor = CompactedDataProducer::new(CompactedDataProducerArgs {
            compactor_id: "compactor-1".into(),
            node_ids: vec![node_id.into()],
            compaction_config: CompactionConfig::default(),
            enterprise_config: Default::default(),
            datafusion_config: Default::default(),
            object_store: Arc::clone(&object_store),
            object_store_url: writer.persister.object_store_url().clone(),
            executor: Arc::clone(&writer.exec),
            parquet_cache_prefetcher: None,
            sys_events_store,
            time_provider,
        })
        .await
        .unwrap();

        let compactor_db_schema = compactor
            .compacted_data
            .compacted_catalog
            .db_schema("testdb")
            .unwrap();
        let table_definition = compactor_db_schema.table_definition("cpu").unwrap();
        let output_generation = Generation {
            id: GenerationId::new(),
            level: GenerationLevel::two(),
            start_time_secs: 0,
            max_time: 0,
        };
        let mut gen1_files = compactor
            .compaction_state
            .lock()
            .await
            .files_to_compact
            .get(&compactor_db_schema.id)
            .unwrap()
            .get(&table_definition.table_id)
            .unwrap()
            .clone();
        // sort the files by generation id so we can pick the first two to compact
        gen1_files.sort_by(|a, b| b.generation().id.cmp(&a.generation().id));
        assert_eq!(gen1_files.len(), 3);
        let input_generations = gen1_files[0..2].iter().map(|f| f.generation()).collect();
        let input_paths = gen1_files[0..2]
            .iter()
            .map(|f| Path::from(f.file.path.clone()))
            .collect();

        compactor
            .run_plan(
                NextCompactionPlan {
                    db_schema: Arc::clone(&compactor_db_schema),
                    table_definition: Arc::clone(&table_definition),
                    output_generation,
                    input_generations,
                    input_paths,
                    leftover_gen1_files: vec![gen1_files[2].clone()],
                },
                CompactionSequenceNumber::new(1),
                compactor
                    .compaction_state
                    .lock()
                    .await
                    .writer_markers
                    .clone(),
            )
            .await
            .unwrap();

        // ensure the generation detail was persisted
        let generation_detail_path =
            GenerationDetailPath::new("compactor-1".into(), output_generation.id);
        let generation_detail =
            get_generation_detail(&generation_detail_path, Arc::clone(&object_store))
                .await
                .unwrap();
        assert_eq!(generation_detail.id, output_generation.id);

        // ensure the compaction detail was persisted
        let compaction_detail = compactor
            .compacted_data
            .compaction_detail(compactor_db_schema.id, table_definition.table_id)
            .unwrap();
        assert_eq!(compaction_detail.compacted_generations.len(), 1);
        let compaction_detail_path = CompactionDetailPath::new(
            "compactor-1".into(),
            compactor_db_schema.id,
            table_definition.table_id,
            CompactionSequenceNumber::new(1),
        );
        let persisted_compaction_detail =
            get_compaction_detail(&compaction_detail_path, Arc::clone(&object_store))
                .await
                .unwrap();
        assert_eq!(compaction_detail.as_ref(), &persisted_compaction_detail);

        // ensure the compacted data structure was updated
        let parquet_files = compactor.compacted_data.parquet_files(
            compactor_db_schema.id,
            table_definition.table_id,
            output_generation.id,
        );
        assert_eq!(parquet_files.len(), 1);
        assert_eq!(parquet_files[0].row_count, 4);
    }

    #[tokio::test]
    async fn load_snapshots_persists_compacted_catalog() {
        let node_id = "test_host";
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer = TestWriter::new(node_id, Arc::clone(&object_store));

        let lp = r#"
            cpu,host=asdf usage=1 1
        "#;
        let _snapshot = writer.persist_lp_and_snapshot(lp, 0).await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let (mut state, compacted_data) = CompactionState::load_or_initialize(
            "compactor-1".into(),
            vec![node_id.into()],
            Arc::clone(&object_store),
            time_provider,
        )
        .await
        .unwrap();
        let db_schema = compacted_data
            .compacted_catalog
            .db_schema("testdb")
            .unwrap();
        let table_definition = db_schema.table_definition("cpu").unwrap();

        let loaded_compacted_catalog =
            CompactedCatalog::load("compactor-1".into(), Arc::clone(&object_store))
                .await
                .unwrap()
                .unwrap();
        let loaded_db_schema = loaded_compacted_catalog.db_schema("testdb").unwrap();
        let loaded_table_definition = loaded_db_schema.table_definition("cpu").unwrap();

        assert_eq!(
            compacted_data.compacted_catalog.sequence_number(),
            CatalogSequenceNumber::new(1)
        );
        assert_eq!(
            loaded_compacted_catalog.sequence_number(),
            CatalogSequenceNumber::new(1)
        );
        assert_eq!(db_schema, loaded_db_schema);
        assert_eq!(table_definition, loaded_table_definition);

        // now write a new snapshot, load snapshots, and reload the compacted catalog to ensure it's updated
        let snapshot = writer
            .persist_lp_and_snapshot("mem,host=foo usage=4 60000000000", 0)
            .await;
        assert_eq!(
            snapshot.snapshot_sequence_number,
            SnapshotSequenceNumber::new(2)
        );

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));
        state
            .load_snapshots(
                &compacted_data,
                Arc::clone(&object_store),
                Arc::clone(&sys_events_store),
            )
            .await
            .unwrap();

        // ensure the in-memory and a newly loaded compacted catalog have the new mem table
        let db_schema = compacted_data
            .compacted_catalog
            .db_schema("testdb")
            .unwrap();
        let table_definition = db_schema.table_definition("mem").unwrap();
        let loaded_compacted_catalog = CompactedCatalog::load_from_id(
            "compactor-1",
            CatalogSequenceNumber::new(2),
            Arc::clone(&object_store),
        )
        .await
        .unwrap()
        .unwrap();
        let loaded_db_schema = loaded_compacted_catalog.db_schema("testdb").unwrap();
        let loaded_table_definition = loaded_db_schema.table_definition("mem").unwrap();

        assert_eq!(db_schema, loaded_db_schema);
        assert_eq!(table_definition, loaded_table_definition);

        // and make sure the cpu table is there
        assert!(db_schema.table_definition("cpu").is_some());
        assert!(loaded_db_schema.table_definition("cpu").is_some());
    }

    #[tokio::test]
    async fn producer_updates_compacted_catalog_and_consumer_picks_up() {
        let node_id = "test_host";
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer = TestWriter::new(node_id, Arc::clone(&object_store));

        // create 3 snapshots so we have enough to compact
        let _snapshot = writer.persist_lp_and_snapshot("cpu,t=a usage=1 1", 0).await;
        let _snapshot = writer
            .persist_lp_and_snapshot("cpu,t=a usage=6 60000000000", 0)
            .await;
        let _snapshot = writer
            .persist_lp_and_snapshot("cpu,t=a usage=12 120000000000", 0)
            .await;

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));

        let compaction_config =
            CompactionConfig::new(&[2], Duration::from_secs(120)).with_per_file_row_limit(10);

        let compactor = CompactedDataProducer::new(CompactedDataProducerArgs {
            compactor_id: "compactor-1".into(),
            node_ids: vec![node_id.into()],
            compaction_config,
            enterprise_config: Default::default(),
            datafusion_config: Default::default(),
            object_store: Arc::clone(&object_store),
            object_store_url: writer.persister.object_store_url().clone(),
            executor: Arc::clone(&writer.exec),
            parquet_cache_prefetcher: None,
            sys_events_store: Arc::clone(&sys_events_store),
            time_provider,
        })
        .await
        .unwrap();

        // run compaction and verify
        compactor
            .plan_and_run_compaction(&[], Arc::clone(&sys_events_store))
            .await
            .unwrap();

        // create the consumer and verify it has the catalog and data
        let consumer = CompactedDataConsumer::new(
            Arc::from("compactor-1"),
            Arc::clone(&object_store),
            None,
            Arc::clone(&sys_events_store),
        )
        .await
        .unwrap();
        let consumer_db = consumer
            .compacted_data
            .compacted_catalog
            .db_schema("testdb")
            .unwrap();
        let consumer_table = consumer_db.table_definition("cpu").unwrap();
        let parquet_files = consumer.compacted_data.parquet_files(
            consumer_db.id,
            consumer_table.table_id,
            GenerationId::from(3),
        );
        assert_eq!(parquet_files.len(), 1);

        // write 2 new snapshots with write to a new table to have a catalog update
        let _snapshot = writer
            .persist_lp_and_snapshot("mem,t=a usage=24 240000000000", 0)
            .await;
        let _snapshot = writer
            .persist_lp_and_snapshot("mem,t=a usage=30 300000000000", 0)
            .await;
        let _snapshot = writer
            .persist_lp_and_snapshot("mem,t=a usage=36 360000000000", 0)
            .await;

        // run the compaction plan 3 times to pick up the 3 snapshots. On the last one it will run
        // an actual compaction.
        compactor
            .plan_and_run_compaction(&[], Arc::clone(&sys_events_store))
            .await
            .unwrap();
        compactor
            .plan_and_run_compaction(&[], Arc::clone(&sys_events_store))
            .await
            .unwrap();
        compactor
            .plan_and_run_compaction(&[], Arc::clone(&sys_events_store))
            .await
            .unwrap();

        // refresh the consumer and verify that it has the updated catalog and the new compacted data
        consumer.refresh().await.unwrap();
        let consumer_db = consumer
            .compacted_data
            .compacted_catalog
            .db_schema("testdb")
            .unwrap();
        let consumer_table = consumer_db.table_definition("cpu").unwrap();
        let parquet_files = consumer.compacted_data.parquet_files(
            consumer_db.id,
            consumer_table.table_id,
            GenerationId::from(3),
        );
        assert_eq!(parquet_files.len(), 1);

        let consumer_table = consumer_db.table_definition("mem").unwrap();
        let parquet_files = consumer.compacted_data.parquet_files(
            consumer_db.id,
            consumer_table.table_id,
            GenerationId::from(7),
        );
        assert_eq!(parquet_files.len(), 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_load_all_snapshots() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));
        let node_id = "host_id";
        let obj_store = Arc::new(InMemory::new());
        let persister = Persister::new(obj_store, node_id, time_provider);

        let persisted_snapshot = PersistedSnapshot {
            node_id: node_id.to_string(),
            next_file_id: ParquetFileId::from(0),
            next_db_id: DbId::from(1),
            next_table_id: TableId::from(1),
            next_column_id: ColumnId::from(1),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        };
        persister
            .persist_snapshot(&persisted_snapshot)
            .await
            .expect("snaphost to be persisted");

        let marker = Arc::new(NodeSnapshotMarker {
            node_id: node_id.to_string(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(123),
            next_file_id: ParquetFileId::new(),
        });
        let res = load_all_snapshots(persister, node_id, &marker, &sys_events_store).await;
        debug!(result = ?res, "load all snapshots for compactor");
        let success_events = sys_events_store.compaction_events_as_vec();
        debug!(events = ?success_events, "events stored after bulk loading all snapshots");
        let first_success_event = success_events.first().unwrap();
        match &first_success_event.data {
            CompactionEvent::SnapshotFetched(snapshot_info) => match snapshot_info {
                SnapshotFetched::Success(success_info) => {
                    assert_eq!(Arc::from(node_id), success_info.node_id);
                    assert_eq!(123, success_info.sequence_number);
                    assert_eq!(0, success_info.db_count);
                    assert_eq!(0, success_info.table_count);
                    assert_eq!(0, success_info.file_count);
                }
                SnapshotFetched::Failed(_) => panic!("should not fail"),
            },
            _ => panic!("no other event type expected"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_load_next_snapshot() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));
        let node_id = "host_id";
        let obj_store = Arc::new(InMemory::new()) as _;
        let persister = Persister::new(Arc::clone(&obj_store), node_id, time_provider);

        let persisted_snapshot = PersistedSnapshot {
            node_id: node_id.to_string(),
            next_file_id: ParquetFileId::from(0),
            next_db_id: DbId::from(1),
            next_table_id: TableId::from(1),
            next_column_id: ColumnId::from(1),
            snapshot_sequence_number: SnapshotSequenceNumber::new(124),
            wal_file_sequence_number: WalFileSequenceNumber::new(100),
            catalog_sequence_number: CatalogSequenceNumber::new(100),
            databases: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
        };
        persister
            .persist_snapshot(&persisted_snapshot)
            .await
            .expect("snaphost to be persisted");

        let marker = Arc::new(NodeSnapshotMarker {
            node_id: node_id.to_string(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(123),
            next_file_id: ParquetFileId::new(),
        });
        let mut snapshots_loaded = Vec::new();
        let res = load_next_snapshot(
            &marker,
            node_id,
            &obj_store,
            &sys_events_store,
            &mut snapshots_loaded,
        )
        .await;

        debug!(result = ?res, "load all snapshots for compactor");
        let success_events = sys_events_store.compaction_events_as_vec();
        debug!(events = ?success_events, "events stored after bulk loading all snapshots");
        let first_success_event = success_events.first().unwrap();
        match &first_success_event.data {
            CompactionEvent::SnapshotFetched(snapshot_info) => match snapshot_info {
                SnapshotFetched::Success(success_info) => {
                    assert_eq!(Arc::from(node_id), success_info.node_id);
                    assert_eq!(124, success_info.sequence_number);
                    assert_eq!(0, success_info.db_count);
                    assert_eq!(0, success_info.table_count);
                    assert_eq!(0, success_info.file_count);
                }
                SnapshotFetched::Failed(_) => panic!("should not fail"),
            },
            _ => panic!("no other event type expected"),
        }
    }
}
