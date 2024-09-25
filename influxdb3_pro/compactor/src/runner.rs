//! Logic for running many compaction tasks in parallel.

use crate::planner::{CompactionPlan, SnapshotAdvancePlan};
use crate::{compact_files, CompactFilesArgs};
use datafusion::execution::object_store::ObjectStoreUrl;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_pro_data_layout::persist::{
    get_compaction_detail, persist_compaction_detail, persist_compaction_summary,
};
use influxdb3_pro_data_layout::{
    CompactedData, CompactionDetail, CompactionDetailPath, CompactionDetailRef,
    CompactionSequenceNumber, CompactionSummary, Generation, GenerationId, HostSnapshotMarker,
    YoungGeneration,
};
use influxdb3_write::ParquetFileId;
use iox_query::exec::Executor;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactRunnerError {
    #[error("error loading or persisting compacted data: {0}")]
    CompactedDataPersistenceError(
        #[from] influxdb3_pro_data_layout::persist::CompactedDataPersistenceError,
    ),
}

pub(crate) type Result<T, E = CompactRunnerError> = std::result::Result<T, E>;

/// Run all gen1 to gen2 compactions in the snapshot plan, writing the `CompactionDetail`s as
/// we go and then writing the `CompactionSummary` to object store and returning it at the end.
pub(crate) async fn run_snapshot_plan(
    snapshot_advance_plan: SnapshotAdvancePlan,
    compactor_id: Arc<str>,
    compacted_data: Arc<Mutex<CompactedData>>,
    catalog: Arc<Catalog>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<CompactionSummary> {
    debug!("Running snapshot advance plan: {:?}", snapshot_advance_plan);
    let last_compaction_summary = compacted_data.lock().await.last_compaction_summary.clone();
    let compaction_sequence_number = CompactionSequenceNumber::next();

    let mut compaction_details = Vec::with_capacity(snapshot_advance_plan.compaction_plans.len());
    let mut snapshot_markers = snapshot_advance_plan
        .host_snapshot_markers
        .values()
        .filter_map(|hc| hc.marker.clone())
        .collect::<Vec<_>>();

    let mut last_compaction_detail_map: HashMap<Arc<str>, HashMap<Arc<str>, CompactionDetailPath>> =
        HashMap::new();

    // pull over all details from the last compaction summary that won't be covered by this run.
    if let Some(last_compaction_summary) = last_compaction_summary {
        // it's possible for some tables to not have a compaction ready or any new gen1 files.
        // In that case, we need to copy over their compaction details from the previous summary.
        for last_detail in last_compaction_summary.compaction_details {
            // put it into the map, we'll look it up later
            let db_table_map = last_compaction_detail_map
                .entry(Arc::clone(&last_detail.db_name))
                .or_default();
            db_table_map.insert(
                Arc::clone(&last_detail.table_name),
                last_detail.path.clone(),
            );

            let has_compaction_plan = snapshot_advance_plan
                .compaction_plans
                .get(&last_detail.db_name)
                .map(|table_plans| {
                    table_plans
                        .iter()
                        .any(|plan| plan.table_name().eq(last_detail.table_name.as_ref()))
                })
                .unwrap_or(false);

            if !has_compaction_plan {
                compaction_details.push(last_detail);
            }
        }

        // if there's a host snapshot marker for a host that didn't have a snapshot this time around, we need to copy it over.
        for last_marker in last_compaction_summary.snapshot_markers {
            if !snapshot_markers
                .iter()
                .any(|marker| marker.host_id.eq(&last_marker.host_id))
            {
                snapshot_markers.push(last_marker);
            }
        }
    }

    debug!(
        "Last compaction details copied over: {:?}",
        last_compaction_detail_map
    );

    for (db_name, table_plans) in snapshot_advance_plan.compaction_plans {
        let db_schema = match catalog.db_schema(db_name.as_ref()) {
            Some(db_schema) => db_schema,
            None => {
                // this is a bug, but we can't panic here because it would cause the compactor to stop.
                // we'll just skip this table and log an error.
                error!("Database schema not found for db_name: {}", db_name);
                continue;
            }
        };

        for plan in table_plans {
            let table_name = plan.table_name();

            let table_definition = match db_schema.get_table(table_name) {
                Some(table_schema) => table_schema,
                None => {
                    // this is a bug, but we can't panic here because it would cause the compactor to stop.
                    // we'll just skip this table and log an error.
                    error!(
                        "Table definition not found for table_name: {} in db: {}",
                        table_name, db_schema.name
                    );
                    continue;
                }
            };

            let last_compaction_detail = if let Some(path) = last_compaction_detail_map
                .get(db_name.as_ref())
                .and_then(|m| m.get(table_name))
            {
                get_compaction_detail(path.clone(), Arc::clone(&object_store)).await
            } else {
                None
            };

            let detail = run_plan_and_write_detail(
                plan,
                last_compaction_detail,
                snapshot_markers.clone(),
                &snapshot_advance_plan.generations,
                table_definition,
                Arc::clone(&compactor_id),
                compaction_sequence_number,
                Arc::clone(&object_store),
                object_store_url.clone(),
                Arc::clone(&exec),
            )
            .await?;

            compaction_details.push(detail);
        }
    }

    let compaction_summary = CompactionSummary {
        compaction_sequence_number,
        last_file_id: ParquetFileId::current(),
        snapshot_markers,
        compaction_details,
    };

    persist_compaction_summary(
        compactor_id.as_ref(),
        &compaction_summary,
        Arc::clone(&object_store),
    )
    .await
    .expect("failed to write compaction summary");

    Ok(compaction_summary)
}

#[allow(clippy::too_many_arguments)]
async fn run_plan_and_write_detail(
    plan: CompactionPlan,
    last_compaction_detail: Option<CompactionDetail>,
    snapshot_markers: Vec<HostSnapshotMarker>,
    gen_map: &HashMap<GenerationId, Arc<dyn Generation>>,
    table_definition: &TableDefinition,
    compactor_id: Arc<str>,
    compaction_sequence_number: CompactionSequenceNumber,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<CompactionDetailRef> {
    debug!("Running compaction plan: {:?}", plan);

    // separate out the young and old generations. These will be cleaned up from the compaction and
    // carried into the new detail.
    let (mut young_generations, mut old_generations) = match last_compaction_detail {
        Some(last_detail) => (last_detail.young_generations, last_detail.old_generations),
        None => (vec![], vec![]),
    };

    let detail = match plan {
        CompactionPlan::Compaction(plan) => {
            let index_columns = table_definition
                .index_columns()
                .iter()
                .map(|c| c.to_string())
                .collect();

            // remove any of the input ids from young, old, and leftover
            young_generations.retain(|g| !plan.input_ids.contains(&g.id));
            old_generations.retain(|g| !plan.input_ids.contains(&g.id));

            // get the paths of all the files getting compacted
            let mut paths = vec![];
            for gen_id in plan.input_ids {
                let gen_paths: Vec<_> = gen_map
                    .get(&gen_id)
                    .unwrap()
                    .files()
                    .await
                    .iter()
                    .map(|f| ObjPath::from(f.path.clone()))
                    .collect();
                paths.extend(gen_paths);
            }

            let args = CompactFilesArgs {
                compactor_id: Arc::clone(&compactor_id),
                compaction_sequence_number,
                db_name: Arc::clone(&plan.db_name),
                table_name: Arc::clone(&plan.table_name),
                table_schema: table_definition.schema.clone(),
                paths,
                limit: 0,
                generation: plan.output_level,
                index_columns,
                object_store: Arc::clone(&object_store),
                object_store_url,
                exec,
            };

            let compactor_output = compact_files(args).await.expect("compaction failed");

            // TODO: if this generation is older, write the file details and add an old generation
            young_generations.push(Arc::new(YoungGeneration {
                id: plan.output_id,
                compaction_sequence_number,
                level: plan.output_level,
                start_time: plan.output_gen_time,
                files: compactor_output
                    .file_metadata
                    .into_iter()
                    .map(Arc::new)
                    .collect::<Vec<_>>(),
            }));

            // TODO: write the file index

            let mut leftover_gen1_files = Vec::with_capacity(plan.leftover_ids.len());

            for id in plan.leftover_ids {
                let gen = gen_map.get(&id).expect("generation should be in map");
                let files = gen.files().await;
                leftover_gen1_files.extend(files);
            }

            let compaction_detail = CompactionDetail {
                sequence_number: compaction_sequence_number,
                snapshot_markers,
                young_generations,
                old_generations,
                leftover_gen1_files,
            };

            persist_compaction_detail(
                compactor_id.as_ref(),
                plan.db_name,
                plan.table_name,
                &compaction_detail,
                object_store,
            )
            .await
        }
        CompactionPlan::LeftoverOnly(leftover_plan) => {
            let mut leftover_gen1_files = Vec::with_capacity(leftover_plan.leftover_ids.len());

            for id in leftover_plan.leftover_ids {
                let gen = gen_map.get(&id).expect("generation should be in map");
                let files = gen.files().await;
                leftover_gen1_files.extend(files);
            }

            let compaction_detail = CompactionDetail {
                sequence_number: compaction_sequence_number,
                snapshot_markers,
                young_generations,
                old_generations,
                leftover_gen1_files,
            };

            persist_compaction_detail(
                compactor_id.as_ref(),
                leftover_plan.db_name,
                leftover_plan.table_name,
                &compaction_detail,
                object_store,
            )
            .await
        }
    };

    Ok(detail?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{HostSnapshotCounter, NextCompactionPlan};
    use arrow_util::assert_batches_eq;
    use executor::register_current_runtime_for_io;
    use influxdb3_pro_data_layout::{Gen1, GenerationLevel};
    use influxdb3_wal::{
        CatalogBatch, CatalogOp, DatabaseDefinition, Field, FieldData, FieldDataType,
        FieldDefinition, Row, SnapshotDetails, SnapshotSequenceNumber, TableChunk, TableChunks,
        TableDefinition, WalContents, WalFileNotifier, WalFileSequenceNumber, WalOp, WriteBatch,
    };
    use influxdb3_write::last_cache::LastCacheProvider;
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
    use influxdb3_write::write_buffer::queryable_buffer::QueryableBuffer;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[tokio::test]
    async fn test_run_snapshot_plan() {
        let obj_store = Arc::new(InMemory::new());
        let persister = Arc::new(Persister::new(
            Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
            "test-host",
        ));
        let exec = Arc::new(Executor::new_testing());
        let catalog = Arc::new(Catalog::new());
        let persisted_files = Arc::new(PersistedFiles::default());

        register_current_runtime_for_io();

        let buffer = QueryableBuffer::new(
            Arc::clone(&exec),
            Arc::clone(&catalog),
            Arc::clone(&persister),
            Arc::new(LastCacheProvider::new()),
            Arc::clone(&persisted_files),
        );
        let write1 = WalContents {
            min_timestamp_ns: 1,
            max_timestamp_ns: 1,
            wal_file_number: WalFileSequenceNumber::new(1),
            ops: vec![
                WalOp::Catalog(CatalogBatch {
                    database_name: "test_db".into(),
                    time_ns: 0,
                    ops: vec![
                        CatalogOp::CreateDatabase(DatabaseDefinition {
                            database_name: "test_db".into(),
                        }),
                        CatalogOp::CreateTable(TableDefinition {
                            database_name: "test_db".into(),
                            table_name: "test_table".into(),
                            field_definitions: vec![
                                FieldDefinition {
                                    name: "tag1".into(),
                                    data_type: FieldDataType::Tag,
                                },
                                FieldDefinition {
                                    name: "field1".into(),
                                    data_type: FieldDataType::Integer,
                                },
                                FieldDefinition {
                                    name: "time".into(),
                                    data_type: FieldDataType::Timestamp,
                                },
                            ],
                            key: Some(vec!["tag1".into()]),
                        }),
                    ],
                }),
                WalOp::Write(WriteBatch {
                    database_name: "test_db".into(),
                    table_chunks: vec![(
                        "test_table".into(),
                        TableChunks {
                            min_time: 1,
                            max_time: 1,
                            chunk_time_to_chunk: vec![(
                                1,
                                TableChunk {
                                    rows: vec![Row {
                                        time: 1,
                                        fields: vec![
                                            Field {
                                                name: "tag1".into(),
                                                value: FieldData::Tag("val".into()),
                                            },
                                            Field {
                                                name: "field1".into(),
                                                value: FieldData::Integer(1),
                                            },
                                            Field {
                                                name: "time".into(),
                                                value: FieldData::Timestamp(1),
                                            },
                                        ],
                                    }],
                                },
                            )]
                            .into_iter()
                            .collect(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    min_time_ns: 1,
                    max_time_ns: 1,
                }),
            ],
            snapshot: None,
        };
        buffer.notify(write1);
        let write2 = WalContents {
            min_timestamp_ns: 2,
            max_timestamp_ns: 2,
            wal_file_number: WalFileSequenceNumber::new(2),
            ops: vec![WalOp::Write(WriteBatch {
                database_name: "test_db".into(),
                table_chunks: vec![(
                    "test_table".into(),
                    TableChunks {
                        min_time: 2,
                        max_time: 2,
                        chunk_time_to_chunk: vec![(
                            2,
                            TableChunk {
                                rows: vec![Row {
                                    time: 2,
                                    fields: vec![
                                        Field {
                                            name: "tag1".into(),
                                            value: FieldData::Tag("val".into()),
                                        },
                                        Field {
                                            name: "field1".into(),
                                            value: FieldData::Integer(1),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(2),
                                        },
                                    ],
                                }],
                            },
                        )]
                        .into_iter()
                        .collect(),
                    },
                )]
                .into_iter()
                .collect(),
                min_time_ns: 2,
                max_time_ns: 2,
            })],
            snapshot: Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                end_time_marker: 2,
                last_wal_sequence_number: WalFileSequenceNumber::new(1),
            }),
        };
        let snapshot_details = write2.snapshot.unwrap();
        let _ = buffer
            .notify_and_snapshot(write2, snapshot_details)
            .await
            .await;
        let write3 = WalContents {
            min_timestamp_ns: 3,
            max_timestamp_ns: 3,
            wal_file_number: WalFileSequenceNumber::new(3),
            ops: vec![WalOp::Write(WriteBatch {
                database_name: "test_db".into(),
                table_chunks: vec![(
                    "test_table".into(),
                    TableChunks {
                        min_time: 3,
                        max_time: 3,
                        chunk_time_to_chunk: vec![(
                            3,
                            TableChunk {
                                rows: vec![Row {
                                    time: 3,
                                    fields: vec![
                                        Field {
                                            name: "tag1".into(),
                                            value: FieldData::Tag("val".into()),
                                        },
                                        Field {
                                            name: "field1".into(),
                                            value: FieldData::Integer(1),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(3),
                                        },
                                    ],
                                }],
                            },
                        )]
                        .into_iter()
                        .collect(),
                    },
                )]
                .into_iter()
                .collect(),
                min_time_ns: 3,
                max_time_ns: 3,
            })],
            snapshot: Some(SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                end_time_marker: 3,
                last_wal_sequence_number: WalFileSequenceNumber::new(2),
            }),
        };
        let snapshot_details = write3.snapshot.unwrap();
        let _ = buffer
            .notify_and_snapshot(write3, snapshot_details)
            .await
            .await;

        let parquet_files: Vec<_> = persisted_files
            .get_files("test_db", "test_table")
            .iter()
            .map(|f| Arc::new(f.clone()))
            .collect();
        let generations: HashMap<GenerationId, Arc<dyn Generation>> = parquet_files
            .iter()
            .map(|f| {
                let id = GenerationId::new();
                let gen: Arc<dyn Generation> = Arc::new(Gen1 {
                    id,
                    file: Arc::clone(f),
                });

                (id, gen)
            })
            .collect();

        let output_id = GenerationId::new();
        let output_level = GenerationLevel::two();

        let compaction_plan = CompactionPlan::Compaction(NextCompactionPlan {
            db_name: "test_db".into(),
            table_name: "test_table".into(),
            output_level,
            output_id,
            output_gen_time: 0,
            input_ids: generations.keys().cloned().collect(),
            leftover_ids: vec![],
        });
        let snapshot_advance_plan = SnapshotAdvancePlan {
            host_snapshot_markers: vec![(
                "test-host".to_string(),
                HostSnapshotCounter {
                    marker: Some(HostSnapshotMarker {
                        host_id: "test-host".to_string(),
                        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                    }),
                    snapshot_count: 2,
                },
            )]
            .into_iter()
            .collect(),
            compaction_plans: vec![("test_db".into(), vec![compaction_plan])]
                .into_iter()
                .collect(),
            generations,
        };

        let compactor_id: Arc<str> = "test-compactor".into();
        let compacted_data = Arc::new(Mutex::new(CompactedData::new(Arc::clone(&compactor_id))));
        let output = run_snapshot_plan(
            snapshot_advance_plan,
            compactor_id,
            compacted_data,
            catalog,
            Arc::clone(&obj_store) as _,
            persister.object_store_url().clone(),
            exec,
        )
        .await
        .unwrap();

        let detail = output.compaction_details.first().unwrap();

        // ensure a compaction detail was written with the expected stuff
        let compaction_detail =
            get_compaction_detail(detail.path.clone(), Arc::clone(&obj_store) as _)
                .await
                .unwrap();
        assert_eq!(
            compaction_detail.sequence_number,
            output.compaction_sequence_number
        );
        assert_eq!(compaction_detail.snapshot_markers, output.snapshot_markers);
        let young_gen = compaction_detail.young_generations.first().unwrap();
        assert_eq!(young_gen.id, output_id);
        assert_eq!(young_gen.level, output_level);

        // read the parquet file and ensure that it has our three rows
        let file_path = young_gen.files.first().unwrap().path.clone();
        let file_path = ObjPath::from(file_path);

        let parquet_file = obj_store
            .get(&file_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.into_iter().map(|r| r.unwrap()).collect();

        assert_batches_eq!(
            [
                "+--------+------+--------------------------------+",
                "| field1 | tag1 | time                           |",
                "+--------+------+--------------------------------+",
                "| 1      | val  | 1970-01-01T00:00:00.000000001Z |",
                "| 1      | val  | 1970-01-01T00:00:00.000000002Z |",
                "+--------+------+--------------------------------+",
            ],
            &batches
        );

        // ensure a compaction summary was written with the expected stuff
        let summary = obj_store
            .get(&ObjPath::from("test-compactor/cs/0.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let summary = serde_json::from_slice::<CompactionSummary>(&summary).unwrap();
        assert_eq!(output, summary);
    }
}
