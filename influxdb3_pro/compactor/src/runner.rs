//! Logic for running many compaction tasks in parallel.

use crate::planner::{CompactionPlan, SnapshotAdvancePlan};
use crate::{compact_files, CompactFilesArgs};
use datafusion::execution::object_store::ObjectStoreUrl;
use hashbrown::HashSet;
use influxdb3_catalog::catalog::{Catalog, TableDefinition};
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::persist::{
    persist_compaction_detail, persist_compaction_summary, persist_generation_detail,
};
use influxdb3_pro_data_layout::{
    CompactionDetail, CompactionDetailPath, CompactionSequenceNumber, CompactionSummary, Gen1File,
    GenerationDetail, GenerationId, HostSnapshotMarker,
};
use influxdb3_write::ParquetFileId;
use iox_query::exec::Executor;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, error};
use std::sync::Arc;

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
    compacted_data: Arc<CompactedData>,
    catalog: Arc<Catalog>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<CompactionSummary> {
    debug!("Running snapshot advance plan: {:?}", snapshot_advance_plan);
    let compaction_sequence_number = compacted_data.next_compaction_sequence_number();

    let mut new_compaction_detail_paths =
        Vec::with_capacity(snapshot_advance_plan.compaction_plans.len());
    let mut new_snapshot_markers = snapshot_advance_plan
        .host_snapshot_markers
        .values()
        .filter_map(|hc| hc.marker.clone())
        .collect::<Vec<_>>();

    // if there is an existing compaction summary, carry forward any compaction details that
    // were not compacted in this cycle. Also carry forward any host snapshot markers that
    // didn't have a snapshot in this cycle.
    if let Some(last_summary) = compacted_data.get_last_summary() {
        let mut was_compacted: HashSet<(&str, &str)> = HashSet::new();
        for plans in snapshot_advance_plan.compaction_plans.values() {
            for plan in plans {
                was_compacted.insert((plan.db_name(), plan.table_name()));
            }
        }

        for detail_path in last_summary.compaction_details.into_iter() {
            let db_name = detail_path.db_name();
            let table_name = detail_path.table_name();

            if !was_compacted.contains(&(db_name.as_ref(), table_name.as_ref())) {
                new_compaction_detail_paths.push(detail_path);
            }
        }

        for marker in last_summary.snapshot_markers.into_iter() {
            if !new_snapshot_markers
                .iter()
                .any(|m| m.host_id == marker.host_id)
            {
                new_snapshot_markers.push(marker);
            }
        }
    }

    for (db_name, table_plans) in snapshot_advance_plan.compaction_plans {
        let Some(db_schema) = catalog.db_schema(db_name.as_ref()) else {
            // this is a bug, but we can't panic here because it would cause the compactor to stop.
            // we'll just skip this table and log an error.
            error!(
                "Database schema not found for db_name: {} while running compaction cycle",
                db_name
            );
            continue;
        };

        for plan in table_plans {
            let table_name = plan.table_name();

            let Some(table_definition) = db_schema.get_table(table_name) else {
                // this is a bug, but we can't panic here because it would cause the compactor to stop.
                // we'll just skip this table and log an error.
                error!(
                    "Table definition not found for table_name: {} in db: {} while running compaction cycle",
                    table_name, db_schema.name
                );
                continue;
            };

            let compaction_detail_path = run_plan_and_write_detail(
                plan,
                Arc::clone(&compacted_data),
                new_snapshot_markers.clone(),
                table_definition,
                compaction_sequence_number,
                object_store_url.clone(),
                Arc::clone(&exec),
            )
            .await?;

            new_compaction_detail_paths.push(compaction_detail_path);
        }
    }

    let compaction_summary = CompactionSummary {
        compaction_sequence_number,
        last_file_id: ParquetFileId::current(),
        last_generation_id: GenerationId::current(),
        snapshot_markers: new_snapshot_markers,
        compaction_details: new_compaction_detail_paths,
    };

    persist_compaction_summary(
        compacted_data.compactor_id.as_ref(),
        &compaction_summary,
        Arc::clone(&compacted_data.object_store),
    )
    .await?;

    compacted_data.set_last_summary(compaction_summary.clone());

    Ok(compaction_summary)
}

#[allow(clippy::too_many_arguments)]
async fn run_plan_and_write_detail(
    plan: CompactionPlan,
    compacted_data: Arc<CompactedData>,
    snapshot_markers: Vec<HostSnapshotMarker>,
    table_definition: &TableDefinition,
    compaction_sequence_number: CompactionSequenceNumber,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<CompactionDetailPath> {
    debug!("Running compaction plan: {:?}", plan);

    let path = match plan {
        CompactionPlan::Compaction(plan) => {
            let index_columns = table_definition
                .index_columns()
                .iter()
                .map(|c| c.to_string())
                .collect();

            // get the paths of all the files getting compacted and the max time of the generation
            let mut paths = vec![];
            let mut max_time = 0;
            for gen_id in &plan.input_ids {
                let gen_paths: Vec<_> = compacted_data
                    .files_for_generation(*gen_id)
                    .iter()
                    .map(|f| {
                        max_time = max_time.max(f.max_time);
                        ObjPath::from(f.path.as_ref())
                    })
                    .collect();
                paths.extend(gen_paths);
            }

            // run the compaction
            let args = CompactFilesArgs {
                compactor_id: Arc::clone(&compacted_data.compactor_id),
                table_name: Arc::clone(&plan.table_name),
                table_schema: table_definition.schema.clone(),
                paths,
                limit: compacted_data.compaction_config.per_file_row_limit,
                generation: plan.output_generation,
                index_columns,
                object_store: Arc::clone(&compacted_data.object_store),
                object_store_url,
                exec,
            };

            let compactor_output = compact_files(args).await.expect("compaction failed");

            // write the generation detail to object store
            let generaton_detail = GenerationDetail {
                id: plan.output_generation.id,
                level: plan.output_generation.level,
                start_time_s: plan.output_generation.start_time_secs,
                max_time_ns: max_time,
                files: compactor_output
                    .file_metadata
                    .into_iter()
                    .map(Arc::new)
                    .collect(),
                file_index: compactor_output.file_index,
            };

            persist_generation_detail(
                compacted_data.compactor_id.as_ref(),
                plan.output_generation.id,
                &generaton_detail,
                Arc::clone(&compacted_data.object_store),
            )
            .await?;

            let leftover_gen1_files: Vec<_> = plan
                .leftover_ids
                .iter()
                .flat_map(|id| {
                    compacted_data
                        .files_for_generation(*id)
                        .into_iter()
                        .map(|file| Gen1File { id: *id, file })
                })
                .collect();

            let compaction_detail =
                match compacted_data.get_last_compaction_detail(&plan.db_name, &plan.table_name) {
                    Some(detail) => detail.new_from_compaction(
                        compaction_sequence_number,
                        &plan.input_ids,
                        plan.output_generation,
                        snapshot_markers,
                        leftover_gen1_files,
                    ),
                    None => CompactionDetail {
                        db_name: Arc::clone(&plan.db_name),
                        table_name: Arc::clone(&plan.table_name),
                        sequence_number: compaction_sequence_number,
                        snapshot_markers,
                        compacted_generations: vec![plan.output_generation],
                        leftover_gen1_files,
                    },
                };

            let path = persist_compaction_detail(
                compacted_data.compactor_id.as_ref(),
                plan.db_name,
                plan.table_name,
                &compaction_detail,
                Arc::clone(&compacted_data.object_store),
            )
            .await?;

            compacted_data.update_compaction_detail_with_generation(
                &plan.input_ids,
                compaction_detail,
                generaton_detail,
            );

            path
        }
        CompactionPlan::LeftoverOnly(plan) => {
            let leftover_gen1_files: Vec<_> = plan
                .leftover_gen1_ids
                .iter()
                .flat_map(|id| {
                    compacted_data
                        .files_for_generation(*id)
                        .into_iter()
                        .map(|file| Gen1File { id: *id, file })
                })
                .collect();

            let compaction_detail =
                match compacted_data.get_last_compaction_detail(&plan.db_name, &plan.table_name) {
                    Some(detail) => detail.new_from_leftovers(
                        compaction_sequence_number,
                        snapshot_markers,
                        leftover_gen1_files,
                    ),
                    None => CompactionDetail {
                        db_name: Arc::clone(&plan.db_name),
                        table_name: Arc::clone(&plan.table_name),
                        sequence_number: compaction_sequence_number,
                        snapshot_markers,
                        compacted_generations: vec![],
                        leftover_gen1_files,
                    },
                };

            let path = persist_compaction_detail(
                compacted_data.compactor_id.as_ref(),
                plan.db_name,
                plan.table_name,
                &compaction_detail,
                Arc::clone(&compacted_data.object_store),
            )
            .await?;

            compacted_data.update_compaction_detail(compaction_detail);

            path
        }
    };

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{HostSnapshotCounter, NextCompactionPlan};
    use arrow_util::assert_batches_eq;
    use executor::register_current_runtime_for_io;
    use influxdb3_id::DbId;
    use influxdb3_pro_data_layout::persist::{get_compaction_detail, get_generation_detail};
    use influxdb3_pro_data_layout::{
        CompactionConfig, Generation, GenerationDetailPath, GenerationLevel,
    };
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
    use object_store::ObjectStore;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[tokio::test]
    async fn test_run_snapshot_plan() {
        let obj_store = Arc::new(InMemory::new());
        let persister = Arc::new(Persister::new(
            Arc::clone(&obj_store) as Arc<dyn ObjectStore>,
            "test-host",
        ));
        let exec = Arc::new(Executor::new_testing());
        let catalog = Arc::new(Catalog::new("test-host".into(), "test-id".into()));
        let persisted_files = Arc::new(PersistedFiles::default());

        register_current_runtime_for_io();

        let buffer = QueryableBuffer::new(
            Arc::clone(&exec),
            Arc::clone(&catalog),
            Arc::clone(&persister),
            Arc::new(LastCacheProvider::new()),
            Arc::clone(&persisted_files),
            None,
        );
        let database_id = DbId::new();
        let write1 = WalContents {
            min_timestamp_ns: 1,
            max_timestamp_ns: 1,
            wal_file_number: WalFileSequenceNumber::new(1),
            ops: vec![
                WalOp::Catalog(CatalogBatch {
                    database_id,
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
                    database_id,
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
                database_id,
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
                database_id,
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

        let compactor_id: Arc<str> = "test-compactor".into();
        let compacted_data = Arc::new(CompactedData::new(
            Arc::clone(&compactor_id),
            CompactionConfig::default(),
            Arc::clone(&obj_store) as _,
        ));

        // create gen1 genrations for the files and add them to the compacted data map
        let input_ids = parquet_files
            .iter()
            .map(|f| {
                let gen1_file = compacted_data.add_gen1_file_to_map(Arc::clone(f));

                gen1_file.id
            })
            .collect();

        let output_id = GenerationId::new();
        let output_level = GenerationLevel::two();

        let compaction_plan = CompactionPlan::Compaction(NextCompactionPlan {
            db_name: "test_db".into(),
            table_name: "test_table".into(),
            output_generation: Generation {
                id: output_id,
                level: output_level,
                start_time_secs: 0,
                max_time: 3,
            },
            input_ids,
            leftover_ids: vec![],
        });
        let snapshot_advance_plan = SnapshotAdvancePlan {
            host_snapshot_markers: vec![(
                "test-host".to_string(),
                HostSnapshotCounter {
                    marker: Some(HostSnapshotMarker {
                        host_id: "test-host".to_string(),
                        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                        next_file_id: ParquetFileId::current(),
                    }),
                    snapshot_count: 2,
                },
            )]
            .into_iter()
            .collect(),
            compaction_plans: vec![("test_db".into(), vec![compaction_plan])]
                .into_iter()
                .collect(),
        };

        let output = run_snapshot_plan(
            snapshot_advance_plan,
            Arc::clone(&compacted_data),
            catalog,
            persister.object_store_url().clone(),
            exec,
        )
        .await
        .unwrap();

        let detail = output.compaction_details.first().unwrap();

        // ensure a compaction detail was written with the expected stuff
        let compaction_detail =
            get_compaction_detail(detail, Arc::clone(&compacted_data.object_store) as _)
                .await
                .unwrap();
        assert_eq!(
            compaction_detail.sequence_number,
            output.compaction_sequence_number
        );
        assert_eq!(compaction_detail.snapshot_markers, output.snapshot_markers);
        let new_gen = compaction_detail.compacted_generations.first().unwrap();
        assert_eq!(new_gen.id, output_id);
        assert_eq!(new_gen.level, output_level);

        // make sure it was added to the map
        let compacted_table = compacted_data
            .get_last_compaction_detail("test_db", "test_table")
            .unwrap();
        assert_eq!(compacted_table.as_ref(), &compaction_detail);

        // read the parquet file and ensure that it has our three rows
        let file_path = compacted_data
            .files_for_generation(output_id)
            .first()
            .unwrap()
            .path
            .clone();
        let file_path = ObjPath::from(file_path);

        let parquet_file = compacted_data
            .object_store
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
        let summary = compacted_data
            .object_store
            .get(&ObjPath::from(
                "test-compactor/cs/18446744073709551614.json",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let summary = serde_json::from_slice::<CompactionSummary>(&summary).unwrap();
        assert_eq!(output, summary);

        // ensure the compacted data structure was updated with everything
        assert_eq!(compacted_data.get_last_summary().unwrap(), summary);
        assert_eq!(
            compacted_data
                .get_last_compaction_detail("test_db", "test_table")
                .unwrap()
                .as_ref(),
            &compaction_detail
        );
        assert_eq!(
            compacted_data
                .files_for_generation(output_id)
                .first()
                .unwrap()
                .path,
            file_path.to_string()
        );

        let generation_detail = compacted_data.get_generation_detail(output_id).unwrap();
        let persisted_generation_detail = get_generation_detail(
            &GenerationDetailPath::new(compactor_id.as_ref(), output_id),
            Arc::clone(&compacted_data.object_store),
        )
        .await
        .unwrap();
        assert_eq!(generation_detail.as_ref(), &persisted_generation_detail);

        assert_eq!(generation_detail.start_time_s, 0);
        assert_eq!(generation_detail.max_time_ns, 2);
        let files = compacted_data.files_for_generation(output_id);
        assert_eq!(generation_detail.files, files);

        // make sure the loader correctly picks everything up from object storage
        let loaded_compacted_data = CompactedData::load_compacted_data(
            compactor_id.as_ref(),
            compacted_data.compaction_config.clone(),
            Arc::clone(&obj_store) as _,
        )
        .await
        .unwrap();

        assert_eq!(loaded_compacted_data, compacted_data);

        // ensure that the next compaction sequence number will be correct
        assert_eq!(
            compacted_data.next_compaction_sequence_number(),
            CompactionSequenceNumber::new(2)
        );
    }
}
