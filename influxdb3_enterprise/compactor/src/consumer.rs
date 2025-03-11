//! Consumer of compacted data. This contains the methods that the query side uses to get the
//! compacted data. It also has the logic to poll for new compacted data from the object store
//! and update the in-memory `CompactedData` representation.

use crate::ParquetCachePreFetcher;
use crate::compacted_data::CompactedData;
use crate::sys_events::{CompactionEventStore, compaction_consumed};
use anyhow::Context;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_enterprise_data_layout::{
    CompactionDetailPath, GenerationDetail, GenerationDetailPath, GenerationId,
};
use influxdb3_enterprise_data_layout::{
    Generation,
    persist::{
        get_compaction_detail, get_generation_detail, load_compaction_summary,
        load_compaction_summary_for_sequence,
    },
};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, warn};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt::Debug, time::Instant};
use tokio::task::JoinSet;

pub struct CompactedDataConsumer {
    pub compactor_id: Arc<str>,
    pub object_store: Arc<dyn ObjectStore>,
    pub compacted_data: Arc<CompactedData>,
    sys_events_store: Arc<dyn CompactionEventStore>,
    parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
}

impl Debug for CompactedDataConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactedDataConsumer")
            .field("compactor_id", &self.compactor_id)
            .finish()
    }
}

impl CompactedDataConsumer {
    /// Create a new `CompactedDataConsumer` with the given `compactor_id` and `object_store`.
    /// Will not return until it has successfully pulled a compacted catalog and compaction
    /// summary from the object store. This means that the compactor must start up successfully
    /// before this will return.
    pub async fn new(
        compactor_id: Arc<str>,
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
        parquet_cache_prefetcher: Option<Arc<ParquetCachePreFetcher>>,
        sys_events_store: Arc<dyn CompactionEventStore>,
    ) -> anyhow::Result<Self> {
        loop {
            // the producer writes the catalog first and then the summary. We loop until we find
            // the summary and then load the catalog and data.
            let summary =
                match load_compaction_summary(Arc::clone(&compactor_id), Arc::clone(&object_store))
                    .await
                    .context("error decoding comapction summary json")?
                {
                    Some(summary) => summary,
                    None => {
                        warn!(
                            "No compaction summary found for compactor id {}, retrying in 1 second",
                            compactor_id
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
            let compacted_data = CompactedData::load_compacted_data(
                Arc::clone(&compactor_id),
                summary,
                catalog,
                Arc::clone(&object_store),
            )
            .await
            .context("error loading compacted data")?;

            return Ok(Self {
                compactor_id,
                object_store,
                compacted_data: Arc::new(compacted_data),
                parquet_cache_prefetcher,
                sys_events_store,
            });
        }
    }

    pub async fn poll_in_background(&self, poll_duration: Duration) {
        loop {
            tokio::time::sleep(poll_duration).await;
            if let Err(e) = self.refresh().await {
                warn!(error = %e, "error refreshing compacted data");
            }
        }
    }

    pub(crate) async fn refresh(&self) -> anyhow::Result<()> {
        let start = Instant::now();
        let last_summary = self.compacted_data.compaction_summary();
        let next_sequence_number = last_summary.compaction_sequence_number.next();
        let summary_sequence_number = next_sequence_number.as_u64();

        let summary = match load_compaction_summary_for_sequence(
            Arc::clone(&self.compactor_id),
            next_sequence_number,
            Arc::clone(&self.object_store),
        )
        .await
        .context(format!(
            "error decoding compaction summary json {:?}",
            next_sequence_number
        ))
        .inspect_err(|err| {
            let event = compaction_consumed::FailedInfo {
                duration: start.elapsed(),
                error: err.to_string(),
                summary_sequence_number,
            };
            self.sys_events_store
                .record_compaction_consumed_failed(event);
        })? {
            Some(summary) => summary,
            None => {
                // it's not there yet, we'll get it on the next poll
                return Ok(());
            }
        };

        self.compacted_data
            .catalog
            .update_to_sequence_number(summary.catalog_sequence_number)
            .await
            .inspect_err(|err| {
                let event = compaction_consumed::FailedInfo {
                    duration: start.elapsed(),
                    error: err.to_string(),
                    summary_sequence_number,
                };
                self.sys_events_store
                    .record_compaction_consumed_failed(event);
            })
            .context("failed to update catalog to sequence found in compaction summary")?;

        // load new compaction details, new generations and remove old generations
        for ((db_id, table_id), sequence_number) in &summary.compaction_details {
            // load the detail if we don't have a detail for this db and table or if the detail is newer
            let load_detail = last_summary
                .compaction_details
                .get(&(*db_id, *table_id))
                .map(|s| s < sequence_number)
                .unwrap_or(true);
            if !load_detail {
                continue;
            }

            let path = CompactionDetailPath::new(
                Arc::clone(&self.compactor_id),
                *db_id,
                *table_id,
                *sequence_number,
            );
            let compaction_detail = get_compaction_detail(&path, Arc::clone(&self.object_store))
                .await
                .context("compaction detail not found")?;
            let last_compaction_detail = self.compacted_data.compaction_detail(*db_id, *table_id);

            let (new_generations, removed_generations) = match last_compaction_detail {
                Some(last_compaction_detail) => {
                    let new_generations = compaction_detail
                        .compacted_generations
                        .iter()
                        .filter(|g| !last_compaction_detail.compacted_generations.contains(g))
                        .copied()
                        .collect::<Vec<_>>();
                    let removed_generations = last_compaction_detail
                        .compacted_generations
                        .iter()
                        .filter(|lg| !compaction_detail.compacted_generations.contains(lg))
                        .copied()
                        .collect::<Vec<_>>();
                    (new_generations, removed_generations)
                }
                None => (compaction_detail.compacted_generations.clone(), vec![]),
            };

            let new_gens_u8 = Generation::to_vec_levels(&new_generations);
            let removed_gens_u8 = Generation::to_vec_levels(&removed_generations);
            // load new generations concurrently
            let new_gen_ids = to_gen_ids_iter(&new_generations);
            let generation_details = self
                .load_generation_details(SizedIter {
                    iter: new_gen_ids,
                    size: new_generations.len(),
                })
                .await?;

            // load removed generations concurrently
            let removed_gen_ids = to_gen_ids_iter(&removed_generations);
            let removed_gen_details = self
                .load_generation_details(SizedIter {
                    iter: removed_gen_ids,
                    size: removed_generations.len(),
                })
                .await?;

            // cache all the new gen files
            let cache_prefetch_timer = Instant::now();
            if let Some(prefetcher) = self.parquet_cache_prefetcher.as_ref() {
                let mut prefetch_files = generation_details
                    .iter()
                    .flat_map(|gen_detail| {
                        gen_detail
                            .files
                            .iter()
                            .map(|f| f.as_ref().clone())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                let gen_1_files = compaction_detail
                    .leftover_gen1_files
                    .iter()
                    .map(|f| f.file.as_ref().clone());
                prefetch_files.extend(gen_1_files);
                debug!(num_files = ?prefetch_files.len(), ">>> prefetching files into the cache");
                prefetcher.prefetch_all(&prefetch_files).await;
            }
            debug!(time_taken = ?cache_prefetch_timer.elapsed(), ">>> time taken for prefetching new generation files");

            let file_index_with_new_parquet_gen_files = self
                .compacted_data
                .build_new_parquet_files_in_gen_and_file_index(
                    db_id,
                    table_id,
                    generation_details,
                    &removed_gen_details,
                );

            let gen_update_timer = Instant::now();
            let old_data = self.compacted_data.update_detail_with_generations(
                compaction_detail,
                file_index_with_new_parquet_gen_files,
            );
            debug!(time_taken = ?gen_update_timer.elapsed(), ">>> time taken for updating detail with generations");
            // This explicit drop shouldn't be necessary but without it there seems to be a
            // significant delay at times (maybe there's some optimization) in line with dropping
            // data when holding the lock, whereas with this explicit drop on the old data here
            // the timings are consistently <1ms with jemalloc and occassional spikes with
            // system allocator
            drop(old_data);

            if let Some(cache_prefetcher) = &self.parquet_cache_prefetcher {
                for gen_detail in removed_gen_details {
                    for file in gen_detail.files {
                        cache_prefetcher.remove_from_cache(&file.path);
                    }
                }
            }

            if let Some(db_schema) = self.compacted_data.catalog.db_schema_by_id(db_id) {
                let db_name = Arc::clone(&db_schema.name);
                let table_defn = db_schema.table_definition_by_id(table_id);
                let event = compaction_consumed::SuccessInfo {
                    duration: start.elapsed(),
                    db_name,
                    table_name: Arc::clone(&table_defn.unwrap().table_name),
                    new_generations: new_gens_u8,
                    removed_generations: removed_gens_u8,
                    summary_sequence_number,
                };
                self.sys_events_store
                    .record_compaction_consumed_success(event);
            }
        }

        self.compacted_data.update_compaction_summary(summary);

        Ok(())
    }

    async fn load_generation_details<I: Iterator<Item = GenerationId>>(
        &self,
        generation_ids: SizedIter<I>,
    ) -> Result<Vec<GenerationDetail>, anyhow::Error> {
        let start = Instant::now();
        let mut join_set = JoinSet::new();
        let mut generation_details = Vec::with_capacity(generation_ids.len());
        for id in generation_ids {
            let compactor_id = Arc::clone(&self.compactor_id);
            let object_store = Arc::clone(&self.object_store);
            // no need to buffer - this is per table, which are expected to not have many
            // generations to load at this point
            join_set.spawn(async move {
                let gen_path = GenerationDetailPath::new(Arc::clone(&compactor_id), id);
                get_generation_detail(&gen_path, Arc::clone(&object_store))
                    .await
                    .context("generation detail not found")
            });
        }
        while let Some(details) = join_set.join_next().await {
            let detail = details??;
            generation_details.push(detail);
        }
        debug!(time_taken = ?start.elapsed(), ">>> time taken to load generation details");
        Ok(generation_details)
    }
}

fn to_gen_ids_iter(new_generations: &[Generation]) -> impl Iterator<Item = GenerationId> {
    new_generations.iter().map(|generation| generation.id)
}

struct SizedIter<I> {
    iter: I,
    size: usize,
}

impl<I: Iterator> SizedIter<I> {
    fn len(&self) -> usize {
        self.size
    }
}

impl<I: Iterator> Iterator for SizedIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.size > 0 {
            return self.iter.next();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_enterprise_data_layout::persist::{
        persist_compaction_detail, persist_compaction_summary, persist_generation_detail,
    };
    use influxdb3_enterprise_data_layout::{
        CompactionDetail, CompactionSequenceNumber, CompactionSummary, Generation,
        GenerationDetail, GenerationId, GenerationLevel, NodeSnapshotMarker,
    };
    use influxdb3_id::ParquetFileId;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_wal::SnapshotSequenceNumber;
    use influxdb3_write::ParquetFile;
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::memory::InMemory;

    async fn setup_compacted_data() -> (
        Arc<dyn ObjectStore>,
        Arc<Catalog>,
        CompactionSummary,
        CompactionDetail,
        GenerationDetail,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cluster_id = Arc::<str>::from("test_cluster");
        let host1 = "host1";
        let host2 = "host2";
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Catalog::new(
            Arc::clone(&cluster_id),
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
        )
        .await
        .unwrap();
        catalog
            .create_table(
                "db1",
                "table1",
                &["tag1"],
                &[("field1", FieldDataType::Integer)],
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "db1",
                "table2",
                &["tag1"],
                &[("field1", FieldDataType::Integer)],
            )
            .await
            .unwrap();

        let db = catalog.db_schema("db1").unwrap();
        let table1 = db.table_definition("table1").unwrap();

        let compaction_sequence_number = CompactionSequenceNumber::new(1);
        let snapshot_markers = vec![
            Arc::new(NodeSnapshotMarker {
                node_id: host1.into(),
                snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                next_file_id: ParquetFileId::next_id(),
            }),
            Arc::new(NodeSnapshotMarker {
                node_id: host2.into(),
                snapshot_sequence_number: SnapshotSequenceNumber::new(3),
                next_file_id: ParquetFileId::next_id(),
            }),
        ];
        let summary = CompactionSummary {
            compaction_sequence_number,
            catalog_sequence_number: catalog.sequence_number(),
            last_file_id: ParquetFileId::next_id(),
            last_generation_id: GenerationId::current(),
            snapshot_markers: snapshot_markers.clone(),
            compaction_details: vec![((db.id, table1.table_id), compaction_sequence_number)]
                .into_iter()
                .collect(),
        };

        let generation = Generation {
            id: GenerationId::new(),
            level: GenerationLevel::two(),
            start_time_secs: 0,
            max_time: 0,
        };
        let generation_detail = GenerationDetail {
            id: generation.id,
            level: GenerationLevel::two(),
            start_time_s: 0,
            max_time_ns: 0,
            files: vec![Arc::new(ParquetFile {
                id: ParquetFileId::next_id(),
                path: "whatevs".to_string(),
                size_bytes: 0,
                row_count: 0,
                chunk_time: 0,
                min_time: 0,
                max_time: 0,
            })],
            file_index: Default::default(),
        };

        let detail = CompactionDetail {
            db_name: "db1".into(),
            db_id: db.id,
            table_name: "table1".into(),
            table_id: table1.table_id,
            sequence_number: compaction_sequence_number,
            snapshot_markers,
            compacted_generations: vec![generation],
            leftover_gen1_files: vec![],
        };

        persist_generation_detail(
            Arc::clone(&cluster_id),
            generation.id,
            &generation_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        persist_compaction_detail(
            Arc::clone(&cluster_id),
            db.id,
            table1.table_id,
            &detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        persist_compaction_summary(Arc::clone(&cluster_id), &summary, Arc::clone(&object_store))
            .await
            .unwrap();

        (
            object_store,
            Arc::new(catalog),
            summary,
            detail,
            generation_detail,
        )
    }

    #[test_log::test(tokio::test)]
    async fn loads_with_compacted_data() {
        let (object_store, catalog, summary, detail, generation) = setup_compacted_data().await;
        let db = catalog.db_schema("db1").unwrap();
        let table1 = db.table_definition("table1").unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let sys_events_store: Arc<dyn CompactionEventStore> =
            Arc::new(SysEventStore::new(Arc::<MockProvider>::clone(
                &time_provider,
            )));

        let consumer = CompactedDataConsumer::new(
            catalog.object_store_prefix(),
            Arc::clone(&object_store),
            Arc::clone(&catalog),
            None,
            sys_events_store,
        )
        .await
        .unwrap();
        let consumer_summary = consumer.compacted_data.compaction_summary();
        assert_eq!(&summary, consumer_summary.as_ref());

        let consumer_db = consumer.compacted_data.catalog.db_schema("db1").unwrap();
        let consumer_table = consumer_db.table_definition("table1").unwrap();

        let consumer_detail = consumer
            .compacted_data
            .compaction_detail(db.id, table1.table_id)
            .unwrap();
        assert_eq!(&detail, consumer_detail.as_ref());

        let files = consumer.compacted_data.parquet_files(
            consumer_db.id,
            consumer_table.table_id,
            generation.id,
        );
        assert_eq!(1, files.len());
        assert_eq!(files[0].path, "whatevs");
    }
}
