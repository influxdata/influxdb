//! Consumer of compacted data. This contains the methods that the query side uses to get the
//! compacted data. It also has the logic to poll for new compacted data from the object store
//! and update the in-memory `CompactedData` representation.

use crate::catalog::CompactedCatalog;
use crate::compacted_data::CompactedData;
use crate::ParquetCachePreFetcher;
use anyhow::Context;
use influxdb3_pro_data_layout::persist::{
    get_compaction_detail, get_generation_detail, load_compaction_summary,
    load_compaction_summary_for_sequence,
};
use influxdb3_pro_data_layout::{CompactionDetailPath, GenerationDetailPath};
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

pub struct CompactedDataConsumer {
    pub compactor_id: Arc<str>,
    pub object_store: Arc<dyn ObjectStore>,
    pub compacted_data: Arc<CompactedData>,
    parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
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
        compactor_id: &str,
        object_store: Arc<dyn ObjectStore>,
        parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
    ) -> anyhow::Result<Self> {
        loop {
            // the producer writes the catalog first and then the summary. We loop until we find
            // the summary and then load the catalog and data.
            let summary = match load_compaction_summary(compactor_id, Arc::clone(&object_store))
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
            let catalog = CompactedCatalog::load(compactor_id, Arc::clone(&object_store))
                .await
                .context("error loading compacted catalog")?
                .context("compacted catalog not found")?;
            let compacted_data = CompactedData::load_compacted_data(
                compactor_id,
                summary,
                catalog,
                Arc::clone(&object_store),
            )
            .await
            .context("error loading compacted data")?;

            return Ok(Self {
                compactor_id: compactor_id.into(),
                object_store,
                compacted_data: Arc::new(compacted_data),
                parquet_cache_prefetcher,
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

    async fn refresh(&self) -> anyhow::Result<()> {
        let last_summary = self.compacted_data.compaction_summary();
        let next_sequence_number = last_summary.compaction_sequence_number.next();

        let summary = match load_compaction_summary_for_sequence(
            self.compactor_id.as_ref(),
            next_sequence_number,
            Arc::clone(&self.object_store),
        )
        .await
        .context(format!(
            "error decoding compaction summary json {:?}",
            next_sequence_number
        ))? {
            Some(summary) => summary,
            None => {
                // it's not there yet, we'll get it on the next poll
                return Ok(());
            }
        };

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
                self.compactor_id.as_ref(),
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

            let mut generation_details = Vec::with_capacity(new_generations.len());
            for gen in new_generations {
                let gen_path = GenerationDetailPath::new(self.compactor_id.as_ref(), gen.id);
                let gen_detail = get_generation_detail(&gen_path, Arc::clone(&self.object_store))
                    .await
                    .context("generation detail not found")?;
                if let Some(prefetcher) = self.parquet_cache_prefetcher.as_ref() {
                    // TODO: convert prefetcher and compaction function to always return Vec<Arc<ParquetFile>> rather than the raw ParquetFile.
                    let prefetch_files = gen_detail
                        .files
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>();
                    prefetcher.prefetch_all(&prefetch_files).await;
                }
                generation_details.push(gen_detail);
            }

            self.compacted_data.update_detail_with_generations(
                compaction_detail,
                generation_details,
                removed_generations,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::test_helpers::create_host_catalog_with_table;
    use influxdb3_id::ParquetFileId;
    use influxdb3_pro_data_layout::persist::{
        persist_compaction_detail, persist_compaction_summary, persist_generation_detail,
    };
    use influxdb3_pro_data_layout::{
        CompactionDetail, CompactionSequenceNumber, CompactionSummary, Generation,
        GenerationDetail, GenerationId, GenerationLevel, HostSnapshotMarker,
    };
    use influxdb3_wal::SnapshotSequenceNumber;
    use influxdb3_write::ParquetFile;
    use object_store::memory::InMemory;

    async fn setup_compacted_data() -> (
        Arc<dyn ObjectStore>,
        CompactedCatalog,
        CompactionSummary,
        CompactionDetail,
        GenerationDetail,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let compactor_id = "compactor_id";
        let host1 = "host1";
        let host2 = "host2";

        let _catalog1 =
            create_host_catalog_with_table(host1, "db1", "table1", Arc::clone(&object_store)).await;
        let _catalog2 =
            create_host_catalog_with_table(host2, "db1", "table2", Arc::clone(&object_store)).await;

        let catalog = CompactedCatalog::load_merged_from_hosts(
            compactor_id,
            vec![host1.into(), host2.into()],
            Arc::clone(&object_store),
        )
        .await
        .expect("failed to load merged catalog");

        catalog.persist(Arc::clone(&object_store)).await.unwrap();
        let db = catalog.catalog.db_schema("db1").unwrap();
        let table1 = db.table_definition("table1").unwrap();

        let compaction_sequence_number = CompactionSequenceNumber::new(1);
        let snapshot_markers = vec![
            Arc::new(HostSnapshotMarker {
                host_id: host1.into(),
                snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                next_file_id: ParquetFileId::next_id(),
            }),
            Arc::new(HostSnapshotMarker {
                host_id: host2.into(),
                snapshot_sequence_number: SnapshotSequenceNumber::new(3),
                next_file_id: ParquetFileId::next_id(),
            }),
        ];
        let summary = CompactionSummary {
            compaction_sequence_number,
            catalog_sequence_number: catalog.catalog.sequence_number(),
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
            compactor_id,
            generation.id,
            &generation_detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        persist_compaction_detail(
            compactor_id,
            db.id,
            table1.table_id,
            &detail,
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        persist_compaction_summary(compactor_id, &summary, Arc::clone(&object_store))
            .await
            .unwrap();

        (object_store, catalog, summary, detail, generation_detail)
    }

    #[tokio::test]
    async fn loads_with_compacted_data() {
        let (object_store, catalog, summary, detail, generation) = setup_compacted_data().await;
        let compactor_id = "compactor_id";
        let db = catalog.catalog.db_schema("db1").unwrap();
        let table1 = db.table_definition("table1").unwrap();

        let consumer = CompactedDataConsumer::new(compactor_id, Arc::clone(&object_store), None)
            .await
            .unwrap();
        let consumer_summary = consumer.compacted_data.compaction_summary();
        assert_eq!(&summary, consumer_summary.as_ref());

        let consumer_db = consumer
            .compacted_data
            .compacted_catalog
            .catalog
            .db_schema("db1")
            .unwrap();
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
