//! Querier Chunks

use crate::cache::CatalogCache;
use data_types::{
    ChunkId, ChunkOrder, ColumnId, DeletePredicate, ParquetFile, ParquetFileId, PartitionId,
    SequenceNumber, SequencerId, TableSummary, TimestampMinMax,
};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use parking_lot::RwLock;
use parquet_file::{chunk::ParquetChunk, storage::ParquetStorage};
use read_buffer::RBChunk;
use schema::{sort::SortKey, Schema};
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

use self::util::create_basic_summary;

mod query_access;
pub(crate) mod util;

/// Immutable metadata attached to a [`QuerierChunk`].
#[derive(Debug)]
pub struct ChunkMeta {
    /// ID of the Parquet file of the chunk
    parquet_file_id: ParquetFileId,

    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Table name
    table_name: Arc<str>,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Sequencer that created the data within this chunk.
    sequencer_id: SequencerId,

    /// Partition ID.
    partition_id: PartitionId,

    /// The minimum sequence number within this chunk.
    min_sequence_number: SequenceNumber,

    /// The maximum sequence number within this chunk.
    max_sequence_number: SequenceNumber,
}

impl ChunkMeta {
    /// ID of the Parquet file of the chunk
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.parquet_file_id
    }

    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Sequencer that created the data within this chunk.
    pub fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    /// Partition ID.
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// The minimum sequence number within this chunk.
    pub fn min_sequence_number(&self) -> SequenceNumber {
        self.min_sequence_number
    }

    /// The maximum sequence number within this chunk.
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
    }
}

/// Internal [`QuerierChunk`] stage, i.e. how the data is loaded/organized.
#[derive(Debug)]
enum ChunkStage {
    ReadBuffer {
        /// Underlying read buffer chunk
        rb_chunk: Arc<RBChunk>,

        /// Table summary
        table_summary: Arc<TableSummary>,
    },
    Parquet {
        /// Chunk of the Parquet file
        parquet_chunk: Arc<ParquetChunk>,

        /// Table summary
        table_summary: Arc<TableSummary>,
    },
}

impl ChunkStage {
    /// Get stage-specific table summary.
    ///
    /// The table summary may improve if we have more chunk information (e.g. when the actual data was loaded into
    /// memory).
    pub fn table_summary(&self) -> &Arc<TableSummary> {
        match self {
            Self::Parquet { table_summary, .. } => table_summary,
            Self::ReadBuffer { table_summary, .. } => table_summary,
        }
    }

    /// Machine- and human-readable name of the stage.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Parquet { .. } => "parquet",
            Self::ReadBuffer { .. } => "read_buffer",
        }
    }
}

#[derive(Debug)]
pub struct QuerierChunk {
    /// Immutable chunk metadata
    meta: Arc<ChunkMeta>,

    /// Schema of the chunk
    schema: Arc<Schema>,

    /// min/max time range of this table (extracted from TableSummary), if known
    timestamp_min_max: Option<TimestampMinMax>,

    /// Delete predicates to be combined with the chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,

    /// Partition sort key (how does the read buffer use this?)
    partition_sort_key: Arc<Option<SortKey>>,

    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    ///
    /// Internally, `ParquetStorage` wraps the actual store implementation in an `Arc`, so
    /// `ParquetStorage` is cheap to clone.
    store: ParquetStorage,

    /// Current stage.
    stage: RwLock<ChunkStage>,
}

impl QuerierChunk {
    /// Create new parquet-backed chunk (object store data).
    pub async fn new(
        parquet_chunk: Arc<ParquetChunk>,
        meta: Arc<ChunkMeta>,
        partition_sort_key: Arc<Option<SortKey>>,
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
    ) -> Self {
        let table_summary = Arc::new(create_basic_summary(
            parquet_chunk.rows() as u64,
            &parquet_chunk.schema(),
            parquet_chunk.timestamp_min_max(),
        ));
        let schema = parquet_chunk.schema();
        let timestamp_min_max = table_summary.time_range();
        let stage = ChunkStage::Parquet {
            parquet_chunk,
            table_summary,
        };
        // TODO: peek RB cache and try to upgrade
        Self {
            meta,
            delete_predicates: Vec::new(),
            partition_sort_key,
            catalog_cache,
            schema,
            timestamp_min_max,
            stage: RwLock::new(stage),
            store,
        }
    }

    /// Upgrade chunks internal data structure from "parquet" to "read buffer"
    pub async fn load_to_read_buffer(&self) {
        let parquet_file = {
            let stage = self.stage.read();

            match &*stage {
                ChunkStage::Parquet { parquet_chunk, .. } => {
                    Arc::clone(parquet_chunk.parquet_file())
                }
                ChunkStage::ReadBuffer { .. } => {
                    // RB already loaded
                    return;
                }
            }
        };

        let rb_chunk = self
            .catalog_cache
            .read_buffer()
            .get(parquet_file, Arc::clone(&self.schema), self.store.clone())
            .await;
        self.load_to_read_buffer_internal(rb_chunk);
    }

    fn load_to_read_buffer_internal(&self, rb_chunk: Arc<RBChunk>) {
        let mut stage = self.stage.write();

        match &*stage {
            ChunkStage::Parquet { .. } => {
                let table_summary = Arc::new(rb_chunk.table_summary());

                *stage = ChunkStage::ReadBuffer {
                    rb_chunk,
                    table_summary,
                };
            }
            ChunkStage::ReadBuffer { .. } => {
                // RB already loaded
            }
        }
    }

    /// Set delete predicates of the given chunk.
    pub fn with_delete_predicates(self, delete_predicates: Vec<Arc<DeletePredicate>>) -> Self {
        Self {
            delete_predicates,
            ..self
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &ChunkMeta {
        self.meta.as_ref()
    }

    /// Set partition sort key
    pub fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }
}

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    ///
    /// Internally, `ParquetStorage` wraps the actual store implementation in an `Arc`, so
    /// `ParquetStorage` is cheap to clone.
    store: ParquetStorage,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Time provider.
    #[allow(dead_code)]
    time_provider: Arc<dyn TimeProvider>,
}

impl ChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            catalog_cache,
            store,
            metric_registry,
            time_provider,
        }
    }

    /// Metric registry getter.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Get underlying catalog cache.
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }

    /// Get underlying catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_cache.catalog()
    }

    pub async fn new_chunk(
        &self,
        namespace_name: Arc<str>,
        parquet_file: Arc<ParquetFile>,
    ) -> Option<QuerierChunk> {
        let parts = self
            .chunk_parts(namespace_name, Arc::clone(&parquet_file))
            .await?;

        let parquet_chunk = Arc::new(ParquetChunk::new(
            parquet_file,
            parts.schema,
            self.store.clone(),
        ));

        Some(
            QuerierChunk::new(
                parquet_chunk,
                parts.meta,
                parts.partition_sort_key,
                Arc::clone(&self.catalog_cache),
                self.store.clone(),
            )
            .await,
        )
    }

    async fn chunk_parts(
        &self,
        namespace_name: Arc<str>,
        parquet_file: Arc<ParquetFile>,
    ) -> Option<ChunkParts> {
        // gather schema information
        let file_column_ids: HashSet<ColumnId> = parquet_file.column_set.iter().copied().collect();
        let table_name = self
            .catalog_cache
            .table()
            .name(parquet_file.table_id)
            .await?;
        let namespace_schema = self
            .catalog_cache
            .namespace()
            .schema(namespace_name, &[(&table_name, &file_column_ids)])
            .await?;
        let table_schema_catalog = namespace_schema.tables.get(table_name.as_ref())?;
        let column_id_lookup = table_schema_catalog.column_id_map();
        let file_columns: HashSet<&str> = file_column_ids
            .iter()
            .flat_map(|id| column_id_lookup.get(id).copied())
            .collect();
        let table_schema: Schema = table_schema_catalog
            .clone()
            .try_into()
            .expect("Invalid table schema in catalog");
        let table_columns: HashSet<&str> = table_schema
            .iter()
            .map(|(_t, field)| field.name().as_str())
            .collect();
        for file_col in &file_columns {
            assert!(
                table_columns.contains(*file_col),
                "Column '{file_col}' occurs in parquet file but is not part of the table schema",
            )
        }

        // gather partition sort key
        let relevant_pk_columns: HashSet<&str> = table_schema
            .primary_key()
            .into_iter()
            .filter(|c| file_columns.contains(c))
            .collect();
        let partition_sort_key = self
            .catalog_cache
            .partition()
            .sort_key(parquet_file.partition_id, &relevant_pk_columns)
            .await;
        let partition_sort_key_ref = partition_sort_key
            .as_ref()
            .as_ref()
            .expect("partition sort key should be set when a parquet file exists");

        // NOTE: Because we've looked up the sort key AFTER the namespace schema, it may contain columns for which we
        //       don't have any schema information yet. This is OK because we've ensured that all file columns are known
        //       withing the schema and if a column is NOT part of the file, it will also not be part of the chunk sort
        //       key, so we have consistency here.

        // calculate schema
        // IMPORTANT: Do NOT use the sort key to list columns because the sort key only contains primary-key columns.
        // NOTE: The schema that we calculate here may have a different column order than the actual parquet file. This
        //       is OK because the IOx parquet reader can deal with that (see #4921).
        let column_names: Vec<_> = table_schema
            .iter()
            .map(|(_t, field)| field.name().as_str())
            .filter(|col| file_columns.contains(*col))
            .collect();
        let schema = Arc::new(
            table_schema
                .select_by_names(&column_names)
                .expect("Bug in schema projection"),
        );

        // calculate sort key
        let pk_cols = schema.primary_key();
        let sort_key = partition_sort_key_ref.filter_to(&pk_cols);
        assert!(
            !sort_key.is_empty(),
            "Sort key can never be empty because there should at least be a time column",
        );

        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file.id.get() as _));

        let order = ChunkOrder::new(parquet_file.min_sequence_number.get());

        let meta = Arc::new(ChunkMeta {
            parquet_file_id: parquet_file.id,
            chunk_id,
            table_name,
            order,
            sort_key: Some(sort_key),
            sequencer_id: parquet_file.sequencer_id,
            partition_id: parquet_file.partition_id,
            min_sequence_number: parquet_file.min_sequence_number,
            max_sequence_number: parquet_file.max_sequence_number,
        });

        Some(ChunkParts {
            meta,
            schema,
            partition_sort_key,
        })
    }
}

struct ChunkParts {
    meta: Arc<ChunkMeta>,
    schema: Arc<Schema>,
    partition_sort_key: Arc<Option<SortKey>>,
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::{datatypes::DataType, record_batch::RecordBatch};
    use arrow_util::assert_batches_eq;
    use data_types::ColumnType;
    use futures::StreamExt;
    use iox_query::{exec::IOxSessionContext, QueryChunk, QueryChunkMeta};
    use iox_tests::util::TestCatalog;
    use metric::{Attributes, Observation, RawReporter};
    use schema::{builder::SchemaBuilder, selection::Selection, sort::SortKeyBuilder};
    use test_helpers::maybe_start_logging;

    #[tokio::test]
    async fn test_new_rb_chunk() {
        maybe_start_logging();
        let catalog = TestCatalog::new();

        let adapter = ChunkAdapter::new(
            Arc::new(CatalogCache::new_testing(
                catalog.catalog(),
                catalog.time_provider(),
                catalog.metric_registry(),
                usize::MAX,
            )),
            ParquetStorage::new(catalog.object_store()),
            catalog.metric_registry(),
            catalog.time_provider(),
        );

        // set up catalog
        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("field_float", ColumnType::F64).await;
        table.create_column("time", ColumnType::Time).await;
        let partition = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .update_sort_key(SortKey::from_columns(["tag1", "tag2", "tag4", "time"]))
            .await;
        let parquet_file = Arc::new(partition.create_parquet_file(&lp).await.parquet_file);

        // create chunk
        let chunk = adapter
            .new_chunk(ns.namespace.name.clone().into(), Arc::clone(&parquet_file))
            .await
            .unwrap();

        // measure catalog access
        let catalog_metrics1 = get_catalog_access_metrics(&catalog.metric_registry());

        // check chunk schema
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema.as_ref(), &expected_schema);

        // check sort key
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);

        // check if chunk can be queried
        let batches = collect_read_filter(&chunk).await;
        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );

        // retrieving the chunk again should not require any catalog requests
        adapter
            .new_chunk(ns.namespace.name.clone().into(), parquet_file)
            .await
            .unwrap();
        let catalog_metrics2 = get_catalog_access_metrics(&catalog.metric_registry());
        assert_eq!(catalog_metrics1, catalog_metrics2);
    }

    /// collect data for the given chunk
    async fn collect_read_filter(chunk: &dyn QueryChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(
                IOxSessionContext::default(),
                &Default::default(),
                Selection::All,
            )
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    /// get catalog access metrics from metric registry
    fn get_catalog_access_metrics(metric_registry: &metric::Registry) -> Vec<(Attributes, u64)> {
        let mut reporter = RawReporter::default();
        metric_registry.report(&mut reporter);

        let mut metrics: Vec<_> = reporter
            .observations()
            .iter()
            .find(|o| o.metric_name == "catalog_op_duration")
            .expect("failed to read metric")
            .observations
            .iter()
            .map(|(a, o)| {
                if let Observation::DurationHistogram(h) = o {
                    (a.clone(), h.sample_count())
                } else {
                    panic!("wrong metric type")
                }
            })
            .collect();
        metrics.sort();
        metrics
    }
}
