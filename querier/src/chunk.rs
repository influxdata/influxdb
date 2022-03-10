use crate::cache::CatalogCache;
use data_types2::{ChunkAddr, ChunkId, ChunkOrder, ParquetFile};
use db::catalog::chunk::{CatalogChunk, ChunkMetadata, ChunkMetrics as CatalogChunkMetrics};
use iox_object_store::IoxObjectStore;
use object_store::ObjectStore;
use parquet_file::chunk::{
    new_parquet_chunk, ChunkMetrics as ParquetChunkMetrics, DecodedParquetFile, ParquetChunk,
};
use std::sync::Arc;
use time::TimeProvider;
use uuid::Uuid;

/// Adapter that can create old-gen chunks for the new-gen catalog.
#[derive(Debug)]
pub struct ParquetChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Old-gen object store.
    iox_object_store: Arc<IoxObjectStore>,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

impl ParquetChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        object_store: Arc<ObjectStore>,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        // create a virtual IOx object store, the UUID won't be used anyways
        let iox_object_store = Arc::new(IoxObjectStore::existing(
            Arc::clone(&object_store),
            IoxObjectStore::root_path_for(&object_store, uuid::Uuid::new_v4()),
        ));

        Self {
            catalog_cache,
            iox_object_store,
            metric_registry,
            time_provider,
        }
    }

    /// Create parquet chunk.
    async fn new_parquet_chunk(&self, decoded_parquet_file: &DecodedParquetFile) -> ParquetChunk {
        let parquet_file = &decoded_parquet_file.parquet_file;
        let table_name = self.catalog_cache.table().name(parquet_file.table_id).await;
        let partition_key = self
            .catalog_cache
            .partition()
            .old_gen_partition_key(parquet_file.partition_id)
            .await;
        let metrics = ParquetChunkMetrics::new(self.metric_registry.as_ref());

        new_parquet_chunk(
            decoded_parquet_file,
            table_name,
            partition_key,
            metrics,
            Arc::clone(&self.iox_object_store),
        )
    }

    /// Create all components to create a catalog chunk using
    /// [`Partition::insert_object_store_only_chunk`](db::catalog::partition::Partition::insert_object_store_only_chunk).
    pub async fn new_catalog_chunk_parts(
        &self,
        parquet_file: ParquetFile,
    ) -> (ChunkAddr, ChunkOrder, ChunkMetadata, Arc<ParquetChunk>) {
        let decoded_parquet_file = DecodedParquetFile::new(parquet_file);
        let chunk = Arc::new(self.new_parquet_chunk(&decoded_parquet_file).await);

        let addr = self
            .old_gen_chunk_addr(&decoded_parquet_file.parquet_file)
            .await;

        let iox_metadata = &decoded_parquet_file.iox_metadata;

        // Somewhat hacky workaround because NG has implicit chunk orders, use min sequence number and hope it doesn't
        // overflow u32. Order is non-zero, se we need to add 1.
        let order = ChunkOrder::new(1 + iox_metadata.min_sequence_number.get() as u32)
            .expect("cannot be zero");

        let metadata = ChunkMetadata {
            table_summary: Arc::clone(chunk.table_summary()),
            schema: chunk.schema(),
            // delete predicates will be set/synced by a dedicated process
            delete_predicates: vec![],
            time_of_first_write: iox_metadata.time_of_first_write,
            time_of_last_write: iox_metadata.time_of_last_write,
            // TODO(marco): get sort key wired up (needs to come via IoxMetadata)
            sort_key: None,
        };

        (addr, order, metadata, chunk)
    }

    /// Create a catalog chunk.
    pub async fn new_catalog_chunk(&self, parquet_file: ParquetFile) -> CatalogChunk {
        let (addr, order, metadata, chunk) = self.new_catalog_chunk_parts(parquet_file).await;

        // TODO: register metrics w/ catalog registry
        let metrics = CatalogChunkMetrics::new_unregistered();

        CatalogChunk::new_object_store_only(
            addr,
            order,
            metadata,
            chunk,
            metrics,
            Arc::clone(&self.time_provider),
        )
    }

    /// Get chunk addr for old gen.
    ///
    /// Mapping of NG->old:
    /// - `namespace.name -> db_name`
    /// - `table.name -> table_name`
    /// - `sequencer.id X partition.name -> partition_key`
    /// - `parquet_file.id -> chunk_id`
    pub async fn old_gen_chunk_addr(&self, parquet_file: &ParquetFile) -> ChunkAddr {
        ChunkAddr {
            db_name: self
                .catalog_cache
                .namespace()
                .name(
                    self.catalog_cache
                        .table()
                        .namespace_id(parquet_file.table_id)
                        .await,
                )
                .await,
            table_name: self.catalog_cache.table().name(parquet_file.table_id).await,
            partition_key: self
                .catalog_cache
                .partition()
                .old_gen_partition_key(parquet_file.partition_id)
                .await,
            chunk_id: ChunkId::from(Uuid::from_u128(parquet_file.id.get() as _)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestCatalog;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use db::chunk::DbChunk;
    use futures::StreamExt;
    use query::{exec::IOxExecutionContext, QueryChunk};
    use schema::selection::Selection;

    #[tokio::test]
    async fn test_create_record() {
        let catalog = TestCatalog::new();

        let adapter = ParquetChunkAdapter::new(
            Arc::new(CatalogCache::new(catalog.catalog())),
            catalog.object_store(),
            catalog.metric_registry(),
            catalog.time_provider(),
        );

        let lp = vec![
            "table,tag1=WA field_int=1000 8000",
            "table,tag1=VT field_int=10 10000",
            "table,tag1=UT field_int=70 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let parquet_file = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .create_parquet_file(&lp)
            .await
            .parquet_file
            .clone();

        let catalog_chunk = adapter.new_catalog_chunk(parquet_file).await;
        assert_eq!(
            catalog_chunk.addr().to_string(),
            "Chunk('ns':'table':'1-part':00000000-0000-0000-0000-000000000001)",
        );

        let db_chunk = DbChunk::snapshot(&catalog_chunk);
        let batches = collect_read_filter(&db_chunk).await;
        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
    }

    async fn collect_read_filter(chunk: &DbChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(
                IOxExecutionContext::default(),
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
}
