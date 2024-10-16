use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::DatabaseSchemaProvider;
use influxdb3_id::{DbId, TableId};
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_wal::LastCacheDefinition;
use influxdb3_write::{
    last_cache::LastCacheProvider, parquet_cache::ParquetCacheOracle,
    write_buffer::Result as WriteBufferResult, BufferedWriteRequest, Bufferer, ChunkContainer,
    LastCacheManager, ParquetFile, PersistedSnapshot, Precision, WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::Time;
use metric::Registry;
use modes::{
    read::ReadMode,
    read_write::{ReadWriteArgs, ReadWriteMode},
};
use object_store::ObjectStore;
use replica::ReplicationConfig;
use tokio::sync::watch::Receiver;

pub mod modes;
pub mod replica;

#[derive(Debug)]
pub struct WriteBufferPro<Mode> {
    mode: Mode,
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone)]
pub struct NoMode;

impl WriteBufferPro<NoMode> {
    pub async fn read(
        last_cache: Arc<LastCacheProvider>,
        object_store: Arc<dyn ObjectStore>,
        metric_registry: Arc<Registry>,
        ReplicationConfig { interval, hosts }: ReplicationConfig,
        parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
        compacted_data: Option<Arc<CompactedData>>,
    ) -> Result<WriteBufferPro<ReadMode>, anyhow::Error> {
        let mode = ReadMode::new(
            last_cache,
            object_store,
            metric_registry,
            interval,
            hosts,
            parquet_cache,
            compacted_data,
        )
        .await?;
        Ok(WriteBufferPro { mode })
    }

    pub async fn read_write(
        args: ReadWriteArgs,
    ) -> Result<WriteBufferPro<ReadWriteMode>, anyhow::Error> {
        let mode = ReadWriteMode::new(args).await?;
        Ok(WriteBufferPro { mode })
    }
}

#[async_trait]
impl<Mode: Bufferer> Bufferer for WriteBufferPro<Mode> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        self.mode
            .write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        self.mode
            .write_lp_v3(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn db_schema_provider(&self) -> Arc<dyn DatabaseSchemaProvider> {
        self.mode.db_schema_provider()
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.mode.parquet_files(db_id, table_id)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        self.mode.watch_persisted_snapshots()
    }
}

impl<Mode: ChunkContainer> ChunkContainer for WriteBufferPro<Mode> {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.mode
            .get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

#[async_trait::async_trait]
impl<Mode: LastCacheManager> LastCacheManager for WriteBufferPro<Mode> {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.mode.last_cache_provider()
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<String>>,
        value_columns: Option<Vec<String>>,
    ) -> WriteBufferResult<Option<LastCacheDefinition>> {
        self.mode
            .create_last_cache(
                db_id,
                tbl_id,
                cache_name,
                count,
                ttl,
                key_columns,
                value_columns,
            )
            .await
    }

    async fn delete_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: &str,
    ) -> WriteBufferResult<()> {
        self.mode.delete_last_cache(db_id, tbl_id, cache_name).await
    }
}

impl<Mode: WriteBuffer> WriteBuffer for WriteBufferPro<Mode> {}
