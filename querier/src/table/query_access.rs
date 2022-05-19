use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    logical_plan::Expr,
    physical_plan::ExecutionPlan,
};
use iox_query::{
    provider::{ChunkPruner, ProviderBuilder},
    pruning::{prune_chunks, PruningObserver},
    QueryChunk,
};
use predicate::{Predicate, PredicateBuilder};
use schema::Schema;

use super::QuerierTable;

#[async_trait]
impl TableProvider for QuerierTable {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema().as_arrow()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // build provider out of all chunks
        // TODO: push down some predicates to catalog
        let mut builder = ProviderBuilder::new(self.table_name(), Arc::clone(self.schema()));
        builder = builder.add_pruner(self.chunk_pruner());

        let predicate = filters
            .iter()
            .fold(PredicateBuilder::new(), |b, expr| b.add_expr(expr.clone()))
            .build();

        let chunks = self
            .chunks(&predicate)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = match builder.build() {
            Ok(provider) => provider,
            Err(e) => panic!("unexpected error: {:?}", e),
        };

        provider.scan(projection, filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        // we may apply filtering (via pruning) but can not guarantee
        // that the filter catches all row during scan
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Debug)]
pub struct QuerierTableChunkPruner {}

impl ChunkPruner for QuerierTableChunkPruner {
    fn prune_chunks(
        &self,
        _table_name: &str,
        table_schema: Arc<Schema>,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: &Predicate,
    ) -> Vec<Arc<dyn QueryChunk>> {
        // TODO: bring back metrics (https://github.com/influxdata/influxdb_iox/issues/4087)
        prune_chunks(&NoopPruningObserver {}, table_schema, chunks, predicate)
    }
}

struct NoopPruningObserver;

impl PruningObserver for NoopPruningObserver {}
