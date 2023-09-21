use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use iox_query::{exec::SessionContextIOxExt, provider::ProviderBuilder, pruning::retention_expr};

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
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // build provider out of all chunks
        // TODO: push down some predicates to catalog

        let mut builder =
            ProviderBuilder::new(Arc::clone(self.table_name()), self.schema().clone());

        let filters = match self.namespace_retention_period {
            Some(d) => {
                let ts = self
                    .chunk_adapter
                    .catalog_cache()
                    .time_provider()
                    .now()
                    .timestamp_nanos()
                    - d.as_nanos() as i64;

                filters
                    .iter()
                    .cloned()
                    .chain(std::iter::once(retention_expr(ts)))
                    .collect::<Vec<_>>()
            }
            None => filters.to_vec(),
        };

        let chunks = self
            .chunks(&filters, ctx.child_span("QuerierTable chunks"), projection)
            .await?;

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = match builder.build() {
            Ok(provider) => provider,
            Err(e) => panic!("unexpected error: {e:?}"),
        };

        provider.scan(ctx, projection, &filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Exact)
    }
}
