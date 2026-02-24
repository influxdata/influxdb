//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use async_trait::async_trait;
use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::{Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::common::{DFSchema, plan_datafusion_err};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, provider_as_source},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
        utils::{conjunction, split_conjunction},
    },
    physical_plan::{
        ExecutionPlan, expressions::col as physical_col, filter::FilterExec,
        projection::ProjectionExec,
    },
    prelude::Expr,
    sql::TableReference,
};
use schema::{Schema, sort::SortKey};
use tracing::trace;

use crate::{CHUNK_ORDER_COLUMN_NAME, QueryChunk, chunk_order_field, util::arrow_sort_key_exprs};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
pub mod overlap;
mod physical;
pub(crate) mod progressive_eval;
mod record_batch_exec;
pub(crate) mod reorder_partitions;
pub use self::overlap::group_potential_duplicates;
pub use deduplicate::{DeduplicateExec, RecordBatchDeduplicator};
pub(crate) use physical::{PartitionedFileExt, chunks_to_physical_nodes};

pub(crate) use record_batch_exec::RecordBatchesExec;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal error: no chunk pruner provided to builder for {}",
        table_name,
    ))]
    InternalNoChunkPruner { table_name: String },

    #[snafu(display("Internal error: Cannot create projection select expr '{}'", source,))]
    InternalSelectExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding sort operator '{}'", source,))]
    InternalSort {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding filter operator '{}'", source,))]
    InternalFilter {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding projection operator '{}'", source,))]
    InternalProjection {
        source: datafusion::error::DataFusionError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        match e {
            e @ Error::InternalNoChunkPruner { .. } => Self::External(Box::new(e)),
            Error::InternalSelectExpr { source }
            | Error::InternalSort { source }
            | Error::InternalFilter { source }
            | Error::InternalProjection { source } => source,
        }
    }
}

/// Builds a `ChunkTableProvider` from a series of `QueryChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder {
    table_name: Arc<str>,
    schema: Schema,
    chunks: Vec<Arc<dyn QueryChunk>>,
    deduplication: bool,
}

impl ProviderBuilder {
    pub fn new(table_name: Arc<str>, schema: Schema) -> Self {
        assert_eq!(schema.find_index_of(CHUNK_ORDER_COLUMN_NAME), None);

        Self {
            table_name,
            schema,
            chunks: Vec::new(),
            deduplication: true,
        }
    }

    pub fn with_enable_deduplication(mut self, enable_deduplication: bool) -> Self {
        self.deduplication = enable_deduplication;
        self
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<dyn QueryChunk>) -> Self {
        self.chunks.push(chunk);
        self
    }

    /// Create the Provider
    pub fn build(self) -> Result<ChunkTableProvider> {
        Ok(ChunkTableProvider {
            iox_schema: self.schema,
            table_name: self.table_name,
            chunks: self.chunks,
            deduplication: self.deduplication,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of QueryChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug, Clone)]
pub struct ChunkTableProvider {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    /// The chunks
    chunks: Vec<Arc<dyn QueryChunk>>,
    /// do deduplication
    deduplication: bool,
}

impl ChunkTableProvider {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> &Schema {
        &self.iox_schema
    }

    /// Return the Arrow schema view for the data provided by this provider
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.iox_schema.as_arrow()
    }

    /// Return the table name
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    /// Running deduplication or not
    pub fn deduplication(&self) -> bool {
        self.deduplication
    }

    /// Convert into a logical plan builder.
    pub fn into_logical_plan_builder(
        self: Arc<Self>,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let table_name = self.table_name().to_owned();
        let source = provider_as_source(self as _);

        // Scan all columns (DataFusion optimizer will prune this
        // later if possible)
        let projection = None;

        // Do not parse the tablename as a SQL identifer, but use as is
        let table_ref = TableReference::bare(table_name);
        LogicalPlanBuilder::scan(table_ref, source, projection)
    }
}

#[async_trait]
impl TableProvider for ChunkTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Schema with all available columns across all chunks
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema()
    }

    /// Creates a plan like the following:
    ///
    /// ```text
    /// Project (keep only columns needed in the rest of the plan)
    ///   Filter (optional, apply any push down predicates)
    ///     Deduplicate (optional, if chunks overlap)
    ///       ... Scan of Chunks (RecordBatchExec / DataSourceExec / UnionExec, etc) ...
    /// ```
    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        trace!("Create a scan node for ChunkTableProvider");

        let schema_with_chunk_order = Arc::new(ArrowSchema::new(
            self.iox_schema
                .as_arrow()
                .fields
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ));
        let pk = self.iox_schema().primary_key();
        let dedup_sort_key = SortKey::from_columns(pk.iter().copied());

        // Create data stream from chunk data. This is the most simple data stream possible and contains duplicates and
        // has no filters at all.
        let plan = chunks_to_physical_nodes(
            &schema_with_chunk_order,
            None,
            self.chunks.clone(),
            ctx.config().target_partitions(),
        );

        // De-dup before doing anything else, because all logical expressions act on de-duplicated data.
        let plan = if self.deduplication {
            let sort_exprs = arrow_sort_key_exprs(&dedup_sort_key, &plan.schema())
                .ok_or_else(|| plan_datafusion_err!("de-dup with empty sort key"))?;
            Arc::new(DeduplicateExec::new(plan, sort_exprs, true))
        } else {
            plan
        };

        // Filter as early as possible (AFTER de-dup!). Predicate pushdown will eventually push down parts of this.
        let plan = if let Some(expr) = filters.iter().cloned().reduce(|a, b| a.and(b)) {
            let maybe_expr = if !self.deduplication {
                let dedup_cols = pk.into_iter().collect::<HashSet<_>>();
                conjunction(
                    split_conjunction(&expr)
                        .into_iter()
                        .filter(|expr| {
                            expr.column_refs()
                                .into_iter()
                                .all(|c| dedup_cols.contains(c.name.as_str()))
                        })
                        .cloned(),
                )
            } else {
                Some(expr)
            };

            if let Some(expr) = maybe_expr {
                let df_schema = DFSchema::try_from(plan.schema())?;
                let filter_expr = ctx.create_physical_expr(expr, &df_schema)?;
                Arc::new(FilterExec::try_new(filter_expr, plan)?)
            } else {
                plan
            }
        } else {
            plan
        };

        // Project at last because it removes columns and hence other operations may fail. Projection pushdown will
        // optimize that later.
        // Always project because we MUST make sure that chunk order col doesn't leak to the user or to our parquet
        // files.
        let default_projection: Vec<_> = (0..self.iox_schema.len()).collect();
        let projection = projection.unwrap_or(&default_projection);
        let select_exprs = self
            .iox_schema()
            .select_by_indices(projection)
            .as_arrow()
            .fields()
            .iter()
            .map(|f| {
                let field_name = f.name();
                let physical_expr =
                    physical_col(field_name, &self.schema()).context(InternalSelectExprSnafu)?;
                Ok((physical_expr, field_name.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;

        let plan = Arc::new(ProjectionExec::try_new(select_exprs, plan)?);

        Ok(plan)
    }

    /// Filter pushdown specification
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        if self.deduplication {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        } else {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod test {
    use std::slice;

    use super::*;
    use crate::{
        exec::IOxSessionContext,
        pruning::retention_expr,
        test::{TestChunk, format_execution_plan},
    };
    use datafusion::prelude::{col, lit};

    #[tokio::test]
    async fn provider_scan_default() {
        let table_name = "t";
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .build()
            .unwrap();

        // simple plan
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        let expr = vec![lit(false)];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![TableProviderFilterPushDown::Exact]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    #[tokio::test]
    async fn provider_scan_no_dedup() {
        let table_name = "t";
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .with_enable_deduplication(false)
            .build()
            .unwrap();

        // simple plan
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        // Expressions on fields are NOT pushed down because they cannot be pushed through de-dup.
        let expr = vec![
            lit(false),
            col("tag1").eq(lit("foo")),
            col("field").eq(lit(1.0)),
        ];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact
            ]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND tag1@1 = CAST(foo AS Dictionary(Int32, Utf8))"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    #[tokio::test]
    async fn provider_scan_retention() {
        let table_name = "t";
        let pred = retention_expr(100);
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .build()
            .unwrap();

        // simple plan
        let plan = provider
            .scan(&state, None, slice::from_ref(&pred), None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), slice::from_ref(&pred), None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        let expr = vec![lit(false), pred.clone()];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact
            ]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[pred], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }
}
