//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use async_trait::async_trait;
use data_types::DeletePredicate;
use hashbrown::HashMap;
use std::{collections::HashSet, sync::Arc};

use arrow::{
    datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    error::ArrowError,
};
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::{conjunction, split_conjunction},
    physical_plan::{
        expressions::col as physical_col, filter::FilterExec, projection::ProjectionExec,
        sorts::sort::SortExec, ExecutionPlan,
    },
    prelude::Expr,
};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::{sort::SortKey, Schema};

use crate::{
    chunk_order_field,
    util::{arrow_sort_key_exprs, df_physical_expr},
    QueryChunk, CHUNK_ORDER_COLUMN_NAME,
};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
pub mod overlap;
mod physical;
mod record_batch_exec;
pub use self::overlap::group_potential_duplicates;
pub use deduplicate::{DeduplicateExec, RecordBatchDeduplicator};
pub(crate) use physical::{chunks_to_physical_nodes, PartitionedFileExt};

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

impl From<Error> for ArrowError {
    // Wrap an error into an arrow error
    fn from(e: Error) -> Self {
        Self::ExternalError(Box::new(e))
    }
}

impl From<Error> for DataFusionError {
    // Wrap an error into a datafusion error
    fn from(e: Error) -> Self {
        Self::ArrowError(e.into())
    }
}

/// Something that can prune chunks based on their metadata
pub trait ChunkPruner: Sync + Send + std::fmt::Debug {
    /// prune `chunks`, if possible, based on predicate.
    fn prune_chunks(
        &self,
        table_name: &str,
        table_schema: &Schema,
        chunks: Vec<Arc<dyn QueryChunk>>,
        predicate: &Predicate,
    ) -> Result<Vec<Arc<dyn QueryChunk>>>;
}

/// Builds a `ChunkTableProvider` from a series of `QueryChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder {
    table_name: Arc<str>,
    schema: Schema,
    chunks: Vec<Arc<dyn QueryChunk>>,
    output_sort_key: Option<SortKey>,
    deduplication: bool,
}

impl ProviderBuilder {
    pub fn new(table_name: Arc<str>, schema: Schema) -> Self {
        assert_eq!(schema.find_index_of(CHUNK_ORDER_COLUMN_NAME), None);

        Self {
            table_name,
            schema,
            chunks: Vec::new(),
            output_sort_key: None,
            deduplication: true,
        }
    }

    pub fn with_enable_deduplication(mut self, enable_deduplication: bool) -> Self {
        self.deduplication = enable_deduplication;
        self
    }

    /// Produce sorted output specified by sort_key
    pub fn with_output_sort_key(self, output_sort_key: SortKey) -> Self {
        Self {
            output_sort_key: Some(output_sort_key),
            ..self
        }
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
            output_sort_key: self.output_sort_key,
            deduplication: self.deduplication,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of QueryChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug)]
pub struct ChunkTableProvider {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    /// The chunks
    chunks: Vec<Arc<dyn QueryChunk>>,
    /// The desired output sort key if any
    output_sort_key: Option<SortKey>,
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

    async fn scan(
        &self,
        ctx: &SessionState,
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
                .collect(),
        ));
        let pk = self.iox_schema().primary_key();
        let dedup_sort_key = SortKey::from_columns(pk.iter().copied());

        let mut chunks_by_delete_predicates =
            HashMap::<&[Arc<DeletePredicate>], Vec<Arc<dyn QueryChunk>>>::new();
        for chunk in &self.chunks {
            chunks_by_delete_predicates
                .entry(chunk.delete_predicates())
                .or_default()
                .push(Arc::clone(chunk));
        }

        let (delete_predicates, chunks) = match chunks_by_delete_predicates.len() {
            0 => (&[] as _, vec![]),
            1 => chunks_by_delete_predicates
                .into_iter()
                .next()
                .expect("checked len"),
            n => {
                return Err(DataFusionError::External(
                    format!("expected at most 1 delete predicate set, got {n}").into(),
                ));
            }
        };

        // Create data stream from chunk data. This is the most simple data stream possible and contains duplicates and
        // has no filters at all.
        let plan = chunks_to_physical_nodes(
            &schema_with_chunk_order,
            None,
            chunks,
            ctx.config().target_partitions(),
        );

        // De-dup before doing anything else, because all logical expressions act on de-duplicated data.
        let plan = if self.deduplication {
            let sort_exprs = arrow_sort_key_exprs(&dedup_sort_key, &plan.schema());
            Arc::new(DeduplicateExec::new(plan, sort_exprs, true))
        } else {
            plan
        };

        // Convert delete predicates to DF
        let del_preds: Vec<Arc<Predicate>> = delete_predicates
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();
        let negated_del_expr_val = Predicate::negated_expr(&del_preds[..]);

        // Filter as early as possible (AFTER de-dup!). Predicate pushdown will eventually push down parts of this.
        let plan = if let Some(expr) = filters
            .iter()
            .cloned()
            .chain(negated_del_expr_val)
            .reduce(|a, b| a.and(b))
        {
            let maybe_expr = if !self.deduplication {
                let dedup_cols = pk.into_iter().collect::<HashSet<_>>();
                conjunction(
                    split_conjunction(&expr)
                        .into_iter()
                        .filter(|expr| {
                            let Ok(expr_cols) = expr.to_columns() else {return false};
                            expr_cols
                                .into_iter()
                                .all(|c| dedup_cols.contains(c.name.as_str()))
                        })
                        .cloned(),
                )
            } else {
                Some(expr)
            };

            if let Some(expr) = maybe_expr {
                Arc::new(FilterExec::try_new(
                    df_physical_expr(plan.as_ref(), expr)?,
                    plan,
                )?)
            } else {
                plan
            }
        } else {
            plan
        };

        // Sort after filter to reduce potential work.
        let plan = if let Some(output_sort_key) = self.output_sort_key.as_ref() {
            let sort_exprs = arrow_sort_key_exprs(output_sort_key, &self.arrow_schema());
            Arc::new(SortExec::try_new(sort_exprs, plan, None)?)
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
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        if self.deduplication {
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            Ok(TableProviderFilterPushDown::Inexact)
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        exec::IOxSessionContext,
        test::{format_execution_plan, TestChunk},
        QueryChunkMeta,
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "       ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "       ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "       ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "     ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "     ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND tag1@1 = CAST(foo AS Dictionary(Int32, Utf8))"
        - "     UnionExec"
        - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "       ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "     ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );
    }

    #[tokio::test]
    async fn provider_scan_sorted() {
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
            .with_output_sort_key(SortKey::from_columns(["tag2", "tag1"]))
            .build()
            .unwrap();

        // simple plan
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   SortExec: expr=[tag2@2 ASC,tag1@1 ASC]"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   SortExec: expr=[tag2@2 ASC,tag1@1 ASC]"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   SortExec: expr=[tag2@2 ASC,tag1@1 ASC]"
        - "     FilterExec: false"
        - "       DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "         UnionExec"
        - "           RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "           ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   SortExec: expr=[tag2@2 ASC,tag1@1 ASC]"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );
    }

    #[tokio::test]
    async fn provider_scan_retention() {
        let table_name = "t";
        let pred = Arc::new(DeletePredicate::retention_delete_predicate(100));
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column()
                .with_delete_predicate(Arc::clone(&pred)),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column()
                .with_delete_predicate(Arc::clone(&pred)),
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 < -9223372036854775808 OR time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   FilterExec: time@3 < -9223372036854775808 OR time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
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
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND (time@3 < -9223372036854775808 OR time@3 > 100)"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 < -9223372036854775808 OR time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        - "         ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, output_ordering=[__chunk_order@4 ASC], projection=[field, tag1, tag2, time, __chunk_order]"
        "###
        );
    }

    #[tokio::test]
    async fn provider_scan_multiple_delete_predicates() {
        let table_name = "t";
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column()
                .with_delete_predicate(Arc::new(DeletePredicate::retention_delete_predicate(100))),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column()
                .with_delete_predicate(Arc::new(DeletePredicate::retention_delete_predicate(200))),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .build()
            .unwrap();

        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "External error: expected at most 1 delete predicate set, got 2"
        );
    }
}
