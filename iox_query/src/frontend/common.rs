use std::sync::Arc;

use datafusion::logical_plan::{provider_as_source, ExprRewritable, LogicalPlanBuilder};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::{sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use crate::{
    exec::IOxSessionContext,
    provider::{ChunkTableProvider, ProviderBuilder},
    util::MissingColumnsToNull,
    QueryChunk,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "gRPC planner got error adding chunk for table {}: {}",
        table_name,
        source
    ))]
    CreatingProvider {
        table_name: String,
        source: crate::provider::Error,
    },

    #[snafu(display(
        "Internal gRPC planner rewriting predicate for {}: {}",
        table_name,
        source
    ))]
    RewritingFilterPredicate {
        table_name: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents scanning one or more [`QueryChunk`]s.
pub struct ScanPlan {
    pub plan_builder: LogicalPlanBuilder,
    pub provider: Arc<ChunkTableProvider>,
}

impl std::fmt::Debug for ScanPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanPlan")
            .field("plan_builder", &"<...>")
            .field("provider", &self.provider)
            .finish()
    }
}

impl ScanPlan {
    /// Return the schema of the source (the merged schema across all tables)
    pub fn schema(&self) -> Arc<Schema> {
        self.provider.iox_schema()
    }
}

/// Builder for [`ScanPlan`]s which scan the data  1 or more [`QueryChunk`] for
/// IOx's custom query frontends (InfluxRPC and Reorg at the time of
/// writing).
///
/// The created plan looks like:
///
/// ```text
///   Filter(predicate) [optional]
///     Scan
/// ```
///
/// NOTE: This function assumes the chunks have already been "pruned"
/// based on statistics and will not attempt to prune them
/// further. Some frontends like influxrpc or the reorg planner manage
/// (and thus prune) their own chunklist.

#[derive(Debug)]
pub struct ScanPlanBuilder<'a> {
    ctx: IOxSessionContext,
    table_name: Option<String>,
    /// The schema of the resulting table (any chunks that don't have
    /// all the necessary columns will be extended appropriately)
    table_schema: Arc<Schema>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    /// The sort key that describes the desired output sort order
    output_sort_key: Option<SortKey>,
    predicate: Option<&'a Predicate>,
}

impl<'a> ScanPlanBuilder<'a> {
    pub fn new(table_schema: Arc<Schema>, ctx: IOxSessionContext) -> Self {
        Self {
            ctx,
            table_name: None,
            table_schema,
            chunks: vec![],
            output_sort_key: None,
            predicate: None,
        }
    }

    /// Adds `chunks` to the list of Chunks to scan
    pub fn with_chunks(mut self, chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>) -> Self {
        self.chunks.extend(chunks.into_iter());
        self
    }

    /// Sets the desired output sort key. If the output of this plan
    /// is not already sorted this way, it will be re-sorted to conform
    /// to this key
    pub fn with_output_sort_key(mut self, output_sort_key: SortKey) -> Self {
        assert!(self.output_sort_key.is_none());
        self.output_sort_key = Some(output_sort_key);
        self
    }

    /// Sets the predicate
    pub fn with_predicate(mut self, predicate: &'a Predicate) -> Self {
        assert!(self.predicate.is_none());
        self.predicate = Some(predicate);
        self
    }

    /// Creates a `ScanPlan` from the specified chunks
    pub fn build(self) -> Result<ScanPlan> {
        let Self {
            ctx,
            table_name,
            chunks,
            output_sort_key,
            table_schema,
            predicate,
        } = self;

        assert!(!chunks.is_empty(), "no chunks provided");

        let table_name = table_name.unwrap_or_else(|| chunks[0].table_name().to_string());
        let table_name = &table_name;

        // Prepare the plan for the table
        let mut builder =
            ProviderBuilder::new(table_name, table_schema, ctx.child_ctx("provider_builder"));

        if let Some(output_sort_key) = output_sort_key {
            // Tell the scan of this provider to sort its output on the given sort_key
            builder = builder.with_output_sort_key(output_sort_key);
        }

        for chunk in chunks {
            // check that it is consistent with this table_name
            assert_eq!(
                chunk.table_name(),
                table_name,
                "Chunk {} expected table mismatch",
                chunk.id(),
            );

            builder = builder.add_chunk(chunk);
        }

        let provider = builder
            .build()
            .context(CreatingProviderSnafu { table_name })?;

        let provider = Arc::new(provider);
        let source = provider_as_source(Arc::clone(&provider) as _);

        // Scan all columns (DataFusion optimizer will prune this
        // later if possible)
        let projection = None;

        let mut plan_builder =
            LogicalPlanBuilder::scan(table_name, source, projection).context(BuildingPlanSnafu)?;

        // Use a filter node to add general predicates + timestamp
        // range, if any
        if let Some(predicate) = predicate {
            if let Some(filter_expr) = predicate.filter_expr() {
                // Rewrite expression so it only refers to columns in this chunk
                let schema = provider.iox_schema();
                trace!(%table_name, ?filter_expr, "Adding filter expr");
                let mut rewriter = MissingColumnsToNull::new(&schema);
                let filter_expr = filter_expr
                    .rewrite(&mut rewriter)
                    .context(RewritingFilterPredicateSnafu { table_name })?;

                trace!(?filter_expr, "Rewritten filter_expr");

                plan_builder = plan_builder
                    .filter(filter_expr)
                    .context(BuildingPlanSnafu)?;
            }
        }

        Ok(ScanPlan {
            plan_builder,
            provider,
        })
    }
}
