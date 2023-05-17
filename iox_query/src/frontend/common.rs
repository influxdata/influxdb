use std::sync::Arc;

use datafusion::{
    catalog::TableReference, common::tree_node::TreeNode, datasource::provider_as_source,
    logical_expr::LogicalPlanBuilder,
};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::Schema;
use snafu::{ResultExt, Snafu};

use crate::{
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
    pub fn schema(&self) -> &Schema {
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
    table_name: Arc<str>,
    /// The schema of the resulting table (any chunks that don't have
    /// all the necessary columns will be extended appropriately)
    table_schema: &'a Schema,
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: Option<&'a Predicate>,
    /// Do deduplication
    deduplication: bool,
}

impl<'a> ScanPlanBuilder<'a> {
    pub fn new(table_name: Arc<str>, table_schema: &'a Schema) -> Self {
        Self {
            table_name,
            table_schema,
            chunks: vec![],
            predicate: None,
            // always do deduplication in query
            deduplication: true,
        }
    }

    /// Adds `chunks` to the list of Chunks to scan
    pub fn with_chunks(mut self, chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>) -> Self {
        self.chunks.extend(chunks.into_iter());
        self
    }

    /// Sets the predicate
    pub fn with_predicate(mut self, predicate: &'a Predicate) -> Self {
        assert!(self.predicate.is_none());
        self.predicate = Some(predicate);
        self
    }

    /// Deduplication
    pub fn enable_deduplication(mut self, deduplication: bool) -> Self {
        self.deduplication = deduplication;
        self
    }

    /// Creates a `ScanPlan` from the specified chunks
    pub fn build(self) -> Result<ScanPlan> {
        let Self {
            table_name,
            chunks,
            table_schema,
            predicate,
            deduplication,
        } = self;

        assert!(!chunks.is_empty(), "no chunks provided");

        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(Arc::clone(&table_name), table_schema.clone())
            .with_enable_deduplication(deduplication);

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = builder.build().context(CreatingProviderSnafu {
            table_name: table_name.as_ref(),
        })?;

        let provider = Arc::new(provider);
        let source = provider_as_source(Arc::clone(&provider) as _);

        // Scan all columns (DataFusion optimizer will prune this
        // later if possible)
        let projection = None;

        // Do not parse the tablename as a SQL identifer, but use as is
        let table_ref = TableReference::bare(table_name.to_string());
        let mut plan_builder =
            LogicalPlanBuilder::scan(table_ref, source, projection).context(BuildingPlanSnafu)?;

        // Use a filter node to add general predicates + timestamp
        // range, if any
        if let Some(predicate) = predicate {
            if let Some(filter_expr) = predicate.filter_expr() {
                // Rewrite expression so it only refers to columns in this chunk
                let schema = provider.iox_schema();
                trace!(%table_name, ?filter_expr, "Adding filter expr");
                let mut rewriter = MissingColumnsToNull::new(schema);
                let filter_expr =
                    filter_expr
                        .rewrite(&mut rewriter)
                        .context(RewritingFilterPredicateSnafu {
                            table_name: table_name.as_ref(),
                        })?;

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
