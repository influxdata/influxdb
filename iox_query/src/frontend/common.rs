use std::sync::Arc;

use datafusion::logical_plan::{provider_as_source, ExprRewritable, LogicalPlanBuilder};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::Schema;
use snafu::{ResultExt, Snafu};

use crate::{
    exec::IOxSessionContext, provider::ProviderBuilder, util::MissingColumnsToNull, QueryChunk,
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

pub(crate) struct TableScanAndFilter {
    /// Represents plan that scans a table and applies optional filtering
    pub plan_builder: LogicalPlanBuilder,
    /// The IOx schema of the result
    pub schema: Arc<Schema>,
}

/// Create a plan that scans the specified table, and applies any
/// filtering specified on the predicate, if any.
///
/// If the table can produce no rows based on predicate
/// evaluation, returns Ok(None)
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
pub(crate) fn scan_and_filter(
    ctx: IOxSessionContext,
    table_name: &str,
    schema: Arc<Schema>,
    predicate: &Predicate,
    chunks: Vec<Arc<dyn QueryChunk>>,
) -> Result<Option<TableScanAndFilter>> {
    // Scan all columns to begin with (DataFusion projection
    // push-down optimization will prune out unneeded columns later)
    let projection = None;

    // Prepare the scan of the table
    let mut builder = ProviderBuilder::new(table_name, schema)
        .with_execution_context(ctx.child_ctx("provider_builder"));

    // No extra pruning (assumes the caller has already done this)
    builder = builder.add_no_op_pruner();

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
    let schema = provider.iox_schema();

    let source = provider_as_source(Arc::new(provider));

    let mut plan_builder =
        LogicalPlanBuilder::scan(table_name, source, projection).context(BuildingPlanSnafu)?;

    // Use a filter node to add general predicates + timestamp
    // range, if any
    if let Some(filter_expr) = predicate.filter_expr() {
        // Rewrite expression so it only refers to columns in this chunk
        trace!(table_name, ?filter_expr, "Adding filter expr");
        let mut rewriter = MissingColumnsToNull::new(&schema);
        let filter_expr = filter_expr
            .rewrite(&mut rewriter)
            .context(RewritingFilterPredicateSnafu { table_name })?;

        trace!(?filter_expr, "Rewritten filter_expr");

        plan_builder = plan_builder
            .filter(filter_expr)
            .context(BuildingPlanSnafu)?;
    }

    Ok(Some(TableScanAndFilter {
        plan_builder,
        schema,
    }))
}
