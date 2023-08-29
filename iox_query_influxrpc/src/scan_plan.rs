use std::sync::Arc;

use datafusion::{common::tree_node::TreeNode, logical_expr::LogicalPlanBuilder};
use observability_deps::tracing::trace;
use predicate::Predicate;
use schema::Schema;
use snafu::{ResultExt, Snafu};

use iox_query::{
    provider::{ChunkTableProvider, ProviderBuilder},
    QueryChunk,
};

use crate::missing_columns::MissingColumnsToNull;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "gRPC planner got error adding chunk for table {}: {}",
        table_name,
        source
    ))]
    CreatingProvider {
        table_name: String,
        source: iox_query::provider::Error,
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
}

impl<'a> ScanPlanBuilder<'a> {
    pub fn new(table_name: Arc<str>, table_schema: &'a Schema) -> Self {
        Self {
            table_name,
            table_schema,
            chunks: vec![],
            predicate: None,
        }
    }

    /// Adds `chunks` to the list of Chunks to scan
    pub fn with_chunks(mut self, chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>) -> Self {
        self.chunks.extend(chunks);
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
            table_name,
            chunks,
            table_schema,
            predicate,
        } = self;

        assert!(!chunks.is_empty(), "no chunks provided");

        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(Arc::clone(&table_name), table_schema.clone())
            .with_enable_deduplication(true);

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = builder.build().context(CreatingProviderSnafu {
            table_name: table_name.as_ref(),
        })?;
        let provider = Arc::new(provider);
        let mut plan_builder = Arc::clone(&provider)
            .into_logical_plan_builder()
            .context(BuildingPlanSnafu)?;

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

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;
    use datafusion_util::test_collect_partition;
    use iox_query::{
        exec::{Executor, ExecutorType},
        test::{format_execution_plan, TestChunk},
    };
    use schema::merge::SchemaMerger;

    use super::*;

    #[tokio::test]
    async fn test_scan_plan_deduplication() {
        test_helpers::maybe_start_logging();
        // Create 2 overlapped chunks
        let (schema, chunks) = get_test_overlapped_chunks();

        // Build a logical plan with deduplication
        let scan_plan = ScanPlanBuilder::new(Arc::from("t"), &schema)
            .with_chunks(chunks)
            .build()
            .unwrap();
        let logical_plan = scan_plan.plan_builder.build().unwrap();

        // Build physical plan
        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(
            format_execution_plan(&physical_plan),
            @r###"
        ---
        - " ProjectionExec: expr=[field_int@1 as field_int, field_int2@2 as field_int2, tag1@3 as tag1, time@4 as time]"
        - "   DeduplicateExec: [tag1@3 ASC,time@4 ASC]"
        - "     SortPreservingMergeExec: [tag1@3 ASC,time@4 ASC,__chunk_order@0 ASC]"
        - "       SortExec: expr=[tag1@3 ASC,time@4 ASC,__chunk_order@0 ASC]"
        - "         RecordBatchesExec: batches_groups=2 batches=2 total_rows=9"
        "###
        );

        // Verify output data
        // Since data is merged due to deduplication, the two input chunks will be merged into one output chunk
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "{:?}",
            physical_plan.output_partitioning()
        );
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;
        // Data is sorted on tag1 & time. One row is removed due to deduplication
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z    |", // other row with the same tag1 and time is removed
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches0);
    }

    fn get_test_overlapped_chunks() -> (Schema, Vec<Arc<dyn QueryChunk>>) {
        let max_time = 70000;
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_order(1)
                .with_partition(1)
                .with_time_column_with_stats(Some(50), Some(max_time))
                .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );

        // Chunk 2 has an extra field, and only 4 rows
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_order(2)
                .with_partition(1)
                .with_time_column_with_stats(Some(28000), Some(220000))
                .with_tag_column_with_stats("tag1", Some("UT"), Some("WA"))
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        );

        let schema = SchemaMerger::new()
            .merge(chunk1.schema())
            .unwrap()
            .merge(chunk2.schema())
            .unwrap()
            .build();

        (schema, vec![chunk1, chunk2])
    }
}
