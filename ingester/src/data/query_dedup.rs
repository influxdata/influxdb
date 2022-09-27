use std::sync::Arc;

use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_query::{
    exec::{Executor, ExecutorType},
    QueryChunk, QueryChunkMeta, ScanPlanBuilder,
};
use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};

use crate::query::QueryableBatch;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error creating plan for querying Ingester data to send to Querier"))]
    Frontend {
        source: iox_query::frontend::common::Error,
    },

    #[snafu(display("Error building logical plan for querying Ingester data to send to Querier"))]
    LogicalPlan { source: DataFusionError },

    #[snafu(display(
        "Error building physical plan for querying Ingester data to send to Querier: {}",
        source
    ))]
    PhysicalPlan { source: DataFusionError },

    #[snafu(display(
        "Error executing the query for getting Ingester data to send to Querier: {}",
        source
    ))]
    ExecutePlan { source: DataFusionError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query a given Queryable Batch, applying selection and filters as appropriate
/// Return stream of record batches
pub(crate) async fn query(
    executor: &Executor,
    data: Arc<QueryableBatch>,
) -> Result<SendableRecordBatchStream> {
    // Build logical plan for filtering data
    // Note that this query will also apply the delete predicates that go with the QueryableBatch

    // TODO: Since we have different type of servers (router,
    // ingester, compactor, and querier), we may want to add more
    // types into the ExecutorType to have better log and resource
    // managment
    let ctx = executor.new_context(ExecutorType::Query);

    // Creates an execution plan for a scan and filter data of a single chunk
    let schema = data.schema();
    let table_name = data.table_name().to_string();

    debug!(%table_name, "Creating single chunk scan plan");

    let logical_plan = ScanPlanBuilder::new(schema, ctx.child_ctx("scan_and_filter planning"))
        .with_chunks([data as _])
        .build()
        .context(FrontendSnafu)?
        .plan_builder
        .build()
        .context(LogicalPlanSnafu)?;

    debug!(%table_name, plan=%logical_plan.display_indent_schema(),
           "created single chunk scan plan");

    // Build physical plan
    let physical_plan = ctx
        .create_physical_plan(&logical_plan)
        .await
        .context(PhysicalPlanSnafu {})?;

    // Execute the plan and return the filtered stream
    let output_stream = ctx
        .execute_stream(physical_plan)
        .await
        .context(ExecutePlanSnafu {})?;

    Ok(output_stream)
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;

    use super::*;
    use crate::test_util::{
        create_one_record_batch_with_influxtype_no_duplicates, create_tombstone,
        make_queryable_batch, make_queryable_batch_with_deletes,
    };

    #[tokio::test]
    async fn test_query() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;

        // build queryable batch from the input batches
        let batch = make_queryable_batch("test_table", 0, 1, batches);

        // query without filters
        let exc = Executor::new(1);
        let stream = query(&exc, batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify data: all rows and columns should be returned
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);

        exc.join().await;
    }

    #[tokio::test]
    async fn test_query_with_delete() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let tombstones = vec![create_tombstone(1, 1, 1, 1, 0, 200000, "tag1=UT")];

        // build queryable batch from the input batches
        let batch = make_queryable_batch_with_deletes("test_table", 0, 1, batches, tombstones);

        let exc = Executor::new(1);
        let stream = query(&exc, batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify data:
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);

        exc.join().await;
    }
}
