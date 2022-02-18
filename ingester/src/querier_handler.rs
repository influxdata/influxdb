//! Handle all requests from Querier

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use predicate::Predicate;
use query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use schema::selection::Selection;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

use crate::data::{IngesterQueryRequest, IngesterQueryResponse, QueryableBatch};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(display(
        "Error while building logical plan for querying Ingester data to send to Querier"
    ))]
    LogicalPlan {
        source: query::frontend::reorg::Error,
    },

    #[snafu(display(
        "Error while building physical plan for querying Ingester data to send to Querier"
    ))]
    PhysicalPlan { source: DataFusionError },

    #[snafu(display(
        "Error while executing the query for getting Ingester data to send to Querier"
    ))]
    ExecutePlan { source: DataFusionError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return data to send as a response back to the Querier's request
pub async fn prepare_data_to_querier(
    _request: &IngesterQueryRequest,
) -> Result<IngesterQueryResponse> {
    // Steps:
    //  1. Read the IngesterData to get TableData for the given request's table
    //  3. For each PartitionData of the TableData above, get its DataBuffer by:
    //     3.1. Snapshot the buffer by invoking DataBuffer::snapshot + implement #3699 to add the right tombstones
    //     3.2. Converting all N snapshots to N QueryableBatches
    //     3.3. Convert PersistingBatch into a QueryableBatch and add just-arrived tomstones if any (See #3699)
    //     3.4. For each QueryableBatch produced in 3.2 and 3.3, invoke the avaialble `query` function to filter data per request.
    //          The request's columns, time range, and predicates will be applied at this step. Each will return a RecordBatch.
    //     3.5. Merge RecordBatches into one RecordBatch
    //  4. Send the RecordBatch to Querier. Or if possible (TBD with Carol), we should send them as a stream amd do not need to merge record batches

    let schema = Arc::new(ArrowSchema::new(vec![]));
    let stream = datafusion::physical_plan::EmptyRecordBatchStream::new(Arc::clone(&schema));

    Ok(IngesterQueryResponse::new(
        Box::pin(stream),
        schema.try_into().unwrap(),
        None,
    ))
}

/// Query a given Queryable Batch, applying selection and filters as appropriate
pub async fn query(
    executor: &Executor,
    data: Arc<QueryableBatch>,
    predicate: Predicate,
    selection: Selection<'_>,
) -> Result<SendableRecordBatchStream> {
    // Build logical plan for filtering data
    // Note that this query will also apply the delete predicates that go with the QueryableBatch

    let indices = match selection {
        Selection::All => None,
        Selection::Some(columns) => Some(
            data.schema()
                .compute_select_indicies(columns)
                .context(SelectColumnsSnafu)?,
        ),
    };

    let mut expr = vec![];
    if let Some(filter_expr) = predicate.filter_expr() {
        expr.push(filter_expr);
    }

    // TODO: Since we no whave different type of servers (router, ingester, compactor, and querier),
    // we may want to add more types into the ExecutorType to have better log and resource managment
    let ctx = executor.new_context(ExecutorType::Query);
    let logical_plan = ReorgPlanner::new()
        .scan_single_chunk_plan_with_filter(data.schema(), data, indices, expr)
        .context(LogicalPlanSnafu {})?;

    // Build physical plan
    let physical_plan = ctx
        .prepare_plan(&logical_plan)
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
    use super::*;
    use crate::test_util::{
        create_one_record_batch_with_influxtype_no_duplicates, create_tombstone,
        make_queryable_batch, make_queryable_batch_with_deletes,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::logical_plan::{col, lit};
    use predicate::PredicateBuilder;

    #[tokio::test]
    async fn test_query() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;

        // build queryable batch from the input batches
        let batch = make_queryable_batch("test_table", 1, batches);

        // query without filters
        let exc = Executor::new(1);
        let stream = query(&exc, batch, Predicate::default(), Selection::All)
            .await
            .unwrap();
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
    }

    #[tokio::test]
    async fn test_query_with_filter() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;

        // build queryable batch from the input batches
        let batch = make_queryable_batch("test_table", 1, batches);

        // make filters
        // Only read 2 columns: "tag1" and "time"
        let selection = Selection::Some(&["tag1", "time"]);

        // tag1=VT
        let expr = col("tag1").eq(lit("VT"));
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let exc = Executor::new(1);
        let stream = query(&exc, batch, pred, selection).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify data: 2  columns and one row of "tag1=VT" should be returned
        let expected = vec![
            "+------+-----------------------------+",
            "| tag1 | time                        |",
            "+------+-----------------------------+",
            "| VT   | 1970-01-01T00:00:00.000010Z |",
            "+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_query_with_filter_with_delete() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let tombstones = vec![create_tombstone(1, 1, 1, 1, 0, 200000, "tag1=UT")];

        // build queryable batch from the input batches
        let batch = make_queryable_batch_with_deletes("test_table", 1, batches, tombstones);

        // make filters
        // Only read 2 columns: "tag1" and "time"
        let selection = Selection::Some(&["tag1", "time"]);

        // tag1=UT
        let expr = col("tag1").eq(lit("UT"));
        let pred = PredicateBuilder::default().add_expr(expr).build();

        let exc = Executor::new(1);
        let stream = query(&exc, batch, pred, selection).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify data: return nothing because the selected row already deleted
        let expected = vec!["++", "++"];
        assert_batches_eq!(&expected, &output_batches);
    }
}
