//! Handle all requests from Querier

use arrow::record_batch::RecordBatch;
use arrow_util::util::merge_record_batches;
use datafusion::{
    error::DataFusionError,
    physical_plan::{
        common::SizedRecordBatchStream,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics},
        SendableRecordBatchStream,
    },
};
use iox_catalog::interface::SequencerId;
use predicate::Predicate;
use query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use schema::{merge::merge_record_batch_schemas, selection::Selection};
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;

use crate::data::{
    self, IngesterData, IngesterQueryRequest, IngesterQueryResponse, QueryableBatch,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(display("Error building logical plan for querying Ingester data to send to Querier"))]
    LogicalPlan {
        source: query::frontend::reorg::Error,
    },

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

    #[snafu(display("Error collecting a stream of record batches: {}", source))]
    CollectStream { source: DataFusionError },

    #[snafu(display(
        "No Table Data found for the given sequencer id {}, namespace name {}, table name {}", sequencer_id.get(), namespace_name, table_name
    ))]
    TableNotFound {
        sequencer_id: SequencerId,
        namespace_name: String,
        table_name: String,
    },

    #[snafu(display("Error snapshotting non-persisting data: {}", source))]
    SnapshotNonPersistingData { source: data::Error },

    #[snafu(display("Error concating same-schema record batches: {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return data to send as a response back to the Querier per its request
pub async fn prepare_data_to_querier(
    ingest_data: &Arc<IngesterData>,
    request: &IngesterQueryRequest,
) -> Result<IngesterQueryResponse> {
    // ------------------------------------------------
    //  Read the IngesterData to get TableData for the given request's table
    let table_data =
        ingest_data.table_data(request.sequencer_id, &request.namespace, &request.table);
    let table_data = table_data.context(TableNotFoundSnafu {
        sequencer_id: request.sequencer_id,
        namespace_name: &request.namespace,
        table_name: &request.table,
    })?;

    // ------------------------------------------------
    // Accumulate data from each partition
    let partition_keys = table_data.partition_keys();

    // Make Filters
    let selection_columns: Vec<_> = request.columns.iter().map(String::as_str).collect();
    let selection = if selection_columns.is_empty() {
        Selection::All
    } else {
        Selection::Some(&selection_columns)
    };
    let predicate = request.predicate.clone().unwrap_or_default();

    let mut filter_not_applied_batches = vec![];
    let mut filter_applied_batches = vec![];
    for p_key in partition_keys {
        let partition = table_data
            .partition_data(&p_key)
            .expect("Partition should have been found");

        // ------------------------------------------------
        // not-yet-in-persisting data

        // Collect non-persisting snpashots without tombstones because all tombstones (if any) were already applied
        let non_persisting_batches = partition
            .get_non_persisting_data()
            .context(SnapshotNonPersistingDataSnafu)?;

        // Collect the snapshots' record bacthes
        // Note that these record batches may have different schema
        filter_not_applied_batches.extend(non_persisting_batches);

        // ------------------------------------------------
        // persisting data

        // Get persting data and tombstones that are not applied yet
        let persisting_queryable_batch = partition.get_persisting_data();

        // Run query on the queryable batch to apply new tombstones & request filters
        if let Some(queryable_batch) = persisting_queryable_batch {
            let record_batch = run_query(
                &ingest_data.exec,
                Arc::new(queryable_batch),
                predicate.clone(),
                selection,
            )
            .await?;

            if let Some(record_batch) = record_batch {
                if record_batch.num_rows() > 0 {
                    filter_applied_batches.push(Arc::new(record_batch));
                }
            }
        }
    }

    // ------------------------------------------------
    // Apply filters on the snapshot bacthes
    if !filter_not_applied_batches.is_empty() {
        // Make a Query able batch for all the snapshot
        let queryable_batch =
            QueryableBatch::new(&request.table, filter_not_applied_batches, vec![]);

        let record_batch = run_query(
            &ingest_data.exec,
            Arc::new(queryable_batch),
            predicate,
            selection,
        )
        .await?;

        if let Some(record_batch) = record_batch {
            if record_batch.num_rows() > 0 {
                filter_applied_batches.push(Arc::new(record_batch));
            }
        }
    }

    // ------------------------------------------------
    // Combine record batches into one batch and pad null values as needed

    // Schema of all record batches after merging
    let schema = merge_record_batch_schemas(&filter_applied_batches);
    let batch = merge_record_batches(schema.as_arrow(), filter_applied_batches)
        .context(ConcatBatchesSnafu)?;

    // ------------------------------------------------
    // Make a stream for this batch
    let dummy_metrics = ExecutionPlanMetricsSet::new();
    let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
    let stream_batch = batch.map(|b| vec![Arc::new(b)]).unwrap_or_default();
    let stream = SizedRecordBatchStream::new(schema.as_arrow(), stream_batch, mem_metrics);

    Ok(IngesterQueryResponse::new(
        Box::pin(stream),
        (*schema).clone(),
        table_data.parquet_max_sequence_number(),
    ))
}

/// Query a given Queryable Batch, applying selection and filters as appropriate
/// Return one record batch
pub async fn run_query(
    executor: &Executor,
    data: Arc<QueryableBatch>,
    predicate: Predicate,
    selection: Selection<'_>,
) -> Result<Option<RecordBatch>> {
    let stream = query(executor, data, predicate, selection).await?;

    let record_batches = datafusion::physical_plan::common::collect(stream)
        .await
        .context(CollectStreamSnafu)?;

    if record_batches.is_empty() {
        return Ok(None);
    }

    // concat all same-schema record batches into one batch
    let record_batch = RecordBatch::concat(&record_batches[0].schema(), &record_batches)
        .context(ConcatBatchesSnafu)?;

    Ok(Some(record_batch))
}

/// Query a given Queryable Batch, applying selection and filters as appropriate
/// Return stream of record batches
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

    // TODO: Since we have different type of servers (router, ingester, compactor, and querier),
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
        make_ingester_data, make_ingester_data_with_tombstones, make_queryable_batch,
        make_queryable_batch_with_deletes, DataLocation, TEST_NAMESPACE, TEST_TABLE,
    };
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
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

        exc.join().await;
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

        exc.join().await;
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

        exc.join().await;
    }

    #[tokio::test]
    async fn test_prepare_data_to_querier() {
        test_helpers::maybe_start_logging();

        // make 14 scenarios for ingester data
        let mut scenarios = vec![];
        for two_partitions in [false, true] {
            for loc in [
                DataLocation::BUFFER,
                DataLocation::BUFFER_SNAPSHOT,
                DataLocation::BUFFER_PERSISTING,
                DataLocation::BUFFER_SNAPSHOT_PERSISTING,
                DataLocation::SNAPSHOT,
                DataLocation::SNAPSHOT_PERSISTING,
                DataLocation::PERSISTING,
            ] {
                let scenario = Arc::new(make_ingester_data(two_partitions, loc));
                scenarios.push(scenario);
            }
        }

        // read data from all scenarios without any filters
        let mut request = IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            SequencerId::new(1), // must be 1
            TEST_TABLE.to_string(),
            vec![],
            None,
        );
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |", // in group 1 - seq_num: 2
            "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 2 - seq_num: 3
            "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |", // in group 1 - seq_num: 1
            "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 3 - seq_num: 5
            "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 7
            "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
            "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 8
            "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
            "+------------+-----+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios and filter out column day
        request.columns = vec!["city".to_string(), "temp".to_string(), "time".to_string()];
        let expected = vec![
            "+------------+------+--------------------------------+",
            "| city       | temp | time                           |",
            "+------------+------+--------------------------------+",
            "| Andover    |      | 1970-01-01T00:00:00.000000046Z |",
            "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Boston     |      | 1970-01-01T00:00:00.000000038Z |",
            "| Boston     | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Medford    |      | 1970-01-01T00:00:00.000000026Z |",
            "| Medford    | 55   | 1970-01-01T00:00:00.000000022Z |",
            "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios, filter out column day, city Medford, time outside range [0, 42)
        request.columns = ["city", "temp", "time"].map(Into::into).into();
        let expr = col("city").not_eq(lit("Medford"));
        let pred = PredicateBuilder::default()
            .add_expr(expr)
            .timestamp_range(0, 42)
            .build();
        request.predicate = Some(pred);
        let expected = vec![
            "+------------+------+--------------------------------+",
            "| city       | temp | time                           |",
            "+------------+------+--------------------------------+",
            "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Boston     |      | 1970-01-01T00:00:00.000000038Z |",
            "| Boston     | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }
    }

    #[tokio::test]
    async fn test_prepare_data_to_querier_with_tombstones() {
        test_helpers::maybe_start_logging();

        // make 7 scenarios for ingester data with tombstones
        let mut scenarios = vec![];
        for loc in &[
            DataLocation::BUFFER,
            DataLocation::BUFFER_SNAPSHOT,
            DataLocation::BUFFER_PERSISTING,
            DataLocation::BUFFER_SNAPSHOT_PERSISTING,
            DataLocation::SNAPSHOT,
            DataLocation::SNAPSHOT_PERSISTING,
            DataLocation::PERSISTING,
        ] {
            let scenario = Arc::new(make_ingester_data_with_tombstones(*loc).await);
            scenarios.push(scenario);
        }

        // read data from all scenarios without any filters
        let mut request = IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            SequencerId::new(1), // must be 1
            TEST_TABLE.to_string(),
            vec![],
            None,
        );
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |",
            "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |",
            "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |",
            "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios and filter out column day
        request.columns = vec!["city".to_string(), "temp".to_string(), "time".to_string()];
        let expected = vec![
            "+------------+------+--------------------------------+",
            "| city       | temp | time                           |",
            "+------------+------+--------------------------------+",
            "| Andover    |      | 1970-01-01T00:00:00.000000046Z |",
            "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Medford    |      | 1970-01-01T00:00:00.000000026Z |",
            "| Medford    | 55   | 1970-01-01T00:00:00.000000022Z |",
            "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios, filter out column day, city Medford, time outside range [0, 42)
        request.columns = vec!["city".to_string(), "temp".to_string(), "time".to_string()];
        let expr = col("city").not_eq(lit("Medford"));
        let pred = PredicateBuilder::default()
            .add_expr(expr)
            .timestamp_range(0, 42)
            .build();
        request.predicate = Some(pred);
        let expected = vec![
            "+------------+------+--------------------------------+",
            "| city       | temp | time                           |",
            "+------------+------+--------------------------------+",
            "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
        }
    }
}
