//! Handle all requests from Querier

use crate::data::{
    self, IngesterData, IngesterQueryResponse, QueryableBatch, UnpersistedPartitionData,
};
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
use generated_types::ingester::IngesterQueryRequest;
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use observability_deps::tracing::debug;
use predicate::Predicate;
use schema::{
    merge::{merge_record_batch_schemas, SchemaMerger},
    selection::Selection,
    Schema,
};
use snafu::{ensure, ResultExt, Snafu};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(display("Error building logical plan for querying Ingester data to send to Querier"))]
    LogicalPlan {
        source: iox_query::frontend::reorg::Error,
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
        "No Namespace Data found for the given namespace name {}",
        namespace_name,
    ))]
    NamespaceNotFound { namespace_name: String },

    #[snafu(display(
        "No Table Data found for the given namespace name {}, table name {}",
        namespace_name,
        table_name
    ))]
    TableNotFound {
        namespace_name: String,
        table_name: String,
    },

    #[snafu(display("Error snapshotting non-persisting data: {}", source))]
    SnapshotNonPersistingData { source: data::Error },

    #[snafu(display("Error concating same-schema record batches: {}", source))]
    ConcatBatches { source: arrow::error::ArrowError },

    #[snafu(display("Error merging schemas: {}", source))]
    MergeSchema { source: schema::merge::Error },

    #[snafu(display("Concurrent query request limit exceeded"))]
    RequestLimit,
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return data to send as a response back to the Querier per its request
pub async fn prepare_data_to_querier(
    ingest_data: &Arc<IngesterData>,
    request: &IngesterQueryRequest,
) -> Result<IngesterQueryResponse> {
    debug!(?request, "prepare_data_to_querier");
    let mut schema_merger = SchemaMerger::new();
    let mut unpersisted_partitions = BTreeMap::new();
    let mut found_namespace = false;
    let mut batches = vec![];
    let mut batch_partition_ids = vec![];
    for (sequencer_id, sequencer_data) in ingest_data.sequencers() {
        debug!(sequencer_id=%sequencer_id.get());
        let namespace_data = match sequencer_data.namespace(&request.namespace) {
            Some(namespace_data) => {
                debug!(namespace=%request.namespace, "found namespace");
                found_namespace = true;
                namespace_data
            }
            None => {
                continue;
            }
        };

        let table_data = match namespace_data.table_data(&request.table) {
            Some(table_data) => {
                debug!(table_name=%request.table, "found table");
                table_data
            }
            None => {
                continue;
            }
        };

        let unpersisted_partition_data = {
            let table_data = table_data.read().await;
            table_data.unpersisted_partition_data()
        };
        debug!(?unpersisted_partition_data);

        for partition in unpersisted_partition_data {
            // include partition in `unpersisted_partitions` even when there we might filter out all the data, because
            // the metadata (e.g. max persisted parquet file) is important for the querier.
            unpersisted_partitions
                .insert(partition.partition_id, partition.partition_status.clone());

            // extract payload
            let partition_id = partition.partition_id;
            let (schema, batch) =
                prepare_data_to_querier_for_partition(ingest_data.exec(), partition, request)
                    .await?;
            schema_merger = schema_merger
                .merge(schema.as_ref())
                .context(MergeSchemaSnafu)?;
            if let Some(batch) = batch {
                batches.push(Arc::new(batch));
                batch_partition_ids.push(partition_id);
            }
        }
    }

    ensure!(
        found_namespace,
        NamespaceNotFoundSnafu {
            namespace_name: &request.namespace,
        },
    );
    debug!(?unpersisted_partitions);
    ensure!(
        !unpersisted_partitions.is_empty(),
        TableNotFoundSnafu {
            namespace_name: &request.namespace,
            table_name: &request.table
        },
    );
    let schema = schema_merger.build();
    debug!(
        num_batches=%batches.len(),
        table_name=%request.table,
        "prepare_data_to_querier found batches"
    );

    // ------------------------------------------------
    // Make a stream for this batch
    let dummy_metrics = ExecutionPlanMetricsSet::new();
    let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
    let stream = SizedRecordBatchStream::new(schema.as_arrow(), batches, mem_metrics);

    Ok(IngesterQueryResponse::new(
        Box::pin(stream),
        schema,
        unpersisted_partitions,
        batch_partition_ids,
    ))
}

async fn prepare_data_to_querier_for_partition(
    executor: &Executor,
    unpersisted_partition_data: UnpersistedPartitionData,
    request: &IngesterQueryRequest,
) -> Result<(Arc<Schema>, Option<RecordBatch>)> {
    // ------------------------------------------------
    // Accumulate data

    // Make Filters
    let selection_columns: Vec<_> = request.columns.iter().map(String::as_str).collect();
    let selection = if selection_columns.is_empty() {
        Selection::All
    } else {
        Selection::Some(&selection_columns)
    };
    let predicate = request.predicate.clone().unwrap_or_default();

    let mut filter_applied_batches = vec![];
    if let Some(queryable_batch) = unpersisted_partition_data.persisting {
        // ------------------------------------------------
        // persisting data

        let record_batch = run_query(
            executor,
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

    // ------------------------------------------------
    // Apply filters on the snapshot batches
    if !unpersisted_partition_data.non_persisted.is_empty() {
        // Make a Query able batch for all the snapshot
        let queryable_batch = QueryableBatch::new(
            &request.table,
            unpersisted_partition_data.non_persisted,
            vec![],
        );

        let record_batch =
            run_query(executor, Arc::new(queryable_batch), predicate, selection).await?;

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

    Ok((schema, batch))
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
        Selection::Some(columns) => {
            let schema = data.schema();
            Some(
                columns
                    .iter()
                    .flat_map(|&column_name| schema.find_index_of(column_name))
                    .collect(),
            )
        }
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
    use std::collections::HashSet;

    use super::*;
    use crate::test_util::{
        create_one_record_batch_with_influxtype_no_duplicates, create_tombstone,
        make_ingester_data, make_ingester_data_with_tombstones, make_queryable_batch,
        make_queryable_batch_with_deletes, DataLocation, TEST_NAMESPACE, TEST_TABLE,
    };
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use assert_matches::assert_matches;
    use data_types::PartitionId;
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
                scenarios.push((loc, scenario));
            }
        }

        // read data from all scenarios without any filters
        let mut request = IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
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
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
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
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
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
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = datafusion::physical_plan::common::collect(stream.data)
                .await
                .unwrap();
            assert_batches_sorted_eq!(&expected, &result);
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
        }

        // test "table not found" handling
        request.table = String::from("table_does_not_exist");
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let err = prepare_data_to_querier(scenario, &request)
                .await
                .unwrap_err();
            assert_matches!(err, Error::TableNotFound { .. });
        }

        // test "namespace not found" handling
        request.namespace = String::from("namespace_does_not_exist");
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let err = prepare_data_to_querier(scenario, &request)
                .await
                .unwrap_err();
            assert_matches!(err, Error::NamespaceNotFound { .. });
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
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
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
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
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
            assert_batch_partition_ids(&result, &stream.batch_partition_ids);
        }
    }

    fn assert_batch_partition_ids(batches: &[RecordBatch], partition_ids: &[PartitionId]) {
        assert_eq!(batches.len(), partition_ids.len());

        // at the moment there is at most one record batch per partition ID
        let partition_ids_unique: HashSet<_> = partition_ids.iter().collect();
        assert_eq!(batches.len(), partition_ids_unique.len());
    }
}
