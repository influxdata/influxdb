//! Handle all requests from Querier

use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::MemoryStream;
use futures::StreamExt;
use generated_types::ingester::IngesterQueryRequest;
use observability_deps::tracing::debug;
use schema::selection::Selection;
use snafu::{ensure, Snafu};

use crate::{
    data::{
        namespace::NamespaceName, partition::UnpersistedPartitionData, IngesterData,
        IngesterQueryPartition, IngesterQueryResponse,
    },
    query::QueryableBatch,
};

/// Number of table data read locks that shall be acquired in parallel
const CONCURRENT_TABLE_DATA_LOCKS: usize = 10;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
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

    #[snafu(display("Concurrent query request limit exceeded"))]
    RequestLimit,
}

/// A specialized `Error` for Ingester's Query errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return data to send as a response back to the Querier per its request
pub async fn prepare_data_to_querier(
    ingest_data: &Arc<IngesterData>,
    request: &Arc<IngesterQueryRequest>,
) -> Result<IngesterQueryResponse> {
    debug!(?request, "prepare_data_to_querier");
    let mut tables_data = vec![];
    let mut found_namespace = false;
    for (shard_id, shard_data) in ingest_data.shards() {
        debug!(shard_id=%shard_id.get());
        let namespace_name = NamespaceName::from(&request.namespace);
        let namespace_data = match shard_data.namespace(&namespace_name) {
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

        tables_data.push(table_data);
    }

    ensure!(
        found_namespace,
        NamespaceNotFoundSnafu {
            namespace_name: &request.namespace,
        },
    );

    // acquire locks in parallel
    let unpersisted_partitions: Vec<_> = futures::stream::iter(tables_data)
        .map(|table_data| async move {
            let table_data = table_data.read().await;
            table_data.unpersisted_partition_data()
        })
        // Note: the order doesn't matter
        .buffer_unordered(CONCURRENT_TABLE_DATA_LOCKS)
        .concat()
        .await;

    ensure!(
        !unpersisted_partitions.is_empty(),
        TableNotFoundSnafu {
            namespace_name: &request.namespace,
            table_name: &request.table
        },
    );

    let request = Arc::clone(request);
    let partitions =
        futures::stream::iter(unpersisted_partitions.into_iter().map(move |partition| {
            // extract payload
            let partition_id = partition.partition_id;
            let status = partition.partition_status.clone();
            let snapshots: Vec<_> = prepare_data_to_querier_for_partition(partition, &request)
                .into_iter()
                .map(Ok)
                .collect();

            // Note: include partition in `unpersisted_partitions` even when there we might filter out all the data, because
            // the metadata (e.g. max persisted parquet file) is important for the querier.
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter(snapshots)),
                partition_id,
                status,
            ))
        }));

    Ok(IngesterQueryResponse::new(Box::pin(partitions)))
}

fn prepare_data_to_querier_for_partition(
    unpersisted_partition_data: UnpersistedPartitionData,
    request: &IngesterQueryRequest,
) -> Vec<SendableRecordBatchStream> {
    // ------------------------------------------------
    // Accumulate data

    // Make Filters
    let selection_columns: Vec<_> = request.columns.iter().map(String::as_str).collect();
    let selection = if selection_columns.is_empty() {
        Selection::All
    } else {
        Selection::Some(&selection_columns)
    };

    // figure out what batches
    let queryable_batch = unpersisted_partition_data
        .persisting
        .unwrap_or_else(|| {
            QueryableBatch::new(
                request.table.clone().into(),
                unpersisted_partition_data.partition_id,
                vec![],
                vec![],
            )
        })
        .with_data(unpersisted_partition_data.non_persisted);

    queryable_batch
        .data
        .iter()
        .map(|snapshot_batch| {
            let batch = snapshot_batch.data.as_ref();
            let schema = batch.schema();

            // Apply selection to in-memory batch
            let batch = match selection {
                Selection::All => batch.clone(),
                Selection::Some(columns) => {
                    let projection = columns
                        .iter()
                        .flat_map(|&column_name| {
                            // ignore non-existing columns
                            schema.index_of(column_name).ok()
                        })
                        .collect::<Vec<_>>();
                    batch.project(&projection).expect("bug in projection")
                }
            };

            // create stream
            Box::pin(MemoryStream::new(vec![batch])) as SendableRecordBatchStream
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::{array::new_null_array, record_batch::RecordBatch};
    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use datafusion::logical_plan::{col, lit};
    use futures::TryStreamExt;
    use predicate::Predicate;
    use schema::merge::SchemaMerger;

    use super::*;
    use crate::{
        data::FlatIngesterQueryResponse,
        test_util::{
            make_ingester_data, make_ingester_data_with_tombstones, DataLocation, TEST_NAMESPACE,
            TEST_TABLE,
        },
    };

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
                let scenario = Arc::new(make_ingester_data(two_partitions, loc).await);
                scenarios.push((loc, scenario));
            }
        }

        // read data from all scenarios without any filters
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec![],
            None,
        ));
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
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios and filter out column day
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec![
                "city".to_string(),
                "temp".to_string(),
                "time".to_string(),
                "a_column_that_does_not_exist".to_string(),
            ],
            None,
        ));
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
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios, filter out column day, city Medford, time outside range [0, 42)
        let expr = col("city").not_eq(lit("Medford"));
        let pred = Predicate::default().with_expr(expr).with_range(0, 42);
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec!["city".to_string(), "temp".to_string(), "time".to_string()],
            Some(pred),
        ));
        // predicates and de-dup are NOT applied!, otherwise this would look like this:
        // let expected = vec![
        //     "+------------+------+--------------------------------+",
        //     "| city       | temp | time                           |",
        //     "+------------+------+--------------------------------+",
        //     "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
        //     "| Boston     |      | 1970-01-01T00:00:00.000000038Z |",
        //     "| Boston     | 60   | 1970-01-01T00:00:00.000000036Z |",
        //     "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
        //     "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
        //     "+------------+------+--------------------------------+",
        // ];
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
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(&expected, &result);
        }

        // test "table not found" handling
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            "table_does_not_exist".to_string(),
            vec![],
            None,
        ));
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let err = prepare_data_to_querier(scenario, &request)
                .await
                .unwrap_err();
            assert_matches!(err, Error::TableNotFound { .. });
        }

        // test "namespace not found" handling
        let request = Arc::new(IngesterQueryRequest::new(
            "namespace_does_not_exist".to_string(),
            TEST_TABLE.to_string(),
            vec![],
            None,
        ));
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
            scenarios.push((loc, scenario));
        }

        // read data from all scenarios without any filters
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec![],
            None,
        ));
        let expected_not_persisting = vec![
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
        // For "persisting" data locations the tombstones were NOT applied because they arrived AFTER the data
        // transitioned into the "persisting" state. In this case, the ingester will apply the tombstones.
        let expected_persisting = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |",
            "| Andover    | tue | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |",
            "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Medford    | sun | 55   | 1970-01-01T00:00:00.000000022Z |",
            "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |",
            "| Reading    | mon | 58   | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        for (loc, scenario) in &scenarios {
            println!("Location: {loc:?}");
            let expected = if loc.intersects(DataLocation::PERSISTING) {
                &expected_persisting
            } else {
                &expected_not_persisting
            };

            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(expected, &result);
        }

        // read data from all scenarios and filter out column day
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec!["city".to_string(), "temp".to_string(), "time".to_string()],
            None,
        ));
        let expected_not_persisting = vec![
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
        // For "persisting" data locations the tombstones were NOT applied because they arrived AFTER the data
        // transitioned into the "persisting" state. In this case, the ingester will apply the tombstones.
        let expected_persisting = vec![
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
            let expected = if loc.intersects(DataLocation::PERSISTING) {
                &expected_persisting
            } else {
                &expected_not_persisting
            };

            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(expected, &result);
        }

        // read data from all scenarios, filter out column day, city Medford, time outside range [0, 42)
        let expr = col("city").not_eq(lit("Medford"));
        let pred = Predicate::default().with_expr(expr).with_range(0, 42);
        let request = Arc::new(IngesterQueryRequest::new(
            TEST_NAMESPACE.to_string(),
            TEST_TABLE.to_string(),
            vec!["city".to_string(), "temp".to_string(), "time".to_string()],
            Some(pred),
        ));
        // predicates and de-dup are NOT applied!, otherwise this would look like this:
        // let expected = vec![
        //     "+------------+------+--------------------------------+",
        //     "| city       | temp | time                           |",
        //     "+------------+------+--------------------------------+",
        //     "| Andover    | 56   | 1970-01-01T00:00:00.000000030Z |",
        //     "| Reading    | 58   | 1970-01-01T00:00:00.000000040Z |",
        //     "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
        //     "+------------+------+--------------------------------+",
        // ];
        let expected_not_persisting = vec![
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
        // For "persisting" data locations the tombstones were NOT applied because they arrived AFTER the data
        // transitioned into the "persisting" state. In this case, the ingester will apply the tombstones.
        let expected_persisting = vec![
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
            let expected = if loc.intersects(DataLocation::PERSISTING) {
                &expected_persisting
            } else {
                &expected_not_persisting
            };

            let stream = prepare_data_to_querier(scenario, &request).await.unwrap();
            let result = ingester_response_to_record_batches(stream).await;
            assert_batches_sorted_eq!(expected, &result);
        }
    }

    /// Convert [`IngesterQueryResponse`] to a set of [`RecordBatch`]es.
    ///
    /// If the response contains multiple snapshots, this will merge the schemas into a single one and create
    /// NULL-columns for snapshots that miss columns. This makes it easier to use the resulting batches with
    /// [`assert_batches_sorted_eq`].
    ///
    /// # Panic
    /// Panics if there are no batches returned at all. Also panics if the snapshot-scoped schemas do not line up with
    /// the snapshot-scoped record batches.
    async fn ingester_response_to_record_batches(
        response: IngesterQueryResponse,
    ) -> Vec<RecordBatch> {
        let mut snapshot_schema = None;
        let mut schema_merger = SchemaMerger::new();
        let mut batches = vec![];

        let mut stream = response.flatten();
        while let Some(msg) = stream.try_next().await.unwrap() {
            match msg {
                FlatIngesterQueryResponse::StartPartition { .. } => (),
                FlatIngesterQueryResponse::RecordBatch { batch } => {
                    let last_schema = snapshot_schema.as_ref().unwrap();
                    assert_eq!(&batch.schema(), last_schema);
                    batches.push(batch);
                }
                FlatIngesterQueryResponse::StartSnapshot { schema } => {
                    snapshot_schema = Some(Arc::clone(&schema));

                    schema_merger = schema_merger
                        .merge(&schema::Schema::try_from(schema).unwrap())
                        .unwrap();
                }
            }
        }

        assert!(!batches.is_empty());

        // equalize schemas
        let common_schema = schema_merger.build().as_arrow();
        batches
            .into_iter()
            .map(|batch| {
                let batch_schema = batch.schema();
                let columns = common_schema
                    .fields()
                    .iter()
                    .map(|field| match batch_schema.index_of(field.name()) {
                        Ok(idx) => Arc::clone(batch.column(idx)),
                        Err(_) => new_null_array(field.data_type(), batch.num_rows()),
                    })
                    .collect();
                RecordBatch::try_new(Arc::clone(&common_schema), columns).unwrap()
            })
            .collect()
    }
}
