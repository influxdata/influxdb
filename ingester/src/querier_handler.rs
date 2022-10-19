//! Handle all requests from Querier

use crate::{
    data::{
        namespace::NamespaceName, partition::UnpersistedPartitionData, table::TableName,
        IngesterData,
    },
    query::QueryableBatch,
};
use arrow::{array::new_null_array, error::ArrowError, record_batch::RecordBatch};
use arrow_util::optimize::{optimize_record_batch, optimize_schema};
use data_types::{PartitionId, SequenceNumber};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::MemoryStream;
use futures::{Stream, StreamExt, TryStreamExt};
use generated_types::ingester::IngesterQueryRequest;
use observability_deps::tracing::debug;
use schema::{merge::SchemaMerger, selection::Selection};
use snafu::{ensure, Snafu};
use std::{pin::Pin, sync::Arc};
use trace::span::{Span, SpanRecorder};

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

/// Stream of snapshots.
///
/// Every snapshot is a dedicated [`SendableRecordBatchStream`].
pub(crate) type SnapshotStream =
    Pin<Box<dyn Stream<Item = Result<SendableRecordBatchStream, ArrowError>> + Send>>;

/// Status of a partition that has unpersisted data.
///
/// Note that this structure is specific to a partition (which itself is bound to a table and
/// shard)!
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct PartitionStatus {
    /// Max sequence number persisted
    pub parquet_max_sequence_number: Option<SequenceNumber>,
}

/// Response data for a single partition.
pub(crate) struct IngesterQueryPartition {
    /// Stream of snapshots.
    snapshots: SnapshotStream,

    /// Partition ID.
    id: PartitionId,

    /// Partition persistence status.
    status: PartitionStatus,
}

impl std::fmt::Debug for IngesterQueryPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryPartition")
            .field("snapshots", &"<SNAPSHOT STREAM>")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish()
    }
}

impl IngesterQueryPartition {
    pub(crate) fn new(snapshots: SnapshotStream, id: PartitionId, status: PartitionStatus) -> Self {
        Self {
            snapshots,
            id,
            status,
        }
    }
}

/// Stream of partitions in this response.
pub(crate) type IngesterQueryPartitionStream =
    Pin<Box<dyn Stream<Item = Result<IngesterQueryPartition, ArrowError>> + Send>>;

/// Response streams for querier<>ingester requests.
///
/// The data structure is constructed to allow lazy/streaming data generation. For easier
/// consumption according to the wire protocol, use the [`flatten`](Self::flatten) method.
pub struct IngesterQueryResponse {
    /// Stream of partitions.
    partitions: IngesterQueryPartitionStream,
}

impl std::fmt::Debug for IngesterQueryResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngesterQueryResponse")
            .field("partitions", &"<PARTITION STREAM>")
            .finish()
    }
}

impl IngesterQueryResponse {
    /// Make a response
    pub(crate) fn new(partitions: IngesterQueryPartitionStream) -> Self {
        Self { partitions }
    }

    /// Flattens the data according to the wire protocol.
    pub fn flatten(self) -> FlatIngesterQueryResponseStream {
        self.partitions
            .flat_map(|partition_res| match partition_res {
                Ok(partition) => {
                    let head = futures::stream::once(async move {
                        Ok(FlatIngesterQueryResponse::StartPartition {
                            partition_id: partition.id,
                            status: partition.status,
                        })
                    });
                    let tail = partition
                        .snapshots
                        .flat_map(|snapshot_res| match snapshot_res {
                            Ok(snapshot) => {
                                let schema = Arc::new(optimize_schema(&snapshot.schema()));

                                let schema_captured = Arc::clone(&schema);
                                let head = futures::stream::once(async {
                                    Ok(FlatIngesterQueryResponse::StartSnapshot {
                                        schema: schema_captured,
                                    })
                                });

                                let tail = snapshot.map(move |batch_res| match batch_res {
                                    Ok(batch) => Ok(FlatIngesterQueryResponse::RecordBatch {
                                        batch: optimize_record_batch(&batch, Arc::clone(&schema))?,
                                    }),
                                    Err(e) => Err(e),
                                });

                                head.chain(tail).boxed()
                            }
                            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
                        });

                    head.chain(tail).boxed()
                }
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            })
            .boxed()
    }

    /// Convert [`IngesterQueryResponse`] to a set of [`RecordBatch`]es.
    ///
    /// If the response contains multiple snapshots, this will merge the schemas into a single one
    /// and create NULL-columns for snapshots that miss columns.
    ///
    /// # Panic
    ///
    /// Panics if there are no batches returned at all. Also panics if the snapshot-scoped schemas
    /// do not line up with the snapshot-scoped record batches.
    pub async fn into_record_batches(self) -> Vec<RecordBatch> {
        let mut snapshot_schema = None;
        let mut schema_merger = SchemaMerger::new();
        let mut batches = vec![];

        let mut stream = self.flatten();
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

/// Flattened version of [`IngesterQueryResponse`].
pub type FlatIngesterQueryResponseStream =
    Pin<Box<dyn Stream<Item = Result<FlatIngesterQueryResponse, ArrowError>> + Send>>;

/// Element within the flat wire protocol.
#[derive(Debug, PartialEq)]
pub enum FlatIngesterQueryResponse {
    /// Start a new partition.
    StartPartition {
        /// Partition ID.
        partition_id: PartitionId,

        /// Partition persistence status.
        status: PartitionStatus,
    },

    /// Start a new snapshot.
    ///
    /// The snapshot belongs to the partition of the last [`StartPartition`](Self::StartPartition)
    /// message.
    StartSnapshot {
        /// Snapshot schema.
        schema: Arc<arrow::datatypes::Schema>,
    },

    /// Add a record batch to the snapshot that was announced by the last
    /// [`StartSnapshot`](Self::StartSnapshot) message.
    RecordBatch {
        /// Record batch.
        batch: RecordBatch,
    },
}

/// Return data to send as a response back to the Querier per its request
pub async fn prepare_data_to_querier(
    ingest_data: &Arc<IngesterData>,
    request: &Arc<IngesterQueryRequest>,
    span: Option<Span>,
) -> Result<IngesterQueryResponse> {
    debug!(?request, "prepare_data_to_querier");

    let span_recorder = SpanRecorder::new(span);

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

        let table_name = TableName::from(&request.table);
        let table_data = match namespace_data.table_data(&table_name) {
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
            let snapshots: Vec<_> = prepare_data_to_querier_for_partition(
                partition,
                &request,
                span_recorder.child_span("ingester prepare data to querier for partition"),
            )
            .into_iter()
            .map(Ok)
            .collect();

            // Note: include partition in `unpersisted_partitions` even when there we might filter
            // out all the data, because the metadata (e.g. max persisted parquet file) is
            // important for the querier.
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
    span: Option<Span>,
) -> Vec<SendableRecordBatchStream> {
    let mut span_recorder = SpanRecorder::new(span);

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
            )
        })
        .with_data(unpersisted_partition_data.non_persisted);

    let streams = queryable_batch
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
        .collect();

    span_recorder.ok("done");

    streams
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use datafusion::{
        physical_plan::RecordBatchStream,
        prelude::{col, lit},
    };
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use predicate::Predicate;

    use super::*;
    use crate::test_util::{make_ingester_data, DataLocation, TEST_NAMESPACE, TEST_TABLE};

    #[tokio::test]
    async fn test_ingester_query_response_flatten() {
        let batch_1_1 = lp_to_batch("table x=1 0");
        let batch_1_2 = lp_to_batch("table x=2 1");
        let batch_2 = lp_to_batch("table y=1 10");
        let batch_3 = lp_to_batch("table z=1 10");

        let schema_1 = batch_1_1.schema();
        let schema_2 = batch_2.schema();
        let schema_3 = batch_3.schema();

        let response = IngesterQueryResponse::new(Box::pin(futures::stream::iter([
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![
                            Ok(batch_1_1.clone()),
                            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
                            Ok(batch_1_2.clone()),
                        ],
                        Arc::clone(&schema_1),
                    )) as _),
                    Err(ArrowError::InvalidArgumentError("invalid arg".into())),
                    Ok(Box::pin(TestRecordBatchStream::new(
                        vec![Ok(batch_2.clone())],
                        Arc::clone(&schema_2),
                    )) as _),
                    Ok(Box::pin(TestRecordBatchStream::new(vec![], Arc::clone(&schema_3))) as _),
                ])),
                PartitionId::new(2),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                },
            )),
            Err(ArrowError::IoError("some io error".into())),
            Ok(IngesterQueryPartition::new(
                Box::pin(futures::stream::iter([])),
                PartitionId::new(1),
                PartitionStatus {
                    parquet_max_sequence_number: None,
                },
            )),
        ])));

        let actual: Vec<_> = response.flatten().collect().await;
        let expected = vec![
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(2),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                },
            }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_1 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_1 }),
            Err(ArrowError::NotYetImplemented("not yet implemeneted".into())),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_1_2 }),
            Err(ArrowError::InvalidArgumentError("invalid arg".into())),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_2 }),
            Ok(FlatIngesterQueryResponse::RecordBatch { batch: batch_2 }),
            Ok(FlatIngesterQueryResponse::StartSnapshot { schema: schema_3 }),
            Err(ArrowError::IoError("some io error".into())),
            Ok(FlatIngesterQueryResponse::StartPartition {
                partition_id: PartitionId::new(1),
                status: PartitionStatus {
                    parquet_max_sequence_number: None,
                },
            }),
        ];

        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.into_iter().zip(expected) {
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected);
                }
                (Err(_), Err(_)) => {
                    // cannot compare `ArrowError`, but it's unlikely that someone changed the error
                }
                (Ok(_), Err(_)) => panic!("Actual is Ok but expected is Err"),
                (Err(_), Ok(_)) => panic!("Actual is Err but expected is Ok"),
            }
        }
    }

    #[tokio::test]
    async fn test_prepare_data_to_querier() {
        test_helpers::maybe_start_logging();

        let span = None;

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
            let result = prepare_data_to_querier(scenario, &request, span.clone())
                .await
                .unwrap()
                .into_record_batches()
                .await;
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
            let result = prepare_data_to_querier(scenario, &request, span.clone())
                .await
                .unwrap()
                .into_record_batches()
                .await;
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
            let result = prepare_data_to_querier(scenario, &request, span.clone())
                .await
                .unwrap()
                .into_record_batches()
                .await;
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
            let err = prepare_data_to_querier(scenario, &request, span.clone())
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
            let err = prepare_data_to_querier(scenario, &request, span.clone())
                .await
                .unwrap_err();
            assert_matches!(err, Error::NamespaceNotFound { .. });
        }
    }

    pub struct TestRecordBatchStream {
        schema: SchemaRef,
        batches: Vec<Result<RecordBatch, ArrowError>>,
    }

    impl TestRecordBatchStream {
        pub fn new(batches: Vec<Result<RecordBatch, ArrowError>>, schema: SchemaRef) -> Self {
            Self { schema, batches }
        }
    }

    impl RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl futures::Stream for TestRecordBatchStream {
        type Item = Result<RecordBatch, ArrowError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.batches.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.batches.remove(0)))
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.batches.len(), Some(self.batches.len()))
        }
    }

    fn lp_to_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }
}
