//! Handle all requests from Querier

use std::{pin::Pin, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    FlightData, IpcMessage,
};
use arrow_util::test_util::equalize_batch_schemas;
use data_types::{NamespaceId, PartitionId, SequenceNumber, TableId};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::MemoryStream;
use flatbuffers::FlatBufferBuilder;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use generated_types::ingester::IngesterQueryRequest;
use observability_deps::tracing::*;
use schema::Projection;
use snafu::{ensure, Snafu};
use trace::span::{Span, SpanRecorder};

use crate::data::IngesterData;

/// Number of table data read locks that shall be acquired in parallel
const CONCURRENT_TABLE_DATA_LOCKS: usize = 10;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("No Namespace Data found for the given namespace ID {}", namespace_id,))]
    NamespaceNotFound { namespace_id: NamespaceId },

    #[snafu(display(
        "No Table Data found for the given namespace ID {}, table ID {}",
        namespace_id,
        table_id
    ))]
    TableNotFound {
        namespace_id: NamespaceId,
        table_id: TableId,
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

fn encode_partition(
    // Partition ID.
    partition_id: PartitionId,
    // Partition persistence status.
    status: PartitionStatus,
) -> std::result::Result<FlightData, FlightError> {
    use generated_types::influxdata::iox::ingester::v1 as proto;
    let mut bytes = bytes::BytesMut::new();
    let app_metadata = proto::IngesterQueryResponseMetadata {
        partition_id: partition_id.get(),
        status: Some(proto::PartitionStatus {
            parquet_max_sequence_number: status.parquet_max_sequence_number.map(|x| x.get()),
        }),
        // These fields are only used in ingester2.
        ingester_uuid: String::new(),
        completed_persistence_count: 0,
    };
    prost::Message::encode(&app_metadata, &mut bytes)
        .map_err(|e| FlightError::from_external_error(Box::new(e)))?;

    Ok(FlightData::new(
        None,
        IpcMessage(build_none_flight_msg().into()),
        bytes.to_vec(),
        vec![],
    ))
}

fn build_none_flight_msg() -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();

    let mut message = arrow::ipc::MessageBuilder::new(&mut fbb);
    message.add_version(arrow::ipc::MetadataVersion::V5);
    message.add_header_type(arrow::ipc::MessageHeader::NONE);
    message.add_bodyLength(0);

    let data = message.finish();
    fbb.finish(data, None);

    fbb.finished_data().to_vec()
}

impl IngesterQueryResponse {
    /// Make a response
    pub(crate) fn new(partitions: IngesterQueryPartitionStream) -> Self {
        Self { partitions }
    }

    /// Flattens the stream of IngesterPartitions into a stream of FlightData
    pub fn flatten(self) -> BoxStream<'static, std::result::Result<FlightData, FlightError>> {
        self.partitions
            .flat_map(|partition_res| match partition_res {
                Ok(partition) => {
                    let head = futures::stream::once(async move {
                        encode_partition(partition.id, partition.status)
                    });

                    let tail = partition
                        .snapshots
                        .flat_map(|snapshot_res| match snapshot_res {
                            Ok(snapshot) => {
                                let snapshot =
                                    snapshot.map_err(|e| FlightError::ExternalError(Box::new(e)));

                                FlightDataEncoderBuilder::new().build(snapshot).boxed()
                            }
                            Err(e) => {
                                futures::stream::once(async { Err(FlightError::Arrow(e)) }).boxed()
                            }
                        });

                    head.chain(tail).boxed()
                }
                Err(e) => futures::stream::once(async { Err(FlightError::Arrow(e)) }).boxed(),
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
        let mut batches = vec![];

        let mut stream = FlightDataDecoder::new(self.flatten());

        while let Some(msg) = stream.try_next().await.unwrap() {
            match msg.payload {
                DecodedPayload::None => {}
                DecodedPayload::RecordBatch(batch) => {
                    let last_schema = snapshot_schema.as_ref().unwrap();
                    assert_eq!(&batch.schema(), last_schema);
                    batches.push(batch);
                }
                DecodedPayload::Schema(schema) => {
                    snapshot_schema = Some(schema);
                }
            }
        }

        assert!(!batches.is_empty());

        equalize_batch_schemas(batches).unwrap()
    }
}

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

    let mut span_recorder = SpanRecorder::new(span);

    let mut table_refs = vec![];
    let mut found_namespace = false;

    for (shard_id, shard_data) in ingest_data.shards() {
        let namespace_data = match shard_data.namespace(request.namespace_id) {
            Some(namespace_data) => {
                trace!(
                    shard_id=%shard_id.get(),
                    namespace_id=%request.namespace_id,
                    "found namespace"
                );
                found_namespace = true;
                namespace_data
            }
            None => {
                continue;
            }
        };

        if let Some(table_data) = namespace_data.table(request.table_id) {
            trace!(
                shard_id=%shard_id.get(),
                namespace_id=%request.namespace_id,
                table_id=%request.table_id,
                "found table"
            );
            table_refs.push(table_data);
        }
    }

    ensure!(
        found_namespace,
        NamespaceNotFoundSnafu {
            namespace_id: request.namespace_id,
        },
    );

    ensure!(
        !table_refs.is_empty(),
        TableNotFoundSnafu {
            namespace_id: request.namespace_id,
            table_id: request.table_id
        },
    );

    // acquire locks and read table data in parallel
    let unpersisted_partitions: Vec<_> = futures::stream::iter(table_refs)
        .map(|table_data| async move {
            table_data
                .partitions()
                .into_iter()
                .map(|p| {
                    let mut p = p.lock();
                    (
                        p.partition_id(),
                        p.get_query_data(),
                        p.max_persisted_sequence_number(),
                    )
                })
                .collect::<Vec<_>>()
        })
        // Note: the order doesn't matter
        .buffer_unordered(CONCURRENT_TABLE_DATA_LOCKS)
        .concat()
        .await;

    let request = Arc::clone(request);
    let partitions = futures::stream::iter(unpersisted_partitions.into_iter().map(
        move |(partition_id, data, max_persisted_sequence_number)| {
            let snapshots = match data {
                None => Box::pin(futures::stream::empty()) as SnapshotStream,

                Some(batch) => {
                    assert_eq!(partition_id, batch.partition_id());

                    // Project the data if necessary
                    let columns = request
                        .columns
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let selection = if columns.is_empty() {
                        Projection::All
                    } else {
                        Projection::Some(columns.as_ref())
                    };

                    let snapshots = batch.project_selection(selection).into_iter().map(|batch| {
                        // Create a stream from the batch.
                        Ok(Box::pin(MemoryStream::new(vec![batch])) as SendableRecordBatchStream)
                    });

                    Box::pin(futures::stream::iter(snapshots)) as SnapshotStream
                }
            };

            // NOTE: the partition persist watermark MUST always be provided to
            // the querier for any partition that has performed (or is aware of)
            // a persist operation.
            //
            // This allows the querier to use the per-partition persist marker
            // when planning queries.
            Ok(IngesterQueryPartition::new(
                snapshots,
                partition_id,
                PartitionStatus {
                    parquet_max_sequence_number: max_persisted_sequence_number,
                },
            ))
        },
    ));

    span_recorder.ok("done");

    Ok(IngesterQueryResponse::new(Box::pin(partitions)))
}

#[cfg(test)]
mod tests {

    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use datafusion::prelude::{col, lit};
    use predicate::Predicate;

    use super::*;
    use crate::test_util::make_ingester_data;

    #[tokio::test]
    async fn test_prepare_data_to_querier() {
        test_helpers::maybe_start_logging();

        // make 14 scenarios for ingester data
        let mut table_id = None;
        let mut ns_id = None;
        let mut scenarios = vec![];
        for two_partitions in [false, true] {
            let (scenario, ns, table) = make_ingester_data(two_partitions).await;

            let old = *table_id.get_or_insert(table);
            assert_eq!(old, table);
            let old = *ns_id.get_or_insert(ns);
            assert_eq!(old, ns);

            scenarios.push(Arc::new(scenario));
        }
        let table_id = table_id.unwrap();
        let ns_id = ns_id.unwrap();

        // read data from all scenarios without any filters
        let request = Arc::new(IngesterQueryRequest::new(ns_id, table_id, vec![], None));
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Andover    | mon |      | 1970-01-01T00:00:00.000000046Z |", // in group 1 - seq_num: 2
            "| Andover    | tue | 56.0 | 1970-01-01T00:00:00.000000030Z |", // in group 2 - seq_num: 3
            "| Boston     | mon |      | 1970-01-01T00:00:00.000000038Z |", // in group 1 - seq_num: 1
            "| Boston     | sun | 60.0 | 1970-01-01T00:00:00.000000036Z |", // in group 3 - seq_num: 5
            "| Medford    | sun | 55.0 | 1970-01-01T00:00:00.000000022Z |", // in group 4 - seq_num: 7
            "| Medford    | wed |      | 1970-01-01T00:00:00.000000026Z |", // in group 2 - seq_num: 4
            "| Reading    | mon | 58.0 | 1970-01-01T00:00:00.000000040Z |", // in group 4 - seq_num: 8
            "| Wilmington | mon |      | 1970-01-01T00:00:00.000000035Z |", // in group 3 - seq_num: 6
            "+------------+-----+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let result = prepare_data_to_querier(scenario, &request, None)
                .await
                .unwrap()
                .into_record_batches()
                .await;
            assert_batches_sorted_eq!(&expected, &result);
        }

        // read data from all scenarios and filter out column day
        let request = Arc::new(IngesterQueryRequest::new(
            ns_id,
            table_id,
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
            "| Andover    | 56.0 | 1970-01-01T00:00:00.000000030Z |",
            "| Boston     |      | 1970-01-01T00:00:00.000000038Z |",
            "| Boston     | 60.0 | 1970-01-01T00:00:00.000000036Z |",
            "| Medford    |      | 1970-01-01T00:00:00.000000026Z |",
            "| Medford    | 55.0 | 1970-01-01T00:00:00.000000022Z |",
            "| Reading    | 58.0 | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let result = prepare_data_to_querier(scenario, &request, None)
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
            ns_id,
            table_id,
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
            "| Andover    | 56.0 | 1970-01-01T00:00:00.000000030Z |",
            "| Boston     |      | 1970-01-01T00:00:00.000000038Z |",
            "| Boston     | 60.0 | 1970-01-01T00:00:00.000000036Z |",
            "| Medford    |      | 1970-01-01T00:00:00.000000026Z |",
            "| Medford    | 55.0 | 1970-01-01T00:00:00.000000022Z |",
            "| Reading    | 58.0 | 1970-01-01T00:00:00.000000040Z |",
            "| Wilmington |      | 1970-01-01T00:00:00.000000035Z |",
            "+------------+------+--------------------------------+",
        ];
        for scenario in &scenarios {
            let result = prepare_data_to_querier(scenario, &request, None)
                .await
                .unwrap()
                .into_record_batches()
                .await;
            assert_batches_sorted_eq!(&expected, &result);
        }

        // test "table not found" handling
        let request = Arc::new(IngesterQueryRequest::new(
            ns_id,
            TableId::new(i64::MAX),
            vec![],
            None,
        ));
        for scenario in &scenarios {
            let err = prepare_data_to_querier(scenario, &request, None)
                .await
                .unwrap_err();
            assert_matches!(err, Error::TableNotFound { .. });
        }

        // test "namespace not found" handling
        let request = Arc::new(IngesterQueryRequest::new(
            NamespaceId::new(i64::MAX),
            table_id,
            vec![],
            None,
        ));
        for scenario in &scenarios {
            let err = prepare_data_to_querier(scenario, &request, None)
                .await
                .unwrap_err();
            assert_matches!(err, Error::NamespaceNotFound { .. });
        }
    }
}
