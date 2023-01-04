use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{utils::flight_data_from_arrow_batch, FlightData, SchemaAsIpc};
use futures::{stream::BoxStream, StreamExt};

/// Creates a stream which encodes a [`Stream`](futures::Stream) of
/// [`RecordBatch`]es into a stream of [`FlightData`], handling the
/// details of conversions.
///
/// This can be used to implement [`FlightService::do_get`] in an
/// Arrow Flight implementation.
///
/// # Caveats:
///   1. Any `DictionaryArray`s are converted to their underlying types prior to transport
///   See <https://github.com/apache/arrow-rs/issues/3389> for more details
///
/// [`FlightService::do_get`]: crate::flight_service_server::FlightService::do_get
#[derive(Debug)]
pub struct StreamEncoderBuilder {
    /// The maximum message size (see details on [`Self::with_max_message_size_bytes`]).
    pub max_batch_size_bytes: usize,
    /// Ipc writer options
    pub options: IpcWriteOptions,
    /// Metadata to add to the schema message
    pub app_metadata: Vec<u8>,
}

/// Default target size for record batches to send.
///
/// Note this value would normally be 4MB, but the size calculation is
/// somehwhat inexact, so we set it to 2MB.
pub const GRPC_TARGET_MAX_BATCH_SIZE_BYTES: usize = 2 * 1024 * 1024;

impl Default for StreamEncoderBuilder {
    fn default() -> Self {
        Self {
            max_batch_size_bytes: GRPC_TARGET_MAX_BATCH_SIZE_BYTES,
            options: IpcWriteOptions::default(),
            app_metadata: vec![],
        }
    }
}

impl StreamEncoderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the (approximate) maximum RecordBatch size to send (as gRPC
    /// servers typically have a maximum message limit.
    ///
    /// The encoder splits up [`RecordBatch`]s (preserving order) to
    /// limit objects of approximately this size (there's a bit of
    /// additional encoding overhead on top of that, so this is an
    /// estimate).
    pub fn with_max_message_size_bytes(mut self, max_batch_size_bytes: usize) -> Self {
        self.max_batch_size_bytes = max_batch_size_bytes;
        self
    }

    /// Include `app_metadata` in the first `Schema` message
    pub fn with_metadata(mut self, app_metadata: Vec<u8>) -> Self {
        self.app_metadata = app_metadata;
        self
    }

    /// With updated options
    pub fn with_options(mut self, options: IpcWriteOptions) -> Self {
        self.options = options;
        self
    }

    /// Return a stream that converts a [`Stream`](futures::Stream) of
    /// [`RecordBatch`] to a stream of [`FlightData`], consuming self.
    pub fn build(
        self,
        schema: SchemaRef,
        input: BoxStream<'static, Result<RecordBatch, tonic::Status>>,
    ) -> BoxStream<'static, Result<FlightData, tonic::Status>> {
        let Self {
            max_batch_size_bytes: max_batch_size,
            app_metadata,
            options,
        } = self;

        // The first message is the schema message, and all batches have
        // to have that schema too
        let schema = Arc::new(prepare_schema_for_flight(&schema));
        let mut schema_flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
        schema_flight_data.app_metadata = app_metadata;

        let schema_stream = futures::stream::once(async move { Ok(schema_flight_data) });

        let options = Arc::new(options);

        let data_stream = input
            .map(move |r| {
                let options = Arc::clone(&options);
                let schema = Arc::clone(&schema);
                async move {
                    match r {
                        Err(e) => futures::stream::iter(vec![Err(e)]).boxed(),
                        Ok(batch) => encode_batch(schema, max_batch_size, batch, &options),
                    }
                }
            })
            // buffer this conversion so record batches can be
            // produced while others are sent over the network
            .buffered(1)
            .flatten();

        // Send the schema first then the data
        schema_stream.chain(data_stream).boxed()
    }
}

/// Encodes `batch` as one or more [`FlightData`] structures
fn encode_batch(
    schema: SchemaRef,
    max_batch_size: usize,
    batch: RecordBatch,
    options: &IpcWriteOptions,
) -> BoxStream<'static, Result<FlightData, tonic::Status>> {
    let batch = match prepare_batch_for_flight(&batch, schema) {
        Ok(batch) => batch,
        Err(e) => {
            let e = tonic::Status::internal(format!(
                "Preparing to send batch over Arrow Flight: {}",
                e
            ));
            return futures::stream::iter(vec![Err(e)]).boxed();
        }
    };

    let output: Vec<_> = split_batch_for_grpc_response(batch, max_batch_size)
        .into_iter()
        .flat_map(|batch| {
            let (flight_dictionaries, flight_batch) = flight_data_from_arrow_batch(&batch, options);
            flight_dictionaries
                .into_iter()
                .chain(std::iter::once(flight_batch))
        })
        .map(Ok)
        .collect();
    futures::stream::iter(output).boxed()
}

/// Prepare an Arrow [`Schema`] for transport over the Arrow Flight protocol
///
/// Convert dictionary types to underlying types
///
/// See `hydrate_dictionary` for more information
pub fn prepare_schema_for_flight(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// Split [`RecordBatch`] so it hopefully fits into a gRPC response.
///
/// Data is zero-copy sliced into batches.
pub fn split_batch_for_grpc_response(
    batch: RecordBatch,
    max_batch_size_bytes: usize,
) -> Vec<RecordBatch> {
    let size = batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum::<usize>();

    let n_batches =
        (size / max_batch_size_bytes + usize::from(size % max_batch_size_bytes != 0)).max(1);
    let rows_per_batch = batch.num_rows() / n_batches;
    let mut out = Vec::with_capacity(n_batches + 1);

    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (offset + rows_per_batch).min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

/// Prepares a `RecordBatch` for transport over the Arrow Flight protocol
///
/// This means hydrating any dictionaries to their underlying type. See
/// `hydrate_dictionary` for more information.
///
pub fn prepare_batch_for_flight(
    batch: &RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch, tonic::Status> {
    let columns = batch
        .columns()
        .iter()
        .map(hydrate_dictionary)
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, columns)
        .map_err(|e| tonic::Status::internal(format!("Error preparing batch for flight: {}", e)))
}

/// Hydrates a dictionary to its underlying type
///
/// An IPC response, streaming or otherwise, defines its schema up front
/// which defines the mapping from dictionary IDs. It then sends these
/// dictionaries over the wire.
///
/// This requires identifying the different dictionaries in use, assigning
/// them IDs, and sending new dictionaries, delta or otherwise, when needed
///
/// This is tracked by <https://github.com/influxdata/influxdb_iox/issues/1318>
///
/// See also:
/// * <https://github.com/influxdata/influxdb_iox/issues/4275>
/// * <https://github.com/apache/arrow-rs/issues/1206>
///
/// For now we just hydrate the dictionaries to their underlying type
fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef, tonic::Status> {
    if let DataType::Dictionary(_, value) = array.data_type() {
        arrow::compute::cast(array, value).map_err(|e| {
            tonic::Status::internal(format!("Error hydrating DictionaryArray for flight: {}", e))
        })
    } else {
        Ok(Arc::clone(array))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{DictionaryArray, StringArray, UInt32Array, UInt8Array},
        compute::concat_batches,
        datatypes::Int32Type,
    };
    use arrow_flight::utils::flight_data_to_arrow_batch;

    use super::*;

    #[test]
    fn test_encode_flight_data() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef)])
            .expect("cannot create record batch");
        let schema = batch.schema();

        let (_, baseline_flight_batch) = flight_data_from_arrow_batch(&batch, &options);

        let big_batch = batch.slice(0, batch.num_rows() - 1);
        let optimized_big_batch =
            prepare_batch_for_flight(&big_batch, Arc::clone(&schema)).expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            flight_data_from_arrow_batch(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = batch.slice(0, 1);
        let optimized_small_batch = prepare_batch_for_flight(&small_batch, Arc::clone(&schema))
            .expect("failed to optimize");
        let (_, optimized_small_flight_batch) =
            flight_data_from_arrow_batch(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len() > optimized_small_flight_batch.data_body.len()
        );
    }

    #[test]
    fn test_encode_flight_data_dictionary() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();

        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let c2: DictionaryArray<Int32Type> = vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]
        .into_iter()
        .collect();

        let batch =
            RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef), ("b", Arc::new(c2))])
                .expect("cannot create record batch");

        let original_schema = batch.schema();
        let optimized_schema = Arc::new(prepare_schema_for_flight(&original_schema));

        let optimized_batch =
            prepare_batch_for_flight(&batch, Arc::clone(&optimized_schema)).unwrap();

        let (_, flight_data) = flight_data_from_arrow_batch(&optimized_batch, &options);

        let dictionary_by_id = std::collections::HashMap::new();
        let batch = flight_data_to_arrow_batch(
            &flight_data,
            Arc::clone(&optimized_schema),
            &dictionary_by_id,
        )
        .unwrap();

        // Should hydrate string dictionary for transport
        assert_eq!(optimized_schema.field(1).data_type(), &DataType::Utf8);
        let array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let expected = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            Some("fiz"),
            None,
            Some("foo"),
        ]);
        assert_eq!(array, &expected)
    }

    #[test]
    fn test_split_batch_for_grpc_response() {
        let max_batch_size = 1024;

        // no split
        let c = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone(), max_batch_size);
        assert_eq!(split.len(), 1);
        assert_eq!(batch, split[0]);

        // split once
        let n_rows = max_batch_size + 1;
        assert!(n_rows % 2 == 1, "should be an odd number");
        let c = UInt8Array::from((0..n_rows).map(|i| (i % 256) as u8).collect::<Vec<_>>());
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone(), max_batch_size);
        assert_eq!(split.len(), 2);
        assert_eq!(
            split.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            n_rows
        );
        assert_eq!(concat_batches(&batch.schema(), &split).unwrap(), batch);
    }
}
