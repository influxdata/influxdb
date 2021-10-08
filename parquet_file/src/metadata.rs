//! Metadata encoding and decoding.
//!
//! # Data Flow
//! The following diagram shows how metadata flows through the system from the perspective of the parquet reader/writer:
//!
//! 1. **Incoming Data:** Incoming data contains one or multiple [Apache Arrow] `RecordBatch`, IOx-specific statistics,
//!    the IOx-specific schema (which also contains key-value metadata) and a timestamp range.
//! 2. **Parquet Creation:** The `RecordBatch` is converted into an [Apache Parquet] file. Note that the `RecordBatch`
//!    itself has a schema associated, so technically it is not required to use the input schema. However if we have
//!    multiple batches it is technically simpler to provide a single schema to the Parquet writer in which case we also
//!    use the input schema. Statistics and timestamp range are NOT provided to the Parquet writer since it needs to
//!    calculate the Parquet-specific statistics anyway.
//! 3. **Parquet Metadata:** From the Parquet file we extract the metadata, which technically is the footer of the file
//!    and does NOT contain any payload data.
//! 4. **Parquet File Lifecycle:** The Parquet file is stored into the object store and can be recovered from there.
//!    With it the metadata can also be recovered. However for performance reasons we also want to keep the metadata
//!    separately which is discussed in the next step.
//! 5. **Thrift Encoding:** Within the parquet file the metadata resides as a serialized [Apache Thrift] message. We
//!    reuse this encoding and serialize the metadata using the same [Thrift Compact Protocol]. The resulting bytes can
//!    then be stored within the catalog.
//! 6. **IOx Metadata Extraction:** From the Parquet metadata we can recover all important metadata parts for IOx,
//!    namely schema, statistics and timestamp range.
//!
//! ```text
//! .....................................    .....................................
//! .                                   .    .                                   .
//! .               Input               .    .             Output                .
//! .                                   .    .                                   .
//! .  ┌─────────────┐ ┌─────────────┐  .    .  ┌─────────────┐ ┌─────────────┐  .
//! .  │             │ │             │  .    .  │             │ │             │  .
//! .  │ RecordBatch │ │ Statistics  │  .    .  │ Store+Path  │ │ Statistics  │  .
//! .  │   (1..n)    │ │             │  .    .  │             │ │             │  .
//! .  └─┬───────────┘ └─────────────┘  .    .  └─────────────┘ └▲────────────┘  .
//! .    │                              .    .                   │               .
//! .  ┌─┼───────────┐ ┌─────────────┐  .    .  ┌─────────────┐ ┌┼────────────┐  .
//! .  │ │           │ │             │  .    .  │             │ ││            │  .
//! .  │ │ Schema    │ │  TS Range   │  .    .  │   Schema    │ ││ TS Range   │  .
//! .  │ │           │ │             │  .    .  │             │ ││            │  .
//! .  └─┼───────────┘ └─────────────┘  .    .  └─▲───────────┘ └┼─▲──────────┘  .
//! .    │                              .    .    │              │ │             .
//! .....│...............................    .....│..............│.│..............
//!      │                                        │              │ │
//!      │ Arrow => Parquet                       ├──────────────┴─┘
//!      │                                        │
//!      │                                        │
//! ┌────▼─────────────────────┐                  │
//! │                          │                  │
//! │       Apache Parquet     │                  │
//! │                          │                  │
//! │  ┌───────────────────────┤                  │
//! │  │      Magic Number     │                  │
//! │  ├───────────────────────┤                  │ Restore
//! │  │                       │ │                │
//! │  │ Row Group 1 ┌─────────┤ │                │
//! │  │             │ ...     │ │                │
//! │  ├─────────────┴─────────┤ │                │
//! │  │ ...                   │ │Payload         │
//! │  ├───────────────────────┤ │                │
//! │  │                       │ │                │
//! │  │ Row Group N ┌─────────┤ │                │
//! │  │             │ ...     │ │                │
//! │  ├─────────────┴─────────┤                ┌─┴────────────────┐
//! │  │                       │ │              │                  │
//! │  │ Footer      ┌─────────┤ │Metadata ─────► Parquet Metadata │
//! │  │             │ ...     │ │              │                  │
//! │  ├─────────────┴─────────┤                └─▲────────────────┘
//! │  │     Footer Length     │                  │
//! │  ├───────────────────────┤                  │ Encode / Decode
//! │  │      Magic Number     │                  │
//! └▲─┴───────────────────────┘                ┌─▼────────────────┐
//!  │                                          │                  │
//!  │                                          │  Thrift Bytes    │
//!  │                                          │                  │
//!  │ Store / Load                             └─▲────────────────┘
//!  │                                            │
//!  │                                            │ Store / Load
//!  │                                            │
//! ┌▼─────────────┐                            ┌─▼────────────────┐
//! │              │                            │                  │
//! │ Object Store │                            │     Catalog      │
//! │              │                            │                  │
//! └──────────────┘                            └──────────────────┘
//! ```
//!
//! [Apache Arrow]: https://arrow.apache.org/
//! [Apache Parquet]: https://parquet.apache.org/
//! [Apache Thrift]: https://thrift.apache.org/
//! [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkId, ChunkOrder},
    partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics},
};
use generated_types::influxdata::iox::catalog::v1 as proto;
use internal_types::schema::{InfluxColumnType, InfluxFieldType, Schema};
use parquet::{
    arrow::parquet_to_arrow_schema,
    file::{
        metadata::{
            FileMetaData as ParquetFileMetaData, ParquetMetaData,
            RowGroupMetaData as ParquetRowGroupMetaData,
        },
        reader::FileReader,
        serialized_reader::{SerializedFileReader, SliceableCursor},
        statistics::Statistics as ParquetStatistics,
    },
    schema::types::SchemaDescriptor as ParquetSchemaDescriptor,
};
use persistence_windows::{
    checkpoint::{DatabaseCheckpoint, PartitionCheckpoint},
    min_max_sequence::OptionalMinMaxSequence,
};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{collections::BTreeMap, convert::TryInto, sync::Arc};
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol, TOutputProtocol};

/// Current version for serialized metadata.
///
/// For breaking changes, this will change.
///
/// **Important: When changing this structure, consider bumping the
///   [catalog transaction version](crate::catalog::core::TRANSACTION_VERSION)!**
pub const METADATA_VERSION: u32 = 10;

/// File-level metadata key to store the IOx-specific data.
///
/// This will contain [`IoxMetadata`] serialized as base64-encoded [Protocol Buffers 3].
///
/// [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3
pub const METADATA_KEY: &str = "IOX:metadata";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot read parquet metadata from bytes: {}", source))]
    ParquetMetaDataRead {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot read thrift message: {}", source))]
    ThriftReadFailure { source: thrift::Error },

    #[snafu(display("Cannot write thrift message: {}", source))]
    ThriftWriteFailure { source: thrift::Error },

    #[snafu(display("Cannot convert parquet schema to thrift: {}", source))]
    ParquetSchemaToThrift {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot convert thrift to parquet schema: {}", source))]
    ParquetSchemaFromThrift {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot convert thrift to parquet row group: {}", source))]
    ParquetRowGroupFromThrift {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("No row group found, cannot recover statistics"))]
    NoRowGroup {},

    #[snafu(display(
        "Cannot find statistics for column {} in row group {}",
        column,
        row_group
    ))]
    StatisticsMissing { row_group: usize, column: String },

    #[snafu(display(
        "Statistics for column {} in row group {} contain deprecated and potentially wrong min/max values",
        column,
        row_group
    ))]
    StatisticsMinMaxDeprecated { row_group: usize, column: String },

    #[snafu(display(
        "Statistics for column {} in row group {} have wrong type: expected {:?} but got {}",
        column,
        row_group,
        expected,
        actual,
    ))]
    StatisticsTypeMismatch {
        row_group: usize,
        column: String,
        expected: InfluxColumnType,
        actual: ParquetStatistics,
    },

    #[snafu(display(
        "Statistics for column {} in row group {} contain invalid UTF8 data: {}",
        column,
        row_group,
        source,
    ))]
    StatisticsUtf8Error {
        row_group: usize,
        column: String,
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot read arrow schema from parquet: {}", source))]
    ArrowFromParquetFailure {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot read IOx schema from arrow: {}", source))]
    IoxFromArrowFailure {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Parquet metadata does not contain IOx metadata"))]
    IoxMetadataMissing {},

    #[snafu(display("Field missing while parsing IOx metadata: {}", field))]
    IoxMetadataFieldMissing { field: String },

    #[snafu(display("Min-max relation wrong while parsing IOx metadata"))]
    IoxMetadataMinMax,

    #[snafu(display("Cannot parse IOx metadata from Protobuf: {}", source))]
    IoxMetadataBroken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Format version of IOx metadata is {} but only {:?} are supported",
        actual,
        expected
    ))]
    IoxMetadataVersionMismatch { actual: u32, expected: Vec<u32> },

    #[snafu(display("Cannot encode ZSTD message for parquet metadata: {}", source))]
    ZstdEncodeFailure { source: std::io::Error },

    #[snafu(display("Cannot decode ZSTD message for parquet metadata: {}", source))]
    ZstdDecodeFailure { source: std::io::Error },

    #[snafu(display("Cannot decode chunk id: {}", source))]
    CannotDecodeChunkId {
        source: data_types::chunk_metadata::BytesToChunkIdError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// IOx-specific metadata.
///
/// # Serialization
/// This will serialized as base64-encoded [Protocol Buffers 3] into the file-level key-value Parquet metadata (under [`METADATA_KEY`]).
///
/// **Important: When changing this structure, consider bumping the
///   [metadata version](METADATA_VERSION)!**
///
/// # Content
/// This struct contains chunk-specific information (like the chunk address), data lineage information (like the
/// creation timestamp) and partition/database-wide checkpoint data. While this struct is stored in a parquet file and
/// is somewhat chunk-bound, it is necessary to also store broader information (like the checkpoints) in it so that the
/// catalog can be rebuilt from parquet files (see [Catalog Properties]).
///
/// [Catalog Properties]: https://github.com/influxdata/influxdb_iox/blob/main/docs/catalog_persistence.md#13-properties
/// [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IoxMetadata {
    /// Timestamp when this file was created.
    pub creation_timestamp: DateTime<Utc>,

    pub time_of_first_write: DateTime<Utc>,
    pub time_of_last_write: DateTime<Utc>,

    /// Table that holds this parquet file.
    pub table_name: Arc<str>,

    /// Partition key of the partition that holds this parquet file.
    pub partition_key: Arc<str>,

    /// Chunk ID.
    pub chunk_id: ChunkId,

    /// Partition checkpoint with pre-split data for the in this file.
    pub partition_checkpoint: PartitionCheckpoint,

    /// Database checkpoint created at the time of the write.
    pub database_checkpoint: DatabaseCheckpoint,

    /// Order of this chunk relative to other overlapping chunks.
    pub chunk_order: ChunkOrder,
}

impl IoxMetadata {
    /// Read from protobuf message
    fn from_protobuf(data: &[u8]) -> Result<Self> {
        // extract protobuf message from bytes
        let proto_msg = proto::IoxMetadata::decode(data)
            .map_err(|err| Box::new(err) as _)
            .context(IoxMetadataBroken)?;

        // check version
        if proto_msg.version != METADATA_VERSION {
            return Err(Error::IoxMetadataVersionMismatch {
                actual: proto_msg.version,
                expected: vec![METADATA_VERSION],
            });
        }

        // extract creation timestamp
        let creation_timestamp =
            decode_timestamp_from_field(proto_msg.creation_timestamp, "creation_timestamp")?;
        // extract time of first write
        let time_of_first_write =
            decode_timestamp_from_field(proto_msg.time_of_first_write, "time_of_first_write")?;
        // extract time of last write
        let time_of_last_write =
            decode_timestamp_from_field(proto_msg.time_of_last_write, "time_of_last_write")?;

        // extract strings
        let table_name = Arc::from(proto_msg.table_name.as_ref());
        let partition_key = Arc::from(proto_msg.partition_key.as_ref());

        // extract partition checkpoint
        let proto_partition_checkpoint =
            proto_msg
                .partition_checkpoint
                .context(IoxMetadataFieldMissing {
                    field: "partition_checkpoint",
                })?;
        let sequencer_numbers = proto_partition_checkpoint
            .sequencer_numbers
            .into_iter()
            .map(|(sequencer_id, min_max)| {
                let min = min_max.min.as_ref().map(|min| min.value);

                if min.map(|min| min <= min_max.max).unwrap_or(true) {
                    Ok((sequencer_id, OptionalMinMaxSequence::new(min, min_max.max)))
                } else {
                    Err(Error::IoxMetadataMinMax)
                }
            })
            .collect::<Result<BTreeMap<u32, OptionalMinMaxSequence>>>()?;
        let max_persisted_timestamp = decode_timestamp_from_field(
            proto_partition_checkpoint.max_persisted_timestamp,
            "partition_checkpoint.max_persisted_timestamp",
        )?;
        let partition_checkpoint = PartitionCheckpoint::new(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
            sequencer_numbers,
            max_persisted_timestamp,
        );

        // extract database checkpoint
        let proto_database_checkpoint =
            proto_msg
                .database_checkpoint
                .context(IoxMetadataFieldMissing {
                    field: "database_checkpoint",
                })?;
        let sequencer_numbers = proto_database_checkpoint
            .sequencer_numbers
            .into_iter()
            .map(|(sequencer_id, min_max)| {
                let min = min_max.min.as_ref().map(|min| min.value);

                if min.map(|min| min <= min_max.max).unwrap_or(true) {
                    Ok((sequencer_id, OptionalMinMaxSequence::new(min, min_max.max)))
                } else {
                    Err(Error::IoxMetadataMinMax)
                }
            })
            .collect::<Result<BTreeMap<u32, OptionalMinMaxSequence>>>()?;
        let database_checkpoint = DatabaseCheckpoint::new(sequencer_numbers);

        Ok(Self {
            creation_timestamp,
            time_of_first_write,
            time_of_last_write,
            table_name,
            partition_key,
            chunk_id: proto_msg.chunk_id.try_into().context(CannotDecodeChunkId)?,
            partition_checkpoint,
            database_checkpoint,
            chunk_order: ChunkOrder::new(proto_msg.chunk_order).ok_or_else(|| {
                Error::IoxMetadataFieldMissing {
                    field: "chunk_order".to_string(),
                }
            })?,
        })
    }

    /// Convert to protobuf v3 message.
    pub(crate) fn to_protobuf(&self) -> std::result::Result<Vec<u8>, prost::EncodeError> {
        let proto_partition_checkpoint = proto::PartitionCheckpoint {
            sequencer_numbers: self
                .partition_checkpoint
                .sequencer_numbers_iter()
                .map(|(sequencer_id, min_max)| {
                    (
                        sequencer_id,
                        proto::OptionalMinMaxSequence {
                            min: min_max
                                .min()
                                .map(|min| proto::OptionalUint64 { value: min }),
                            max: min_max.max(),
                        },
                    )
                })
                .collect(),
            max_persisted_timestamp: Some(
                self.partition_checkpoint.max_persisted_timestamp().into(),
            ),
        };

        let proto_database_checkpoint = proto::DatabaseCheckpoint {
            sequencer_numbers: self
                .database_checkpoint
                .sequencer_numbers_iter()
                .map(|(sequencer_id, min_max)| {
                    (
                        sequencer_id,
                        proto::OptionalMinMaxSequence {
                            min: min_max
                                .min()
                                .map(|min| proto::OptionalUint64 { value: min }),
                            max: min_max.max(),
                        },
                    )
                })
                .collect(),
        };

        let proto_msg = proto::IoxMetadata {
            version: METADATA_VERSION,
            creation_timestamp: Some(self.creation_timestamp.into()),
            time_of_first_write: Some(self.time_of_first_write.into()),
            time_of_last_write: Some(self.time_of_last_write.into()),
            table_name: self.table_name.to_string(),
            partition_key: self.partition_key.to_string(),
            chunk_id: self.chunk_id.into(),
            partition_checkpoint: Some(proto_partition_checkpoint),
            database_checkpoint: Some(proto_database_checkpoint),
            chunk_order: self.chunk_order.get(),
        };

        let mut buf = Vec::new();
        proto_msg.encode(&mut buf)?;

        Ok(buf)
    }
}

fn decode_timestamp_from_field(
    value: Option<pbjson_types::Timestamp>,
    field: &'static str,
) -> Result<DateTime<Utc>> {
    value
        .context(IoxMetadataFieldMissing { field })?
        .try_into()
        .map_err(|e| Box::new(e) as _)
        .context(IoxMetadataBroken)
}

/// Parquet metadata with IOx-specific wrapper.
#[derive(Debug, Clone)]
pub struct IoxParquetMetaData {
    /// [Apache Parquet] metadata as freestanding [Apache Thrift]-encoded, and [Zstandard]-compressed bytes.
    ///
    /// This can be used to store metadata separate from the related payload data. The usage of [Apache Thrift] allows the
    /// same stability guarantees as the usage of an ordinary [Apache Parquet] file. To encode a thrift message into bytes
    /// the [Thrift Compact Protocol] is used.
    ///
    /// [Apache Parquet]: https://parquet.apache.org/
    /// [Apache Thrift]: https://thrift.apache.org/
    /// [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
    /// [Zstandard]: http://facebook.github.io/zstd/
    data: Vec<u8>,
}

impl IoxParquetMetaData {
    /// Read parquet metadata from a parquet file.
    pub fn from_file_bytes(data: Vec<u8>) -> Result<Self> {
        let cursor = SliceableCursor::new(data);
        let reader = SerializedFileReader::new(cursor).context(ParquetMetaDataRead {})?;
        let parquet_md = reader.metadata().clone();
        let data = Self::parquet_md_to_thrift(parquet_md)?;
        Ok(Self::from_thrift_bytes(data))
    }

    /// Read parquet metadata from thrift bytes.
    pub fn from_thrift_bytes(mut data: Vec<u8>) -> Self {
        data.shrink_to_fit();
        Self { data }
    }

    /// [Apache Parquet] metadata as freestanding [Apache Thrift]-encoded, and [Zstandard]-compressed bytes.
    ///
    /// This can be used to store metadata separate from the related payload data. The usage of [Apache Thrift] allows the
    /// same stability guarantees as the usage of an ordinary [Apache Parquet] file. To encode a thrift message into bytes
    /// the [Thrift Compact Protocol] is used.
    ///
    /// [Apache Parquet]: https://parquet.apache.org/
    /// [Apache Thrift]: https://thrift.apache.org/
    /// [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
    /// [Zstandard]: http://facebook.github.io/zstd/
    pub fn thrift_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    /// Encode [Apache Parquet] metadata as freestanding [Apache Thrift]-encoded, and [Zstandard]-compressed bytes.
    ///
    /// This can be used to store metadata separate from the related payload data. The usage of [Apache Thrift] allows the
    /// same stability guarantees as the usage of an ordinary [Apache Parquet] file. To encode a thrift message into bytes
    /// the [Thrift Compact Protocol] is used.
    ///
    /// [Apache Parquet]: https://parquet.apache.org/
    /// [Apache Thrift]: https://thrift.apache.org/
    /// [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
    /// [Zstandard]: http://facebook.github.io/zstd/
    fn parquet_md_to_thrift(parquet_md: ParquetMetaData) -> Result<Vec<u8>> {
        // step 1: assemble a thrift-compatible struct
        use parquet::schema::types::to_thrift as schema_to_thrift;

        let file_metadata = parquet_md.file_metadata();
        let thrift_schema =
            schema_to_thrift(file_metadata.schema()).context(ParquetSchemaToThrift {})?;
        let thrift_row_groups: Vec<_> = parquet_md
            .row_groups()
            .iter()
            .map(|rg| rg.to_thrift())
            .collect();

        let thrift_file_metadata = parquet_format::FileMetaData {
            version: file_metadata.version(),
            schema: thrift_schema,

            // TODO: column order thrift wrapper (https://github.com/influxdata/influxdb_iox/issues/1408)
            // NOTE: currently the column order is `None` for all written files, see https://github.com/apache/arrow-rs/blob/4dfbca6e5791be400d2fd3ae863655445327650e/parquet/src/file/writer.rs#L193
            column_orders: None,
            num_rows: file_metadata.num_rows(),
            row_groups: thrift_row_groups,
            key_value_metadata: file_metadata.key_value_metadata().clone(),
            created_by: file_metadata.created_by().clone(),
        };

        // step 2: serialize the thrift struct into bytes
        let mut buffer = Vec::new();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut buffer);
            thrift_file_metadata
                .write_to_out_protocol(&mut protocol)
                .context(ThriftWriteFailure {})?;
            protocol.flush().context(ThriftWriteFailure {})?;
        }

        // step 3: compress data
        // Note: level 0 is the zstd-provided default
        let buffer = zstd::encode_all(&buffer[..], 0).context(ZstdEncodeFailure)?;

        Ok(buffer)
    }

    /// Decode [Apache Parquet] metadata from [Apache Thrift]-encoded, and [Zstandard]-compressed bytes.
    ///
    /// [Apache Parquet]: https://parquet.apache.org/
    /// [Apache Thrift]: https://thrift.apache.org/
    /// [Zstandard]: http://facebook.github.io/zstd/
    pub fn decode(&self) -> Result<DecodedIoxParquetMetaData> {
        // step 1: decompress
        let data = zstd::decode_all(&self.data[..]).context(ZstdDecodeFailure)?;

        // step 2: load thrift data from byte stream
        let thrift_file_metadata = {
            let mut protocol = TCompactInputProtocol::new(&data[..]);
            parquet_format::FileMetaData::read_from_in_protocol(&mut protocol)
                .context(ThriftReadFailure {})?
        };

        // step 3: convert thrift to in-mem structs
        use parquet::schema::types::from_thrift as schema_from_thrift;

        let schema =
            schema_from_thrift(&thrift_file_metadata.schema).context(ParquetSchemaFromThrift {})?;
        let schema_descr = Arc::new(ParquetSchemaDescriptor::new(schema));
        let mut row_groups = Vec::with_capacity(thrift_file_metadata.row_groups.len());
        for rg in thrift_file_metadata.row_groups {
            row_groups.push(
                ParquetRowGroupMetaData::from_thrift(Arc::clone(&schema_descr), rg)
                    .context(ParquetRowGroupFromThrift {})?,
            );
        }
        // TODO: parse column order, or ignore it: https://github.com/influxdata/influxdb_iox/issues/1408
        let column_orders = None;

        let file_metadata = ParquetFileMetaData::new(
            thrift_file_metadata.version,
            thrift_file_metadata.num_rows,
            thrift_file_metadata.created_by,
            thrift_file_metadata.key_value_metadata,
            schema_descr,
            column_orders,
        );
        let md = ParquetMetaData::new(file_metadata, row_groups);
        Ok(DecodedIoxParquetMetaData { md })
    }

    /// In-memory size in bytes, including `self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.capacity()
    }
}

/// Parquet metadata with IOx-specific wrapper, in decoded form.
#[derive(Debug)]
pub struct DecodedIoxParquetMetaData {
    /// Low-level parquet metadata that stores all relevant information.
    md: ParquetMetaData,
}

impl DecodedIoxParquetMetaData {
    /// Return the number of rows in the parquet file
    pub fn row_count(&self) -> usize {
        self.md.file_metadata().num_rows() as usize
    }

    /// Read IOx metadata from file-level key-value parquet metadata.
    pub fn read_iox_metadata(&self) -> Result<IoxMetadata> {
        // find file-level key-value metadata entry
        let kv = self
            .md
            .file_metadata()
            .key_value_metadata()
            .as_ref()
            .context(IoxMetadataMissing)?
            .iter()
            .find(|kv| kv.key == METADATA_KEY)
            .context(IoxMetadataMissing)?;

        // extract protobuf message from key-value entry
        let proto_base64 = kv.value.as_ref().context(IoxMetadataMissing)?;
        let proto_bytes = base64::decode(proto_base64)
            .map_err(|err| Box::new(err) as _)
            .context(IoxMetadataBroken)?;

        // convert to Rust object
        IoxMetadata::from_protobuf(proto_bytes.as_slice())
    }

    /// Read IOx schema from parquet metadata.
    pub fn read_schema(&self) -> Result<Arc<Schema>> {
        let file_metadata = self.md.file_metadata();

        let arrow_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .context(ArrowFromParquetFailure {})?;

        let arrow_schema_ref = Arc::new(arrow_schema);

        let schema: Schema = arrow_schema_ref
            .try_into()
            .context(IoxFromArrowFailure {})?;
        Ok(Arc::new(schema))
    }

    /// Read IOx statistics (including timestamp range) from parquet metadata.
    pub fn read_statistics(&self, schema: &Schema) -> Result<Vec<ColumnSummary>> {
        ensure!(!self.md.row_groups().is_empty(), NoRowGroup);

        let mut column_summaries = Vec::with_capacity(schema.len());

        for (row_group_idx, row_group) in self.md.row_groups().iter().enumerate() {
            let row_group_column_summaries =
                read_statistics_from_parquet_row_group(row_group, row_group_idx, schema)?;

            combine_column_summaries(&mut column_summaries, row_group_column_summaries);
        }

        Ok(column_summaries)
    }
}

/// Read IOx statistics from parquet row group metadata.
fn read_statistics_from_parquet_row_group(
    row_group: &ParquetRowGroupMetaData,
    row_group_idx: usize,
    schema: &Schema,
) -> Result<Vec<ColumnSummary>> {
    let mut column_summaries = Vec::with_capacity(schema.len());

    for ((iox_type, field), column_chunk_metadata) in schema.iter().zip(row_group.columns()) {
        if let Some(iox_type) = iox_type {
            let parquet_stats = column_chunk_metadata
                .statistics()
                .context(StatisticsMissing {
                    row_group: row_group_idx,
                    column: field.name().clone(),
                })?;

            let min_max_set = parquet_stats.has_min_max_set();
            if min_max_set && parquet_stats.is_min_max_deprecated() {
                StatisticsMinMaxDeprecated {
                    row_group: row_group_idx,
                    column: field.name().clone(),
                }
                .fail()?;
            }

            let count =
                (row_group.num_rows().max(0) as u64).saturating_sub(parquet_stats.null_count());

            let stats = extract_iox_statistics(
                parquet_stats,
                min_max_set,
                iox_type,
                count,
                row_group_idx,
                field.name(),
            )?;
            column_summaries.push(ColumnSummary {
                name: field.name().clone(),
                influxdb_type: Some(match iox_type {
                    InfluxColumnType::IOx(_) => todo!(),
                    InfluxColumnType::Tag => InfluxDbType::Tag,
                    InfluxColumnType::Field(_) => InfluxDbType::Field,
                    InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                }),
                stats,
            });
        }
    }

    Ok(column_summaries)
}

fn combine_column_summaries(total: &mut Vec<ColumnSummary>, other: Vec<ColumnSummary>) {
    for col in total.iter_mut() {
        if let Some(other_col) = other.iter().find(|c| c.name == col.name) {
            col.update_from(other_col);
        }
    }

    for other_col in other.into_iter() {
        if !total.iter().any(|c| c.name == other_col.name) {
            total.push(other_col);
        }
    }
}

/// Extract IOx statistics from parquet statistics.
///
/// This is required because upstream does not have a mapper from
/// parquet statistics back to arrow or Rust native types.
fn extract_iox_statistics(
    parquet_stats: &ParquetStatistics,
    min_max_set: bool,
    iox_type: InfluxColumnType,
    total_count: u64,
    row_group_idx: usize,
    column_name: &str,
) -> Result<Statistics> {
    let null_count = parquet_stats.null_count();

    match (parquet_stats, iox_type) {
        (ParquetStatistics::Boolean(stats), InfluxColumnType::Field(InfluxFieldType::Boolean)) => {
            Ok(Statistics::Bool(StatValues {
                min: min_max_set.then(|| *stats.min()),
                max: min_max_set.then(|| *stats.max()),
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        (ParquetStatistics::Int64(stats), InfluxColumnType::Field(InfluxFieldType::Integer)) => {
            Ok(Statistics::I64(StatValues {
                min: min_max_set.then(|| *stats.min()),
                max: min_max_set.then(|| *stats.max()),
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        (ParquetStatistics::Int64(stats), InfluxColumnType::Field(InfluxFieldType::UInteger)) => {
            Ok(Statistics::U64(StatValues {
                min: min_max_set.then(|| *stats.min() as u64),
                max: min_max_set.then(|| *stats.max() as u64),
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        (ParquetStatistics::Double(stats), InfluxColumnType::Field(InfluxFieldType::Float)) => {
            Ok(Statistics::F64(StatValues {
                min: min_max_set.then(|| *stats.min()),
                max: min_max_set.then(|| *stats.max()),
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        (ParquetStatistics::Int64(stats), InfluxColumnType::Timestamp) => {
            Ok(Statistics::I64(StatValues {
                min: Some(*stats.min()),
                max: Some(*stats.max()),
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        (ParquetStatistics::ByteArray(stats), InfluxColumnType::Tag)
        | (ParquetStatistics::ByteArray(stats), InfluxColumnType::Field(InfluxFieldType::String)) => {
            Ok(Statistics::String(StatValues {
                min: min_max_set
                    .then(|| {
                        stats
                            .min()
                            .as_utf8()
                            .context(StatisticsUtf8Error {
                                row_group: row_group_idx,
                                column: column_name.to_string(),
                            })
                            .map(|x| x.to_string())
                    })
                    .transpose()?,
                max: min_max_set
                    .then(|| {
                        stats
                            .max()
                            .as_utf8()
                            .context(StatisticsUtf8Error {
                                row_group: row_group_idx,
                                column: column_name.to_string(),
                            })
                            .map(|x| x.to_string())
                    })
                    .transpose()?,
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count,
                total_count,
            }))
        }
        _ => Err(Error::StatisticsTypeMismatch {
            row_group: row_group_idx,
            column: column_name.to_string(),
            expected: iox_type,
            actual: parquet_stats.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use internal_types::schema::TIME_COLUMN_NAME;

    use crate::test_utils::{
        chunk_addr, create_partition_and_database_checkpoint, load_parquet_from_store, make_chunk,
        make_chunk_no_row_group, make_iox_object_store, TestSize,
    };

    #[tokio::test]
    async fn test_restore_from_file() {
        // setup: preserve chunk to object store
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let decoded = parquet_metadata.decode().unwrap();

        // step 1: read back schema
        let schema_actual = decoded.read_schema().unwrap();
        let schema_expected = chunk.schema();
        assert_eq!(schema_actual, schema_expected);

        // step 2: read back statistics
        let table_summary_actual = decoded.read_statistics(&schema_actual).unwrap();
        let table_summary_expected = chunk.table_summary();
        for (actual_column, expected_column) in table_summary_actual
            .iter()
            .zip(table_summary_expected.columns.iter())
        {
            assert_eq!(actual_column, expected_column);
        }
    }

    #[tokio::test]
    async fn test_restore_from_thrift() {
        // setup: write chunk to object store and only keep thrift-encoded metadata
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let data = parquet_metadata.thrift_bytes().to_vec();
        let parquet_metadata = IoxParquetMetaData::from_thrift_bytes(data);
        let decoded = parquet_metadata.decode().unwrap();

        // step 1: read back schema
        let schema_actual = decoded.read_schema().unwrap();
        let schema_expected = chunk.schema();
        assert_eq!(schema_actual, schema_expected);

        // step 2: read back statistics
        let table_summary_actual = decoded.read_statistics(&schema_actual).unwrap();
        let table_summary_expected = chunk.table_summary();
        assert_eq!(table_summary_actual, table_summary_expected.columns);
    }

    #[tokio::test]
    async fn test_restore_from_file_no_row_group() {
        // setup: preserve chunk to object store
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk_no_row_group(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let decoded = parquet_metadata.decode().unwrap();

        // step 1: read back schema
        let schema_actual = decoded.read_schema().unwrap();
        let schema_expected = chunk.schema();
        assert_eq!(schema_actual, schema_expected);

        // step 2: reading back statistics fails
        let res = decoded.read_statistics(&schema_actual);
        assert_eq!(
            res.unwrap_err().to_string(),
            "No row group found, cannot recover statistics"
        );
    }

    #[tokio::test]
    async fn test_restore_from_thrift_no_row_group() {
        // setup: write chunk to object store and only keep thrift-encoded metadata
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk_no_row_group(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let data = parquet_metadata.thrift_bytes().to_vec();
        let parquet_metadata = IoxParquetMetaData::from_thrift_bytes(data);
        let decoded = parquet_metadata.decode().unwrap();

        // step 1: read back schema
        let schema_actual = decoded.read_schema().unwrap();
        let schema_expected = chunk.schema();
        assert_eq!(schema_actual, schema_expected);

        // step 2: reading back statistics fails
        let res = decoded.read_statistics(&schema_actual);
        assert_eq!(
            res.unwrap_err().to_string(),
            "No row group found, cannot recover statistics"
        );
    }

    #[tokio::test]
    async fn test_make_chunk() {
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let decoded = parquet_metadata.decode().unwrap();

        assert!(decoded.md.num_row_groups() > 1);
        assert_ne!(decoded.md.file_metadata().schema_descr().num_columns(), 0);

        // column count in summary including the timestamp column
        assert_eq!(
            chunk.table_summary().columns.len(),
            decoded.md.file_metadata().schema_descr().num_columns()
        );

        // check that column counts are consistent
        let n_rows = decoded.md.file_metadata().num_rows() as u64;
        assert!(n_rows >= decoded.md.num_row_groups() as u64);
        for summary in &chunk.table_summary().columns {
            assert!(summary.total_count() <= n_rows);
        }

        // check column names
        for column in decoded.md.file_metadata().schema_descr().columns() {
            assert!((column.name() == TIME_COLUMN_NAME) || column.name().starts_with("foo_"));
        }
    }

    #[tokio::test]
    async fn test_make_chunk_no_row_group() {
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk_no_row_group(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        let decoded = parquet_metadata.decode().unwrap();

        assert_eq!(decoded.md.num_row_groups(), 0);
        assert_ne!(decoded.md.file_metadata().schema_descr().num_columns(), 0);
        assert_eq!(decoded.md.file_metadata().num_rows(), 0);

        // column count in summary including the timestamp column
        assert_eq!(
            chunk.table_summary().columns.len(),
            decoded.md.file_metadata().schema_descr().num_columns()
        );

        // check column names
        for column in decoded.md.file_metadata().schema_descr().columns() {
            assert!((column.name() == TIME_COLUMN_NAME) || column.name().starts_with("foo_"));
        }
    }

    #[test]
    fn test_iox_metadata_from_protobuf_checks_version() {
        let table_name = Arc::from("table1");
        let partition_key = Arc::from("part1");
        let (partition_checkpoint, database_checkpoint) = create_partition_and_database_checkpoint(
            Arc::clone(&table_name),
            Arc::clone(&partition_key),
        );
        let metadata = IoxMetadata {
            creation_timestamp: Utc::now(),
            table_name,
            partition_key,
            chunk_id: ChunkId::new_test(1337),
            partition_checkpoint,
            database_checkpoint,
            time_of_first_write: Utc::now(),
            time_of_last_write: Utc::now(),
            chunk_order: ChunkOrder::new(5).unwrap(),
        };

        let proto_bytes = metadata.to_protobuf().unwrap();

        // tamper message
        let mut proto_msg = proto::IoxMetadata::decode(proto_bytes.as_slice()).unwrap();
        proto_msg.version = 42;
        let mut proto_bytes = Vec::new();
        proto_msg.encode(&mut proto_bytes).unwrap();

        // decoding should fail now
        assert_eq!(
            IoxMetadata::from_protobuf(&proto_bytes)
                .unwrap_err()
                .to_string(),
            format!(
                "Format version of IOx metadata is 42 but only [{}] are supported",
                METADATA_VERSION
            )
        );
    }

    #[tokio::test]
    async fn test_parquet_metadata_size() {
        // setup: preserve chunk to object store
        let iox_object_store = make_iox_object_store().await;
        let chunk = make_chunk(
            Arc::clone(&iox_object_store),
            "foo",
            chunk_addr(1),
            TestSize::Full,
        )
        .await;
        let parquet_data = load_parquet_from_store(&chunk, iox_object_store)
            .await
            .unwrap();
        let parquet_metadata = IoxParquetMetaData::from_file_bytes(parquet_data).unwrap();
        assert_eq!(parquet_metadata.size(), 3730);
    }
}
