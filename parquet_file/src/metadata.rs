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
use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::Bytes;
use data_types::{
    ColumnId, ColumnSet, ColumnSummary, CompactionLevel, InfluxDbType, NamespaceId,
    ParquetFileParams, PartitionKey, StatValues, Statistics, TableId, Timestamp,
    TransitionPartitionId,
};
use generated_types::influxdata::iox::ingester::v1 as proto;
use iox_time::Time;
use observability_deps::tracing::{debug, trace};
use parquet::{
    arrow::parquet_to_arrow_schema,
    file::{
        metadata::{
            FileMetaData as ParquetFileMetaData, ParquetMetaData,
            RowGroupMetaData as ParquetRowGroupMetaData,
        },
        reader::FileReader,
        serialized_reader::SerializedFileReader,
        statistics::Statistics as ParquetStatistics,
    },
    schema::types::SchemaDescriptor as ParquetSchemaDescriptor,
};
use prost::Message;
use schema::{
    sort::{SortKey, SortKeyBuilder},
    InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{convert::TryInto, fmt::Debug, mem, sync::Arc};
use thrift::protocol::{
    TCompactInputProtocol, TCompactOutputProtocol, TOutputProtocol, TSerializable,
};
use uuid::Uuid;

/// Current version for serialized metadata.
///
/// For breaking changes, this will change.
///
/// **Important: When changing this structure, consider bumping the catalog transaction version (`TRANSACTION_VERSION`
///              in the `parquet_catalog` crate)!**
pub const METADATA_VERSION: u32 = 10;

/// File-level metadata key to store the IOx-specific data.
pub const METADATA_KEY: &str = "IOX:metadata";

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
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
    IoxFromArrowFailure { source: schema::Error },

    #[snafu(display("Parquet metadata does not contain IOx metadata"))]
    IoxMetadataMissing {},

    #[snafu(display("Field missing while parsing IOx metadata: {}", field))]
    IoxMetadataFieldMissing { field: String },

    #[snafu(display("Cannot parse IOx metadata from Protobuf: {}", source))]
    IoxMetadataBroken {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Cannot encode ZSTD message for parquet metadata: {}", source))]
    ZstdEncodeFailure { source: std::io::Error },

    #[snafu(display("Cannot decode ZSTD message for parquet metadata: {}", source))]
    ZstdDecodeFailure { source: std::io::Error },

    #[snafu(display("Cannot parse UUID: {}", source))]
    UuidParse { source: uuid::Error },

    #[snafu(display("{}: `{}`", source, compaction_level))]
    InvalidCompactionLevel {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        compaction_level: i32,
    },
}

#[allow(missing_docs)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// IOx-specific metadata.
///
/// # Serialization
/// This will serialized as base64-encoded [Protocol Buffers 3] into the file-level key-value
/// Parquet metadata (under [`METADATA_KEY`]).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IoxMetadata {
    /// The uuid used as the location of the parquet file in the OS.
    /// This uuid will later be used as the catalog's ParquetFileId
    pub object_store_id: Uuid,

    /// Timestamp when this file was created.
    pub creation_timestamp: Time,

    /// namespace id of the data
    pub namespace_id: NamespaceId,

    /// namespace name of the data
    pub namespace_name: Arc<str>,

    /// table id of the data
    pub table_id: TableId,

    /// table name of the data
    pub table_name: Arc<str>,

    /// partition key of the data
    pub partition_key: PartitionKey,

    /// The compaction level of the file.
    ///
    ///  * 0 (`CompactionLevel::Initial`): represents a level-0 file that is persisted by an
    ///      Ingester. Partitions with level-0 files are usually hot/recent partitions.
    ///  * 1 (`CompactionLevel::FileOverlapped`): represents a level-1 file that is persisted by a
    ///      Compactor and potentially overlaps with other level-1 files. Partitions with level-1
    ///      files are partitions with a lot of or/and large overlapped files that have to go
    ///      through many compaction cycles before they are fully compacted to non-overlapped
    ///      files.
    ///  * 2 (`CompactionLevel::FileNonOverlapped`): represents a level-1 file that is persisted by
    ///      a Compactor and does not overlap with other files except level 0 ones. Eventually,
    ///      cold partitions (partitions that no longer needs to get compacted) will only include
    ///      one or many level-1 files
    pub compaction_level: CompactionLevel,

    /// Sort key of this chunk
    pub sort_key: Option<SortKey>,

    /// Max timestamp of creation timestamp of L0 files
    /// If this metadata is for an L0 file, this value will be the same as the `creation_timestamp`
    /// If this metadata is for an L1/L2 file, this value will be the max of all L0 files
    ///  that are compacted into this file
    pub max_l0_created_at: Time,
}

impl IoxMetadata {
    /// Convert to base64 encoded protobuf format
    pub fn to_base64(&self) -> std::result::Result<String, prost::EncodeError> {
        Ok(BASE64_STANDARD.encode(self.to_protobuf()?))
    }

    /// Read from base64 encoded protobuf format
    pub fn from_base64(proto_base64: &[u8]) -> Result<Self> {
        let proto_bytes = BASE64_STANDARD
            .decode(proto_base64)
            .map_err(|err| Box::new(err) as _)
            .context(IoxMetadataBrokenSnafu)?;

        Self::from_protobuf(&proto_bytes)
    }

    /// Convert to protobuf v3 message.
    pub(crate) fn to_protobuf(&self) -> std::result::Result<Vec<u8>, prost::EncodeError> {
        let sort_key = self.sort_key.as_ref().map(|key| proto::SortKey {
            expressions: key
                .iter()
                .map(|(name, options)| proto::sort_key::Expr {
                    column: name.to_string(),
                    descending: options.descending,
                    nulls_first: options.nulls_first,
                })
                .collect(),
        });

        let proto_msg = proto::IoxMetadata {
            object_store_id: self.object_store_id.as_bytes().to_vec(),
            creation_timestamp: Some(self.creation_timestamp.date_time().into()),
            namespace_id: self.namespace_id.get(),
            namespace_name: self.namespace_name.to_string(),
            table_id: self.table_id.get(),
            table_name: self.table_name.to_string(),
            partition_key: self.partition_key.to_string(),
            sort_key,
            compaction_level: self.compaction_level as i32,
            max_l0_created_at: Some(self.max_l0_created_at.date_time().into()),
        };

        let mut buf = Vec::new();
        proto_msg.encode(&mut buf)?;

        Ok(buf)
    }

    /// Read from protobuf message
    fn from_protobuf(data: &[u8]) -> Result<Self> {
        // extract protobuf message from bytes
        let proto_msg = proto::IoxMetadata::decode(data)
            .map_err(|err| Box::new(err) as _)
            .context(IoxMetadataBrokenSnafu)?;

        // extract creation timestamp
        let creation_timestamp =
            decode_timestamp_from_field(proto_msg.creation_timestamp, "creation_timestamp")?;
        let max_l0_created_at =
            decode_timestamp_from_field(proto_msg.max_l0_created_at, "max_l0_created_at")?;

        // extract strings
        let namespace_name = Arc::from(proto_msg.namespace_name.as_ref());
        let table_name = Arc::from(proto_msg.table_name.as_ref());
        let partition_key = PartitionKey::from(proto_msg.partition_key);

        // sort key
        let sort_key = proto_msg.sort_key.map(|proto_key| {
            let mut builder = SortKeyBuilder::with_capacity(proto_key.expressions.len());
            for expr in proto_key.expressions {
                builder = builder.with_col_opts(expr.column, expr.descending, expr.nulls_first)
            }
            builder.build()
        });

        Ok(Self {
            object_store_id: parse_uuid(&proto_msg.object_store_id)?.ok_or_else(|| {
                Error::IoxMetadataFieldMissing {
                    field: "object_store_id".to_string(),
                }
            })?,
            creation_timestamp,
            namespace_id: NamespaceId::new(proto_msg.namespace_id),
            namespace_name,
            table_id: TableId::new(proto_msg.table_id),
            table_name,
            partition_key,
            sort_key,
            compaction_level: proto_msg.compaction_level.try_into().context(
                InvalidCompactionLevelSnafu {
                    compaction_level: proto_msg.compaction_level,
                },
            )?,
            max_l0_created_at,
        })
    }

    /// Generate metadata for a file generated from some process other than IOx ingesting.
    ///
    /// This metadata will not have valid catalog values; inserting files with this metadata into
    /// the catalog should get valid values out-of-band.
    pub fn external(creation_timestamp_ns: i64, table_name: impl Into<Arc<str>>) -> Self {
        Self {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(creation_timestamp_ns),
            namespace_id: NamespaceId::new(1),
            namespace_name: "external".into(),
            table_id: TableId::new(1),
            table_name: table_name.into(),
            partition_key: "unknown".into(),
            compaction_level: CompactionLevel::Initial,
            sort_key: None,
            max_l0_created_at: Time::from_timestamp_nanos(creation_timestamp_ns),
        }
    }

    /// verify uuid
    pub fn match_object_store_id(&self, uuid: Uuid) -> bool {
        uuid == self.object_store_id
    }

    /// Create a corresponding iox catalog's ParquetFile
    ///
    /// # Panics
    ///
    /// This method panics if the [`IoxParquetMetaData`] structure does not
    /// contain valid metadata bytes, has no readable schema, or has no field
    /// statistics.
    ///
    /// A [`RecordBatch`] serialized without the embedded metadata found in the
    /// IOx [`Schema`] type will cause a statistic resolution failure due to
    /// lack of the IOx field type metadata for the time column. Batches
    /// produced from the through the IOx write path always include this
    /// metadata.
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    pub fn to_parquet_file<F>(
        &self,
        partition_id: TransitionPartitionId,
        file_size_bytes: usize,
        metadata: &IoxParquetMetaData,
        column_id_map: F,
    ) -> ParquetFileParams
    where
        F: for<'a> Fn(&'a str) -> ColumnId,
    {
        let decoded = metadata.decode().expect("invalid IOx metadata");
        trace!(
            ?partition_id,
            ?decoded,
            "DecodedIoxParquetMetaData decoded from its IoxParquetMetaData"
        );
        let row_count = decoded.row_count();
        if decoded.md.row_groups().is_empty() {
            debug!(
                ?partition_id,
                "Decoded IoxParquetMetaData has no row groups to provide useful statistics"
            );
        }

        // Derive the min/max timestamp from the Parquet column statistics.
        let schema = decoded
            .read_schema()
            .expect("failed to read encoded schema");
        let stats = decoded
            .read_statistics(&schema)
            .expect("invalid statistics");
        let columns: Vec<_> = stats.iter().map(|v| column_id_map(&v.name)).collect();
        let time_summary = stats
            .into_iter()
            .find(|v| v.name == TIME_COLUMN_NAME)
            .expect("no time column in metadata statistics");

        // Sanity check the type of this column before using the values.
        assert_eq!(time_summary.influxdb_type, InfluxDbType::Timestamp);

        // Extract the min/max timestamps.
        let (min_time, max_time) = match time_summary.stats {
            Statistics::I64(stats) => {
                let min = Timestamp::new(stats.min.expect("no min time statistic"));
                let max = Timestamp::new(stats.max.expect("no max time statistic"));
                (min, max)
            }
            _ => panic!("unexpected physical type for timestamp column"),
        };

        ParquetFileParams {
            namespace_id: self.namespace_id,
            table_id: self.table_id,
            partition_id,
            object_store_id: self.object_store_id,
            min_time,
            max_time,
            file_size_bytes: file_size_bytes as i64,
            compaction_level: self.compaction_level,
            row_count: row_count.try_into().expect("row count overflows i64"),
            created_at: Timestamp::from(self.creation_timestamp),
            column_set: ColumnSet::new(columns),
            max_l0_created_at: Timestamp::from(self.max_l0_created_at),
        }
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        // size of this structure, including inlined size + heap sizes
        let size_without_sortkey_refs = mem::size_of_val(self)
            + self.namespace_name.as_bytes().len()
            + self.table_name.as_bytes().len()
            + std::mem::size_of::<PartitionKey>();

        if let Some(sort_key) = self.sort_key.as_ref() {
            size_without_sortkey_refs +
                sort_key.size()
            // already included in `size_of_val(self)` above so remove to avoid double counting
                - mem::size_of_val(sort_key)
        } else {
            size_without_sortkey_refs
        }
    }
}

/// Parse big-endian UUID from protobuf.
pub fn parse_uuid(bytes: &[u8]) -> Result<Option<Uuid>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        let uuid = Uuid::from_slice(bytes).context(UuidParseSnafu {})?;
        Ok(Some(uuid))
    }
}

fn decode_timestamp_from_field(
    value: Option<pbjson_types::Timestamp>,
    field: &'static str,
) -> Result<Time> {
    let date_time = value
        .context(IoxMetadataFieldMissingSnafu { field })?
        .try_into()
        .map_err(|e| Box::new(e) as _)
        .context(IoxMetadataBrokenSnafu)?;

    Ok(Time::from_date_time(date_time))
}

/// Parquet metadata with IOx-specific wrapper.
#[derive(Clone, PartialEq, Eq)]
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

impl Debug for IoxParquetMetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoxParquetMetaData")
            .field("data", &format!("<{} bytes>", self.data.len()))
            .finish()
    }
}

impl IoxParquetMetaData {
    /// Read parquet metadata from a parquet file.
    pub fn from_file_bytes(data: Bytes) -> Result<Option<Self>> {
        if data.is_empty() {
            return Ok(None);
        }

        let reader = SerializedFileReader::new(data).context(ParquetMetaDataReadSnafu {})?;
        let parquet_md = reader.metadata().clone();

        let data = Self::parquet_md_to_thrift(parquet_md)?;
        Ok(Some(Self::from_thrift_bytes(data)))
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
            schema_to_thrift(file_metadata.schema()).context(ParquetSchemaToThriftSnafu {})?;
        let thrift_row_groups: Vec<_> = parquet_md
            .row_groups()
            .iter()
            .map(|rg| rg.to_thrift())
            .collect();

        let thrift_file_metadata = parquet::format::FileMetaData {
            version: file_metadata.version(),
            schema: thrift_schema,

            // TODO: column order thrift wrapper (https://github.com/influxdata/influxdb_iox/issues/1408)
            // NOTE: currently the column order is `None` for all written files, see https://github.com/apache/arrow-rs/blob/4dfbca6e5791be400d2fd3ae863655445327650e/parquet/src/file/writer.rs#L193
            column_orders: None,
            num_rows: file_metadata.num_rows(),
            row_groups: thrift_row_groups,
            key_value_metadata: file_metadata.key_value_metadata().cloned(),
            created_by: file_metadata.created_by().map(|s| s.to_string()),
            encryption_algorithm: None,
            footer_signing_key_metadata: None,
        };

        // step 2: serialize the thrift struct into bytes
        Self::try_from(thrift_file_metadata).map(|opt| opt.data)
    }

    /// Decode [Apache Parquet] metadata from [Apache Thrift]-encoded, and [Zstandard]-compressed bytes.
    ///
    /// [Apache Parquet]: https://parquet.apache.org/
    /// [Apache Thrift]: https://thrift.apache.org/
    /// [Zstandard]: http://facebook.github.io/zstd/
    pub fn decode(&self) -> Result<DecodedIoxParquetMetaData> {
        // step 1: decompress
        let data = zstd::decode_all(&self.data[..]).context(ZstdDecodeFailureSnafu)?;

        // step 2: load thrift data from byte stream
        let thrift_file_metadata = {
            let mut protocol = TCompactInputProtocol::new(&data[..]);
            parquet::format::FileMetaData::read_from_in_protocol(&mut protocol)
                .context(ThriftReadFailureSnafu {})?
        };

        // step 3: convert thrift to in-mem structs
        use parquet::schema::types::from_thrift as schema_from_thrift;

        let schema = schema_from_thrift(&thrift_file_metadata.schema)
            .context(ParquetSchemaFromThriftSnafu {})?;
        let schema_descr = Arc::new(ParquetSchemaDescriptor::new(schema));
        let mut row_groups = Vec::with_capacity(thrift_file_metadata.row_groups.len());
        for rg in thrift_file_metadata.row_groups {
            row_groups.push(
                ParquetRowGroupMetaData::from_thrift(Arc::clone(&schema_descr), rg)
                    .context(ParquetRowGroupFromThriftSnafu {})?,
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
        assert_eq!(self.data.len(), self.data.capacity(), "data is not trimmed");
        mem::size_of_val(self) + self.data.capacity()
    }
}

impl TryFrom<parquet::format::FileMetaData> for IoxParquetMetaData {
    type Error = Error;

    fn try_from(v: parquet::format::FileMetaData) -> Result<Self, Self::Error> {
        let mut buffer = Vec::new();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut buffer);
            v.write_to_out_protocol(&mut protocol)
                .context(ThriftWriteFailureSnafu {})?;
            protocol.flush().context(ThriftWriteFailureSnafu {})?;
        }

        // step 3: compress data
        // Note: level 0 is the zstd-provided default
        let buffer = zstd::encode_all(&buffer[..], 0).context(ZstdEncodeFailureSnafu)?;

        Ok(Self::from_thrift_bytes(buffer))
    }
}

/// Parquet metadata with IOx-specific wrapper, in decoded form.
#[derive(Debug)]
pub struct DecodedIoxParquetMetaData {
    /// Low-level parquet metadata that stores all relevant information.
    md: ParquetMetaData,
}

impl DecodedIoxParquetMetaData {
    /// Return parquet file metadata
    pub fn parquet_file_meta(&self) -> &ParquetFileMetaData {
        self.md.file_metadata()
    }

    /// return row group metadata
    pub fn parquet_row_group_metadata(&self) -> &[ParquetRowGroupMetaData] {
        self.md.row_groups()
    }

    /// Return the number of rows in the parquet file
    pub fn row_count(&self) -> usize {
        self.md.file_metadata().num_rows() as usize
    }

    /// Read IOx metadata from file-level key-value parquet metadata.
    pub fn read_iox_metadata_new(&self) -> Result<IoxMetadata> {
        // find file-level key-value metadata entry
        let kv = self
            .md
            .file_metadata()
            .key_value_metadata()
            .as_ref()
            .context(IoxMetadataMissingSnafu)?
            .iter()
            .find(|kv| kv.key == METADATA_KEY)
            .context(IoxMetadataMissingSnafu)?;

        // extract protobuf message from key-value entry
        let proto_base64 = kv.value.as_ref().context(IoxMetadataMissingSnafu)?;
        // read to rust object
        IoxMetadata::from_base64(proto_base64.as_bytes())
    }

    /// Read IOx schema from parquet metadata.
    pub fn read_schema(&self) -> Result<Schema> {
        let file_metadata = self.md.file_metadata();

        let arrow_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .context(ArrowFromParquetFailureSnafu {})?;

        // The parquet reader will propagate any metadata keys present in the parquet
        // metadata onto the arrow schema. This will include the encoded IOxMetadata
        //
        // We strip this out to avoid false negatives when comparing schemas for equality,
        // as this metadata will vary from file to file
        let arrow_schema_ref = Arc::new(arrow_schema.with_metadata(Default::default()));

        arrow_schema_ref
            .try_into()
            .context(IoxFromArrowFailureSnafu {})
    }

    /// Read IOx statistics (including timestamp range) from parquet metadata.
    pub fn read_statistics(&self, schema: &Schema) -> Result<Vec<ColumnSummary>> {
        ensure!(!self.md.row_groups().is_empty(), NoRowGroupSnafu);

        let mut column_summaries = Vec::with_capacity(schema.len());

        for (row_group_idx, row_group) in self.md.row_groups().iter().enumerate() {
            let row_group_column_summaries =
                read_statistics_from_parquet_row_group(row_group, row_group_idx, schema)?;

            combine_column_summaries(&mut column_summaries, row_group_column_summaries);
        }

        Ok(column_summaries)
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        // This is likely a wild under count as it doesn't include
        // memory pointed to in the `ParquetMetaData` structues.
        // Feature tracked in arrow-rs: https://github.com/apache/arrow-rs/issues/1729
        mem::size_of_val(self)
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
        let parquet_stats = column_chunk_metadata
            .statistics()
            .context(StatisticsMissingSnafu {
                row_group: row_group_idx,
                column: field.name().clone(),
            })?;

        let min_max_set = parquet_stats.has_min_max_set();
        if min_max_set && parquet_stats.is_min_max_deprecated() {
            StatisticsMinMaxDeprecatedSnafu {
                row_group: row_group_idx,
                column: field.name().clone(),
            }
            .fail()?;
        }

        let count = row_group.num_rows().max(0) as u64;

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
            influxdb_type: match iox_type {
                InfluxColumnType::Tag => InfluxDbType::Tag,
                InfluxColumnType::Field(_) => InfluxDbType::Field,
                InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
            },
            stats,
        });
    }

    Ok(column_summaries)
}

fn combine_column_summaries(total: &mut Vec<ColumnSummary>, other: Vec<ColumnSummary>) {
    for col in &mut *total {
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
                null_count: Some(null_count),
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
                null_count: Some(null_count),
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
                null_count: Some(null_count),
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
                null_count: Some(null_count),
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
                null_count: Some(null_count),
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
                            .context(StatisticsUtf8Snafu {
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
                            .context(StatisticsUtf8Snafu {
                                row_group: row_group_idx,
                                column: column_name.to_string(),
                            })
                            .map(|x| x.to_string())
                    })
                    .transpose()?,
                distinct_count: parquet_stats
                    .distinct_count()
                    .and_then(|x| x.try_into().ok()),
                null_count: Some(null_count),
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
    use arrow::{
        array::{ArrayRef, StringArray, TimestampNanosecondArray},
        record_batch::RecordBatch,
    };
    use data_types::CompactionLevel;
    use datafusion_util::{unbounded_memory_pool, MemoryStream};
    use schema::builder::SchemaBuilder;

    #[test]
    fn iox_metadata_protobuf_round_trip() {
        let object_store_id = Uuid::new_v4();

        let sort_key = SortKeyBuilder::new().with_col("sort_col").build();

        let create_time = Time::from_timestamp(3234, 0).unwrap();

        let iox_metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: create_time,
            namespace_id: NamespaceId::new(2),
            namespace_name: Arc::from("hi"),
            table_id: TableId::new(3),
            table_name: Arc::from("weather"),
            partition_key: PartitionKey::from("part"),
            compaction_level: CompactionLevel::Initial,
            sort_key: Some(sort_key),
            max_l0_created_at: create_time,
        };

        let proto = iox_metadata.to_protobuf().unwrap();

        let iox_metadata_again = IoxMetadata::from_protobuf(&proto).unwrap();

        assert_eq!(iox_metadata, iox_metadata_again);
    }

    #[tokio::test]
    async fn test_metadata_from_parquet_metadata() {
        let meta = IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_key: "potato".into(),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
            max_l0_created_at: Time::from_timestamp_nanos(42),
        };

        let array = StringArray::from_iter([Some("bananas")]);
        let data: ArrayRef = Arc::new(array);

        let timestamps = to_timestamp_array(&[1647695292000000000]);

        // Build a schema that contains the IOx metadata, ensuring it is
        // correctly populated in the final parquet file's metadata to be read
        // back later in the test.
        let schema = SchemaBuilder::new()
            .influx_field("a", InfluxFieldType::String)
            .timestamp()
            .build()
            .expect("could not create schema")
            .as_arrow();

        let batch = RecordBatch::try_new(schema, vec![data, timestamps]).unwrap();
        let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

        let (bytes, file_meta) =
            crate::serialize::to_parquet_bytes(stream, &meta, unbounded_memory_pool())
                .await
                .expect("should serialize");

        // Verify if the parquet file meta data has values
        assert!(!file_meta.row_groups.is_empty());

        // Read the metadata from the file bytes.
        //
        // This is quite wordy...
        let iox_parquet_meta = IoxParquetMetaData::from_file_bytes(Bytes::from(bytes))
            .expect("should decode")
            .expect("should contain metadata");
        assert_metadata(&iox_parquet_meta.decode().unwrap());

        // Read the metadata directly from the file metadata returned from
        // encoding, should be the same
        let iox_from_file_meta = IoxParquetMetaData::try_from(file_meta)
            .expect("failed to decode IoxParquetMetaData from file metadata");
        assert_metadata(&iox_from_file_meta.decode().unwrap());

        // Reproducer of https://github.com/influxdata/influxdb_iox/issues/4714
        // Convert IOx meta data back to parquet meta data and verify it is still the same
        let decoded = iox_from_file_meta.decode().unwrap();
        assert_metadata(&decoded);
    }

    fn assert_metadata(decoded: &DecodedIoxParquetMetaData) {
        let new_file_meta = decoded.parquet_file_meta();
        assert!(new_file_meta.key_value_metadata().is_some());

        let new_row_group_meta = decoded.parquet_row_group_metadata();
        assert!(!new_row_group_meta.is_empty());
        let col_meta = new_row_group_meta[0].column(0);
        assert!(col_meta.statistics().is_some()); // There is statistics for column "a"

        // Read the schema from the metadata.
        //
        // If this is not specified in the metadata when writing, it will be
        // automatically inferred at read time and will not contain the IOx
        // metadata.
        let schema = decoded.read_schema().unwrap();
        let (_, field) = schema.field(0);
        assert_eq!(field.name(), "a");

        // Try and access the IOx metadata that was embedded above (with the
        // SchemaBuilder)
        let col_summary = decoded.read_statistics(&schema).unwrap();
        assert!(!col_summary.is_empty());
    }

    fn to_timestamp_array(timestamps: &[i64]) -> ArrayRef {
        let array: TimestampNanosecondArray = timestamps.iter().map(|v| Some(*v)).collect();
        Arc::new(array)
    }
}
