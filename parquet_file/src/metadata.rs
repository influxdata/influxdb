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
use std::sync::Arc;

use data_types::{
    partition_metadata::{ColumnSummary, StatValues, Statistics, TableSummary},
    timestamp::TimestampRange,
};
use internal_types::schema::{InfluxColumnType, InfluxFieldType, Schema};
use parquet::{
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
use snafu::{OptionExt, ResultExt, Snafu};
use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol, TOutputProtocol};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot read parquet metadata from bytes: {}", source))]
    ParquetMetaDataRead {
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
        "Statistics for column {} in row group {} do not set min/max values",
        column,
        row_group
    ))]
    StatisticsMinMaxMissing { row_group: usize, column: String },

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
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Read parquet metadata from a parquet file.
pub fn read_parquet_metadata_from_file(data: Vec<u8>) -> Result<ParquetMetaData> {
    let cursor = SliceableCursor::new(data);
    let reader = SerializedFileReader::new(cursor).context(ParquetMetaDataRead {})?;
    Ok(reader.metadata().clone())
}

/// Read IOx statistics (including timestamp range) from parquet metadata.
pub fn read_statistics_from_parquet_metadata(
    parquet_md: &ParquetMetaData,
    schema: &Schema,
    table_name: &str,
) -> Result<(TableSummary, Option<TimestampRange>)> {
    let mut table_summary_agg: Option<TableSummary> = None;
    let mut timestamp_range_agg = None;

    for (row_group_idx, row_group) in parquet_md.row_groups().iter().enumerate() {
        let (table_summary, timestamp_range) =
            read_statistics_from_parquet_row_group(row_group, row_group_idx, schema, table_name)?;

        match table_summary_agg.as_mut() {
            Some(existing) => existing.update_from(&table_summary),
            None => table_summary_agg = Some(table_summary),
        }

        timestamp_range_agg = match (timestamp_range_agg, timestamp_range) {
            (Some(a), Some(b)) => Some(TimestampRange {
                start: a.start.min(b.start),
                end: a.end.max(b.end),
            }),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
    }

    match table_summary_agg {
        Some(table_summary) => Ok((table_summary, timestamp_range_agg)),
        None => Err(Error::NoRowGroup {}),
    }
}

/// Read IOx statistics (including timestamp range) from parquet row group metadata.
fn read_statistics_from_parquet_row_group(
    row_group: &ParquetRowGroupMetaData,
    row_group_idx: usize,
    schema: &Schema,
    table_name: &str,
) -> Result<(TableSummary, Option<TimestampRange>)> {
    let mut column_summaries = vec![];
    let mut timestamp_range = None;

    for ((iox_type, field), column_chunk_metadata) in schema.iter().zip(row_group.columns()) {
        if let Some(iox_type) = iox_type {
            let parquet_stats = column_chunk_metadata
                .statistics()
                .context(StatisticsMissing {
                    row_group: row_group_idx,
                    column: field.name().clone(),
                })?;

            if !parquet_stats.has_min_max_set() || parquet_stats.is_min_max_deprecated() {
                StatisticsMinMaxMissing {
                    row_group: row_group_idx,
                    column: field.name().clone(),
                }
                .fail()?;
            }

            let count =
                (row_group.num_rows().max(0) as u64).saturating_sub(parquet_stats.null_count());

            match extract_iox_statistics(
                parquet_stats,
                iox_type,
                count,
                row_group_idx,
                field.name(),
            )? {
                ExtractedStatistics::Statistics(stats) => column_summaries.push(ColumnSummary {
                    name: field.name().clone(),
                    stats,
                }),
                ExtractedStatistics::TimestampRange(range) => {
                    assert!(timestamp_range.is_none());
                    timestamp_range = Some(range);
                }
            }
        }
    }

    let table_summary = TableSummary {
        name: table_name.to_string(),
        columns: column_summaries,
    };

    Ok((table_summary, timestamp_range))
}

/// Result of [`extract_iox_statistics`].
enum ExtractedStatistics {
    /// Found statistics.
    Statistics(Statistics),

    /// Found timestamp range.
    TimestampRange(TimestampRange),
}

/// Extract IOx statistics from parquet statistics.
///
/// This is required because upstream does not have a mapper from parquet statistics back to arrow or Rust native types.
fn extract_iox_statistics(
    parquet_stats: &ParquetStatistics,
    iox_type: InfluxColumnType,
    count: u64,
    row_group_idx: usize,
    column_name: &str,
) -> Result<ExtractedStatistics> {
    match (parquet_stats, iox_type) {
        (ParquetStatistics::Boolean(stats), InfluxColumnType::Field(InfluxFieldType::Boolean)) => {
            Ok(ExtractedStatistics::Statistics(Statistics::Bool(
                StatValues {
                    min: *stats.min(),
                    max: *stats.max(),
                    count,
                },
            )))
        }
        (ParquetStatistics::Int64(stats), InfluxColumnType::Field(InfluxFieldType::Integer)) => Ok(
            ExtractedStatistics::Statistics(Statistics::I64(StatValues {
                min: *stats.min(),
                max: *stats.max(),
                count,
            })),
        ),
        (ParquetStatistics::Int64(stats), InfluxColumnType::Field(InfluxFieldType::UInteger)) => {
            // TODO: that's very likely wrong, but blocked by https://github.com/apache/arrow-rs/issues/254
            Ok(ExtractedStatistics::Statistics(Statistics::U64(
                StatValues {
                    min: *stats.min() as u64,
                    max: *stats.max() as u64,
                    count,
                },
            )))
        }
        (ParquetStatistics::Double(stats), InfluxColumnType::Field(InfluxFieldType::Float)) => Ok(
            ExtractedStatistics::Statistics(Statistics::F64(StatValues {
                min: *stats.min(),
                max: *stats.max(),
                count,
            })),
        ),
        (ParquetStatistics::Int64(stats), InfluxColumnType::Timestamp) => Ok(
            ExtractedStatistics::TimestampRange(TimestampRange::new(*stats.min(), *stats.max())),
        ),
        (ParquetStatistics::ByteArray(stats), InfluxColumnType::Tag)
        | (ParquetStatistics::ByteArray(stats), InfluxColumnType::Field(InfluxFieldType::String)) => {
            Ok(ExtractedStatistics::Statistics(Statistics::String(
                StatValues {
                    min: stats
                        .min()
                        .as_utf8()
                        .context(StatisticsUtf8Error {
                            row_group: row_group_idx,
                            column: column_name.to_string(),
                        })?
                        .to_string(),
                    max: stats
                        .max()
                        .as_utf8()
                        .context(StatisticsUtf8Error {
                            row_group: row_group_idx,
                            column: column_name.to_string(),
                        })?
                        .to_string(),
                    count,
                },
            )))
        }
        _ => Err(Error::StatisticsTypeMismatch {
            row_group: row_group_idx,
            column: column_name.to_string(),
            expected: iox_type,
            actual: parquet_stats.clone(),
        }),
    }
}

/// Encode [Apache Parquet] metadata as freestanding [Apache Thrift]-encoded bytes.
///
/// This can be used to store metadata separate from the related payload data. The usage of [Apache Thrift] allows the
/// same stability guarantees as the usage of an ordinary [Apache Parquet] file. To encode a thrift message into bytes
/// the [Thrift Compact Protocol] is used. See [`thrift_to_parquet_metadata`] for decoding.
///
/// [Apache Parquet]: https://parquet.apache.org/
/// [Apache Thrift]: https://thrift.apache.org/
/// [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
pub fn parquet_metadata_to_thrift(parquet_md: &ParquetMetaData) -> Result<Vec<u8>> {
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

    Ok(buffer)
}

/// Decode [Apache Parquet] metadata from [Apache Thrift]-encoded bytes.
///
/// See [`parquet_metadata_to_thrift`] for encoding. Note that only the [Thrift Compact Protocol] is supported.
///
/// [Apache Parquet]: https://parquet.apache.org/
/// [Apache Thrift]: https://thrift.apache.org/
/// [Thrift Compact Protocol]: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
pub fn thrift_to_parquet_metadata(data: &[u8]) -> Result<ParquetMetaData> {
    // step 1: load thrift data from byte stream
    let thrift_file_metadata = {
        let mut protocol = TCompactInputProtocol::new(data);
        parquet_format::FileMetaData::read_from_in_protocol(&mut protocol)
            .context(ThriftReadFailure {})?
    };

    // step 2: convert thrift to in-mem structs
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
    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

#[cfg(test)]
mod tests {
    use super::*;

    use internal_types::selection::Selection;

    use crate::{
        storage::read_schema_from_parquet_metadata,
        utils::{load_parquet_from_store, make_chunk, make_chunk_no_row_group, make_object_store},
    };

    #[tokio::test]
    async fn test_restore_from_file() {
        // setup: preserve chunk to object store
        let store = make_object_store();
        let chunk = make_chunk(Arc::clone(&store)).await;
        let (table, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();

        // step 1: read back schema
        let schema_actual = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        let schema_expected = chunk.table_schema(&table, Selection::All).unwrap();
        assert_eq!(schema_actual, schema_expected);

        // step 2: read back statistics
        let (table_summary_actual, timestamp_range_actual) =
            read_statistics_from_parquet_metadata(&parquet_metadata, &schema_actual, &table)
                .unwrap();
        let table_summary_expected = chunk.table_summaries().first().cloned().unwrap();
        let timestamp_range_expected = chunk.timestamp_range(&table).unwrap();
        assert_eq!(table_summary_actual, table_summary_expected);
        assert_eq!(timestamp_range_actual, timestamp_range_expected)
    }

    #[tokio::test]
    async fn test_restore_from_thrift() {
        // setup: write chunk to object store and only keep thrift-encoded metadata
        let store = make_object_store();
        let chunk = make_chunk(Arc::clone(&store)).await;
        let (table, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();
        let data = parquet_metadata_to_thrift(&parquet_metadata).unwrap();
        let parquet_metadata = thrift_to_parquet_metadata(&data).unwrap();

        // step 1: read back schema
        let schema_actual = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        let schema_expected = chunk.table_schema(&table, Selection::All).unwrap();
        assert_eq!(schema_actual, schema_expected);

        // step 2: read back statistics
        let (table_summary_actual, timestamp_range_actual) =
            read_statistics_from_parquet_metadata(&parquet_metadata, &schema_actual, &table)
                .unwrap();
        let table_summary_expected = chunk.table_summaries().first().cloned().unwrap();
        let timestamp_range_expected = chunk.timestamp_range(&table).unwrap();
        assert_eq!(table_summary_actual, table_summary_expected);
        assert_eq!(timestamp_range_actual, timestamp_range_expected)
    }

    #[tokio::test]
    async fn test_restore_from_file_no_row_group() {
        // setup: preserve chunk to object store
        let store = make_object_store();
        let chunk = make_chunk_no_row_group(Arc::clone(&store)).await;
        let (table, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();

        // step 1: read back schema
        let schema_actual = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        let schema_expected = chunk.table_schema(&table, Selection::All).unwrap();
        assert_eq!(schema_actual, schema_expected);

        // step 2: reading back statistics fails
        let res = read_statistics_from_parquet_metadata(&parquet_metadata, &schema_actual, &table);
        assert_eq!(
            res.unwrap_err().to_string(),
            "No row group found, cannot recover statistics"
        );
    }

    #[tokio::test]
    async fn test_restore_from_thrift_no_row_group() {
        // setup: write chunk to object store and only keep thrift-encoded metadata
        let store = make_object_store();
        let chunk = make_chunk_no_row_group(Arc::clone(&store)).await;
        let (table, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();
        let data = parquet_metadata_to_thrift(&parquet_metadata).unwrap();
        let parquet_metadata = thrift_to_parquet_metadata(&data).unwrap();

        // step 1: read back schema
        let schema_actual = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        let schema_expected = chunk.table_schema(&table, Selection::All).unwrap();
        assert_eq!(schema_actual, schema_expected);

        // step 2: reading back statistics fails
        let res = read_statistics_from_parquet_metadata(&parquet_metadata, &schema_actual, &table);
        assert_eq!(
            res.unwrap_err().to_string(),
            "No row group found, cannot recover statistics"
        );
    }

    #[tokio::test]
    async fn test_make_chunk() {
        let store = make_object_store();
        let chunk = make_chunk(Arc::clone(&store)).await;
        let (_, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();

        assert!(parquet_metadata.num_row_groups() > 1);
        assert_ne!(
            parquet_metadata
                .file_metadata()
                .schema_descr()
                .num_columns(),
            0
        );

        // column count in summary misses the timestamp column
        assert_eq!(
            chunk.table_summaries().first().unwrap().columns.len() + 1,
            parquet_metadata
                .file_metadata()
                .schema_descr()
                .num_columns()
        );

        // check that column counts are consistent
        let n_rows = parquet_metadata.file_metadata().num_rows() as u64;
        assert!(n_rows >= parquet_metadata.num_row_groups() as u64);
        for summary in &chunk.table_summaries().first().unwrap().columns {
            assert_eq!(summary.count(), n_rows);
        }
    }

    #[tokio::test]
    async fn test_make_chunk_no_row_group() {
        let store = make_object_store();
        let chunk = make_chunk_no_row_group(Arc::clone(&store)).await;
        let (_, parquet_data) = load_parquet_from_store(&chunk, store).await;
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data).unwrap();

        assert_eq!(parquet_metadata.num_row_groups(), 0);
        assert_ne!(
            parquet_metadata
                .file_metadata()
                .schema_descr()
                .num_columns(),
            0
        );
        assert_eq!(parquet_metadata.file_metadata().num_rows(), 0);

        // column count in summary misses the timestamp column
        assert_eq!(
            chunk.table_summaries().first().unwrap().columns.len() + 1,
            parquet_metadata
                .file_metadata()
                .schema_descr()
                .num_columns()
        );
    }
}
