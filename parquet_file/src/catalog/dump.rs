//! Tooling to dump the catalog content for debugging.
use std::{fmt::Debug, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use generated_types::influxdata::iox::catalog::v1 as proto;
use iox_object_store::{IoxObjectStore, TransactionFilePath};
use object_store::{ObjectStore, ObjectStoreApi};
use snafu::{ResultExt, Snafu};

use crate::metadata::{DecodedIoxParquetMetaData, IoxParquetMetaData};

use super::internals::proto_io::load_transaction_proto;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error while listing files from object store: {}", source))]
    ListFiles {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Error while writing result to output: {}", source))]
    WriteOutput { source: std::io::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Options to control the output of [`dump`].
#[derive(Debug, Default, Clone, Copy)]
pub struct DumpOptions {
    /// Show debug output of [`DecodedIoxParquetMetaData`] if decoding succeeds, show the decoding error otherwise.
    ///
    /// Since this contains the entire Apache Parquet metadata object this is quite verbose and is usually not
    /// recommended.
    pub show_parquet_metadata: bool,

    /// Show debug output of [`IoxMetadata`](crate::metadata::IoxMetadata) if decoding succeeds, show the decoding
    /// error otherwise.
    pub show_iox_metadata: bool,

    /// Show debug output of [`Schema`](schema::Schema) if decoding succeeds, show the decoding
    /// error otherwise.
    pub show_schema: bool,

    /// Show debug output of [`ColumnSummary`](data_types::partition_metadata::ColumnSummary) if decoding succeeds,
    /// show the decoding error otherwise.
    pub show_statistics: bool,

    /// Show unparsed [`IoxParquetMetaData`] -- which are Apache Thrift bytes -- as part of the transaction actions.
    ///
    /// Since this binary data is usually quite hard to read, it is recommended to set this to `false` which will
    /// replace the actual bytes with `b"metadata omitted"`. Use the other toggles to instead show the content of the
    /// Apache Thrift message.
    pub show_unparsed_metadata: bool,
}

/// Dump catalog content in text form to `writer`.
///
/// This is mostly useful for debugging. The output of this might change at any point in time and should not be parsed
/// (except for one-time debugging).
///
/// Errors that happen while listing the object store or while writing the output to `writer` will be bubbled up. All
/// other errors (e.g. reading a transaction file from object store, deserializing protobuf data) will be shown as part
/// of the output and will NOT result in a failure. Also note that NO validation of the catalog content (e.g. version
/// checks, fork detection) will be performed.
pub async fn dump<W>(
    iox_object_store: &IoxObjectStore,
    writer: &mut W,
    options: DumpOptions,
) -> Result<()>
where
    W: std::io::Write + Send,
{
    let mut files = iox_object_store
        .catalog_transaction_files()
        .await
        .context(ListFiles)?
        .try_concat()
        .await
        .context(ListFiles)?;
    files.sort_by_key(|f| (f.revision_counter, f.uuid, !f.is_checkpoint()));
    let options = Arc::new(options);

    for file in files {
        writeln!(
            writer,
            "{:#?}",
            File::read(iox_object_store, &file, Arc::clone(&options)).await
        )
        .context(WriteOutput)?;
    }

    Ok(())
}

/// Wrapper around [`proto::Transaction`] with additional debug output (e.g. to show nested data).
struct File {
    path: TransactionFilePath,
    proto: Result<proto::Transaction, crate::catalog::internals::proto_io::Error>,
    md: Option<Vec<Result<Metadata, crate::metadata::Error>>>,
}

impl File {
    /// Read transaction data (in form of [`proto::Transaction`]) from object store.
    async fn read(
        iox_object_store: &IoxObjectStore,
        path: &TransactionFilePath,
        options: Arc<DumpOptions>,
    ) -> Self {
        let (proto, md) = match load_transaction_proto(iox_object_store, path).await {
            Ok(transaction) => {
                let mut md = vec![];

                // Rebuild transaction object and:
                // 1. Scan for contained `IoxParquetMetaData`.
                // 2. Replace encoded metadata (in byte form) w/ placeholder (if requested via flags).
                let transaction = proto::Transaction {
                    actions: transaction
                        .actions
                        .into_iter()
                        .map(|action| proto::transaction::Action {
                            action: action.action.map(|action| match action {
                                proto::transaction::action::Action::AddParquet(add_parquet) => {
                                    let iox_md =
                                        Metadata::read(&add_parquet.metadata, Arc::clone(&options));
                                    md.push(iox_md);

                                    proto::transaction::action::Action::AddParquet(
                                        proto::AddParquet {
                                            metadata: if options.show_unparsed_metadata {
                                                add_parquet.metadata
                                            } else {
                                                Bytes::from(format!(
                                                    "metadata omitted ({} bytes)",
                                                    add_parquet.metadata.len()
                                                ))
                                            },
                                            ..add_parquet
                                        },
                                    )
                                }
                                other => other,
                            }),
                        })
                        .collect(),
                    ..transaction
                };

                (Ok(transaction), Some(md))
            }
            Err(e) => (Err(e), None),
        };

        Self {
            path: *path,
            proto,
            md,
        }
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File")
            .field("path", &self.path.relative_dirs_and_file_name().to_string())
            .field("revision_counter", &self.path.revision_counter)
            .field("uuid", &self.path.uuid)
            .field("is_checkpoint", &self.path.is_checkpoint())
            .field("proto", &self.proto)
            .field("metadata", &self.md)
            .finish()
    }
}

/// Wrapper around [`IoxParquetMetaData`] with additional debug output (e.g. to also show nested data).
struct Metadata {
    md: DecodedIoxParquetMetaData,
    options: Arc<DumpOptions>,
}

impl Metadata {
    /// Read metadata (in form of [`IoxParquetMetaData`]) from bytes, encoded as Apache Thrift.
    fn read(data: &Bytes, options: Arc<DumpOptions>) -> Result<Self, crate::metadata::Error> {
        let iox_md = IoxParquetMetaData::from_thrift_bytes(data.as_ref().to_vec());
        let md = iox_md.decode()?;
        Ok(Self { md, options })
    }
}

impl Debug for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let schema = self.md.read_schema();
        let statistics = schema
            .as_ref()
            .ok()
            .map(|schema| self.md.read_statistics(schema));

        let mut dbg = f.debug_struct("Metadata");

        if self.options.show_parquet_metadata {
            dbg.field("parquet_metadata", &self.md);
        }

        if self.options.show_iox_metadata {
            dbg.field("iox_metadata", &self.md.read_iox_metadata());
        }

        if self.options.show_schema {
            dbg.field("schema", &schema);
        }

        if self.options.show_statistics {
            dbg.field("statistics", &statistics);
        }

        dbg.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::{
            core::PreservedCatalog,
            interface::CatalogParquetInfo,
            test_helpers::{TestCatalogState, DB_NAME},
        },
        test_utils::{chunk_addr, make_config, make_metadata, TestSize},
    };
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_dump_default_options() {
        let config = make_config()
            .await
            .with_fixed_uuid(Uuid::nil())
            .with_fixed_timestamp(Utc.timestamp(10, 20));

        let iox_object_store = &config.iox_object_store;

        // build catalog with some data
        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(DB_NAME, config.clone(), ())
                .await
                .unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(0), TestSize::Minimal).await;
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            transaction.add_parquet(&info);

            transaction.commit().await.unwrap();
        }

        let mut buf = std::io::Cursor::new(Vec::new());
        let options = DumpOptions::default();
        dump(iox_object_store, &mut buf, options).await.unwrap();
        let actual = String::from_utf8(buf.into_inner()).unwrap();
        let actual = actual.trim();

        let expected = r#"
File {
    path: "00000000000000000000/00000000-0000-0000-0000-000000000000.txn",
    revision_counter: 0,
    uuid: 00000000-0000-0000-0000-000000000000,
    is_checkpoint: false,
    proto: Ok(
        Transaction {
            version: 19,
            actions: [],
            revision_counter: 0,
            uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            previous_uuid: b"",
            start_timestamp: Some(
                Timestamp {
                    seconds: 10,
                    nanos: 20,
                },
            ),
            encoding: Delta,
        },
    ),
    metadata: Some(
        [],
    ),
}
File {
    path: "00000000000000000001/00000000-0000-0000-0000-000000000000.txn",
    revision_counter: 1,
    uuid: 00000000-0000-0000-0000-000000000000,
    is_checkpoint: false,
    proto: Ok(
        Transaction {
            version: 19,
            actions: [
                Action {
                    action: Some(
                        AddParquet(
                            AddParquet {
                                path: Some(
                                    Path {
                                        directories: [
                                            "table1",
                                            "part1",
                                        ],
                                        file_name: "00000000-0000-0000-0000-000000000000.parquet",
                                    },
                                ),
                                file_size_bytes: 33,
                                metadata: b"metadata omitted (937 bytes)",
                            },
                        ),
                    ),
                },
            ],
            revision_counter: 1,
            uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            previous_uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            start_timestamp: Some(
                Timestamp {
                    seconds: 10,
                    nanos: 20,
                },
            ),
            encoding: Delta,
        },
    ),
    metadata: Some(
        [
            Ok(
                Metadata,
            ),
        ],
    ),
}
        "#
        .trim();

        assert_eq!(
            actual, expected,
            "actual:\n{}\n\nexpected:\n{}",
            actual, expected
        );
    }

    #[tokio::test]
    async fn test_dump_show_parsed_data() {
        let config = make_config()
            .await
            .with_fixed_uuid(Uuid::nil())
            .with_fixed_timestamp(Utc.timestamp(10, 20));
        let iox_object_store = &config.iox_object_store;

        // build catalog with some data
        let (catalog, _state) =
            PreservedCatalog::new_empty::<TestCatalogState>(DB_NAME, config.clone(), ())
                .await
                .unwrap();
        {
            let mut transaction = catalog.open_transaction().await;

            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(0), TestSize::Minimal).await;
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            transaction.add_parquet(&info);

            transaction.commit().await.unwrap();
        }

        let mut buf = std::io::Cursor::new(Vec::new());
        let options = DumpOptions {
            show_iox_metadata: true,
            show_schema: true,
            show_statistics: true,
            ..Default::default()
        };
        dump(iox_object_store, &mut buf, options).await.unwrap();
        let actual = String::from_utf8(buf.into_inner()).unwrap();
        let actual = actual.trim();

        let expected = r#"
File {
    path: "00000000000000000000/00000000-0000-0000-0000-000000000000.txn",
    revision_counter: 0,
    uuid: 00000000-0000-0000-0000-000000000000,
    is_checkpoint: false,
    proto: Ok(
        Transaction {
            version: 19,
            actions: [],
            revision_counter: 0,
            uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            previous_uuid: b"",
            start_timestamp: Some(
                Timestamp {
                    seconds: 10,
                    nanos: 20,
                },
            ),
            encoding: Delta,
        },
    ),
    metadata: Some(
        [],
    ),
}
File {
    path: "00000000000000000001/00000000-0000-0000-0000-000000000000.txn",
    revision_counter: 1,
    uuid: 00000000-0000-0000-0000-000000000000,
    is_checkpoint: false,
    proto: Ok(
        Transaction {
            version: 19,
            actions: [
                Action {
                    action: Some(
                        AddParquet(
                            AddParquet {
                                path: Some(
                                    Path {
                                        directories: [
                                            "table1",
                                            "part1",
                                        ],
                                        file_name: "00000000-0000-0000-0000-000000000000.parquet",
                                    },
                                ),
                                file_size_bytes: 33,
                                metadata: b"metadata omitted (937 bytes)",
                            },
                        ),
                    ),
                },
            ],
            revision_counter: 1,
            uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            previous_uuid: b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            start_timestamp: Some(
                Timestamp {
                    seconds: 10,
                    nanos: 20,
                },
            ),
            encoding: Delta,
        },
    ),
    metadata: Some(
        [
            Ok(
                Metadata {
                    iox_metadata: Ok(
                        IoxMetadata {
                            creation_timestamp: 1970-01-01T00:00:10.000000020Z,
                            time_of_first_write: 1970-01-01T00:00:30.000000040Z,
                            time_of_last_write: 1970-01-01T00:00:50.000000060Z,
                            table_name: "table1",
                            partition_key: "part1",
                            chunk_id: ChunkId(
                                0,
                            ),
                            partition_checkpoint: PartitionCheckpoint {
                                table_name: "table1",
                                partition_key: "part1",
                                sequencer_numbers: {
                                    1: OptionalMinMaxSequence {
                                        min: None,
                                        max: 18,
                                    },
                                    2: OptionalMinMaxSequence {
                                        min: Some(
                                            25,
                                        ),
                                        max: 28,
                                    },
                                },
                                flush_timestamp: 1970-01-01T00:00:10.000000020+00:00,
                            },
                            database_checkpoint: DatabaseCheckpoint {
                                sequencer_numbers: {
                                    1: OptionalMinMaxSequence {
                                        min: None,
                                        max: 18,
                                    },
                                    2: OptionalMinMaxSequence {
                                        min: Some(
                                            24,
                                        ),
                                        max: 29,
                                    },
                                    3: OptionalMinMaxSequence {
                                        min: Some(
                                            35,
                                        ),
                                        max: 38,
                                    },
                                },
                            },
                            chunk_order: ChunkOrder(
                                5,
                            ),
                        },
                    ),
                    schema: Ok(
                        Schema {
                            inner: Schema {
                                fields: [
                                    Field {
                                        name: "foo_tag_normal",
                                        data_type: Dictionary(
                                            Int32,
                                            Utf8,
                                        ),
                                        nullable: true,
                                        dict_id: 0,
                                        dict_is_ordered: false,
                                        metadata: Some(
                                            {
                                                "iox::column::type": "iox::column_type::tag",
                                            },
                                        ),
                                    },
                                    Field {
                                        name: "foo_field_i64_normal",
                                        data_type: Int64,
                                        nullable: true,
                                        dict_id: 0,
                                        dict_is_ordered: false,
                                        metadata: Some(
                                            {
                                                "iox::column::type": "iox::column_type::field::integer",
                                            },
                                        ),
                                    },
                                    Field {
                                        name: "time",
                                        data_type: Timestamp(
                                            Nanosecond,
                                            None,
                                        ),
                                        nullable: false,
                                        dict_id: 0,
                                        dict_is_ordered: false,
                                        metadata: Some(
                                            {
                                                "iox::column::type": "iox::column_type::timestamp",
                                            },
                                        ),
                                    },
                                ],
                                metadata: {},
                            },
                        },
                    ),
                    statistics: Some(
                        Ok(
                            [
                                ColumnSummary {
                                    name: "foo_tag_normal",
                                    influxdb_type: Some(
                                        Tag,
                                    ),
                                    stats: String(
                                        StatValues {
                                            min: Some(
                                                "bar",
                                            ),
                                            max: Some(
                                                "foo",
                                            ),
                                            total_count: 4,
                                            null_count: 0,
                                            distinct_count: None,
                                        },
                                    ),
                                },
                                ColumnSummary {
                                    name: "foo_field_i64_normal",
                                    influxdb_type: Some(
                                        Field,
                                    ),
                                    stats: I64(
                                        StatValues {
                                            min: Some(
                                                -1,
                                            ),
                                            max: Some(
                                                4,
                                            ),
                                            total_count: 4,
                                            null_count: 0,
                                            distinct_count: None,
                                        },
                                    ),
                                },
                                ColumnSummary {
                                    name: "time",
                                    influxdb_type: Some(
                                        Timestamp,
                                    ),
                                    stats: I64(
                                        StatValues {
                                            min: Some(
                                                1000,
                                            ),
                                            max: Some(
                                                4000,
                                            ),
                                            total_count: 4,
                                            null_count: 0,
                                            distinct_count: None,
                                        },
                                    ),
                                },
                            ],
                        ),
                    ),
                },
            ),
        ],
    ),
}
        "#
        .trim();

        assert_eq!(
            actual, expected,
            "actual:\n{}\n\nexpected:\n{}",
            actual, expected
        );
    }
}
