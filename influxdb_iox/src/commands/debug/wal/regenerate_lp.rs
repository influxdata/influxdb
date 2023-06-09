//! A module providing a CLI command for regenerating line protocol from a WAL file.
use std::fs::{create_dir_all, File, OpenOptions};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use data_types::{NamespaceId, TableId};
use hashbrown::HashMap;
use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::schema::Client as SchemaClient;
use observability_deps::tracing::{debug, error, info};
use wal::{ClosedSegmentFileReader, WriteOpEntry, WriteOpEntryDecoder};
use wal_inspect::{LineProtoWriter, NamespaceDemultiplexer, TableBatchWriter, WriteError};

use super::{Error, RegenerateError};

/// The set of errors which may occur when trying to look up a table name
/// index for a namespace.
#[derive(Debug, Error)]
pub enum TableIndexLookupError {
    #[error("encountered error when making index request: {0}")]
    RequestFailed(#[from] Box<influxdb_iox_client::error::Error>),

    #[error("no namespace known for id: {0:?}")]
    NamespaceNotKnown(NamespaceId),
}

// This type provides a convenience wrapper around the namespace and schema APIs
// to enable the fetching of table name indexes from namespace and table IDs.
struct TableIndexFetcher {
    namespace_index: HashMap<NamespaceId, String>,
    schema_client: SchemaClient,
}

impl TableIndexFetcher {
    async fn new(connection: Connection) -> Result<Self, TableIndexLookupError> {
        let mut namespace_client = influxdb_iox_client::namespace::Client::new(connection.clone());
        Ok(Self {
            namespace_index: namespace_client
                .get_namespaces()
                .await
                .map_err(Box::new)?
                .into_iter()
                .map(|ns| (NamespaceId::new(ns.id), ns.name))
                .collect(),
            schema_client: SchemaClient::new(connection),
        })
    }

    async fn get_table_name_index(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<HashMap<TableId, String>, TableIndexLookupError> {
        let namespace_name = self
            .namespace_index
            .get(&namespace_id)
            .ok_or(TableIndexLookupError::NamespaceNotKnown(namespace_id))?;

        info!(
            %namespace_id,
            %namespace_name,
            "requesting namespace schema to construct table ID to name mapping"
        );

        let ns_schema = self
            .schema_client
            .clone()
            .get_schema(namespace_name)
            .await
            .map_err(Box::new)?;

        Ok(ns_schema
            .tables
            .into_iter()
            .map(|(table_name, table_schema)| {
                let table_id = TableId::new(table_schema.id);
                debug!(%table_name, %table_id, %namespace_id, %namespace_name, "discovered ID to name mapping for table in namespace");
                (table_id, table_name)
            })
            .collect())
    }
}

/// A container for the possible arguments & flags of a `regenerate-lp` command.
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The path to the input WAL file
    #[clap(value_parser)]
    input: PathBuf,

    /// The directory to write regenerated line protocol to. Creates the directory
    /// if it does not exist.
    ///
    /// When unspecified the line protocol is written to stdout
    #[clap(long, short, value_parser)]
    output_directory: Option<PathBuf>,

    /// When enabled, pre-existing line protocol files will be overwritten
    #[clap(long, short)]
    force: bool,

    /// When enabled, lookup of the measurement and database names is skipped.
    /// This means that regenerated line protocol will only contain the table
    /// ID for each measurement, rather than the original name
    #[clap(long, short)]
    skip_measurement_lookup: bool,
}

/// Executes the `regenerate-lp` command with the provided configuration, reading
/// write operation entries from a WAL file and mapping them to line protocol.
pub async fn command<C, CFut>(connection: C, config: Config) -> Result<(), Error>
where
    C: Send + FnOnce() -> CFut,
    CFut: Send + Future<Output = Connection>,
{
    let decoder = WriteOpEntryDecoder::from(
        ClosedSegmentFileReader::from_path(&config.input).map_err(Error::UnableToReadWalFile)?,
    );

    let table_name_indexer = if config.skip_measurement_lookup {
        Ok(None)
    } else {
        let connection = connection().await;
        TableIndexFetcher::new(connection)
            .await
            .map(Some)
            .map_err(Error::UnableToInitTableNameFetcher)
    }?
    .map(Arc::new);

    match config.output_directory {
        Some(d) => {
            let d = Arc::new(d);
            create_dir_all(d.as_path())?;
            let namespace_demux = NamespaceDemultiplexer::new(move |namespace_id| {
                new_line_proto_file_writer(
                    namespace_id,
                    Arc::clone(&d),
                    config.force,
                    table_name_indexer.as_ref().map(Arc::clone),
                )
            });
            decode_and_write_entries(decoder, namespace_demux).await
        }
        None => {
            let namespace_demux = NamespaceDemultiplexer::new(move |namespace_id| {
                let table_name_indexer = table_name_indexer.as_ref().map(Arc::clone);
                async move {
                    let table_name_lookup = match table_name_indexer {
                        Some(indexer) => Some(indexer.get_table_name_index(namespace_id).await?),
                        None => None,
                    };
                    let result: Result<LineProtoWriter<std::io::Stdout>, RegenerateError> =
                        Ok(LineProtoWriter::new(std::io::stdout(), table_name_lookup));
                    result
                }
            });
            decode_and_write_entries(decoder, namespace_demux).await
        }
    }
}

// Creates a new [`LineProtoWriter`] backed by a [`File`] in `output_dir` using
// the format "namespace_id_`namespace_id`.lp". If `replace_existing` is set
// then any pre-existing file is replaced.
async fn new_line_proto_file_writer(
    namespace_id: NamespaceId,
    output_dir: Arc<PathBuf>,
    replace_existing: bool,
    table_name_indexer: Option<Arc<TableIndexFetcher>>,
) -> Result<LineProtoWriter<File>, RegenerateError> {
    let file_path = output_dir
        .as_path()
        .join(format!("namespace_id_{}.lp", namespace_id));

    let mut open_options = OpenOptions::new().write(true).to_owned();
    if replace_existing {
        open_options.create(true);
    } else {
        open_options.create_new(true);
    }

    info!(
        ?file_path,
        %namespace_id,
        "creating namespaced file as destination for regenerated line protocol",
    );

    let table_name_lookup = match table_name_indexer {
        Some(indexer) => Some(indexer.get_table_name_index(namespace_id).await?),
        None => None,
    };

    Ok(LineProtoWriter::new(
        open_options.open(&file_path).map_err(WriteError::IoError)?,
        table_name_lookup,
    ))
}

// Consumes [`wal::WriteOpEntry`]s from `decoder` until end of stream or a fatal decode error is hit,
// rewriting each table-keyed batch of writes using the provided implementation [`TableBatchWriter`]
// and initialisation function used by `namespace_demux`.
//
// Errors returned by the `namespace_demux` or any [`TableBatchWriter`] returned
async fn decode_and_write_entries<T, F, I>(
    decoder: WriteOpEntryDecoder,
    mut namespace_demux: NamespaceDemultiplexer<T, F>,
) -> Result<(), Error>
where
    T: TableBatchWriter<WriteError = wal_inspect::WriteError> + Send,
    F: (Fn(NamespaceId) -> I) + Send + Sync,
    I: Future<Output = Result<T, RegenerateError>> + Send,
{
    let mut regenerate_errors = vec![];

    for (wal_entry_number, entry_batch) in decoder.enumerate() {
        regenerate_errors.extend(
            regenerate_wal_entry((wal_entry_number, entry_batch?), &mut namespace_demux).await,
        );
    }

    if !regenerate_errors.is_empty() {
        Err(Error::UnableToFullyRegenerateLineProtocol {
            sources: regenerate_errors,
        })
    } else {
        Ok(())
    }
}

// Given a `wal_entry` containing the entry number and a list of write
// operations, this function will regenerate the entries using the
// provided `namespace_demux`.
async fn regenerate_wal_entry<T, F, I>(
    wal_entry: (usize, Vec<WriteOpEntry>),
    namespace_demux: &mut NamespaceDemultiplexer<T, F>,
) -> Vec<RegenerateError>
where
    T: TableBatchWriter<WriteError = wal_inspect::WriteError> + Send,
    F: (Fn(NamespaceId) -> I) + Send + Sync,
    I: Future<Output = Result<T, RegenerateError>> + Send,
{
    let mut regenerate_errors = vec![];

    let (wal_entry_number, entry_batch) = wal_entry;
    for (write_op_number, entry) in entry_batch.into_iter().enumerate() {
        let namespace_id = entry.namespace;
        debug!(%namespace_id, %wal_entry_number, %write_op_number, "regenerating line protocol for namespace from WAL write op entry");
        namespace_demux
            .get(namespace_id)
            .await
            .map(|writer| writer.write_table_batches(entry.table_batches.into_iter()))
            .and_then(|write_result| write_result.map_err(RegenerateError::TableBatchWriteFailure)) // flatten out the write result if Ok
            .unwrap_or_else(|err| {
                error!(
                        %namespace_id,
                        %wal_entry_number,
                        %write_op_number,
                        %err,
                        "failed to rewrite table batches for write op");
                regenerate_errors.push(err);
            });
    }

    regenerate_errors
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::iter::from_fn;

    use assert_matches::assert_matches;
    use mutable_batch::MutableBatch;

    use super::*;

    #[derive(Debug)]
    struct MockTableBatchWriter {
        return_results:
            VecDeque<Result<(), <MockTableBatchWriter as TableBatchWriter>::WriteError>>,
        got_calls: Vec<(TableId, MutableBatch)>,
    }

    impl TableBatchWriter for MockTableBatchWriter {
        type WriteError = WriteError;

        fn write_table_batches<B>(&mut self, table_batches: B) -> Result<(), Self::WriteError>
        where
            B: Iterator<Item = (TableId, MutableBatch)>,
        {
            self.got_calls.extend(table_batches);

            self.return_results
                .pop_front()
                .expect("excess calls to write_table_batches were made")
        }
    }

    #[tokio::test]
    async fn regenerate_entries_continues_on_error() {
        const NAMESPACE_ONE: NamespaceId = NamespaceId::new(1);
        const NON_EXISTENT_NAMESPACE: NamespaceId = NamespaceId::new(2);
        const NAMESPACE_OTHER: NamespaceId = NamespaceId::new(3);

        // Set up a demux to simulate a happy namespace, an always error-ing
        // namespace and a namespace with a temporarily erroring writer.
        let mut demux = NamespaceDemultiplexer::new(move |namespace_id| async move {
            if namespace_id == NAMESPACE_ONE {
                Ok(MockTableBatchWriter {
                    return_results: from_fn(|| Some(Ok(()))).take(10).collect(),
                    got_calls: Default::default(),
                })
            } else if namespace_id == NON_EXISTENT_NAMESPACE {
                Err(RegenerateError::NamespaceSchemaDiscoveryFailed(
                    TableIndexLookupError::NamespaceNotKnown(NON_EXISTENT_NAMESPACE),
                ))
            } else {
                let mut iter = VecDeque::new();
                iter.push_back(Err(WriteError::RecordBatchTranslationFailure(
                    "bananas".to_string(),
                )));
                iter.extend(from_fn(|| Some(Ok(()))).take(9).collect::<VecDeque<_>>());
                Ok(MockTableBatchWriter {
                    return_results: iter,
                    got_calls: Default::default(),
                })
            }
        });

        //  Write some batches happily,
        let result = regenerate_wal_entry(
            (
                1,
                vec![
                    WriteOpEntry {
                        namespace: NAMESPACE_ONE,
                        table_batches: vec![(TableId::new(1), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                    WriteOpEntry {
                        namespace: NAMESPACE_ONE,
                        table_batches: vec![(TableId::new(2), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                ],
            ),
            &mut demux,
        )
        .await;
        assert!(result.is_empty());

        // Then try to write for an unknown namespace, but continue on error
        let result = regenerate_wal_entry(
            (
                2,
                vec![
                    WriteOpEntry {
                        namespace: NON_EXISTENT_NAMESPACE,
                        table_batches: vec![(TableId::new(1), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                    WriteOpEntry {
                        namespace: NAMESPACE_ONE,
                        table_batches: vec![(TableId::new(1), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                ],
            ),
            &mut demux,
        )
        .await;
        assert_eq!(result.len(), 1);
        assert_matches!(
            result.get(0),
            Some(RegenerateError::NamespaceSchemaDiscoveryFailed(..))
        );

        // And finally continue writing after a "corrupt" entry is unable
        // to be translated.
        let result = regenerate_wal_entry(
            (
                3,
                vec![
                    WriteOpEntry {
                        namespace: NAMESPACE_OTHER,
                        table_batches: vec![(TableId::new(1), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                    WriteOpEntry {
                        namespace: NAMESPACE_OTHER,
                        table_batches: vec![(TableId::new(1), Default::default())]
                            .into_iter()
                            .collect(),
                    },
                ],
            ),
            &mut demux,
        )
        .await;
        assert_eq!(result.len(), 1);
        assert_matches!(
            result.get(0),
            Some(RegenerateError::TableBatchWriteFailure(..))
        );

        // There should be five write calls made.
        assert_matches!(demux.get(NAMESPACE_ONE).await, Ok(mock) => {
            assert_eq!(mock.got_calls.len(), 3);
        });
        assert_matches!(demux.get(NAMESPACE_OTHER).await, Ok(mock) => {
            assert_eq!(mock.got_calls.len(), 2);
        });
    }
}
