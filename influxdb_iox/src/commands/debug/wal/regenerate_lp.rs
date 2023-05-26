//! A module providing a CLI command for regenerating line protocol from a WAL file.
use std::fs::{create_dir_all, OpenOptions};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use data_types::{NamespaceId, TableId};
use hashbrown::HashMap;
use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::schema::Client as SchemaClient;
use observability_deps::tracing::{debug, error, info};
use tokio::sync::Mutex;
use wal::{ClosedSegmentFileReader, WriteOpEntryDecoder};
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
    schema_client: Mutex<SchemaClient>,
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
            schema_client: Mutex::new(SchemaClient::new(connection)),
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
            .lock()
            .await
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
        ClosedSegmentFileReader::from_path(&config.input).map_err(Error::UnableToOpenWalFile)?,
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
                let d = Arc::clone(&d);
                let table_name_indexer = table_name_indexer.as_ref().map(Arc::clone);
                async move {
                    let file_path = d
                        .as_path()
                        .join(format!("namespace_id_{}.lp", namespace_id));

                    let mut open_options = OpenOptions::new().write(true).to_owned();
                    if config.force {
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
    let mut write_errors = vec![];

    for (wal_entry_number, entry_batch) in decoder.enumerate() {
        for (write_op_number, entry) in entry_batch?.into_iter().enumerate() {
            let namespace_id = entry.namespace;
            debug!(%namespace_id, %wal_entry_number, %write_op_number, "regenerating line protocol for namespace from WAL write op entry");
            namespace_demux
                .get(namespace_id)
                .await
                .map(|writer| writer.write_table_batches(entry.table_batches.into_iter()))
                .and_then(|write_result| {
                    write_result.map_err(RegenerateError::TableBatchWriteFailure)
                }) // flatten out the write result if Ok
                .unwrap_or_else(|err| {
                    error!(
                        %namespace_id,
                        %wal_entry_number,
                        %write_op_number,
                        %err,
                        "failed to rewrite table batches for write op");
                    write_errors.push(err);
                });
        }
    }

    if !write_errors.is_empty() {
        Err(Error::UnableToFullyRegenerateLineProtocol {
            sources: write_errors,
        })
    } else {
        Ok(())
    }
}
