//! A module providing a CLI command for regenerating line protocol from a WAL file.
use std::fs::{create_dir_all, OpenOptions};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use data_types::NamespaceId;
use observability_deps::tracing::{debug, error, info};
use wal::{ClosedSegmentFileReader, WriteOpEntryDecoder};
use wal_inspect::{LineProtoWriter, NamespaceDemultiplexer, TableBatchWriter, WriteError};

use super::Error;

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
}

/// Executes the `regenerate-lp` command with the provided configuration, reading
/// write operation entries from a WAL file and mapping them to line protocol.
pub async fn command(config: Config) -> Result<(), Error> {
    let decoder = WriteOpEntryDecoder::from(
        ClosedSegmentFileReader::from_path(&config.input)
            .map_err(|wal_err| Error::UnableToOpenWalFile { source: wal_err })?,
    );

    match config.output_directory {
        Some(d) => {
            let d = Arc::new(d);
            create_dir_all(d.as_path())?;
            let namespace_demux = NamespaceDemultiplexer::new(move |namespace_id| {
                let d = Arc::clone(&d);
                async move {
                    let file_path = d.as_path().join(format!("namespace_{}.lp", namespace_id));

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

                    Ok(LineProtoWriter::new(
                        open_options.open(&file_path).map_err(WriteError::IoError)?,
                        None,
                    ))
                }
            });
            decode_and_write_entries(decoder, namespace_demux).await
        }
        None => {
            let namespace_demux = NamespaceDemultiplexer::new(|_namespace_id| async {
                let result: Result<LineProtoWriter<std::io::Stdout>, WriteError> =
                    Ok(LineProtoWriter::new(std::io::stdout(), None));
                result
            });
            decode_and_write_entries(decoder, namespace_demux).await
        }
    }
}

async fn decode_and_write_entries<T, F, I>(
    decoder: WriteOpEntryDecoder,
    mut namespace_demux: NamespaceDemultiplexer<T, F>,
) -> Result<(), Error>
where
    T: TableBatchWriter<WriteError = wal_inspect::WriteError> + Send,
    F: (Fn(NamespaceId) -> I) + Send + Sync,
    I: Future<Output = Result<T, WriteError>> + Send,
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
                .and_then(|write_result| write_result) // flatten out the write result if Ok
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
