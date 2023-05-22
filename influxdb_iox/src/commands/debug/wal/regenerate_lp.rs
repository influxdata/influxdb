//! A module providing a CLI command for regenerating line protocol from a WAL file.
use std::fs::{create_dir_all, OpenOptions};
use std::path::PathBuf;

use observability_deps::tracing::{debug, error, info};
use wal::{ClosedSegmentFileReader, WriteOpEntryDecoder};
use wal_inspect::{LineProtoWriter, NamespacedBatchWriter, WriteError};

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
pub fn command(config: Config) -> Result<(), Error> {
    let decoder = WriteOpEntryDecoder::from(
        ClosedSegmentFileReader::from_path(&config.input)
            .map_err(|wal_err| Error::UnableToOpenWalFile { source: wal_err })?,
    );

    match config.output_directory {
        Some(d) => {
            create_dir_all(d.as_path())?;
            let writer = LineProtoWriter::new(
                |namespace_id| {
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

                    open_options.open(&file_path).map_err(WriteError::IoError)
                },
                None,
            );
            decode_and_write_entries(decoder, writer)
        }
        None => {
            let writer = LineProtoWriter::new(|_namespace_id| Ok(std::io::stdout()), None);
            decode_and_write_entries(decoder, writer)
        }
    }
}

fn decode_and_write_entries<W: NamespacedBatchWriter>(
    decoder: WriteOpEntryDecoder,
    mut writer: W,
) -> Result<(), Error> {
    let mut write_errors = vec![];

    for (wal_entry_number, entry_batch) in decoder.enumerate() {
        for (write_op_number, entry) in entry_batch?.into_iter().enumerate() {
            let namespace_id = entry.namespace;
            debug!(%namespace_id, %wal_entry_number, %write_op_number, "regenerating line protocol for namespace from WAL write op entry");
            writer
                .write_namespaced_table_batches(entry.namespace, entry.table_batches)
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
