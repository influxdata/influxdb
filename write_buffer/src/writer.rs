#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]
use crate::{Error as WalError, SequenceNumber, WalBuilder, WritePayload};

use futures::{channel::mpsc, SinkExt, StreamExt};
use snafu::{ResultExt, Snafu};

use observability_deps::tracing::{error, info};
use serde::{Deserialize, Serialize};

use std::path::PathBuf;

#[derive(Debug, Snafu)]
/// Error type
pub enum Error {
    #[snafu(display("Wal Writer error using WAL: {}", source))]
    UnderlyingWalError { source: WalError },

    #[snafu(display("Error serializing metadata: {}", source))]
    SerializeMetadata { source: serde_json::error::Error },

    #[snafu(display("Error writing to WAL: {}", source))]
    WrtitingToWal { source: std::io::Error },

    #[snafu(display("Error writing metadata to '{:?}': {}", metadata_path, source))]
    WritingMetadata {
        metadata_path: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WalDetails {
    pub metadata_path: PathBuf,
    pub metadata: WalMetadata,
    pub write_tx: mpsc::Sender<WalWrite>,
}

#[derive(Debug)]
pub struct WalWrite {
    payload: WritePayload,
    notify_tx: mpsc::Sender<Result<SequenceNumber, WalError>>,
}

impl WalDetails {
    pub async fn write_metadata(&self) -> Result<()> {
        Ok(tokio::fs::write(
            self.metadata_path.clone(),
            serde_json::to_string(&self.metadata).context(SerializeMetadata)?,
        )
        .await
        .context(WritingMetadata {
            metadata_path: &self.metadata_path,
        })?)
    }

    pub async fn write_and_sync(&self, data: Vec<u8>) -> Result<()> {
        let payload = WritePayload::new(data).context(UnderlyingWalError {})?;

        let (notify_tx, mut notify_rx) = mpsc::channel(1);

        let write = WalWrite { payload, notify_tx };

        let mut tx = self.write_tx.clone();
        tx.send(write)
            .await
            .expect("The WAL thread should always be running to receive a write");

        let _ = notify_rx
            .next()
            .await
            .expect("The WAL thread should always be running to send a response.")
            .context(UnderlyingWalError {})?;

        Ok(())
    }
}

/// Metadata about this particular WAL
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WalMetadata {
    pub format: WalFormat,
}

impl Default for WalMetadata {
    fn default() -> Self {
        Self {
            format: WalFormat::FlatBuffers,
        }
    }
}

/// Supported WAL formats that can be restored
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum WalFormat {
    FlatBuffers,
    #[serde(other)]
    Unknown,
}

pub async fn start_wal_sync_task(wal_builder: WalBuilder) -> Result<WalDetails> {
    let mut wal = wal_builder.wal().context(UnderlyingWalError)?;

    let metadata = tokio::fs::read_to_string(wal.metadata_path())
        .await
        .and_then(|raw_metadata| {
            serde_json::from_str::<WalMetadata>(&raw_metadata).map_err(Into::into)
        })
        .unwrap_or_default();
    let metadata_path = wal.metadata_path();

    let (write_tx, mut write_rx) = mpsc::channel::<WalWrite>(100);

    tokio::spawn({
        async move {
            loop {
                match write_rx.next().await {
                    Some(write) => {
                        let payload = write.payload;
                        let mut tx = write.notify_tx;

                        let result = wal.append(payload).and_then(|seq| {
                            wal.sync_all()?;
                            Ok(seq)
                        });

                        if let Err(e) = tx.send(result).await {
                            error!("error sending result back to writer {:?}", e);
                        }
                    }
                    None => {
                        info!("shutting down WAL for {:?}", wal.metadata_path());
                        return;
                    }
                }
            }
        }
    });

    Ok(WalDetails {
        metadata_path,
        metadata,
        write_tx,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works_but_has_no_tests() {
        // :thinking_face:
    }
}
