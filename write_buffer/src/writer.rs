#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]
use crate::{Error as WriteBufferError, SequenceNumber, WriteBufferBuilder, WritePayload};

use futures::{channel::mpsc, SinkExt, StreamExt};
use snafu::{ResultExt, Snafu};

use observability_deps::tracing::{error, info};
use serde::{Deserialize, Serialize};

use std::path::PathBuf;

#[derive(Debug, Snafu)]
/// Error type
pub enum Error {
    #[snafu(display("Write Buffer Writer error using Write Buffer: {}", source))]
    UnderlyingWriteBufferError { source: WriteBufferError },

    #[snafu(display("Error serializing metadata: {}", source))]
    SerializeMetadata { source: serde_json::error::Error },

    #[snafu(display("Error writing to Write Buffer: {}", source))]
    WrtitingToWriteBuffer { source: std::io::Error },

    #[snafu(display("Error writing metadata to '{:?}': {}", metadata_path, source))]
    WritingMetadata {
        metadata_path: PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteBufferDetails {
    pub metadata_path: PathBuf,
    pub metadata: WriteBufferMetadata,
    pub write_tx: mpsc::Sender<WriteBufferWrite>,
}

#[derive(Debug)]
pub struct WriteBufferWrite {
    payload: WritePayload,
    notify_tx: mpsc::Sender<Result<SequenceNumber, WriteBufferError>>,
}

impl WriteBufferDetails {
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
        let payload = WritePayload::new(data).context(UnderlyingWriteBufferError {})?;

        let (notify_tx, mut notify_rx) = mpsc::channel(1);

        let write = WriteBufferWrite { payload, notify_tx };

        let mut tx = self.write_tx.clone();
        tx.send(write)
            .await
            .expect("The Write Buffer thread should always be running to receive a write");

        let _ = notify_rx
            .next()
            .await
            .expect("The Write Buffer thread should always be running to send a response.")
            .context(UnderlyingWriteBufferError {})?;

        Ok(())
    }
}

/// Metadata about this particular Write Buffer
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WriteBufferMetadata {
    pub format: WriteBufferFormat,
}

impl Default for WriteBufferMetadata {
    fn default() -> Self {
        Self {
            format: WriteBufferFormat::FlatBuffers,
        }
    }
}

/// Supported WriteBuffer formats that can be restored
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum WriteBufferFormat {
    FlatBuffers,
    #[serde(other)]
    Unknown,
}

pub async fn start_write_buffer_sync_task(
    write_buffer_builder: WriteBufferBuilder,
) -> Result<WriteBufferDetails> {
    let mut write_buffer = write_buffer_builder
        .write_buffer()
        .context(UnderlyingWriteBufferError)?;

    let metadata = tokio::fs::read_to_string(write_buffer.metadata_path())
        .await
        .and_then(|raw_metadata| {
            serde_json::from_str::<WriteBufferMetadata>(&raw_metadata).map_err(Into::into)
        })
        .unwrap_or_default();
    let metadata_path = write_buffer.metadata_path();

    let (write_tx, mut write_rx) = mpsc::channel::<WriteBufferWrite>(100);

    tokio::spawn({
        async move {
            loop {
                match write_rx.next().await {
                    Some(write) => {
                        let payload = write.payload;
                        let mut tx = write.notify_tx;

                        let result = write_buffer.append(payload).and_then(|seq| {
                            write_buffer.sync_all()?;
                            Ok(seq)
                        });

                        if let Err(e) = tx.send(result).await {
                            error!("error sending result back to writer {:?}", e);
                        }
                    }
                    None => {
                        info!(
                            "shutting down Write Buffer for {:?}",
                            write_buffer.metadata_path()
                        );
                        return;
                    }
                }
            }
        }
    });

    Ok(WriteBufferDetails {
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
