use datafusion::arrow::{error::ArrowError, record_batch::RecordBatch};
use observability_deps::tracing::debug;
use tokio::sync::mpsc::Sender;

use crate::AutoAbortJoinHandle;

/// Watches the output of a tokio task for an error. If the task
/// errors, send an arror error to the channel.
///
/// This pattern is used to run the operator logic in one task that
/// sends to a channel that can return an error
pub async fn watch_task(
    description: &str,
    tx: Sender<Result<RecordBatch, ArrowError>>,
    task: AutoAbortJoinHandle<Result<(), ArrowError>>,
) {
    let task_result = task.await;

    let msg = match task_result {
        Err(join_err) => {
            debug!(e=%join_err, %description, "Error joining");
            Some(ArrowError::ExternalError(Box::new(join_err)))
        }
        Ok(Err(e)) => {
            debug!(%e, %description, "Error in task itself");
            Some(e)
        }
        Ok(Ok(())) => {
            // successful
            None
        }
    };

    // If there is a message to send down the channel, try and do so
    if let Some(e) = msg {
        // try and tell the receiver something went
        // wrong. Note we ignore errors sending this message
        // as that means the receiver has already been
        // shutdown and no one cares anymore lol
        if tx.send(Err(e)).await.is_err() {
            debug!(%description, "receiver hung up");
        }
    }
}
