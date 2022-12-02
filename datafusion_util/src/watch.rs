use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{error::ArrowError, record_batch::RecordBatch},
    common::DataFusionError,
};
use observability_deps::tracing::debug;
use pin_project::{pin_project, pinned_drop};
use tokio::task::{JoinError, JoinHandle};

use crate::sender::AbstractSender;

/// A [`RecordBatch`]-producing task that is watched.
///
/// This includes:
/// - when the task panics, an error will be sent to all receiving channels
/// - task is aborted on drop
#[pin_project]
#[derive(Debug)]
pub struct WatchedTask(#[pin] AutoAbortJoinHandle<()>);

impl WatchedTask {
    pub fn new<F, S>(fut: F, tx: Vec<S>, description: &'static str) -> Arc<Self>
    where
        F: Future<Output = Result<(), ArrowError>> + Send + 'static,
        S: AbstractSender<T = Result<RecordBatch, ArrowError>>,
    {
        let handle = AutoAbortJoinHandle::new(tokio::task::spawn(fut));
        let handle = AutoAbortJoinHandle(tokio::task::spawn(watch_task(description, tx, handle)));
        Arc::new(Self(handle))
    }
}

impl Future for WatchedTask {
    type Output = Result<(), JoinError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.0.poll(cx)
    }
}

/// Watches the output of a tokio task for an error. If the task
/// errors, send an arror error to the channel.
///
/// This pattern is used to run the operator logic in one task that
/// sends to a channel that can return an error
async fn watch_task<S>(
    description: &'static str,
    tx: Vec<S>,
    task: AutoAbortJoinHandle<Result<(), ArrowError>>,
) where
    S: AbstractSender<T = Result<RecordBatch, ArrowError>>,
{
    let task_result = task.await;

    let msg = match task_result {
        Err(join_err) => {
            debug!(e=%join_err, %description, "Error joining");
            Some(DataFusionError::Context(
                format!("Join error for '{description}'"),
                Box::new(DataFusionError::External(Box::new(join_err))),
            ))
        }
        Ok(Err(e)) => {
            debug!(%e, %description, "Error in task itself");
            Some(DataFusionError::Context(
                format!("Execution error for '{description}'"),
                Box::new(DataFusionError::ArrowError(e)),
            ))
        }
        Ok(Ok(())) => {
            // successful
            None
        }
    };

    // If there is a message to send down the channel, try and do so
    if let Some(e) = msg {
        let e = Arc::new(e);
        for tx in tx {
            // try and tell the receiver something went
            // wrong. Note we ignore errors sending this message
            // as that means the receiver has already been
            // shutdown and no one cares anymore lol
            let err = ArrowError::ExternalError(Box::new(Arc::clone(&e)));
            if tx.send(Err(err)).await.is_err() {
                debug!(%description, "receiver hung up");
            }
        }
    }
}

/// A [`JoinHandle`] that is aborted on drop.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
struct AutoAbortJoinHandle<T>(#[pin] JoinHandle<T>);

impl<T> AutoAbortJoinHandle<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for AutoAbortJoinHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        self.0.abort();
    }
}

impl<T> Future for AutoAbortJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.0.poll(cx)
    }
}
