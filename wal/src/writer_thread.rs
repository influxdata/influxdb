use std::{sync::Arc, thread::JoinHandle};

use data_types::{sequence_number_set::SequenceNumberSet, SequenceNumber};
use generated_types::influxdata::iox::wal::v1 as proto;
use observability_deps::tracing::{debug, error};
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::mpsc;

use crate::{Segments, WalBuffer, WriteResult};

/// The number of [`WalBuffer`] that may be enqueued for persistence.
const FLUSH_QUEUE_DEPTH: usize = 1;

/// An inner/non-pub struct that contains the [`WriterIoThreadHandle`] state -
/// this lets the [`Drop`] impl of [`WriterIoThreadHandle`] destroy the channel
/// tx and consume the [`JoinHandle`].
struct HandleInner {
    batch_tx: mpsc::Sender<WalBuffer>,
    join_handle: JoinHandle<()>,
}

/// A [`WriterIoThreadHandle`] provides an interface for the caller to interact
/// with an I/O thread.
///
/// Constructing a [`WriterIoThreadHandle`] spawns an I/O thread, and dropping
/// the handle gracefully stops the I/O thread after it finishes flushing the
/// current batch it is processing, if any.
pub(crate) struct WriterIoThreadHandle {
    inner: Option<HandleInner>,
}

impl WriterIoThreadHandle {
    /// Spawn an I/O writer thread, flushing batches to the open segment in
    /// `segments`.
    pub(crate) fn new(segments: Arc<Mutex<Segments>>) -> Self {
        let (batch_tx, batch_rx) = mpsc::channel(FLUSH_QUEUE_DEPTH);

        // Spawn the I/O thread and retain a handle to wait for shutdown when
        // this handle is dropped.
        let join_handle = std::thread::Builder::new()
            .name("WAL writer I/O thread".to_string())
            .spawn(move || {
                let writer = WriterIoThread::new(batch_rx, segments);
                writer.run();
            })
            .expect("failed to spawn WAL I/O thread");

        Self {
            inner: Some(HandleInner {
                batch_tx,
                join_handle,
            }),
        }
    }

    /// Enqueue `batch` to be wrote to the current open segment file.
    ///
    /// Once flushed to disk and durable (or the write failed) the write result
    /// is broadcast through the embedded channel.
    ///
    /// # Panics
    ///
    /// Panics if the I/O thread is not running.
    pub(crate) async fn enqueue_batch(&self, batch: WalBuffer) {
        self.inner
            .as_ref()
            .unwrap()
            .batch_tx
            .send(batch)
            .await
            .expect("wal writer IO thread is dead")
    }
}

impl Drop for WriterIoThreadHandle {
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();
        // Signal to the IO thread that it should stop.
        drop(inner.batch_tx);
        // Wait for the IO thread to gracefully stop, discarding any errors -
        // attempting to unwrap/panic in this Drop implwill cause an immediate
        // SIGSEV.
        let _ = inner.join_handle.join();
    }
}

/// The state of the I/O actor thread.
struct WriterIoThread {
    /// A channel to receive batches to flush.
    batch_rx: mpsc::Receiver<WalBuffer>,
    /// The set of segments, used to obtain the current open segment handle.
    segments: Arc<Mutex<Segments>>,
}

impl WriterIoThread {
    fn new(batch_rx: mpsc::Receiver<WalBuffer>, segments: Arc<Mutex<Segments>>) -> Self {
        Self { batch_rx, segments }
    }

    fn run(mut self) {
        // Maintain a serialisation buffer for re-use, instead of allocating a
        // new one for each batch.
        let mut proto_data = Vec::with_capacity(4 * 1024);

        loop {
            proto_data.clear();

            let batch = match self.batch_rx.blocking_recv() {
                Some(batch) => batch,
                None => {
                    // The batch channel has closed - all handles have been
                    // dropped.
                    debug!("stopping WAL IO thread");
                    return;
                }
            };

            // Encode the batch into the proto types, and extract the
            // SequenceNumberSet for this batch.
            let (ops, ids): (Vec<_>, SequenceNumberSet) = batch
                .ops
                .into_iter()
                .map(|v| {
                    let op_ids: SequenceNumberSet = v
                        .table_write_sequence_numbers
                        .values()
                        .map(|&id| SequenceNumber::new(id as _))
                        .collect();
                    (proto::SequencedWalOp::from(v), op_ids)
                })
                .unzip();
            let proto_batch = proto::WalOpBatch { ops };

            // Generate the binary protobuf message, storing it into proto_data
            proto_batch
                .encode(&mut proto_data)
                .expect("encoding batch into vec cannot fail");

            // Obtain the segments lock - this prevents concurrent rotation, but
            // has no impact on concurrent writers.
            {
                // Write the serialised data to the current open segment file.
                let mut segments = self.segments.lock();
                match segments.open_segment.write(&proto_data) {
                    Ok(summary) => {
                        // Broadcast the result to all writers to this batch.
                        //
                        // Do not panic if no thread is waiting for the flush
                        // notification - this may be the case if all writes
                        // disconnected before the WAL was flushed.
                        let _ = batch.notify_flush.send(Some(WriteResult::Ok(summary)));

                        // Now the blocked writers are making progress, union
                        // this batch sequence number set with the cumulative
                        // segment file set before releasing the segment lock.
                        segments.open_segment_ids.add_set(&ids);
                    }
                    Err(e) => {
                        error!(error=%e, "failed to write WAL batch");
                        let _ = batch
                            .notify_flush
                            .send(Some(WriteResult::Err(e.to_string())));
                    }
                };
            };
        }
    }
}
