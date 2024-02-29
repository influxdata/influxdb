//! Buffers writes and flushes them to the configured wal

use crate::write_buffer::buffer_segment::{BufferedWrite, DatabaseWrite, WriteBatch, WriteSummary};
use crate::write_buffer::{Error, SegmentState, TableBatch};
use crate::{wal, SequenceNumber, WalOp};
use crossbeam_channel::{bounded, Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use data_types::NamespaceName;
use observability_deps::tracing::debug;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

// Duration to buffer writes before flushing them to the wal
const BUFFER_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
// The maximum number of buffered writes that can be queued up before backpressure is applied
const BUFFER_CHANNEL_LIMIT: usize = 10_000;

// buffered writes should only fail if the underlying WAL throws an error. They are validated before they
// are buffered. If there is an error, it'll be here
#[derive(Debug, Clone)]
pub enum BufferedWriteResult {
    Success(WriteSummary),
    Error(String),
}

/// The WriteBufferFlusher buffers writes and flushes them to the configured wal. The wal IO is done in a native
/// thread rather than a tokio task to avoid blocking the tokio runtime. As referenced in this post, continuous
/// long-running IO threads should be off the tokio runtime: `<https://ryhl.io/blog/async-what-is-blocking/>`.
#[derive(Debug)]
pub struct WriteBufferFlusher {
    join_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    wal_io_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    #[allow(dead_code)]
    shutdown_tx: watch::Sender<()>,
    buffer_tx: mpsc::Sender<BufferedWrite>,
}

impl WriteBufferFlusher {
    pub fn new(segment_state: Arc<RwLock<SegmentState>>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (buffer_tx, buffer_rx) = mpsc::channel(BUFFER_CHANNEL_LIMIT);
        let (io_flush_tx, io_flush_rx) = bounded(1);
        let (io_flush_notify_tx, io_flush_notify_rx) = bounded(1);

        let flusher = Self {
            join_handle: Default::default(),
            wal_io_handle: Default::default(),
            shutdown_tx,
            buffer_tx,
        };

        let wal_op_buffer_segment_state = Arc::clone(&segment_state);

        *flusher.wal_io_handle.lock() = Some(
            std::thread::Builder::new()
                .name("write buffer io flusher".to_string())
                .spawn(move || {
                    run_io_flush(segment_state, io_flush_rx, io_flush_notify_tx);
                })
                .expect("failed to spawn write buffer io flusher thread"),
        );

        *flusher.join_handle.lock() = Some(tokio::task::spawn(async move {
            run_wal_op_buffer(
                wal_op_buffer_segment_state,
                buffer_rx,
                io_flush_tx,
                io_flush_notify_rx,
                shutdown_rx,
            )
            .await;
        }));

        flusher
    }

    pub async fn write_to_open_segment(
        &self,
        db_name: NamespaceName<'static>,
        table_batches: HashMap<String, TableBatch>,
        wal_op: WalOp,
    ) -> crate::write_buffer::Result<WriteSummary> {
        let (response_tx, response_rx) = oneshot::channel();

        self.buffer_tx
            .send(BufferedWrite {
                wal_op,
                database_write: DatabaseWrite::new(db_name, table_batches),
                response_tx,
            })
            .await
            .expect("wal op buffer thread is dead");

        let summary = response_rx.await.expect("wal op buffer thread is dead");

        match summary {
            BufferedWriteResult::Success(summary) => Ok(summary),
            BufferedWriteResult::Error(e) => Err(Error::BufferSegmentError(e)),
        }
    }
}

async fn run_wal_op_buffer(
    segment_state: Arc<RwLock<SegmentState>>,
    mut buffer_rx: mpsc::Receiver<BufferedWrite>,
    io_flush_tx: CrossbeamSender<Vec<WalOp>>,
    io_flush_notify_rx: CrossbeamReceiver<wal::Result<SequenceNumber>>,
    mut shutdown: watch::Receiver<()>,
) {
    let mut ops = Vec::new();
    let mut write_batch = crate::write_buffer::buffer_segment::WriteBatch::default();
    let mut notifies = Vec::new();
    let mut interval = tokio::time::interval(BUFFER_FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        // select on either buffering an op, ticking the flush interval, or shutting down
        select! {
            Some(buffered_write) = buffer_rx.recv() => {
                ops.push(buffered_write.wal_op);
                write_batch.add_db_write(buffered_write.database_write.db_name, buffered_write.database_write.table_batches);
                notifies.push(buffered_write.response_tx);
            },
            _ = interval.tick() => {
                if ops.is_empty() {
                    continue;
                }

                let ops_written = ops.len();

                // send ops into IO flush channel and wait for response
                io_flush_tx.send(ops).expect("wal io thread is dead");

                let res = match io_flush_notify_rx.recv().expect("wal io thread is dead") {
                  Ok(sequence_number) => {
                    let open_segment = &mut segment_state.write().open_segment;

                        match open_segment.buffer_writes(write_batch) {
                            Ok(buffer_size) => BufferedWriteResult::Success(WriteSummary {
                                segment_id: open_segment.segment_id(),
                                sequence_number,
                                buffer_size,
                            }),
                            Err(e) => BufferedWriteResult::Error(e.to_string()),
                        }
                    },
                    Err(e) => BufferedWriteResult::Error(e.to_string()),
                };

                // notify the watchers of the write response
                for response_tx in notifies {
                    let _ = response_tx.send(res.clone());
                }

                // reset the buffers
                ops = Vec::with_capacity(ops_written);
                write_batch = WriteBatch::default();
                notifies = Vec::with_capacity(ops_written);
            },
            _ = shutdown.changed() => {
                // shutdown has been requested
                debug!("stopping wal op buffer thread");
                return;
            }
        }
    }
}

fn run_io_flush(
    segment_state: Arc<RwLock<SegmentState>>,
    buffer_rx: CrossbeamReceiver<Vec<WalOp>>,
    buffer_notify: CrossbeamSender<wal::Result<SequenceNumber>>,
) {
    loop {
        let batch = match buffer_rx.recv() {
            Ok(batch) => batch,
            Err(_) => {
                // the buffer channel has closed, it's shutdown
                debug!("stopping wal io thread");
                return;
            }
        };

        let mut state = segment_state.write();
        let res = state.open_segment.write_batch(batch);

        buffer_notify.send(res).expect("buffer flusher is dead");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::lp_to_table_batches;
    use crate::wal::WalSegmentWriterNoopImpl;
    use crate::write_buffer::buffer_segment::OpenBufferSegment;
    use crate::{LpWriteOp, SegmentId};

    #[tokio::test]
    async fn flushes_to_open_segment() {
        let segment_id = SegmentId::new(3);
        let open_segment = OpenBufferSegment::new(
            segment_id,
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(segment_id)),
            None,
        );
        let segment_state = Arc::new(RwLock::new(SegmentState::new(open_segment)));
        let flusher = WriteBufferFlusher::new(Arc::clone(&segment_state));

        let db_name = NamespaceName::new("db1").unwrap();
        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: "cpu bar=1 10".to_string(),
            default_time: 0,
        });
        let data = lp_to_table_batches("cpu bar=1 10", 0);
        let write_summary = flusher
            .write_to_open_segment(db_name.clone(), data, wal_op)
            .await
            .unwrap();

        assert_eq!(write_summary.segment_id, segment_id);
        assert_eq!(write_summary.sequence_number, SequenceNumber::new(1));

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: "cpu bar=1 20".to_string(),
            default_time: 0,
        });
        let data = lp_to_table_batches("cpu bar=1 20", 0);
        let write_summary = flusher
            .write_to_open_segment(db_name.clone(), data, wal_op)
            .await
            .unwrap();

        assert_eq!(write_summary.sequence_number, SequenceNumber::new(2));

        let state = segment_state.read();
        assert_eq!(state.open_segment.segment_id(), segment_id);

        let table_buffer = state
            .open_segment
            .table_buffer(db_name.as_str(), "cpu")
            .unwrap();
        let partition_buffer = table_buffer.partition_buffer("1970-01-01").unwrap();
        assert_eq!(partition_buffer.row_count(), 2);
    }
}
