//! Buffers writes and flushes them to the configured wal

use crate::write_buffer::buffer_segment::{BufferedWrite, WriteBatch};
use crate::write_buffer::{Error, SegmentState, ValidSegmentedData};
use crate::{wal, Wal, WalOp};
use crossbeam_channel::{bounded, Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use iox_time::Time;
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
    Success(()),
    Error(String),
}

type SegmentedWalOps = HashMap<Time, Vec<WalOp>>;
type SegmentedWriteBatch = HashMap<Time, WriteBatch>;

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
    pub fn new<W: Wal>(segment_state: Arc<RwLock<SegmentState<W>>>) -> Self {
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
        segmented_data: Vec<ValidSegmentedData>,
    ) -> crate::write_buffer::Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.buffer_tx
            .send(BufferedWrite {
                segmented_data,
                response_tx,
            })
            .await
            .expect("wal op buffer thread is dead");

        let summary = response_rx.await.expect("wal op buffer thread is dead");

        match summary {
            BufferedWriteResult::Success(_) => Ok(()),
            BufferedWriteResult::Error(e) => Err(Error::BufferSegmentError(e)),
        }
    }
}

async fn run_wal_op_buffer<W: Wal>(
    segment_state: Arc<RwLock<SegmentState<W>>>,
    mut buffer_rx: mpsc::Receiver<BufferedWrite>,
    io_flush_tx: CrossbeamSender<SegmentedWalOps>,
    io_flush_notify_rx: CrossbeamReceiver<wal::Result<()>>,
    mut shutdown: watch::Receiver<()>,
) {
    let mut ops = SegmentedWalOps::new();
    let mut write_batch = SegmentedWriteBatch::new();
    let mut notifies = Vec::new();
    let mut interval = tokio::time::interval(BUFFER_FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        // select on either buffering an op, ticking the flush interval, or shutting down
        select! {
            Some(buffered_write) = buffer_rx.recv() => {
                for segmented_data in buffered_write.segmented_data {
                    let segment_ops = ops.entry(segmented_data.segment_start).or_default();
                    segment_ops.push(segmented_data.wal_op);

                    let segment_write_batch = write_batch.entry(segmented_data.segment_start).or_default();
                    segment_write_batch.add_db_write(segmented_data.database_name, segmented_data.table_batches);
                }
                notifies.push(buffered_write.response_tx);
            },
            _ = interval.tick() => {
                if ops.is_empty() {
                    continue;
                }

                // send ops into IO flush channel and wait for response
                io_flush_tx.send(ops).expect("wal io thread is dead");

                let res = match io_flush_notify_rx.recv().expect("wal io thread is dead") {
                  Ok(()) => {
                       let mut err = BufferedWriteResult::Success(());

                        let mut segment_state = segment_state.write();

                        for (time, write_batch) in write_batch {
                            if let Err(e) = segment_state.write_batch_to_segment(time, write_batch) {
                                err = BufferedWriteResult::Error(e.to_string());
                                break;
                            }
                        }

                        err
                    },
                    Err(e) => BufferedWriteResult::Error(e.to_string()),
                };

                // notify the watchers of the write response
                for response_tx in notifies {
                    let _ = response_tx.send(res.clone());
                }

                // reset the buffers
                ops = SegmentedWalOps::new();
                write_batch = SegmentedWriteBatch::new();
                notifies = Vec::new();
            },
            _ = shutdown.changed() => {
                // shutdown has been requested
                debug!("stopping wal op buffer thread");
                return;
            }
        }
    }
}

fn run_io_flush<W: Wal>(
    segment_state: Arc<RwLock<SegmentState<W>>>,
    buffer_rx: CrossbeamReceiver<SegmentedWalOps>,
    buffer_notify: CrossbeamSender<wal::Result<()>>,
) {
    loop {
        let segmented_wal_ops = match buffer_rx.recv() {
            Ok(batch) => batch,
            Err(_) => {
                // the buffer channel has closed, it's shutdown
                debug!("stopping wal io thread");
                return;
            }
        };

        let mut state = segment_state.write();

        // write the ops to the segment files, or return on first error
        for (time, wal_ops) in segmented_wal_ops {
            let res = state.write_ops_to_segment(time, wal_ops);
            if res.is_err() {
                buffer_notify.send(res).expect("buffer flusher is dead");
                continue;
            }
        }

        buffer_notify.send(Ok(())).expect("buffer flusher is dead");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::wal::{WalImpl, WalSegmentWriterNoopImpl};
    use crate::write_buffer::buffer_segment::OpenBufferSegment;
    use crate::write_buffer::parse_validate_and_update_catalog;
    use crate::{Precision, SegmentDuration, SegmentId, SegmentRange, SequenceNumber};
    use data_types::NamespaceName;

    #[tokio::test]
    async fn flushes_to_open_segment() {
        let segment_id = SegmentId::new(3);
        let open_segment = OpenBufferSegment::new(
            segment_id,
            SegmentRange::test_range(),
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(segment_id)),
            None,
        );
        let next_segment_id = segment_id.next();
        let next_segment_range = SegmentRange::test_range().next();

        let next_segment = OpenBufferSegment::new(
            next_segment_id,
            next_segment_range,
            SequenceNumber::new(0),
            Box::new(WalSegmentWriterNoopImpl::new(next_segment_id)),
            None,
        );
        let catalog = Arc::new(Catalog::new());
        let segment_state = Arc::new(RwLock::new(SegmentState::<WalImpl>::new(
            SegmentDuration::FiveMinutes,
            next_segment_id,
            Arc::clone(&catalog),
            open_segment,
            next_segment,
            None,
        )));
        let flusher = WriteBufferFlusher::new(Arc::clone(&segment_state));

        let db_name = NamespaceName::new("db1").unwrap();
        let ingest_time = Time::from_timestamp_nanos(0);
        let res = parse_validate_and_update_catalog(
            db_name.clone(),
            "cpu bar=1 10",
            &catalog,
            ingest_time,
            SegmentDuration::FiveMinutes,
            false,
            Precision::Nanosecond,
        )
        .unwrap();

        let _ = flusher
            .write_to_open_segment(res.valid_segmented_data)
            .await
            .unwrap();

        let res = parse_validate_and_update_catalog(
            db_name.clone(),
            "cpu bar=1 20",
            &catalog,
            ingest_time,
            SegmentDuration::FiveMinutes,
            false,
            Precision::Nanosecond,
        )
        .unwrap();
        let _ = flusher
            .write_to_open_segment(res.valid_segmented_data)
            .await
            .unwrap();

        let state = segment_state.read();
        assert_eq!(state.current_segment.segment_id(), segment_id);

        let table_buffer = state
            .current_segment
            .table_buffer(db_name.as_str(), "cpu")
            .unwrap();
        assert_eq!(table_buffer.row_count(), 2);
    }
}
