use influxdb3_write::persister::PersisterImpl;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::Wal;
use iox_time::TimeProvider;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;

#[derive(Debug)]
pub struct Compactor<W, T> {
    #[allow(dead_code)]
    write_buffer: Arc<WriteBufferImpl<W, T>>,
    #[allow(dead_code)]
    persister: Arc<PersisterImpl>,
}

impl<W: Wal, T: TimeProvider> Compactor<W, T> {
    pub fn new(write_buffer: Arc<WriteBufferImpl<W, T>>, persister: Arc<PersisterImpl>) -> Self {
        Self {
            write_buffer,
            persister,
        }
    }

    pub async fn compact(self) {
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }
}
