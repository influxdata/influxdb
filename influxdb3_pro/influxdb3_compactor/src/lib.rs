use influxdb3_write::persister::PersisterImpl;
use influxdb3_write::write_buffer::WriteBufferImpl;
use iox_time::TimeProvider;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;

#[derive(Debug)]
pub struct Compactor<T> {
    #[allow(dead_code)]
    write_buffer: Arc<WriteBufferImpl<T>>,
    #[allow(dead_code)]
    persister: Arc<PersisterImpl>,
}

impl<T: TimeProvider> Compactor<T> {
    pub fn new(write_buffer: Arc<WriteBufferImpl<T>>, persister: Arc<PersisterImpl>) -> Self {
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
