use std::sync::Arc;

use data_types::write_buffer::WriteBufferConnection;
use time::SystemProvider;
use trace::TraceCollector;
use write_buffer::{
    config::WriteBufferConfigFactory,
    core::{WriteBufferError, WriteBufferWriting},
};

#[derive(Debug, clap::Parser)]
pub struct WriteBufferConfig {
    /// The type of write buffer to use.
    ///
    /// Valid options are: file, kafka, rskafka
    #[clap(long = "--write-buffer", env = "INFLUXDB_IOX_WRITE_BUFFER_TYPE")]
    pub(crate) type_: String,

    /// The address to the write buffer.
    #[clap(long = "--write-buffer-addr", env = "INFLUXDB_IOX_WRITE_BUFFER_ADDR")]
    pub(crate) connection_string: String,

    /// Write buffer topic/database that should be used.
    #[clap(
        long = "--write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared"
    )]
    pub(crate) topic: String,
}

impl WriteBufferConfig {
    /// Initialize the [`WriteBufferWriting`].
    pub async fn init_write_buffer(
        &self,
        metrics: Arc<metric::Registry>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        let write_buffer_config = WriteBufferConnection {
            type_: self.type_.clone(),
            connection: self.connection_string.clone(),
            connection_config: Default::default(),
            creation_config: None,
        };

        let write_buffer =
            WriteBufferConfigFactory::new(Arc::new(SystemProvider::default()), metrics);
        let write_buffer = write_buffer
            .new_config_write(&self.topic, trace_collector.as_ref(), &write_buffer_config)
            .await?;
        Ok(write_buffer)
    }
}
