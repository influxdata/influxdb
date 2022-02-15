use data_types::write_buffer::{WriteBufferConnection, WriteBufferCreationConfig};
use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};
use time::SystemProvider;
use trace::TraceCollector;
use write_buffer::{
    config::WriteBufferConfigFactory,
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
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

    /// Write buffer connection config.
    ///
    /// The concrete options depend on the write buffer type.
    ///
    /// Command line arguments are passed as `--write-buffer-connection-config key1=value1 key2=value2` or
    /// `--write-buffer-connection-config key1=value1,key2=value2`.
    ///
    /// Environment variables are passed as `key1=value1,key2=value2,...`.
    #[clap(
        long = "--write-buffer-connection-config",
        env = "INFLUXDB_IOX_WRITE_BUFFER_CONNECTION_CONFIG",
        default_value = "",
        multiple_values = true,
        use_delimiter = true
    )]
    pub(crate) connection_config: Vec<String>,

    /// The number of topics to create automatically, if any. Default is to not create any topics.
    #[clap(
        long = "--write-buffer-auto-create-topics",
        env = "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS"
    )]
    pub(crate) auto_create_topics: Option<NonZeroU32>,
}

impl WriteBufferConfig {
    /// Initialize a [`WriteBufferWriting`].
    pub async fn writing(
        &self,
        metrics: Arc<metric::Registry>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        let conn = self.conn();
        let factory = Self::factory(metrics);
        factory
            .new_config_write(&self.topic, trace_collector.as_ref(), &conn)
            .await
    }

    /// Initialize a [`WriteBufferReading`].
    pub async fn reading(
        &self,
        metrics: Arc<metric::Registry>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Arc<dyn WriteBufferReading>, WriteBufferError> {
        let conn = self.conn();
        let factory = Self::factory(metrics);
        factory
            .new_config_read(&self.topic, trace_collector.as_ref(), &conn)
            .await
    }

    fn connection_config(&self) -> BTreeMap<String, String> {
        let mut cfg = BTreeMap::new();

        for s in &self.connection_config {
            if s.is_empty() {
                continue;
            }

            if let Some((k, v)) = s.split_once('=') {
                cfg.insert(k.to_owned(), v.to_owned());
            } else {
                cfg.insert(s.clone(), String::from(""));
            }
        }

        cfg
    }

    fn conn(&self) -> WriteBufferConnection {
        let creation_config =
            self.auto_create_topics
                .map(|n_sequencers| WriteBufferCreationConfig {
                    n_sequencers,
                    ..Default::default()
                });
        WriteBufferConnection {
            type_: self.type_.clone(),
            connection: self.connection_string.clone(),
            connection_config: self.connection_config(),
            creation_config,
        }
    }

    fn factory(metrics: Arc<metric::Registry>) -> WriteBufferConfigFactory {
        WriteBufferConfigFactory::new(Arc::new(SystemProvider::default()), metrics)
    }
}

#[cfg(test)]
mod tests {
    use clap::StructOpt;

    use super::*;

    #[test]
    fn test_connection_config() {
        let cfg = WriteBufferConfig::try_parse_from([
            "my_binary",
            "--write-buffer",
            "kafka",
            "--write-buffer-addr",
            "localhost:1234",
            "--write-buffer-connection-config",
            "foo=bar",
            "",
            "x=",
            "y",
            "foo=baz",
            "so=many=args",
        ])
        .unwrap();
        let actual = cfg.connection_config();
        let expected = BTreeMap::from([
            (String::from("foo"), String::from("baz")),
            (String::from("x"), String::from("")),
            (String::from("y"), String::from("")),
            (String::from("so"), String::from("many=args")),
        ]);
        assert_eq!(actual, expected);
    }
}
