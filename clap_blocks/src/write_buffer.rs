//! Config for [`write_buffer`].
use iox_time::SystemProvider;
use observability_deps::tracing::*;
use std::{collections::BTreeMap, num::NonZeroU32, ops::Range, path::PathBuf, sync::Arc};
use tempfile::TempDir;
use trace::TraceCollector;
use write_buffer::{
    config::{WriteBufferConfigFactory, WriteBufferConnection, WriteBufferCreationConfig},
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
};

/// Config for [`write_buffer`].
#[derive(Debug, clap::Parser)]
pub struct WriteBufferConfig {
    /// The type of write buffer to use.
    ///
    /// Valid options are: file, kafka
    #[clap(long = "write-buffer", env = "INFLUXDB_IOX_WRITE_BUFFER_TYPE", action)]
    pub(crate) type_: String,

    /// The address to the write buffer.
    #[clap(
        long = "write-buffer-addr",
        env = "INFLUXDB_IOX_WRITE_BUFFER_ADDR",
        action
    )]
    pub(crate) connection_string: String,

    /// Write buffer topic/database that should be used.
    #[clap(
        long = "write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared",
        action
    )]
    pub(crate) topic: String,

    /// Write buffer connection config.
    ///
    /// The concrete options depend on the write buffer type.
    ///
    /// Command line arguments are passed as
    /// `--write-buffer-connection-config key1=value1,key2=value2`.
    ///
    /// Environment variables are passed as `key1=value1,key2=value2,...`.
    #[clap(
        long = "write-buffer-connection-config",
        env = "INFLUXDB_IOX_WRITE_BUFFER_CONNECTION_CONFIG",
        default_value = "",
        use_value_delimiter = true,
        action = clap::ArgAction::Append
    )]
    pub(crate) connection_config: Vec<String>,

    /// The number of topics to create automatically, if any. Default is to not create any topics.
    #[clap(
        long = "write-buffer-auto-create-topics",
        env = "INFLUXDB_IOX_WRITE_BUFFER_AUTO_CREATE_TOPICS"
    )]
    pub(crate) auto_create_topics: Option<NonZeroU32>,
}

impl WriteBufferConfig {
    /// Create a new instance for all-in-one mode, only allowing some arguments.
    /// If `database_directory` is not specified, creates a new temporary directory.
    pub fn new(topic: &str, database_directory: Option<PathBuf>) -> Self {
        let connection_string = database_directory
            .map(|pathbuf| pathbuf.display().to_string())
            .unwrap_or_else(|| {
                TempDir::new()
                    .expect("Creating a temporary directory should work")
                    .into_path()
                    .display()
                    .to_string()
            });

        info!("Write buffer: File-based in `{}`", connection_string);

        Self {
            type_: "file".to_string(),
            connection_string,
            topic: topic.to_string(),
            connection_config: Default::default(),
            auto_create_topics: Some(NonZeroU32::new(1).unwrap()),
        }
    }

    /// Initialize a [`WriteBufferWriting`].
    pub async fn writing(
        &self,
        metrics: Arc<metric::Registry>,
        partitions: Option<Range<i32>>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        let conn = self.conn();
        let factory = Self::factory(metrics);
        factory
            .new_config_write(&self.topic, partitions, trace_collector.as_ref(), &conn)
            .await
    }

    /// Initialize a [`WriteBufferReading`].
    pub async fn reading(
        &self,
        metrics: Arc<metric::Registry>,
        partitions: Option<Range<i32>>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Arc<dyn WriteBufferReading>, WriteBufferError> {
        let conn = self.conn();
        let factory = Self::factory(metrics);
        factory
            .new_config_read(&self.topic, partitions, trace_collector.as_ref(), &conn)
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
        let creation_config = self
            .auto_create_topics
            .map(|n_shards| WriteBufferCreationConfig {
                n_shards,
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

    /// Get a reference to the write buffer config's topic.
    pub fn topic(&self) -> &str {
        self.topic.as_ref()
    }

    /// Get the write buffer config's auto create topics.
    pub fn auto_create_topics(&self) -> Option<NonZeroU32> {
        self.auto_create_topics
    }

    /// Set the write buffer config's auto create topics.
    pub fn set_auto_create_topics(&mut self, auto_create_topics: Option<NonZeroU32>) {
        self.auto_create_topics = auto_create_topics;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
            "--write-buffer-connection-config",
            "",
            "--write-buffer-connection-config",
            "x=",
            "--write-buffer-connection-config",
            "y",
            "--write-buffer-connection-config",
            "foo=baz",
            "--write-buffer-connection-config",
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
