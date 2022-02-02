use std::{collections::BTreeMap, time::Duration};

use data_types::write_buffer::WriteBufferCreationConfig;

use crate::core::WriteBufferError;

/// Generic client config that is used for consumers, producers as well as admin operations (like "create topic").
#[derive(Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// Maximum message size in bytes.
    ///
    /// extracted from `max_message_size`. Defaults to `None` (rskafka default).
    pub max_message_size: Option<usize>,
}

impl TryFrom<&BTreeMap<String, String>> for ClientConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Self {
            max_message_size: cfg.get("max_message_size").map(|s| s.parse()).transpose()?,
        })
    }
}

/// Config for topic creation.
#[derive(Debug, PartialEq, Eq)]
pub struct TopicCreationConfig {
    /// Number of partitions.
    pub num_partitions: i32,

    /// Replication factor.
    ///
    /// Extracted from `replication_factor` option. Defaults to `1`.
    pub replication_factor: i16,

    /// Timeout in ms.
    ///
    /// Extracted from `timeout_ms` option. Defaults to `5_000`.
    pub timeout_ms: i32,
}

impl TryFrom<&WriteBufferCreationConfig> for TopicCreationConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &WriteBufferCreationConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            num_partitions: i32::try_from(cfg.n_sequencers.get())?,
            replication_factor: cfg
                .options
                .get("replication_factor")
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(1),
            timeout_ms: cfg
                .options
                .get("timeout_ms")
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(5_000),
        })
    }
}

/// Config for consumers.
#[derive(Debug, PartialEq, Eq)]
pub struct ConsumerConfig {
    /// Will wait for at least `min_batch_size` bytes of data
    ///
    /// Extracted from `consumer_max_wait_ms`. Defaults to `None` (rskafka default).
    pub max_wait_ms: Option<i32>,

    /// The maximum amount of data to fetch in a single batch
    ///
    /// Extracted from `consumer_min_batch_size`. Defaults to `None` (rskafka default).
    pub min_batch_size: Option<i32>,

    /// The maximum amount of time to wait for data before returning
    ///
    /// Extracted from `consumer_max_batch_size`. Defaults to `None` (rskafka default).
    pub max_batch_size: Option<i32>,
}

impl TryFrom<&BTreeMap<String, String>> for ConsumerConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Self {
            max_wait_ms: cfg
                .get("consumer_max_wait_ms")
                .map(|s| s.parse())
                .transpose()?,
            min_batch_size: cfg
                .get("consumer_min_batch_size")
                .map(|s| s.parse())
                .transpose()?,
            max_batch_size: cfg
                .get("consumer_max_batch_size")
                .map(|s| s.parse())
                .transpose()?,
        })
    }
}

/// Config for producers.
#[derive(Debug, PartialEq, Eq)]
pub struct ProducerConfig {
    /// Linger time.
    ///
    /// Extracted from `producer_linger_ms`. Defaults to `None` (rskafka default).
    pub linger: Option<Duration>,

    /// Maximum batch size in bytes.
    ///
    /// Extracted from `producer_max_batch_size`. Defaults to `100 * 1024`.
    pub max_batch_size: usize,
}

impl TryFrom<&BTreeMap<String, String>> for ProducerConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let linger_ms: Option<u64> = cfg
            .get("producer_linger_ms")
            .map(|s| s.parse())
            .transpose()?;

        Ok(Self {
            linger: linger_ms.map(Duration::from_millis),
            max_batch_size: cfg
                .get("producer_max_batch_size")
                .map(|s| s.parse())
                .transpose()?
                .unwrap_or(100 * 1024),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroU32};

    use super::*;

    #[test]
    fn test_client_config_default() {
        let actual = ClientConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ClientConfig {
            max_message_size: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_client_config_parse() {
        let actual = ClientConfig::try_from(&BTreeMap::from([
            (String::from("max_message_size"), String::from("1024")),
            (String::from("foo"), String::from("bar")),
        ]))
        .unwrap();
        let expected = ClientConfig {
            max_message_size: Some(1024),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_topic_creation_config_default() {
        let actual = TopicCreationConfig::try_from(&WriteBufferCreationConfig {
            n_sequencers: NonZeroU32::new(2).unwrap(),
            options: BTreeMap::default(),
        })
        .unwrap();
        let expected = TopicCreationConfig {
            num_partitions: 2,
            replication_factor: 1,
            timeout_ms: 5_000,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_topic_creation_config_parse() {
        let actual = TopicCreationConfig::try_from(&WriteBufferCreationConfig {
            n_sequencers: NonZeroU32::new(2).unwrap(),
            options: BTreeMap::from([
                (String::from("replication_factor"), String::from("3")),
                (String::from("timeout_ms"), String::from("100")),
                (String::from("foo"), String::from("bar")),
            ]),
        })
        .unwrap();
        let expected = TopicCreationConfig {
            num_partitions: 2,
            replication_factor: 3,
            timeout_ms: 100,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_consumer_config_default() {
        let actual = ConsumerConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ConsumerConfig {
            max_wait_ms: None,
            min_batch_size: None,
            max_batch_size: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_consumer_config_parse() {
        let actual = ConsumerConfig::try_from(&BTreeMap::from([
            (String::from("consumer_max_wait_ms"), String::from("11")),
            (String::from("consumer_min_batch_size"), String::from("22")),
            (String::from("consumer_max_batch_size"), String::from("33")),
            (String::from("foo"), String::from("bar")),
        ]))
        .unwrap();
        let expected = ConsumerConfig {
            max_wait_ms: Some(11),
            min_batch_size: Some(22),
            max_batch_size: Some(33),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_producer_config_default() {
        let actual = ProducerConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ProducerConfig {
            linger: None,
            max_batch_size: 100 * 1024,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_producer_config_parse() {
        let actual = ProducerConfig::try_from(&BTreeMap::from([
            (String::from("producer_linger_ms"), String::from("42")),
            (
                String::from("producer_max_batch_size"),
                String::from("1337"),
            ),
            (String::from("foo"), String::from("bar")),
        ]))
        .unwrap();
        let expected = ProducerConfig {
            linger: Some(Duration::from_millis(42)),
            max_batch_size: 1337,
        };
        assert_eq!(actual, expected);
    }
}
