use crate::{config::WriteBufferCreationConfig, core::WriteBufferError};
use std::{collections::BTreeMap, fmt::Display, str::FromStr, time::Duration};

/// Generic client config that is used for consumers, producers as well as admin operations (like
/// "create topic").
#[derive(Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// Maximum message size in bytes.
    ///
    /// extracted from `max_message_size`. Defaults to `None` (rskafka default).
    pub max_message_size: Option<usize>,

    /// Optional SOCKS5 proxy to use for connecting to the brokers.
    ///
    /// extracted from `socks5_proxy`. Defaults to `None`.
    pub socks5_proxy: Option<String>,
}

impl TryFrom<&BTreeMap<String, String>> for ClientConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        Ok(Self {
            // TODO: Revert this back to after we have proper prod config management.
            //       See https://github.com/influxdata/influxdb_iox/issues/3723
            //
            //       max_message_size: parse_key(cfg, "max_message_size")?,
            max_message_size: Some(parse_key(cfg, "max_message_size")?.unwrap_or(10485760)),
            socks5_proxy: parse_key(cfg, "socks5_proxy")?,
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
            num_partitions: i32::try_from(cfg.n_shards.get())
                .map_err(WriteBufferError::invalid_input)?,
            replication_factor: parse_key(&cfg.options, "replication_factor")?.unwrap_or(1),
            timeout_ms: parse_key(&cfg.options, "timeout_ms")?.unwrap_or(5_000),
        })
    }
}

/// Config for consumers.
#[derive(Debug, PartialEq, Eq, Clone)]
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
            max_wait_ms: parse_key(cfg, "consumer_max_wait_ms")?,
            min_batch_size: parse_key(cfg, "consumer_min_batch_size")?,
            // TODO: Revert this back to after we have proper prod config management.
            //       See https://github.com/influxdata/influxdb_iox/issues/3723
            //
            //       max_batch_size: parse_key(cfg, "consumer_max_batch_size")?,
            max_batch_size: Some(parse_key(cfg, "consumer_max_batch_size")?.unwrap_or(5242880)),
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
    /// Extracted from `producer_max_batch_size`. Defaults to `512 * 1024`.
    pub max_batch_size: usize,
}

impl TryFrom<&BTreeMap<String, String>> for ProducerConfig {
    type Error = WriteBufferError;

    fn try_from(cfg: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let linger_ms: Option<u64> = parse_key(cfg, "producer_linger_ms")?;

        Ok(Self {
            linger: linger_ms.map(Duration::from_millis),
            // TODO: Revert this back to after we have proper prod config management.
            //       See https://github.com/influxdata/influxdb_iox/issues/3723
            //
            //       max_batch_size: parse_key(cfg, "producer_max_batch_size")?.unwrap_or(512 * 1024),
            max_batch_size: parse_key(cfg, "producer_max_batch_size")?.unwrap_or(2621440),
        })
    }
}

fn parse_key<T>(cfg: &BTreeMap<String, String>, key: &str) -> Result<Option<T>, WriteBufferError>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(s) = cfg.get(key) {
        s.parse()
            .map(Some)
            .map_err(|e| format!("Cannot parse `{key}` from '{s}': {e}").into())
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::NonZeroU32};
    use test_helpers::assert_contains;

    use super::*;

    #[test]
    fn test_client_config_default() {
        let actual = ClientConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ClientConfig {
            max_message_size: Some(10485760),
            socks5_proxy: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_client_config_parse() {
        let actual = ClientConfig::try_from(&BTreeMap::from([
            (String::from("max_message_size"), String::from("1024")),
            (String::from("socks5_proxy"), String::from("my_proxy")),
            (String::from("foo"), String::from("bar")),
        ]))
        .unwrap();
        let expected = ClientConfig {
            max_message_size: Some(1024),
            socks5_proxy: Some(String::from("my_proxy")),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_client_config_error() {
        let err = ClientConfig::try_from(&BTreeMap::from([(
            String::from("max_message_size"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `max_message_size` from 'xyz': invalid digit found in string"
        );
    }

    #[test]
    fn test_topic_creation_config_default() {
        let actual = TopicCreationConfig::try_from(&WriteBufferCreationConfig {
            n_shards: NonZeroU32::new(2).unwrap(),
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
            n_shards: NonZeroU32::new(2).unwrap(),
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
    fn test_topic_creation_config_err() {
        let err = TopicCreationConfig::try_from(&WriteBufferCreationConfig {
            n_shards: NonZeroU32::new(2).unwrap(),
            options: BTreeMap::from([(String::from("replication_factor"), String::from("xyz"))]),
        })
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `replication_factor` from 'xyz': invalid digit found in string"
        );

        let err = TopicCreationConfig::try_from(&WriteBufferCreationConfig {
            n_shards: NonZeroU32::new(2).unwrap(),
            options: BTreeMap::from([(String::from("timeout_ms"), String::from("xyz"))]),
        })
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `timeout_ms` from 'xyz': invalid digit found in string"
        );
    }

    #[test]
    fn test_consumer_config_default() {
        let actual = ConsumerConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ConsumerConfig {
            max_wait_ms: None,
            min_batch_size: None,
            max_batch_size: Some(5242880),
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
    fn test_consumer_config_err() {
        let err = ConsumerConfig::try_from(&BTreeMap::from([(
            String::from("consumer_max_wait_ms"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `consumer_max_wait_ms` from 'xyz': invalid digit found in string"
        );

        let err = ConsumerConfig::try_from(&BTreeMap::from([(
            String::from("consumer_min_batch_size"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `consumer_min_batch_size` from 'xyz': invalid digit found in string"
        );

        let err = ConsumerConfig::try_from(&BTreeMap::from([(
            String::from("consumer_max_batch_size"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `consumer_max_batch_size` from 'xyz': invalid digit found in string"
        );
    }

    #[test]
    fn test_producer_config_default() {
        let actual = ProducerConfig::try_from(&BTreeMap::default()).unwrap();
        let expected = ProducerConfig {
            linger: None,
            max_batch_size: 2621440,
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

    #[test]
    fn test_producer_config_err() {
        let err = ProducerConfig::try_from(&BTreeMap::from([(
            String::from("producer_linger_ms"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `producer_linger_ms` from 'xyz': invalid digit found in string"
        );

        let err = ProducerConfig::try_from(&BTreeMap::from([(
            String::from("producer_max_batch_size"),
            String::from("xyz"),
        )]))
        .unwrap_err();
        assert_contains!(
            err.to_string(),
            "Cannot parse `producer_max_batch_size` from 'xyz': invalid digit found in string"
        );
    }
}
