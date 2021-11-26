use std::{collections::BTreeMap, num::NonZeroU32};

pub const DEFAULT_N_SEQUENCERS: u32 = 1;

/// Configures the use of a write buffer.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct WriteBufferConnection {
    /// Which type should be used (e.g. "kafka", "mock")
    pub type_: String,

    /// Connection string, depends on [`type_`](Self::type_).
    pub connection: String,

    /// Special configs to be applied when establishing the connection.
    ///
    /// This depends on [`type_`](Self::type_) and can configure aspects like timeouts.
    ///
    /// Note: This config should be a [`BTreeMap`] to ensure that a stable hash.
    pub connection_config: BTreeMap<String, String>,

    /// Specifies if the sequencers (e.g. for Kafka in form of a topic) should be automatically created if they do not
    /// existing prior to reading or writing.
    pub creation_config: Option<WriteBufferCreationConfig>,
}

impl Default for WriteBufferConnection {
    fn default() -> Self {
        Self {
            type_: "unspecified".to_string(),
            connection: Default::default(),
            connection_config: Default::default(),
            creation_config: Default::default(),
        }
    }
}

/// Configs sequencer auto-creation for write buffers.
///
/// What that means depends on the used write buffer, e.g. for Kafka this will create a new topic w/
/// [`n_sequencers`](Self::n_sequencers) partitions.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct WriteBufferCreationConfig {
    /// Number of sequencers.
    ///
    /// How they are implemented depends on [type](WriteBufferConnection::type_), e.g. for Kafka this is mapped to the
    /// number of partitions.
    pub n_sequencers: NonZeroU32,

    /// Special configs to by applied when sequencers are created.
    ///
    /// This depends on [type](WriteBufferConnection::type_) and can setup parameters like retention policy.
    ///
    /// Note: This config should be a [`BTreeMap`] to ensure that a stable hash.
    pub options: BTreeMap<String, String>,
}

impl Default for WriteBufferCreationConfig {
    fn default() -> Self {
        Self {
            n_sequencers: NonZeroU32::try_from(DEFAULT_N_SEQUENCERS).unwrap(),
            options: Default::default(),
        }
    }
}
