use std::{collections::HashMap, num::NonZeroU32};

/// If the buffer is used for reading or writing.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum WriteBufferDirection {
    /// Writes into the buffer aka "producer".
    Write,

    /// Reads from the buffer aka "consumer".
    Read,
}

pub const DEFAULT_N_SEQUENCERS: u32 = 1;

/// Configures the use of a write buffer.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WriteBufferConnection {
    /// If the buffer is used for reading or writing.
    pub direction: WriteBufferDirection,

    /// Which type should be used (e.g. "kafka", "mock")
    pub type_: String,

    /// Connection string, depends on [`type_`](Self::type_).
    pub connection: String,

    /// Special configs to be applied when establishing the connection.
    ///
    /// This depends on [`type_`](Self::type_) and can configure aspects like timeouts.
    pub connection_config: HashMap<String, String>,

    /// Specifies if the sequencers (e.g. for Kafka in form of a topic) should be automatically created if they do not
    /// existing prior to reading or writing.
    pub creation_config: Option<WriteBufferCreationConfig>,
}

impl Default for WriteBufferConnection {
    fn default() -> Self {
        Self {
            direction: WriteBufferDirection::Read,
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
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct WriteBufferCreationConfig {
    /// Number of sequencers.
    ///
    /// How they are implemented depends on [type](WriteBufferConnection::type_), e.g. for Kafka this is mapped to the
    /// number of partitions.
    pub n_sequencers: NonZeroU32,

    /// Special configs to by applied when sequencers are created.
    ///
    /// This depends on [type](WriteBufferConnection::type_) and can setup parameters like retention policy.
    pub options: HashMap<String, String>,
}

impl Default for WriteBufferCreationConfig {
    fn default() -> Self {
        Self {
            n_sequencers: NonZeroU32::try_from(DEFAULT_N_SEQUENCERS).unwrap(),
            options: Default::default(),
        }
    }
}
