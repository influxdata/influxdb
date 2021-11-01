use std::fmt::Display;

use generated_types::influxdata::iox::catalog::v1 as proto;
use uuid::Uuid;

/// Type of catalog file.
#[derive(Debug, Clone, Copy)]
pub enum FileType {
    /// Ordinary transaction with delta encoding.
    Transaction,

    /// Checkpoints with full state.
    Checkpoint,
}

impl FileType {
    /// Get encoding that should be used for this file.
    pub fn encoding(&self) -> proto::transaction::Encoding {
        match self {
            Self::Transaction => proto::transaction::Encoding::Delta,
            Self::Checkpoint => proto::transaction::Encoding::Full,
        }
    }
}

/// Key to address transactions.
#[derive(Clone, Debug, Copy)]
pub struct TransactionKey {
    pub revision_counter: u64,
    pub uuid: Uuid,
}

impl Display for TransactionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.revision_counter, self.uuid)
    }
}
