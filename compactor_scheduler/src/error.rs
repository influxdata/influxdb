use thiserror::Error;

/// Scheduler error.
#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("Commit error: {0}")]
    Commit(#[from] crate::commit::Error),

    #[error("ThrottleError error: {0}")]
    ThrottleError(#[from] crate::ThrottleError),

    #[error("UniquePartitions error: {0}")]
    UniqueSource(#[from] crate::UniquePartitionsError),
}

/// Compactor Error classification.
/// What kind of error did we occur during compaction?
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorKind {
    /// Could not access the object store.
    ObjectStore,

    /// We ran out of memory (OOM).
    OutOfMemory,

    /// Partition took too long.
    Timeout,

    /// Unknown/unexpected error.
    ///
    /// This will likely mark the affected partition as "skipped" and the compactor will no longer touch it.
    Unknown(String),
}

impl ErrorKind {
    /// Return static name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ObjectStore => "object_store",
            Self::OutOfMemory => "out_of_memory",
            Self::Timeout => "timeout",
            Self::Unknown(_) => "unknown",
        }
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
