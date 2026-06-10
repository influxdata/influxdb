#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum NodeMode {
    Core,
    Query,
    Ingest,
    Compact,
    Process,
    All,
}
impl NodeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeMode::Core => "core",
            NodeMode::Query => "query",
            NodeMode::Ingest => "ingest",
            NodeMode::Compact => "compact",
            NodeMode::Process => "process",
            NodeMode::All => "all",
        }
    }
}

impl std::fmt::Display for NodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy)]
pub enum EnterpriseCatalogError {
    #[error("Node doesn't have proper mode, expect {expected}")]
    InvalidNodeMode { expected: NodeMode },

    #[error(
        "too many compacted generations requested, maximum allowed: 254, requested: {requested}"
    )]
    TooManyCompactedGenerations { requested: usize },

    #[error("gen1 duration is not configured in the catalog")]
    MissingGen1Duration,

    #[error(
        "gen2 duration must be an even multiple of gen1 duration, \
        provided gen2 duration: {gen2_duration_secs} seconds, \
        existing gen1 duration: {gen1_duration_secs} seconds"
    )]
    InvalidGen2Duration {
        gen2_duration_secs: u64,
        gen1_duration_secs: u64,
    },

    #[error("each subsequent generation must be an even multiple of its predecessor")]
    MisalignedGenerations,
}
