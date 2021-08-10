/// Simple representation of the state a database can be in.
#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseStateCode {
    /// Database is known but nothing is loaded.
    Known,

    /// Rules are loaded
    RulesLoaded,

    /// Catalog is loaded but data from sequencers / write buffers is not yet replayed.
    CatalogLoaded,

    /// Error loading rules
    RulesLoadError,

    /// Error loading catalog
    CatalogLoadError,

    /// Error during replay
    ReplayError,

    /// Fully initialized database.
    Initialized,
}

impl DatabaseStateCode {
    /// Returns a human readable description
    pub fn description(&self) -> &'static str {
        match self {
            DatabaseStateCode::Known => "Known",
            DatabaseStateCode::RulesLoaded => "RulesLoaded",
            DatabaseStateCode::CatalogLoaded => "CatalogLoaded",
            DatabaseStateCode::RulesLoadError => "RulesLoadError",
            DatabaseStateCode::CatalogLoadError => "CatalogLoadError",
            DatabaseStateCode::ReplayError => "ReplayError",
            DatabaseStateCode::Initialized => "Initialized",
        }
    }
}

impl std::fmt::Display for DatabaseStateCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.description().fmt(f)
    }
}
