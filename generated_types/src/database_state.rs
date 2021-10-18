pub use crate::influxdata::iox::management::v1::database_status::DatabaseState;
use std::fmt::Formatter;

impl DatabaseState {
    /// Returns a human readable description
    pub fn description(&self) -> &'static str {
        match self {
            DatabaseState::Known => "Known",
            DatabaseState::RulesLoaded => "RulesLoaded",
            DatabaseState::CatalogLoaded => "CatalogLoaded",
            DatabaseState::RulesLoadError => "RulesLoadError",
            DatabaseState::CatalogLoadError => "CatalogLoadError",
            DatabaseState::ReplayError => "ReplayError",
            DatabaseState::Initialized => "Initialized",
            DatabaseState::DatabaseObjectStoreFound => "DatabaseObjectStoreFound",
            DatabaseState::DatabaseObjectStoreLookupError => "DatabaseObjectStoreLookupError",
            DatabaseState::NoActiveDatabase => "NoActiveDatabase",
            DatabaseState::OwnerInfoLoaded => "OwnerInfoLoaded",
            DatabaseState::OwnerInfoLoadError => "OwnerInfoLoadError",
            DatabaseState::Unspecified => "Unspecified",
        }
    }
}

impl std::fmt::Display for DatabaseState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.description())
    }
}
