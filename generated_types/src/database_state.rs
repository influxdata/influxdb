use crate::influxdata::iox::management::v1 as management;
use data_types::database_state::DatabaseStateCode;

impl From<DatabaseStateCode> for management::database_status::DatabaseState {
    fn from(state_code: DatabaseStateCode) -> Self {
        match state_code {
            DatabaseStateCode::Known => Self::Known,
            DatabaseStateCode::ObjectStoreFound => Self::ObjectStoreFound,
            DatabaseStateCode::RulesLoaded => Self::RulesLoaded,
            DatabaseStateCode::CatalogLoaded => Self::CatalogLoaded,
            DatabaseStateCode::Initialized => Self::Initialized,
            DatabaseStateCode::RulesLoadError => Self::RulesLoadError,
            DatabaseStateCode::CatalogLoadError => Self::CatalogLoadError,
            DatabaseStateCode::ReplayError => Self::ReplayError,
        }
    }
}

impl From<Option<DatabaseStateCode>> for management::database_status::DatabaseState {
    fn from(state_code: Option<DatabaseStateCode>) -> Self {
        match state_code {
            Some(state_code) => state_code.into(),
            None => Self::Unspecified,
        }
    }
}
