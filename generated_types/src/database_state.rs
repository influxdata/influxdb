use crate::influxdata::iox::management::v1 as management;
use data_types::database_state::DatabaseStateCode;

impl From<DatabaseStateCode> for management::database_status::DatabaseState {
    fn from(state_code: DatabaseStateCode) -> Self {
        match state_code {
            DatabaseStateCode::Known => Self::Known,
            DatabaseStateCode::RulesLoaded => Self::RulesLoaded,
            DatabaseStateCode::Replay => Self::Replay,
            DatabaseStateCode::Initialized => Self::Initialized,
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
