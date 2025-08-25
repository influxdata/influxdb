//! Module to provide lossless conversions from v3 to v4

use crate::log::versions::v3;
use crate::log::versions::v4;
use std::time::Duration;

impl From<v3::RetentionPeriod> for v4::RetentionPeriod {
    fn from(value: v3::RetentionPeriod) -> Self {
        use v3::RetentionPeriod::*;
        match value {
            Indefinite => Self::Indefinite,
            Duration(d) => Self::Duration(d),
        }
    }
}

impl From<v3::RetentionPeriod> for Option<Duration> {
    fn from(value: v3::RetentionPeriod) -> Self {
        match value {
            v3::RetentionPeriod::Indefinite => Self::None,
            v3::RetentionPeriod::Duration(d) => Self::Some(d),
        }
    }
}

impl From<v3::LastCacheSize> for v4::LastCacheSize {
    fn from(value: v3::LastCacheSize) -> Self {
        Self(value.0)
    }
}

impl From<v3::LastCacheTtl> for v4::LastCacheTtl {
    fn from(value: v3::LastCacheTtl) -> Self {
        Self(value.0)
    }
}

impl From<v3::MaxCardinality> for v4::MaxCardinality {
    fn from(value: v3::MaxCardinality) -> Self {
        Self::from_usize_unchecked(value.into())
    }
}

impl From<v3::MaxAge> for v4::MaxAge {
    fn from(value: v3::MaxAge) -> Self {
        // Convert through secs since the inner field is private
        Self::from_secs(value.as_secs())
    }
}

impl From<v3::TriggerDefinition> for v4::TriggerDefinition {
    fn from(value: v3::TriggerDefinition) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            plugin_filename: value.plugin_filename,
            database_name: value.database_name,
            node_id: value.node_id,
            trigger: value.trigger.into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments,
            disabled: value.disabled,
        }
    }
}

impl From<v3::TriggerSpecificationDefinition> for v4::TriggerSpecificationDefinition {
    fn from(value: v3::TriggerSpecificationDefinition) -> Self {
        use v3::TriggerSpecificationDefinition::*;
        match value {
            SingleTableWalWrite { table_name } => Self::SingleTableWalWrite { table_name },
            AllTablesWalWrite => Self::AllTablesWalWrite,
            Schedule { schedule } => Self::Schedule { schedule },
            RequestPath { path } => Self::RequestPath { path },
            Every { duration } => Self::Every { duration },
        }
    }
}

impl From<v3::TriggerSettings> for v4::TriggerSettings {
    fn from(value: v3::TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<v3::ErrorBehavior> for v4::ErrorBehavior {
    fn from(value: v3::ErrorBehavior) -> Self {
        use v3::ErrorBehavior::*;
        match value {
            Log => Self::Log,
            Retry => Self::Retry,
            Disable => Self::Disable,
        }
    }
}

impl From<v3::NodeMode> for v4::NodeMode {
    fn from(value: v3::NodeMode) -> Self {
        use v3::NodeMode::*;
        match value {
            Core => Self::Core,
        }
    }
}
