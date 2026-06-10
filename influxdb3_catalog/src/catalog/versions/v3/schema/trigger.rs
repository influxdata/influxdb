use std::{ops::Deref, str::FromStr, sync::Arc, time::Duration};

use hashbrown::HashMap;
use humantime::{format_duration, parse_duration};
use influxdb3_id::{DbId, TriggerId};
use serde::{Deserialize, Serialize};

use crate::{CatalogError, Result, resource::CatalogResource};

use super::node::NodeSpec;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    WalRows,
    Schedule,
    Request,
}

impl std::fmt::Display for PluginType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ValidPluginFilename<'a>(&'a str);

impl<'a> ValidPluginFilename<'a> {
    pub fn from_validated_name(name: &'a str) -> Self {
        Self(name)
    }
}

impl Deref for ValidPluginFilename<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerDefinition {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub plugin_filename: String,
    pub database_name: Arc<str>,
    /// Specify the node(s) which operate this trigger
    ///
    /// # Implementation Note
    ///
    /// This uses a default for trigger definitions from core which do not specify a `node_spec`.
    #[serde(default)]
    pub node_spec: NodeSpec,
    pub trigger: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

impl CatalogResource for TriggerDefinition {
    type Identifier = TriggerId;

    const CATEGORY: &'static str = "processing_engine_triggers";

    fn id(&self) -> Self::Identifier {
        self.trigger_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.trigger_name)
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub struct TriggerSettings {
    pub run_async: bool,
    pub error_behavior: ErrorBehavior,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ErrorBehavior {
    #[default]
    /// Log the error to the service output and system.processing_engine_logs table.
    Log,
    /// Rerun the trigger on error.
    Retry,
    /// Turn off the plugin until it is manually re-enabled.
    Disable,
}

impl From<crate::log::ErrorBehavior> for ErrorBehavior {
    fn from(value: crate::log::ErrorBehavior) -> Self {
        match value {
            crate::log::ErrorBehavior::Log => Self::Log,
            crate::log::ErrorBehavior::Retry => Self::Retry,
            crate::log::ErrorBehavior::Disable => Self::Disable,
        }
    }
}

impl From<ErrorBehavior> for crate::log::ErrorBehavior {
    fn from(value: ErrorBehavior) -> Self {
        match value {
            ErrorBehavior::Log => Self::Log,
            ErrorBehavior::Retry => Self::Retry,
            ErrorBehavior::Disable => Self::Disable,
        }
    }
}

impl From<crate::log::TriggerSettings> for TriggerSettings {
    fn from(value: crate::log::TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<TriggerSettings> for crate::log::TriggerSettings {
    fn from(value: TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<PluginType> for crate::log::PluginType {
    fn from(value: PluginType) -> Self {
        match value {
            PluginType::WalRows => Self::WalRows,
            PluginType::Schedule => Self::Schedule,
            PluginType::Request => Self::Request,
        }
    }
}

impl From<crate::log::PluginType> for PluginType {
    fn from(value: crate::log::PluginType) -> Self {
        match value {
            crate::log::PluginType::WalRows => Self::WalRows,
            crate::log::PluginType::Schedule => Self::Schedule,
            crate::log::PluginType::Request => Self::Request,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct TriggerIdentifier {
    pub db_id: DbId,
    pub db_name: Arc<str>,
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TriggerSpecificationDefinition {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
    Schedule { schedule: String },
    RequestPath { path: String },
    Every { duration: Duration },
}

impl From<TriggerSpecificationDefinition> for crate::log::TriggerSpecificationDefinition {
    fn from(value: TriggerSpecificationDefinition) -> Self {
        match value {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                Self::SingleTableWalWrite { table_name }
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => Self::AllTablesWalWrite,
            TriggerSpecificationDefinition::Schedule { schedule } => Self::Schedule { schedule },
            TriggerSpecificationDefinition::RequestPath { path } => Self::RequestPath { path },
            TriggerSpecificationDefinition::Every { duration } => Self::Every { duration },
        }
    }
}

impl From<&TriggerSpecificationDefinition> for crate::log::TriggerSpecificationDefinition {
    fn from(value: &TriggerSpecificationDefinition) -> Self {
        match value {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                Self::SingleTableWalWrite {
                    table_name: table_name.clone(),
                }
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => Self::AllTablesWalWrite,
            TriggerSpecificationDefinition::Schedule { schedule } => Self::Schedule {
                schedule: schedule.clone(),
            },
            TriggerSpecificationDefinition::RequestPath { path } => {
                Self::RequestPath { path: path.clone() }
            }
            TriggerSpecificationDefinition::Every { duration } => Self::Every {
                duration: *duration,
            },
        }
    }
}

impl From<crate::log::TriggerSpecificationDefinition> for TriggerSpecificationDefinition {
    fn from(value: crate::log::TriggerSpecificationDefinition) -> Self {
        match value {
            crate::log::TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                Self::SingleTableWalWrite { table_name }
            }
            crate::log::TriggerSpecificationDefinition::AllTablesWalWrite => {
                Self::AllTablesWalWrite
            }
            crate::log::TriggerSpecificationDefinition::Schedule { schedule } => {
                Self::Schedule { schedule }
            }
            crate::log::TriggerSpecificationDefinition::RequestPath { path } => {
                Self::RequestPath { path }
            }
            crate::log::TriggerSpecificationDefinition::Every { duration } => {
                Self::Every { duration }
            }
        }
    }
}

impl From<TriggerDefinition> for crate::log::TriggerDefinition {
    fn from(value: TriggerDefinition) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            plugin_filename: value.plugin_filename,
            database_name: value.database_name,
            node_spec: (&value.node_spec).into(),
            trigger: value.trigger.into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments,
            disabled: value.disabled,
        }
    }
}

impl From<&TriggerDefinition> for crate::log::TriggerDefinition {
    fn from(value: &TriggerDefinition) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: Arc::clone(&value.trigger_name),
            plugin_filename: value.plugin_filename.clone(),
            database_name: Arc::clone(&value.database_name),
            node_spec: (&value.node_spec).into(),
            trigger: (&value.trigger).into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments.clone(),
            disabled: value.disabled,
        }
    }
}

impl<'a> From<ValidPluginFilename<'a>> for crate::log::ValidPluginFilename<'a> {
    fn from(value: ValidPluginFilename<'a>) -> Self {
        crate::log::ValidPluginFilename::from_validated_name(value.0)
    }
}

impl TriggerSpecificationDefinition {
    pub fn from_string_rep(spec_str: &str) -> Result<TriggerSpecificationDefinition> {
        let spec_str = spec_str.trim();
        match spec_str {
            s if s.starts_with("table:") => {
                let table_name = s.trim_start_matches("table:").trim();
                if table_name.is_empty() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("table name is empty".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::SingleTableWalWrite {
                    table_name: table_name.to_string(),
                })
            }
            "all_tables" => Ok(TriggerSpecificationDefinition::AllTablesWalWrite),
            s if s.starts_with("cron:") => {
                let cron_schedule = s.trim_start_matches("cron:").trim();
                if cron_schedule.is_empty()
                    || cron::Schedule::from_str(cron_schedule).is_err()
                {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::Schedule {
                    schedule: cron_schedule.to_string(),
                })
            }
            s if s.starts_with("every:") => {
                let duration_str = s.trim_start_matches("every:").trim();
                let Ok(duration) = parse_duration(duration_str) else {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("couldn't parse to duration".to_string()),
                    });
                };
                if duration > parse_duration("1 year").unwrap() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: Some("don't support every schedules of over 1 year".to_string()),
                    });
                }
                Ok(TriggerSpecificationDefinition::Every { duration })
            }
            s if s.starts_with("request:") => {
                let path = s.trim_start_matches("request:").trim();
                if path.is_empty() {
                    return Err(CatalogError::TriggerSpecificationParseError {
                        trigger_spec: spec_str.to_string(),
                        context: None,
                    });
                }
                Ok(TriggerSpecificationDefinition::RequestPath {
                    path: path.to_string(),
                })
            }
            _ => Err(CatalogError::TriggerSpecificationParseError {
                trigger_spec: spec_str.to_string(),
                context: Some("expect one of the following forms: 'table:<TABLE_NAME>', 'all_tables', 'cron:<CRON_EXPRESSION>', 'every:<DURATION>', or 'request:<PATH>'".to_string()),
            }),
        }
    }

    pub fn string_rep(&self) -> String {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                format!("table:{table_name}")
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => "all_tables".to_string(),
            TriggerSpecificationDefinition::Schedule { schedule } => {
                format!("cron:{schedule}")
            }
            TriggerSpecificationDefinition::Every { duration } => {
                format!("every:{}", format_duration(*duration))
            }
            TriggerSpecificationDefinition::RequestPath { path } => {
                format!("request:{path}")
            }
        }
    }

    pub fn plugin_type(&self) -> PluginType {
        match self {
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
            | TriggerSpecificationDefinition::AllTablesWalWrite => PluginType::WalRows,
            TriggerSpecificationDefinition::Schedule { .. }
            | TriggerSpecificationDefinition::Every { .. } => PluginType::Schedule,
            TriggerSpecificationDefinition::RequestPath { .. } => PluginType::Request,
        }
    }
}

impl FromStr for TriggerSpecificationDefinition {
    type Err = CatalogError;

    fn from_str(s: &str) -> Result<TriggerSpecificationDefinition> {
        Self::from_string_rep(s)
    }
}

impl std::fmt::Display for TriggerSpecificationDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string_rep())
    }
}
