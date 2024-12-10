use influxdb3_wal::{PluginDefinition, TriggerSpecificationDefinition};
use serde::de::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ProcessingEnginePlugin {
    pub plugin_name: String,
    pub code: String,
    pub function_name: String,
    pub plugin_type: PluginType,
}

impl From<&PluginDefinition> for ProcessingEnginePlugin {
    fn from(plugin_definition: &PluginDefinition) -> Self {
        ProcessingEnginePlugin {
            plugin_name: plugin_definition.plugin_name.to_string(),
            code: plugin_definition.code.to_string(),
            function_name: plugin_definition.function_name.to_string(),
            plugin_type: serde_json::from_str(&plugin_definition.plugin_type).unwrap(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    WalRows,
}

impl TryFrom<String> for PluginType {
    type Error = serde_json::Error; // or your own error type

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_str() {
            "wal_rows" => Ok(PluginType::WalRows),
            _ => Err(serde_json::Error::custom("unexpected plugin type")),
        }
    }
}

impl ProcessingEnginePlugin {
    pub fn new(
        plugin_name: String,
        code: String,
        function_name: String,
        plugin_type: PluginType,
    ) -> Self {
        Self {
            plugin_name,
            code,
            function_name,
            plugin_type,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ProcessingEngineTrigger {
    pub trigger_name: String,
    pub plugin: ProcessingEnginePlugin,
    pub trigger: TriggerSpecification,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerSpecification {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
}

impl From<&TriggerSpecificationDefinition> for TriggerSpecification {
    fn from(trigger_specification: &TriggerSpecificationDefinition) -> Self {
        match trigger_specification {
            TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                TriggerSpecification::SingleTableWalWrite {
                    table_name: table_name.to_string(),
                }
            }
            TriggerSpecificationDefinition::AllTablesWalWrite => {
                TriggerSpecification::AllTablesWalWrite
            }
        }
    }
}

impl From<TriggerSpecification> for TriggerSpecificationDefinition {
    fn from(trigger_specification: TriggerSpecification) -> Self {
        match trigger_specification {
            TriggerSpecification::SingleTableWalWrite { table_name } => {
                TriggerSpecificationDefinition::SingleTableWalWrite { table_name }
            }
            TriggerSpecification::AllTablesWalWrite => {
                TriggerSpecificationDefinition::AllTablesWalWrite
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Deserialize)]
pub enum TriggerType {
    OnRead,
}
