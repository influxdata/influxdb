//! Trigger operations (record_ids 12-15).

use std::sync::Arc;
use std::time::Duration;

use super::types::{
    ErrorBehavior as WireErrorBehavior, NodeSpec as WireNodeSpec,
    TriggerSettings as WireTriggerSettings, TriggerSpec as WireTriggerSpec,
};
use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::NodeSpec as SchemaNodeSpec;
use crate::catalog::versions::v3::schema::trigger::{
    ErrorBehavior, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
};
use crate::format::apply::ApplyError;
use crate::format::{RecordApply, record_ids};
use influxdb3_id::{DbId, TriggerId};

/// Create a processing engine trigger.
#[catalog_record(id = record_ids::CREATE_TRIGGER, shape = 0x1b3f7476)]
pub struct CreateTrigger {
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
    /// Plugin filename.
    pub plugin_filename: String,
    /// Database catalog ID.
    pub database_id: u32,
    /// Node specification for trigger execution.
    pub node_spec: WireNodeSpec,
    /// Trigger specification.
    pub trigger: WireTriggerSpec,
    /// Trigger settings.
    pub trigger_settings: WireTriggerSettings,
    /// Optional trigger arguments as key-value pairs.
    pub trigger_arguments: Option<Vec<(String, String)>>,
    /// Whether the trigger is disabled.
    pub disabled: bool,
}

impl RecordApply for CreateTrigger {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            let trigger_def = TriggerDefinition {
                trigger_id,
                trigger_name: Arc::from(self.trigger_name.as_str()),
                plugin_filename: self.plugin_filename.clone(),
                database_name: db.name(),
                node_spec: SchemaNodeSpec::from(&self.node_spec),
                trigger: TriggerSpecificationDefinition::from(&self.trigger),
                trigger_settings: TriggerSettings::from(self.trigger_settings),
                trigger_arguments: self
                    .trigger_arguments
                    .as_ref()
                    .map(|args| args.iter().cloned().collect()),
                disabled: self.disabled,
            };

            db.processing_engine_triggers
                .insert(trigger_id, trigger_def)?;
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerCreated {
            db_id: DbId::new(self.database_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

/// Delete a trigger.
#[catalog_record(id = record_ids::DELETE_TRIGGER, shape = 0x00ff5210)]
pub struct DeleteTrigger {
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
    /// Database catalog ID.
    pub database_id: u32,
    /// Whether to force deletion.
    pub force: bool,
}

impl RecordApply for DeleteTrigger {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            db.processing_engine_triggers.remove(&trigger_id);
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerDeleted {
            db_id: DbId::new(self.database_id),
            trigger_id: TriggerId::new(self.trigger_id),
            force: self.force,
        }
    }
}

/// Enable a disabled trigger.
#[catalog_record(id = record_ids::ENABLE_TRIGGER, shape = 0x96117d65)]
pub struct EnableTrigger {
    /// Database catalog ID.
    pub db_id: u32,
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
}

impl RecordApply for EnableTrigger {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            db.processing_engine_triggers
                .modify_by_id_in_place(&trigger_id, |trigger| {
                    trigger.disabled = false;
                    Ok(())
                })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerEnabled {
            db_id: DbId::new(self.db_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

/// Disable a trigger.
#[catalog_record(id = record_ids::DISABLE_TRIGGER, shape = 0x96117d65)]
pub struct DisableTrigger {
    /// Database catalog ID.
    pub db_id: u32,
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
}

impl RecordApply for DisableTrigger {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        catalog.databases.modify_by_id_in_place(&db_id, |db| {
            db.processing_engine_triggers
                .modify_by_id_in_place(&trigger_id, |trigger| {
                    trigger.disabled = true;
                    Ok(())
                })
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerDisabled {
            db_id: DbId::new(self.db_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire → schema conversions
// ---------------------------------------------------------------------------

impl From<&WireTriggerSpec> for TriggerSpecificationDefinition {
    fn from(value: &WireTriggerSpec) -> Self {
        match value {
            WireTriggerSpec::SingleTableWalWrite { table_name } => Self::SingleTableWalWrite {
                table_name: table_name.clone(),
            },
            WireTriggerSpec::AllTablesWalWrite => Self::AllTablesWalWrite,
            WireTriggerSpec::Schedule { schedule } => Self::Schedule {
                schedule: schedule.clone(),
            },
            WireTriggerSpec::RequestPath { path } => Self::RequestPath { path: path.clone() },
            WireTriggerSpec::Every { duration_ns } => Self::Every {
                duration: Duration::from_nanos(*duration_ns),
            },
        }
    }
}

impl From<WireTriggerSettings> for TriggerSettings {
    fn from(value: WireTriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: ErrorBehavior::from(value.error_behavior),
        }
    }
}

impl From<WireErrorBehavior> for ErrorBehavior {
    fn from(value: WireErrorBehavior) -> Self {
        match value {
            WireErrorBehavior::Log => Self::Log,
            WireErrorBehavior::Retry => Self::Retry,
            WireErrorBehavior::Disable => Self::Disable,
        }
    }
}

// ---------------------------------------------------------------------------
// Schema → wire conversions
// ---------------------------------------------------------------------------

impl From<&TriggerSpecificationDefinition> for WireTriggerSpec {
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
                duration_ns: u64::try_from(duration.as_nanos())
                    .expect("duration exceeds u64 range"),
            },
        }
    }
}

impl From<&TriggerSettings> for WireTriggerSettings {
    fn from(value: &TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: (&value.error_behavior).into(),
        }
    }
}

impl From<&ErrorBehavior> for WireErrorBehavior {
    fn from(value: &ErrorBehavior) -> Self {
        match value {
            ErrorBehavior::Log => Self::Log,
            ErrorBehavior::Retry => Self::Retry,
            ErrorBehavior::Disable => Self::Disable,
        }
    }
}

#[cfg(test)]
mod tests;
