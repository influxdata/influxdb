//! Trigger operations (record_ids 12-15).

use std::sync::Arc;
use std::time::Duration;

use super::impl_bitcode_encoding;
use super::types::{
    ErrorBehavior as WireErrorBehavior, NodeSpec as WireNodeSpec,
    TriggerSettings as WireTriggerSettings, TriggerSpec as WireTriggerSpec,
};
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::NodeSpec as SchemaNodeSpec;
use crate::catalog::versions::v3::schema::trigger::{
    ErrorBehavior, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
};
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::{DbId, TriggerId};

/// Create a processing engine trigger.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
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

impl CatalogRecord for CreateTrigger {
    const ID: RecordId = record_ids::CREATE_TRIGGER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "CreateTrigger";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;

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

        Arc::make_mut(&mut db)
            .processing_engine_triggers
            .insert(trigger_id, trigger_def)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerCreated {
            db_id: DbId::new(self.database_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<CreateTrigger>()
}

/// Delete a trigger.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
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

impl CatalogRecord for DeleteTrigger {
    const ID: RecordId = record_ids::DELETE_TRIGGER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DeleteTrigger";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.database_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;

        Arc::make_mut(&mut db)
            .processing_engine_triggers
            .remove(&trigger_id);
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerDeleted {
            db_id: DbId::new(self.database_id),
            trigger_id: TriggerId::new(self.trigger_id),
            force: self.force,
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DeleteTrigger>()
}

/// Enable a disabled trigger.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct EnableTrigger {
    /// Database catalog ID.
    pub db_id: u32,
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
}

impl CatalogRecord for EnableTrigger {
    const ID: RecordId = record_ids::ENABLE_TRIGGER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "EnableTrigger";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;
        let mut trigger = db.processing_engine_triggers.require_by_id(&trigger_id)?;

        Arc::make_mut(&mut trigger).disabled = false;

        let d = Arc::make_mut(&mut db);
        d.processing_engine_triggers.update(trigger_id, trigger)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerEnabled {
            db_id: DbId::new(self.db_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<EnableTrigger>()
}

/// Disable a trigger.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct DisableTrigger {
    /// Database catalog ID.
    pub db_id: u32,
    /// Trigger catalog ID.
    pub trigger_id: u32,
    /// Trigger name.
    pub trigger_name: String,
}

impl CatalogRecord for DisableTrigger {
    const ID: RecordId = record_ids::DISABLE_TRIGGER;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "DisableTrigger";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let db_id = DbId::new(self.db_id);
        let trigger_id = TriggerId::new(self.trigger_id);

        let mut db = catalog.databases.require_by_id(&db_id)?;
        let mut trigger = db.processing_engine_triggers.require_by_id(&trigger_id)?;

        Arc::make_mut(&mut trigger).disabled = true;

        let d = Arc::make_mut(&mut db);
        d.processing_engine_triggers.update(trigger_id, trigger)?;
        catalog.databases.update(db_id, db).map_err(Into::into)
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::TriggerDisabled {
            db_id: DbId::new(self.db_id),
            trigger_id: TriggerId::new(self.trigger_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<DisableTrigger>()
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

impl_bitcode_encoding!(CreateTrigger, DeleteTrigger, EnableTrigger, DisableTrigger);

#[cfg(test)]
mod tests;
