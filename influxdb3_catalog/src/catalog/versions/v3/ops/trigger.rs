//! Trigger operations: Create, Delete, Enable, Disable.

use std::sync::Arc;

use hashbrown::HashMap;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::NodeSpec;
use crate::catalog::versions::v3::schema::trigger::{
    TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
};
use crate::format::RecordBatch;
use crate::format::records::{CreateTrigger, DeleteTrigger, DisableTrigger, EnableTrigger};
use crate::resource::CatalogResource;
use influxdb3_id::TriggerId;

// ---------------------------------------------------------------------------
// CreateTrigger
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct CreateTriggerArgs {
    pub db_name: String,
    pub trigger_name: String,
    pub plugin_filename: String,
    pub node_spec: NodeSpec,
    pub trigger_specification: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

pub(crate) struct CreateTriggerOp {
    db_name: String,
    trigger_id: TriggerId,
}

impl CatalogOp for CreateTriggerOp {
    type Input = CreateTriggerArgs;
    type Output = Arc<TriggerDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;

        if db
            .processing_engine_triggers
            .contains_name(&args.trigger_name)
        {
            return Err(CatalogError::AlreadyExists);
        }

        let trigger_id = db.processing_engine_triggers.next_id();

        let trigger_arguments: Option<Vec<(String, String)>> = args
            .trigger_arguments
            .as_ref()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

        records.push(&CreateTrigger {
            trigger_id: trigger_id.get(),
            trigger_name: args.trigger_name.clone(),
            plugin_filename: args.plugin_filename.clone(),
            database_id: db.id().get(),
            node_spec: (&args.node_spec).into(),
            trigger: (&args.trigger_specification).into(),
            trigger_settings: (&args.trigger_settings).into(),
            trigger_arguments,
            disabled: args.disabled,
        });

        Ok(Self {
            db_name: args.db_name.clone(),
            trigger_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_name(&self.db_name)
            .expect("database should exist after apply");
        db.processing_engine_triggers
            .get_by_id(&self.trigger_id)
            .expect("trigger should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// DeleteTrigger
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeleteTriggerArgs {
    pub db_name: String,
    pub trigger_name: String,
    pub force: bool,
}

pub(crate) struct DeleteTriggerOp {
    trigger: Arc<TriggerDefinition>,
}

impl CatalogOp for DeleteTriggerOp {
    type Input = DeleteTriggerArgs;
    type Output = Arc<TriggerDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;

        let trigger = db
            .processing_engine_triggers
            .get_by_name(&args.trigger_name)
            .ok_or_else(|| CatalogError::NotFound(args.trigger_name.clone()))?;

        if !trigger.disabled && !args.force {
            return Err(CatalogError::ProcessingEngineTriggerRunning {
                trigger_name: trigger.trigger_name.to_string(),
            });
        }

        records.push(&DeleteTrigger {
            trigger_id: trigger.trigger_id.get(),
            trigger_name: args.trigger_name.clone(),
            database_id: db.id().get(),
            force: args.force,
        });

        Ok(Self { trigger })
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {
        Arc::clone(&self.trigger)
    }
}

// ---------------------------------------------------------------------------
// EnableTrigger
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct EnableTriggerArgs {
    pub db_name: String,
    pub trigger_name: String,
}

pub(crate) struct EnableTriggerOp {
    db_name: String,
    trigger_id: TriggerId,
}

impl CatalogOp for EnableTriggerOp {
    type Input = EnableTriggerArgs;
    type Output = Arc<TriggerDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;

        let trigger = db
            .processing_engine_triggers
            .get_by_name(&args.trigger_name)
            .ok_or_else(|| CatalogError::NotFound(args.trigger_name.clone()))?;

        if !trigger.disabled {
            return Err(CatalogError::TriggerAlreadyEnabled);
        }

        records.push(&EnableTrigger {
            db_id: db.id().get(),
            trigger_id: trigger.trigger_id.get(),
            trigger_name: args.trigger_name.clone(),
        });

        Ok(Self {
            db_name: args.db_name.clone(),
            trigger_id: trigger.trigger_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_name(&self.db_name)
            .expect("database should exist after apply");
        db.processing_engine_triggers
            .get_by_id(&self.trigger_id)
            .expect("trigger should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// DisableTrigger
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DisableTriggerArgs {
    pub db_name: String,
    pub trigger_name: String,
}

pub(crate) struct DisableTriggerOp {
    db_name: String,
    trigger_id: TriggerId,
}

impl CatalogOp for DisableTriggerOp {
    type Input = DisableTriggerArgs;
    type Output = Arc<TriggerDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let db = super::active_database(catalog, &args.db_name)?;

        let trigger = db
            .processing_engine_triggers
            .get_by_name(&args.trigger_name)
            .ok_or_else(|| CatalogError::NotFound(args.trigger_name.clone()))?;

        if trigger.disabled {
            return Err(CatalogError::TriggerAlreadyDisabled);
        }

        records.push(&DisableTrigger {
            db_id: db.id().get(),
            trigger_id: trigger.trigger_id.get(),
            trigger_name: args.trigger_name.clone(),
        });

        Ok(Self {
            db_name: args.db_name.clone(),
            trigger_id: trigger.trigger_id,
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        let db = catalog
            .databases
            .get_by_name(&self.db_name)
            .expect("database should exist after apply");
        db.processing_engine_triggers
            .get_by_id(&self.trigger_id)
            .expect("trigger should exist after apply")
    }
}

#[cfg(test)]
mod tests;
