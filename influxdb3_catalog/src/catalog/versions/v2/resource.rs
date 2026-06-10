use std::sync::Arc;

use influxdb3_id::{
    ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldIdentifier, LastCacheId, NodeId,
    TableId, TagId, TriggerId,
};

use super::{
    ColumnDefinition, DatabaseSchema, DistinctCacheDefinition, FieldColumn, FieldFamilyDefinition,
    FieldFamilyName, LastCacheDefinition, NodeDefinition, TableDefinition, TagColumn,
    TriggerDefinition,
};
use crate::resource::CatalogResource;

impl CatalogResource for NodeDefinition {
    type Identifier = NodeId;

    const CATEGORY: &'static str = "nodes";

    fn id(&self) -> Self::Identifier {
        self.node_catalog_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }
}

impl CatalogResource for DatabaseSchema {
    type Identifier = DbId;

    const CATEGORY: &'static str = "databases";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for TableDefinition {
    type Identifier = TableId;

    const CATEGORY: &'static str = "tables";

    fn id(&self) -> Self::Identifier {
        self.table_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
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

impl CatalogResource for FieldFamilyDefinition {
    type Identifier = FieldFamilyId;

    const CATEGORY: &'static str = "field_families";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        match &self.name {
            FieldFamilyName::User(name) => Arc::clone(name),
            FieldFamilyName::Auto(v) => format!("__{v}").into(),
        }
    }
}

impl CatalogResource for ColumnDefinition {
    type Identifier = ColumnIdentifier;

    const CATEGORY: &'static str = "columns";

    fn id(&self) -> Self::Identifier {
        self.id()
    }

    fn name(&self) -> Arc<str> {
        self.name()
    }
}

impl CatalogResource for TagColumn {
    type Identifier = TagId;

    const CATEGORY: &'static str = "tag_columns";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for FieldColumn {
    type Identifier = FieldIdentifier;

    const CATEGORY: &'static str = "fields";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for LastCacheDefinition {
    type Identifier = LastCacheId;

    const CATEGORY: &'static str = "last_caches";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for DistinctCacheDefinition {
    type Identifier = DistinctCacheId;

    const CATEGORY: &'static str = "distinct_caches";

    fn id(&self) -> Self::Identifier {
        self.cache_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.cache_name)
    }
}
