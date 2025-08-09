use std::sync::Arc;

use influxdb3_id::{ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, TableId, TriggerId};

use crate::{
    catalog::{ColumnDefinition, DatabaseSchema, NodeDefinition, TableDefinition},
    log::{DistinctCacheDefinition, LastCacheDefinition, TriggerDefinition},
};

pub trait CatalogResource: Clone {
    type Identifier;

    const KIND: &'static str;

    fn id(&self) -> Self::Identifier;
    fn name(&self) -> Arc<str>;
}

impl CatalogResource for NodeDefinition {
    type Identifier = NodeId;

    fn id(&self) -> Self::Identifier {
        self.node_catalog_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }
}

impl CatalogResource for DatabaseSchema {
    type Identifier = DbId;

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for TableDefinition {
    type Identifier = TableId;

    fn id(&self) -> Self::Identifier {
        self.table_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
}

impl CatalogResource for TriggerDefinition {
    type Identifier = TriggerId;

    fn id(&self) -> Self::Identifier {
        self.trigger_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.trigger_name)
    }
}

impl CatalogResource for ColumnDefinition {
    type Identifier = ColumnId;

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for LastCacheDefinition {
    type Identifier = LastCacheId;

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl CatalogResource for DistinctCacheDefinition {
    type Identifier = DistinctCacheId;

    fn id(&self) -> Self::Identifier {
        self.cache_id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.cache_name)
    }
}
