use std::sync::Arc;

use bimap::BiHashMap;
use influxdb3_authz::{
    Actions, CrudActions, DatabaseActions, Permission, ResourceIdentifier, ResourceType, TokenInfo,
};
use influxdb3_id::{CatalogId, TokenId};
use iox_time::Time;
use schema::{InfluxColumnType, InfluxFieldType};
use v3::{
    ActionsSnapshot, CatalogSnapshot, ColumnDefinitionSnapshot, CrudActionsSnapshot,
    DatabaseActionsSnapshot, DatabaseSnapshot, DistinctCacheSnapshot, GenerationConfigSnapshot,
    InfluxType, LastCacheSnapshot, NodeSnapshot, NodeStateSnapshot, PermissionSnapshot,
    ProcessingEngineTriggerSnapshot, RepositorySnapshot, ResourceIdentifierSnapshot,
    ResourceTypeSnapshot, RetentionPeriodSnapshot, TableSnapshot, TokenInfoSnapshot,
};

use crate::{
    catalog::{
        ColumnDefinition, DatabaseSchema, GenerationConfig, InnerCatalog, NodeDefinition,
        NodeState, Repository, RetentionPeriod, TableDefinition, TokenRepository,
    },
    log::{
        DistinctCacheDefinition, LastCacheDefinition, LastCacheTtl, LastCacheValueColumnsDef,
        TriggerDefinition,
    },
    resource::CatalogResource,
};

pub(crate) mod v1;
pub(crate) mod v2;
pub(crate) mod v3;

/// This trait is implemented only for the most recent snapshot version. The reasoning is explained
/// below
///
/// When snapshots are loaded following is the sequence
///   - load snapshot file from object store
///   - run through conversion function to convert it to $latest:CatalogSnapshot
///   - call InnerCatalog::from_snapshot (from this trait) to get to latest in memory types
///     (InnerCatalog et. al)
///
/// When snapshots are written back following is the sequence
///   - call InnerCatalog::snapshot to get $latest:CatalogSnapshot
///   - write CatalogSnapshot to object store
///
/// As you can see the method from_snapshot is called after the conversion function is called so
/// any version migration would have to happen in conversion functions to get
/// $latest:CatalogSnapshot. Usually anything that is populated on the fly is implemented here, i.e
/// types that need to be populated only for in-memory representation (eg. bihashmaps, token
/// permissions map etc)
///
/// When writing another migration the snapshot types (CatalogSnapshot and others referenced by it)
/// should come from snapshot::versions::$latest.rs and then impls go here as this is expected to
/// only be implemented for going back and forth between InnerCatalog and $latest:CatalogSnapshot
pub(crate) trait Snapshot {
    type Serialized;

    fn snapshot(&self) -> Self::Serialized;
    fn from_snapshot(snap: Self::Serialized) -> Self;
}

impl Snapshot for InnerCatalog {
    type Serialized = CatalogSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            nodes: self.nodes.snapshot(),
            databases: self.databases.snapshot(),
            sequence: self.sequence,
            catalog_id: Arc::clone(&self.catalog_id),
            tokens: self.tokens.repo().snapshot(),
            catalog_uuid: self.catalog_uuid,
            generation_config: self.generation_config.snapshot(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let repository: Repository<TokenId, TokenInfo> = Repository::from_snapshot(snap.tokens);
        let mut hash_lookup_map = BiHashMap::new();
        repository.repo.iter().for_each(|(id, info)| {
            // this clone should maybe be switched to arc?
            hash_lookup_map.insert(*id, info.hash.clone());
        });

        let token_info_repo = TokenRepository::new(repository, hash_lookup_map);
        Self {
            sequence: snap.sequence,
            catalog_id: snap.catalog_id,
            catalog_uuid: snap.catalog_uuid,
            nodes: Repository::from_snapshot(snap.nodes),
            databases: Repository::from_snapshot(snap.databases),
            tokens: token_info_repo,
            generation_config: GenerationConfig::from_snapshot(snap.generation_config),
        }
    }
}

impl Snapshot for GenerationConfig {
    type Serialized = GenerationConfigSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            generation_durations: self
                .generation_durations
                .iter()
                .map(|(level, duration)| (*level, *duration))
                .collect(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            generation_durations: snap.generation_durations.into_iter().collect(),
        }
    }
}

impl Snapshot for TokenInfo {
    type Serialized = TokenInfoSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            hash: self.hash.clone(),
            created_at: self.created_at,
            expiry: self.expiry_millis,
            created_by: self.created_by,
            updated_at: self.updated_at,
            updated_by: self.updated_by,
            description: self.description.clone(),
            permissions: self
                .permissions
                .iter()
                .map(|perm| perm.snapshot())
                .collect(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            hash: snap.hash,
            created_at: snap.created_at,
            expiry_millis: snap.expiry,
            created_by: snap.created_by,
            updated_by: snap.updated_by,
            updated_at: snap.updated_at,
            permissions: snap
                .permissions
                .into_iter()
                .map(Permission::from_snapshot)
                .collect(),
            description: snap.description,
        }
    }
}

impl Snapshot for Permission {
    type Serialized = PermissionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        PermissionSnapshot {
            resource_type: self.resource_type.snapshot(),
            resource_identifier: self.resource_identifier.snapshot(),
            actions: self.actions.snapshot(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            resource_type: ResourceType::from_snapshot(snap.resource_type),
            resource_identifier: ResourceIdentifier::from_snapshot(snap.resource_identifier),
            actions: Actions::from_snapshot(snap.actions),
        }
    }
}

impl Snapshot for ResourceType {
    type Serialized = ResourceTypeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceType::Database => ResourceTypeSnapshot::Database,
            ResourceType::Token => ResourceTypeSnapshot::Token,
            ResourceType::Wildcard => ResourceTypeSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceTypeSnapshot::Database => ResourceType::Database,
            ResourceTypeSnapshot::Token => ResourceType::Token,
            ResourceTypeSnapshot::Wildcard => ResourceType::Wildcard,
        }
    }
}

impl Snapshot for ResourceIdentifier {
    type Serialized = ResourceIdentifierSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceIdentifier::Database(db_id) => {
                ResourceIdentifierSnapshot::Database(db_id.clone())
            }
            ResourceIdentifier::Token(token_id) => {
                ResourceIdentifierSnapshot::Token(token_id.clone())
            }
            ResourceIdentifier::Wildcard => ResourceIdentifierSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceIdentifierSnapshot::Database(db_id) => ResourceIdentifier::Database(db_id),
            ResourceIdentifierSnapshot::Token(token_id) => ResourceIdentifier::Token(token_id),
            ResourceIdentifierSnapshot::Wildcard => ResourceIdentifier::Wildcard,
        }
    }
}

impl Snapshot for Actions {
    type Serialized = ActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            Actions::Database(database_actions) => {
                ActionsSnapshot::Database(database_actions.snapshot())
            }
            Actions::Token(crud_actions) => ActionsSnapshot::Token(crud_actions.snapshot()),
            Actions::Wildcard => ActionsSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ActionsSnapshot::Database(db_actions) => {
                Actions::Database(DatabaseActions::from_snapshot(db_actions))
            }
            ActionsSnapshot::Token(crud_actions) => {
                Actions::Token(CrudActions::from_snapshot(crud_actions))
            }
            ActionsSnapshot::Wildcard => Actions::Wildcard,
        }
    }
}

impl Snapshot for DatabaseActions {
    type Serialized = DatabaseActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        DatabaseActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

impl Snapshot for CrudActions {
    type Serialized = CrudActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        CrudActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

impl Snapshot for NodeDefinition {
    type Serialized = NodeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            node_id: Arc::clone(&self.node_id),
            node_catalog_id: self.node_catalog_id,
            instance_id: Arc::clone(&self.instance_id),
            mode: self.mode.clone(),
            state: self.state.snapshot(),
            core_count: self.core_count,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            node_id: snap.node_id,
            node_catalog_id: snap.node_catalog_id,
            instance_id: snap.instance_id,
            mode: snap.mode,
            core_count: snap.core_count,
            state: NodeState::from_snapshot(snap.state),
        }
    }
}

impl Snapshot for NodeState {
    type Serialized = NodeStateSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            NodeState::Running { registered_time_ns } => NodeStateSnapshot::Running {
                registered_time_ns: *registered_time_ns,
            },
            NodeState::Stopped { stopped_time_ns } => NodeStateSnapshot::Stopped {
                stopped_time_ns: *stopped_time_ns,
            },
        }
    }
    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            NodeStateSnapshot::Running { registered_time_ns } => {
                Self::Running { registered_time_ns }
            }
            NodeStateSnapshot::Stopped { stopped_time_ns } => Self::Stopped { stopped_time_ns },
        }
    }
}

impl Snapshot for RetentionPeriod {
    type Serialized = RetentionPeriodSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            Self::Indefinite => RetentionPeriodSnapshot::Indefinite,
            Self::Duration(d) => RetentionPeriodSnapshot::Duration(*d),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            RetentionPeriodSnapshot::Indefinite => Self::Indefinite,
            RetentionPeriodSnapshot::Duration(d) => Self::Duration(d),
        }
    }
}

impl Snapshot for DatabaseSchema {
    type Serialized = DatabaseSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            tables: self.tables.snapshot(),
            retention_period: Some(self.retention_period.snapshot()),
            processing_engine_triggers: self.processing_engine_triggers.snapshot(),
            deleted: self.deleted,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            tables: Repository::from_snapshot(snap.tables),
            retention_period: snap
                .retention_period
                .map(Snapshot::from_snapshot)
                .unwrap_or(RetentionPeriod::Indefinite),
            processing_engine_triggers: Repository::from_snapshot(snap.processing_engine_triggers),
            deleted: snap.deleted,
        }
    }
}

/// A snapshot of a [`TableDefinition`] used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
impl Snapshot for TableDefinition {
    type Serialized = TableSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table_name: Arc::clone(&self.table_name),
            key: self.series_key.clone(),
            columns: self.columns.snapshot(),
            last_caches: self.last_caches.snapshot(),
            distinct_caches: self.distinct_caches.snapshot(),
            deleted: self.deleted,
            hard_delete_time: self.hard_delete_time.as_ref().map(Time::timestamp_nanos),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let table_id = snap.table_id;
        // use the TableDefinition constructor here since it handles
        // Schema construction:
        let mut table_def = Self::new(
            table_id,
            snap.table_name,
            snap.columns
                .repo
                .into_iter()
                .map(|(id, def)| {
                    (
                        id,
                        def.name,
                        match def.influx_type {
                            InfluxType::Tag => InfluxColumnType::Tag,
                            InfluxType::Field => {
                                InfluxColumnType::Field(InfluxFieldType::from(def.r#type))
                            }
                            InfluxType::Time => InfluxColumnType::Timestamp,
                        },
                    )
                })
                .collect(),
            snap.key,
        )
        .expect("serialized table definition from catalog should be valid");
        // ensure next col id is set from the snapshot incase we ever allow
        // hard-deletes:
        table_def.columns.set_next_id(snap.columns.next_id);
        Self {
            table_id,
            table_name: table_def.table_name,
            schema: table_def.schema,
            columns: table_def.columns,
            series_key: table_def.series_key,
            series_key_names: table_def.series_key_names,
            sort_key: table_def.sort_key,
            last_caches: Repository::from_snapshot(snap.last_caches),
            distinct_caches: Repository::from_snapshot(snap.distinct_caches),
            deleted: snap.deleted,
            hard_delete_time: snap.hard_delete_time.map(Time::from_timestamp_nanos),
        }
    }
}

impl Snapshot for TriggerDefinition {
    type Serialized = ProcessingEngineTriggerSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            trigger_id: self.trigger_id,
            trigger_name: Arc::clone(&self.trigger_name),
            node_id: Arc::clone(&self.node_id),
            plugin_filename: self.plugin_filename.clone(),
            database_name: Arc::clone(&self.database_name),
            trigger_specification: self.trigger.clone(),
            trigger_settings: self.trigger_settings,
            trigger_arguments: self.trigger_arguments.clone(),
            disabled: self.disabled,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            trigger_id: snap.trigger_id,
            trigger_name: snap.trigger_name,
            node_id: snap.node_id,
            plugin_filename: snap.plugin_filename,
            database_name: snap.database_name,
            trigger: snap.trigger_specification,
            trigger_settings: snap.trigger_settings,
            trigger_arguments: snap.trigger_arguments,
            disabled: snap.disabled,
        }
    }
}

impl Snapshot for ColumnDefinition {
    type Serialized = ColumnDefinitionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            name: Arc::clone(&self.name),
            id: self.id,
            r#type: self.data_type.into(),
            influx_type: self.data_type.into(),
            nullable: self.nullable,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            data_type: match snap.influx_type {
                InfluxType::Tag => InfluxColumnType::Tag,
                InfluxType::Field => InfluxColumnType::Field(InfluxFieldType::from(&snap.r#type)),
                InfluxType::Time => InfluxColumnType::Timestamp,
            },
            nullable: snap.nullable,
        }
    }
}

impl Snapshot for LastCacheDefinition {
    type Serialized = LastCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table: Arc::clone(&self.table),
            id: self.id,
            name: Arc::clone(&self.name),
            keys: self.key_columns.to_vec(),
            vals: match &self.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => Some(columns.to_vec()),
                LastCacheValueColumnsDef::AllNonKeyColumns => None,
            },
            n: self.count.into(),
            ttl: self.ttl.as_secs(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table: snap.table,
            id: snap.id,
            name: snap.name,
            key_columns: snap.keys,
            value_columns: snap.vals.map_or_else(Default::default, |columns| {
                LastCacheValueColumnsDef::Explicit { columns }
            }),
            count: snap
                .n
                .try_into()
                .expect("catalog contains invalid last cache size"),
            ttl: LastCacheTtl::from_secs(snap.ttl),
        }
    }
}

impl Snapshot for DistinctCacheDefinition {
    type Serialized = DistinctCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table: Arc::clone(&self.table_name),
            id: self.cache_id,
            name: Arc::clone(&self.cache_name),
            cols: self.column_ids.clone(),
            max_cardinality: self.max_cardinality,
            max_age_seconds: self.max_age_seconds,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table_name: snap.table,
            cache_id: snap.id,
            cache_name: snap.name,
            column_ids: snap.cols,
            max_cardinality: snap.max_cardinality,
            max_age_seconds: snap.max_age_seconds,
        }
    }
}

impl<I, R> Snapshot for Repository<I, R>
where
    I: CatalogId,
    R: Snapshot + CatalogResource,
{
    type Serialized = RepositorySnapshot<I, R::Serialized>;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            repo: self
                .repo
                .iter()
                .map(|(id, res)| (*id, res.snapshot()))
                .collect(),
            next_id: self.next_id,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let mut repo = Self::new();
        for (id, res) in snap.repo {
            repo.insert(id, Arc::new(R::from_snapshot(res)))
                .expect("catalog should contain no duplicates");
        }
        Self {
            id_name_map: repo.id_name_map,
            repo: repo.repo,
            next_id: snap.next_id,
        }
    }
}
