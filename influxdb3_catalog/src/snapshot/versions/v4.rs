#![allow(unreachable_pub, dead_code, clippy::wrong_self_convention)]

use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v2::{
    ColumnDefinition, FieldColumn, FieldFamilyMode, FieldFamilyName, TagColumn, TimestampColumn,
};
use crate::log::versions::v4::{
    MaxAge, MaxCardinality, NodeMode, TriggerSettings, TriggerSpecificationDefinition,
};
use crate::serialize::VersionedFileType;
use hashbrown::HashMap;
use influxdb3_id::{
    CatalogId, ColumnId, ColumnIdentifier, DbId, DistinctCacheId, FieldFamilyId, FieldIdentifier,
    LastCacheId, NodeId, SerdeVecMap, TableId, TagId, TokenId, TriggerId,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    // NOTE(tjh): added as part of https://github.com/influxdata/influxdb_pro/issues/911
    #[serde(default)]
    pub(crate) generation_config: GenerationConfigSnapshot,
    pub(crate) nodes: RepositorySnapshot<NodeId, NodeSnapshot>,
    pub(crate) databases: RepositorySnapshot<DbId, DatabaseSnapshot>,
    pub(crate) sequence: CatalogSequenceNumber,
    #[serde(default)]
    pub(crate) tokens: RepositorySnapshot<TokenId, TokenInfoSnapshot>,
    pub(crate) catalog_id: Arc<str>,
    pub(crate) catalog_uuid: Uuid,
}

impl VersionedFileType for CatalogSnapshot {
    const VERSION_ID: [u8; 10] = *b"idb3.004.s";
}

impl CatalogSnapshot {
    pub(crate) fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct GenerationConfigSnapshot {
    pub(crate) generation_durations: SerdeVecMap<u8, Duration>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct TokenInfoSnapshot {
    pub id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub expiry: i64,
    pub updated_by: Option<TokenId>,
    pub updated_at: Option<i64>,
    pub permissions: Vec<PermissionSnapshot>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PermissionSnapshot {
    pub resource_type: ResourceTypeSnapshot,
    pub resource_identifier: ResourceIdentifierSnapshot,
    pub actions: ActionsSnapshot,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceTypeSnapshot {
    Database,
    Token,
    Wildcard,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceIdentifierSnapshot {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    Wildcard,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum ActionsSnapshot {
    Database(DatabaseActionsSnapshot),
    Token(CrudActionsSnapshot),
    Wildcard,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct DatabaseActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CrudActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SystemActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NodeSnapshot {
    pub(crate) node_id: Arc<str>,
    pub(crate) node_catalog_id: NodeId,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<NodeMode>,
    pub(crate) state: NodeStateSnapshot,
    pub(crate) core_count: u64,
    #[serde(default)]
    pub(crate) cli_params: Option<Arc<str>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RetentionPeriodSnapshot {
    Indefinite,
    Duration(Duration),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DatabaseSnapshot {
    pub(crate) id: DbId,
    pub(crate) name: Arc<str>,
    pub(crate) tables: RepositorySnapshot<TableId, TableSnapshot>,
    pub(crate) retention_period: Option<RetentionPeriodSnapshot>,
    pub(crate) processing_engine_triggers:
        RepositorySnapshot<TriggerId, ProcessingEngineTriggerSnapshot>,
    pub(crate) deleted: bool,
    pub(crate) hard_delete_time: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table_name: Arc<str>,
    pub(crate) key: Vec<TagId>,
    pub(crate) columns: ColumnSetSnapshot,
    pub(crate) field_families: RepositorySnapshot<FieldFamilyId, FieldFamilySnapshot>,
    pub(crate) last_caches: RepositorySnapshot<LastCacheId, LastCacheSnapshot>,
    pub(crate) distinct_caches: RepositorySnapshot<DistinctCacheId, DistinctCacheSnapshot>,
    pub(crate) deleted: bool,
    pub(crate) hard_delete_time: Option<i64>,
    pub(crate) field_family_mode: FieldFamilyMode,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProcessingEngineTriggerSnapshot {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub node_id: Arc<str>,
    pub plugin_filename: String,
    pub database_name: Arc<str>,
    pub trigger_specification: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnDefinitionSnapshot {
    Timestamp(TimestampColumnSnapshot),
    Tag(TagColumnSnapshot),
    Field(FieldColumnSnapshot),
}

impl ColumnDefinitionSnapshot {
    pub fn column_id(&self) -> ColumnId {
        match self {
            ColumnDefinitionSnapshot::Timestamp(v) => v.column_id,
            ColumnDefinitionSnapshot::Tag(v) => v.column_id,
            ColumnDefinitionSnapshot::Field(v) => v.column_id,
        }
    }
}

impl From<ColumnDefinitionSnapshot> for ColumnDefinition {
    fn from(snapshot: ColumnDefinitionSnapshot) -> Self {
        match snapshot {
            ColumnDefinitionSnapshot::Timestamp(v) => {
                ColumnDefinition::Timestamp(Arc::new(v.into()))
            }
            ColumnDefinitionSnapshot::Tag(v) => ColumnDefinition::Tag(Arc::new(v.into())),
            ColumnDefinitionSnapshot::Field(v) => ColumnDefinition::Field(Arc::new(v.into())),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampColumnSnapshot {
    pub(crate) column_id: ColumnId,
    pub(crate) name: Arc<str>,
}

impl From<TimestampColumnSnapshot> for TimestampColumn {
    fn from(snapshot: TimestampColumnSnapshot) -> Self {
        TimestampColumn {
            column_id: snapshot.column_id,
            name: Arc::clone(&snapshot.name),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagColumnSnapshot {
    pub id: TagId,
    pub column_id: ColumnId,
    pub name: Arc<str>,
}

impl From<TagColumnSnapshot> for TagColumn {
    fn from(snapshot: TagColumnSnapshot) -> Self {
        TagColumn {
            id: snapshot.id,
            column_id: snapshot.column_id,
            name: snapshot.name,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldColumnSnapshot {
    pub id: FieldIdentifier,
    pub column_id: ColumnId,
    pub name: Arc<str>,
    pub data_type: FieldDataType,
}

impl From<FieldColumnSnapshot> for FieldColumn {
    fn from(snapshot: FieldColumnSnapshot) -> Self {
        FieldColumn {
            id: snapshot.id,
            column_id: snapshot.column_id,
            name: snapshot.name,
            data_type: snapshot.data_type.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
}

impl From<FieldDataType> for schema::InfluxFieldType {
    fn from(data_type: FieldDataType) -> Self {
        match data_type {
            FieldDataType::String => schema::InfluxFieldType::String,
            FieldDataType::Integer => schema::InfluxFieldType::Integer,
            FieldDataType::UInteger => schema::InfluxFieldType::UInteger,
            FieldDataType::Float => schema::InfluxFieldType::Float,
            FieldDataType::Boolean => schema::InfluxFieldType::Boolean,
        }
    }
}

impl From<schema::InfluxFieldType> for FieldDataType {
    fn from(data_type: schema::InfluxFieldType) -> Self {
        match data_type {
            schema::InfluxFieldType::String => FieldDataType::String,
            schema::InfluxFieldType::Integer => FieldDataType::Integer,
            schema::InfluxFieldType::UInteger => FieldDataType::UInteger,
            schema::InfluxFieldType::Float => FieldDataType::Float,
            schema::InfluxFieldType::Boolean => FieldDataType::Boolean,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FieldFamilySnapshot {
    pub(crate) name: FieldFamilyName,
    pub(crate) id: FieldFamilyId,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LastCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: LastCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) keys: Vec<ColumnIdentifier>,
    pub(crate) vals: Option<Vec<ColumnIdentifier>>,
    pub(crate) n: usize,
    pub(crate) ttl: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DistinctCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: DistinctCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) cols: Vec<ColumnIdentifier>,
    pub(crate) max_cardinality: MaxCardinality,
    pub(crate) max_age_seconds: MaxAge,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct ColumnSetSnapshot {
    pub(crate) repo: Vec<ColumnDefinitionSnapshot>,
    pub(crate) next_id: ColumnId,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RepositorySnapshot<I, R>
where
    I: CatalogId,
{
    pub(crate) repo: SerdeVecMap<I, R>,
    pub(crate) next_id: I,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum NodeStateSnapshot {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}
