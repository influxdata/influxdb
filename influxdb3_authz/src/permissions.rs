use std::{fmt::Display, str::FromStr, sync::Arc};

use influxdb3_id::{DbId, TokenId};
use observability_deps::tracing::{debug, error};
use thiserror::Error;

use crate::{
    AccessRequest, Actions, CrudActions, DatabaseActions, Permission, ResourceIdentifier,
    ResourceType, SystemActions, SystemResourceIdentifier,
};

pub const AUTHZ_DB_RESOURCE_TYPE: &str = "db";
pub const AUTHZ_SYSTEM_RESOURCE_TYPE: &str = "system";
pub const AUTHZ_TOKEN_RESOURCE_TYPE: &str = "token";

pub const AUTHZ_CREATE_DB_ACTION: &str = "create";
pub const AUTHZ_DESCRIBE_DB_ACTION: &str = "describe";
pub const AUTHZ_WRITE_DB_ACTION: &str = "write";
pub const AUTHZ_READ_DB_ACTION: &str = "read";

pub const AUTHZ_DELETE_ROW_ACTION: &str = "delete";

pub const AUTHZ_CREATE_CRUD_ACTION: &str = "create";
pub const AUTHZ_READ_CRUD_ACTION: &str = "read";
pub const AUTHZ_UPDATE_CRUD_ACTION: &str = "update";
pub const AUTHZ_DELETE_CRUD_ACTION: &str = "delete";

pub const AUTHZ_WILDCARD: &str = "*";

#[derive(Debug, Error)]
pub enum ResourceMappingError {
    #[error("resource type not supported {0}")]
    ResourceTypeNotSupported(String),
    #[error("invalid resource name {0}")]
    InvalidResourceName(String),
    #[error("mixed wildcard (*) and resource name")]
    MixedWildcardAndRegularResourceName,
    #[error("missing resource name {0}")]
    MissingResourceName(String),
    #[error("action not supported, {0}")]
    ActionNotSupported(String),
    #[error("missing resource id {0}")]
    MissingResourceId(String),
}

pub trait ResourceNameToIdProvider {
    fn resource_name_to_id(
        &self,
        resource_type: ResourceType,
        names: &[String],
    ) -> Result<ResourceIdentifier, ResourceMappingError>;
}

pub trait ResourceIdToNameProvider {
    fn resource_id_to_name(
        &self,
        identifier: &ResourceIdentifier,
    ) -> Vec<Result<String, ResourceMappingError>>;
}

/// This is the equivalent of internal Permissions that can be used to capture strings from cli
/// when parsed. Once it's parsed you can use this type for talking to catalog API creation.
/// This type can then be converted into "PermissionDetails" log type that goes in the op log.
#[derive(Debug, Clone)]
pub struct PermissionDetailsSpec {
    pub resource_type: String,
    pub resource_identifier: Vec<String>,
    pub actions: Vec<String>,
}

const ADMIN_ACTIONS_BITMAP: ActionsBitmap = ActionsBitmap::MAX;

impl FromStr for ResourceType {
    type Err = ResourceMappingError;

    fn from_str(resource_type: &str) -> Result<Self, Self::Err> {
        match resource_type {
            AUTHZ_DB_RESOURCE_TYPE => Ok(ResourceType::Database),
            AUTHZ_SYSTEM_RESOURCE_TYPE => Ok(ResourceType::System),
            AUTHZ_WILDCARD => Ok(ResourceType::Wildcard),
            _ => Err(ResourceMappingError::ResourceTypeNotSupported(
                resource_type.to_owned(),
            )),
        }
    }
}

impl Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stringified = match self {
            ResourceType::Database => AUTHZ_DB_RESOURCE_TYPE.to_string(),
            ResourceType::System => AUTHZ_SYSTEM_RESOURCE_TYPE.to_string(),
            ResourceType::Wildcard => AUTHZ_WILDCARD.to_string(),
            ResourceType::Token => AUTHZ_TOKEN_RESOURCE_TYPE.to_string(),
        };
        write!(f, "{stringified}")
    }
}

impl Display for ResourceIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stringified = match self {
            ResourceIdentifier::Database(db_ids) => db_ids
                .iter()
                .map(|id| id.get().to_string())
                .collect::<Vec<String>>()
                .join(","),
            ResourceIdentifier::Token(token_ids) => token_ids
                .iter()
                .map(|id| id.get().to_string())
                .collect::<Vec<String>>()
                .join(","),
            ResourceIdentifier::System(system_resource_ids) => system_resource_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(","),
            ResourceIdentifier::Wildcard => "*".to_string(),
        };
        write!(f, "{stringified}")
    }
}

impl ResourceIdentifier {
    pub fn database(ids: Vec<DbId>) -> ResourceIdentifier {
        ResourceIdentifier::Database(ids)
    }

    pub fn wildcard() -> ResourceIdentifier {
        ResourceIdentifier::Wildcard
    }

    pub fn build_resource_ids_for_type(
        resource_type: ResourceType,
        name_to_id_provider: Arc<dyn ResourceNameToIdProvider>,
        resource_names: &[String],
    ) -> Result<ResourceIdentifier, ResourceMappingError> {
        name_to_id_provider.resource_name_to_id(resource_type, resource_names)
    }
}

impl Display for Actions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stringified = match self {
            Actions::Database(database_actions) => database_actions.to_string(),
            Actions::Token(crud_actions) => crud_actions.to_string(),
            Actions::System(system_actions) => system_actions.to_string(),
            Actions::Wildcard => "*".to_string(),
        };
        write!(f, "{stringified}")
    }
}

impl Actions {
    pub fn build_actions_for_type(
        resource_type: ResourceType,
        actions: &[String],
    ) -> Result<Actions, ResourceMappingError> {
        match resource_type {
            ResourceType::Database => Ok(Actions::Database(DatabaseActions::new(actions)?)),
            ResourceType::System => Ok(Actions::System(SystemActions::new(actions)?)),
            ResourceType::Wildcard => Ok(Actions::Wildcard),
            _ => Err(ResourceMappingError::ResourceTypeNotSupported(
                resource_type.to_string(),
            )),
        }
    }

    pub fn to_bitmap(&self) -> ActionsBitmap {
        match self {
            Actions::Database(database_actions) => database_actions.to_bitmap(),
            Actions::Token(crud_actions) => crud_actions.to_bitmap(),
            Actions::System(system_actions) => system_actions.to_bitmap(),
            Actions::Wildcard => ADMIN_ACTIONS_BITMAP,
        }
    }
}

impl Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.resource_type, self.resource_identifier, self.actions
        )
    }
}

impl Permission {
    pub fn fmt_with_resource_names(&self, resource_names: &str) -> String {
        format!("{}:{}:{}", self.resource_type, resource_names, self.actions)
    }
}

/// This type generically represents the bitmap for actions on all resources
pub type ActionsBitmap = u16;

impl SystemResourceIdentifier {
    pub const HEALTH: u16 = 0;
    pub const METRICS: u16 = 1;
    pub const PING: u16 = 2;
    pub const READY: u16 = 3;

    pub const HEALTH_NAME: &'static str = "health";
    pub const METRICS_NAME: &'static str = "metrics";
    pub const PING_NAME: &'static str = "ping";
    pub const READY_NAME: &'static str = "ready";

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl Display for SystemResourceIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stringified = match self.0 {
            Self::HEALTH => Self::HEALTH_NAME,
            Self::METRICS => Self::METRICS_NAME,
            Self::PING => Self::PING_NAME,
            Self::READY => Self::READY_NAME,
            _ => {
                error!(identifier = ?self.0, "cannot map system resource identifier");
                panic!("unrecognized system resource identifier")
            }
        };
        write!(f, "{stringified}")
    }
}

impl From<u16> for SystemResourceIdentifier {
    fn from(value: u16) -> Self {
        SystemResourceIdentifier(value)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PermissionAttributes {
    actions: ActionsBitmap,
}

impl PermissionAttributes {
    pub fn new(actions: ActionsBitmap) -> Self {
        Self { actions }
    }

    pub fn actions(&self) -> ActionsBitmap {
        self.actions
    }

    pub fn add_action(&mut self, new_allowed_actions: &ActionsBitmap) {
        self.actions |= new_allowed_actions;
    }

    pub fn get(self) -> Self {
        self
    }
}

// These impl blocks are in enterprise to see if we could allow DatabaseActions type in core but
// actual impls are within enterprise
impl DatabaseActions {
    // NB: Only CREATE/DESCRIBE/READ/WRITE/DELETE are supported at the moment. All others are just ignored
    pub const CREATE: u16 = 1 << 0; // create a database (IOx namespace)
    pub const DESCRIBE: u16 = 1 << 1; // read metadata/retention polices/etc of the database (IOx namespace)
    pub const UPDATE: u16 = 1 << 2; // update metadata/retention polices/etc of the database (IOx namespace)
    pub const DROP: u16 = 1 << 3; // delete database and all of its data
    pub const READ: u16 = 1 << 4; // query any table
    pub const WRITE: u16 = 1 << 5; // insert/update/upsert to any table
    pub const DELETE: u16 = 1 << 6; // delete rows (points)/columns or drop any table

    pub const ALL_BITS: &[u16] = &[
        Self::CREATE,
        Self::DESCRIBE,
        Self::UPDATE,
        Self::DROP,
        Self::READ,
        Self::WRITE,
        Self::DELETE,
    ];
}

impl DatabaseActions {
    pub fn new(allowed_db_actions: &[String]) -> Result<Self, ResourceMappingError> {
        let mut actions_allowed: u16 = 0;
        for perm in allowed_db_actions {
            match perm.as_str() {
                AUTHZ_CREATE_DB_ACTION => actions_allowed |= DatabaseActions::CREATE,
                AUTHZ_DESCRIBE_DB_ACTION => actions_allowed |= DatabaseActions::DESCRIBE,
                AUTHZ_READ_DB_ACTION => actions_allowed |= DatabaseActions::READ,
                AUTHZ_WRITE_DB_ACTION => actions_allowed |= DatabaseActions::WRITE,
                AUTHZ_DELETE_ROW_ACTION => actions_allowed |= DatabaseActions::DELETE,
                _ => {
                    return Err(ResourceMappingError::ActionNotSupported(format!(
                        "database action {perm}"
                    )));
                }
            }
        }
        Ok(DatabaseActions(actions_allowed))
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }

    pub fn contains(&self, perm: u16) -> bool {
        (self.0 & perm) == perm
    }

    pub fn is_allowed(&self, perm: &Self) -> bool {
        (self.0 & perm.0) == perm.0
    }

    pub fn to_bitmap(&self) -> ActionsBitmap {
        self.0
    }

    pub fn to_database_actions(&self) -> Vec<super::role::DatabaseAction> {
        use super::role::DatabaseAction;

        Self::ALL_BITS
            .iter()
            .filter(|bit| self.contains(**bit))
            .filter_map(|bit| match *bit {
                Self::READ => Some(DatabaseAction::Read),
                Self::WRITE => Some(DatabaseAction::Write),
                Self::CREATE => Some(DatabaseAction::Create),
                Self::DELETE => Some(DatabaseAction::Delete),
                Self::DESCRIBE => Some(DatabaseAction::Describe),
                _ => {
                    debug!(bit, "unsupported database action bit requested");
                    None
                }
            })
            .collect()
    }
}

impl std::ops::BitOrAssign for DatabaseActions {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl Display for DatabaseActions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut all_actions_set = vec![];
        if self.contains(DatabaseActions::CREATE) {
            all_actions_set.push("create");
        }
        if self.contains(DatabaseActions::DESCRIBE) {
            all_actions_set.push("describe");
        }
        if self.contains(DatabaseActions::UPDATE) {
            all_actions_set.push("update");
        }
        if self.contains(DatabaseActions::DROP) {
            all_actions_set.push("drop");
        }
        if self.contains(DatabaseActions::READ) {
            all_actions_set.push("read");
        }
        if self.contains(DatabaseActions::WRITE) {
            all_actions_set.push("write");
        }
        if self.contains(DatabaseActions::DELETE) {
            all_actions_set.push("delete");
        }
        write!(f, "{}", all_actions_set.join(","))
    }
}

impl From<u16> for DatabaseActions {
    fn from(value: u16) -> Self {
        DatabaseActions(value)
    }
}

impl SystemActions {
    pub const READ: u16 = 1 << 0;
}

impl SystemActions {
    pub fn new(allowed_actions: &[String]) -> Result<Self, ResourceMappingError> {
        let mut actions: u16 = 0;
        for perm in allowed_actions {
            if perm == "read" {
                actions |= SystemActions::READ;
            } else {
                return Err(ResourceMappingError::ActionNotSupported(format!(
                    "system action {perm}"
                )));
            }
        }
        Ok(SystemActions(actions))
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }

    pub fn contains(&self, perm: u16) -> bool {
        (self.as_u16() & perm) == perm
    }

    pub fn is_allowed(&self, perm: &Self) -> bool {
        (self.as_u16() & perm.as_u16()) == perm.as_u16()
    }

    pub fn to_bitmap(&self) -> ActionsBitmap {
        self.as_u16()
    }
}

impl Display for SystemActions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut all_actions_set = vec![];
        if self.contains(SystemActions::READ) {
            all_actions_set.push("read");
        }
        write!(f, "{}", all_actions_set.join(","))
    }
}

impl From<u16> for SystemActions {
    fn from(value: u16) -> Self {
        SystemActions(value)
    }
}

impl CrudActions {
    pub const CREATE: u16 = 1 << 0;
    pub const READ: u16 = 1 << 1;
    pub const UPDATE: u16 = 1 << 2;
    pub const DELETE: u16 = 1 << 3;
}

impl CrudActions {
    pub fn new(allowed_crud_actions: &[String]) -> Result<Self, ResourceMappingError> {
        let mut actions_allowed: u16 = 0;
        for perm in allowed_crud_actions {
            match perm.as_str() {
                AUTHZ_READ_CRUD_ACTION => actions_allowed |= CrudActions::READ,
                AUTHZ_CREATE_CRUD_ACTION => actions_allowed |= CrudActions::CREATE,
                AUTHZ_UPDATE_CRUD_ACTION => actions_allowed |= CrudActions::UPDATE,
                AUTHZ_DELETE_CRUD_ACTION => actions_allowed |= CrudActions::DELETE,
                _ => {
                    return Err(ResourceMappingError::ActionNotSupported(format!(
                        "crud action {perm}"
                    )));
                }
            }
        }
        Ok(CrudActions(actions_allowed))
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }

    pub fn contains(&self, perm: u16) -> bool {
        (self.as_u16() & perm) == perm
    }

    pub fn is_allowed(&self, perm: &Self) -> bool {
        (self.as_u16() & perm.as_u16()) == perm.as_u16()
    }

    pub fn to_bitmap(&self) -> ActionsBitmap {
        self.as_u16()
    }
}

impl Display for CrudActions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut all_actions_set = vec![];
        if self.contains(CrudActions::CREATE) {
            all_actions_set.push("create");
        }

        if self.contains(CrudActions::READ) {
            all_actions_set.push("read");
        }

        if self.contains(CrudActions::UPDATE) {
            all_actions_set.push("update");
        }

        if self.contains(CrudActions::DELETE) {
            all_actions_set.push("delete");
        }

        write!(f, "{}", all_actions_set.join(","))
    }
}

impl From<u16> for CrudActions {
    fn from(value: u16) -> Self {
        CrudActions(value)
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub struct PermissionKey {
    resource_type: ResourceType,
    resource_identifier: TokenPermissionResourceIdentifier,
}

impl PermissionKey {
    pub fn new(
        resource_type: ResourceType,
        resource_identifier: TokenPermissionResourceIdentifier,
    ) -> Self {
        Self {
            resource_type,
            resource_identifier,
        }
    }
}

/// This is equivalent to ResourceIdentifier but the difference is this represents how this token
/// permission maps to a resource, and at this level we just hold a "single" identifier not
/// multiple ones.
///   for eg.
///     db:db1,db2:read
///       - will have ResourceIdentifier::Database(vec![db1_id, db2_id]) repr
///       - but it'll be saved in TokenPermissions as,
///           TokenPermissionResourceIdentifier::Database(db1_id),
///           TokenPermissionResourceIdentifier::Database(db2_id),
///
/// This means we don't require vec![] of db ids.
///
#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub enum TokenPermissionResourceIdentifier {
    Database(DbId),
    Token(TokenId),
    System(SystemResourceIdentifier),
    Wildcard,
}

#[derive(Debug, Clone, Default)]
pub struct TokenPermissions {
    map: hashbrown::HashMap<TokenId, hashbrown::HashMap<PermissionKey, PermissionAttributes>>,
}

impl TokenPermissions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_permission(
        &mut self,
        resource_type: ResourceType,
        resource_id: TokenPermissionResourceIdentifier,
        token_id: TokenId,
        actions_bitmap: ActionsBitmap,
    ) {
        let perm_map = self.map.entry(token_id).or_default();
        // upsert actions here
        let key = PermissionKey::new(resource_type, resource_id);
        let allowed_permissions = perm_map.entry(key).or_default();
        allowed_permissions.add_action(&actions_bitmap);
    }

    pub fn is_allowed_access(
        &self,
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Option<bool> {
        match access_request {
            AccessRequest::MaybeDatabase(db_id, requested_database_actions) => {
                if let Some(db_id) = db_id {
                    let perm = self.get_permission(
                        ResourceType::Database,
                        TokenPermissionResourceIdentifier::Database(db_id),
                        token_id,
                    )?;
                    Some(
                        DatabaseActions::from(perm.actions())
                            .is_allowed(&requested_database_actions),
                    )
                } else {
                    let perm = self.get_permission_without_resource_identifier(
                        ResourceType::Database,
                        token_id,
                    )?;
                    Some(
                        DatabaseActions::from(perm.actions())
                            .is_allowed(&requested_database_actions),
                    )
                }
            }
            AccessRequest::Database(db_id, requested_database_actions) => {
                let perm = self.get_permission(
                    ResourceType::Database,
                    TokenPermissionResourceIdentifier::Database(db_id),
                    token_id,
                )?;
                Some(DatabaseActions::from(perm.actions()).is_allowed(&requested_database_actions))
            }
            AccessRequest::AnyDatabase(required_action) => {
                let perms_for_token = self.map.get(token_id)?;
                let any_match = perms_for_token.iter().any(|(key, perm)| {
                    let relevant = matches!(
                        (key.resource_type, key.resource_identifier),
                        (ResourceType::Database, _)
                            | (
                                ResourceType::Wildcard,
                                TokenPermissionResourceIdentifier::Wildcard
                            ),
                    );
                    relevant && DatabaseActions::from(perm.actions()).is_allowed(&required_action)
                });
                Some(any_match)
            }
            AccessRequest::Admin => {
                let perm = self.get_permission(
                    ResourceType::Wildcard,
                    TokenPermissionResourceIdentifier::Wildcard,
                    token_id,
                )?;
                Some(perm.actions() == ADMIN_ACTIONS_BITMAP)
            }
            AccessRequest::System(system_resource_identifier, requested_system_actions) => {
                let perm = self.get_permission(
                    ResourceType::System,
                    TokenPermissionResourceIdentifier::System(system_resource_identifier),
                    token_id,
                )?;
                Some(SystemActions::from(perm.actions()).is_allowed(&requested_system_actions))
            }
            // User, Role, AdminToken, and ResourceToken management require admin token
            AccessRequest::User(_)
            | AccessRequest::Role(_)
            | AccessRequest::AdminToken(_)
            | AccessRequest::ResourceToken(_) => {
                let perm = self.get_permission(
                    ResourceType::Wildcard,
                    TokenPermissionResourceIdentifier::Wildcard,
                    token_id,
                )?;
                Some(perm.actions() == ADMIN_ACTIONS_BITMAP)
            }
            AccessRequest::Token(_token_id, _crud_actions) => unimplemented!(),
        }
    }

    pub fn get_permission_without_resource_identifier(
        &self,
        resource_type: ResourceType,
        token_id: &TokenId,
    ) -> Option<PermissionAttributes> {
        let perms_for_token = self.map.get(token_id)?;
        // Resource id is missing, so you cannot do an exact match, this happens when db has not
        // been created but the token has a `db:*:write` permission for example. In this case
        // access request itself is just for db resource type.
        perms_for_token
            .get(&PermissionKey::new(
                resource_type,
                TokenPermissionResourceIdentifier::Wildcard,
            ))
            // lookup `*` just for type and identifier, match *:*:read for example
            .or_else(|| {
                perms_for_token.get(&PermissionKey::new(
                    ResourceType::Wildcard,
                    TokenPermissionResourceIdentifier::Wildcard,
                ))
            })
            .cloned()
    }

    pub fn get_permission(
        &self,
        resource_type: ResourceType,
        resource_id: TokenPermissionResourceIdentifier,
        token_id: &TokenId,
    ) -> Option<PermissionAttributes> {
        let perms_for_token = self.map.get(token_id)?;
        // Here, resource_id is either db_id or token_id but they'll come in as specific ids
        // and if there's a `*` (wildcard) for that resource type we need to do extra lookups.
        // This is probably better than exhaustively iterating over a list of permissions
        perms_for_token
            // lookup exact first, match db:db1:read for example
            .get(&PermissionKey::new(resource_type, resource_id))
            // lookup `*` just for identifier, match db:*:read for example
            .or_else(|| {
                perms_for_token.get(&PermissionKey::new(
                    resource_type,
                    TokenPermissionResourceIdentifier::Wildcard,
                ))
            })
            // lookup `*` just for type and identifier, match *:*:read for example
            .or_else(|| {
                perms_for_token.get(&PermissionKey::new(
                    ResourceType::Wildcard,
                    TokenPermissionResourceIdentifier::Wildcard,
                ))
            })
            .cloned()
    }

    pub fn remove(&mut self, token_id: &TokenId) {
        self.map.remove(token_id);
    }

    pub fn iter(
        &self,
    ) -> hashbrown::hash_map::Iter<
        '_,
        TokenId,
        hashbrown::HashMap<PermissionKey, PermissionAttributes>,
    > {
        self.map.iter()
    }
}

#[cfg(test)]
mod tests;
