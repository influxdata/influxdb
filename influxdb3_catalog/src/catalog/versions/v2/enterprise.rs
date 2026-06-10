use super::*;
use crate::log::versions::v4::NodeModes;
use crate::object_store::versions::v2::ObjectStoreCatalog;
use crate::{
    CatalogError, Result,
    log::versions::v4::{
        CatalogBatch, ClearRetentionPeriodLog, DatabaseCatalogOp, NodeSpec, OrderedCatalogBatch,
        RetentionPeriod, SetRetentionPeriodLog, TokenBatch, TokenCatalogOp,
        enterprise::{CreateTokenDetails, PermissionDetails},
    },
};
use anyhow::Context;
use indexmap::IndexMap;
use influxdb3_authz::{
    AccessRequest, Actions, CatalogProvider, IdProvider, Permission, ResourceIdentifier,
    ResourceType, SystemResourceIdentifier, TokenInfo, TokenPermissionProvider,
    permissions::{
        ActionsBitmap, PermissionAttributes, PermissionDetailsSpec, ResourceIdToNameProvider,
        ResourceMappingError, ResourceNameToIdProvider, TokenPermissionResourceIdentifier,
    },
};
use influxdb3_id::{DbId, TokenId};
use influxdb3_process::ProcessUuidGetter;
use influxdb3_shutdown::ShutdownToken;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::{ObjectStore, path::Path};
use observability_deps::tracing::{error, info};
use parking_lot::{Mutex, RwLock};
use std::collections::btree_set;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};

pub(super) struct EnterpriseCatalogAttrs {
    /// The `--node-id` of the current node, if different from the cluster ID.
    pub(super) current_node_id: Option<Arc<str>>,
    pub(super) last_catalog_check_time: RwLock<Time>,
}

impl CatalogLimits {
    pub fn new(num_dbs: usize, num_tables: usize, num_columns_per_table: usize) -> Self {
        Self {
            num_dbs,
            num_tables,
            num_columns_per_table,
        }
    }

    pub fn new_with_defaults() -> Self {
        Self::new(
            Catalog::NUM_DBS_LIMIT,
            Catalog::NUM_TABLES_LIMIT,
            Catalog::NUM_COLUMNS_PER_TABLE_LIMIT,
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MaximumColumnCountLimiter {
    max_total_columns: u32,
}

impl MaximumColumnCountLimiter {
    pub fn new(max_total_columns: u32) -> Self {
        Self { max_total_columns }
    }
}

impl Default for MaximumColumnCountLimiter {
    fn default() -> Self {
        Self {
            max_total_columns: Catalog::MAX_TOTAL_COLUMNS,
        }
    }
}

impl CatalogLimiter for MaximumColumnCountLimiter {
    fn database_count_limit(&self, current: &CurrentCatalogUsage) -> usize {
        let total_column_count = current.total_column_count();
        let max_total_columns = self.max_total_columns as usize;
        if total_column_count < max_total_columns {
            current
                .total_db_count()
                .saturating_add(max_total_columns - total_column_count)
        } else {
            current.total_db_count()
        }
    }

    fn table_count_limit(&self, current: &CurrentCatalogUsage) -> usize {
        let total_column_count = current.total_column_count();
        let max_total_columns = self.max_total_columns as usize;
        if total_column_count < max_total_columns {
            current
                .total_table_count()
                .saturating_add(max_total_columns - total_column_count)
        } else {
            current.total_table_count()
        }
    }

    fn column_per_table_limit(&self, current: &CurrentCatalogUsage) -> usize {
        (self.max_total_columns as usize).saturating_sub(current.total_column_count())
    }
}

impl Catalog {
    pub const MAX_TOTAL_COLUMNS: u32 = 10_000_000;

    pub async fn new_enterprise(
        current_node_id: impl Into<Arc<str>>,
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metrics_registry: Arc<Registry>,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
    ) -> Result<Arc<Self>> {
        let current_node_id = current_node_id.into();
        let cluster_id = catalog_id.into();

        let enterprise_store = ObjectStoreCatalog::new(
            Arc::clone(&cluster_id),
            CATALOG_CHECKPOINT_INTERVAL,
            Arc::clone(&store),
            args.storage_mode,
        );
        let enterprise_exists = enterprise_store.checkpoint_exists().await?;

        // If an enterprise catalog does not exist, check the current node directory on object store
        // for catalog files in the event that this is migrating from a core node...
        if !enterprise_exists {
            let core_store = ObjectStoreCatalog::new(
                Arc::clone(&current_node_id),
                u64::MAX,
                Arc::clone(&store),
                args.storage_mode,
            );
            let core_exists = core_store.checkpoint_exists().await? || {
                // If not, Check if this is a core node with a V1 catalog
                let core_store = crate::object_store::versions::v1::ObjectStoreCatalog::new(
                    Arc::clone(&current_node_id),
                    Arc::clone(&store),
                );
                core_store.checkpoint_exists().await?
            };

            // only if there is a catalog in the node directory, i.e., from a Core version of the DB
            // running, AND, there is not a catalog in the cluster directory, i.e., from an Enterprise \
            // version of the DB running, do we need to migrate the existing core catalog to the cluster
            // directory on the object store.
            if core_exists {
                let mut inner = core_store.load_catalog().await?.context(
                    "there should be a catalog loaded from the core node since its object \
                        store directory contains a checkpoint file",
                )?;
                inner.catalog_id = cluster_id;
                // Discard the result here, so long as persisting the checkpoint does not result in
                // error. If this returns AlreadyExists, then perhaps the catalog was already initialized
                // by another process, so we just proceed.
                let _ = enterprise_store
                    .persist_catalog_checkpoint(&inner.snapshot())
                    .await?;
            }
        }

        let subscriptions = Default::default();
        let last_catalog_check_time = RwLock::new(time_provider.now());
        let metrics = Arc::new(CatalogMetrics::new(&metrics_registry));
        let catalog = enterprise_store
            .load_or_create_catalog()
            .await
            .map(RwLock::new)
            .map(|inner| {
                let usage = inner.read().current_usage();
                Self {
                    enterprise: EnterpriseCatalogAttrs {
                        current_node_id: Some(current_node_id),
                        last_catalog_check_time,
                    },
                    metric_registry: metrics_registry,
                    state: Mutex::new(CatalogState::Active),
                    subscriptions,
                    time_provider,
                    store: enterprise_store,
                    inner,
                    metrics,
                    limits,
                    usage,
                    args,
                }
            })?;

        create_internal_db(&catalog).await;

        catalog.metrics.operation_observer(
            catalog
                .subscribe_to_updates("catalog_operation_metrics")
                .await,
        );
        let catalog = Arc::new(catalog);
        catalog.usage.operation_observer(
            Arc::clone(&catalog),
            catalog.subscribe_to_updates("catalog_usage_tracker").await,
        );

        Ok(catalog)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_enterprise_with_shutdown(
        current_node_id: impl Into<Arc<str>>,
        catalog_id: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metrics_registry: Arc<Registry>,
        shutdown_token: ShutdownToken,
        limits: Arc<dyn CatalogLimiter>,
        args: CatalogArgs,
        process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    ) -> Result<Arc<Self>> {
        let catalog = Self::new_enterprise(
            current_node_id,
            catalog_id,
            store,
            time_provider,
            metrics_registry,
            limits,
            args,
        )
        .await?;
        let catalog_cloned = Arc::clone(&catalog);

        // Background task to set node state to stopped on shutdown:
        tokio::spawn(async move {
            shutdown_token.wait_for_shutdown().await;
            let node_id = catalog_cloned.current_node_id();
            if catalog_cloned.state.lock().is_shutdown() {
                // NB: if the catalog is in `Shutdown` state, that is because the node this process
                // is running as was shutdown from a separate process. So, do not attempt to update
                // the node state to `Stopped` here...
                return;
            }
            match catalog_cloned
                .update_node_state_stopped(node_id.as_ref(), process_uuid_getter)
                .await
            {
                Ok(_) => {
                    info!(
                        node_id = node_id.as_ref(),
                        "successfully updated node to stopped in catalog"
                    );
                }
                error @ Err(CatalogError::NodeAlreadyStopped { .. }) => {
                    info!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "node is already stopped for this process, do not need to update catalog"
                    );
                }
                Err(error) => {
                    error!(
                        ?error,
                        node_id = node_id.as_ref(),
                        "encountered error while updating node to stopped state in catalog"
                    );
                }
            }
        });

        Ok(catalog)
    }

    pub fn current_node_id(&self) -> Arc<str> {
        Arc::clone(
            self.enterprise
                .current_node_id
                .as_ref()
                .unwrap_or(&self.store.prefix),
        )
    }

    pub fn current_node(&self) -> Result<Arc<NodeDefinition>> {
        let node_id = Arc::clone(
            self.enterprise
                .current_node_id
                .as_ref()
                .context("the current node is not set in the enterprise catalog")?,
        );
        self.inner
            .read()
            .nodes
            .get_by_name(&node_id)
            .context(
                "the current node set on the catalog did not correspond \
                to any nodes registered in the catalog",
            )
            .map_err(Into::into)
    }

    pub fn matches_node_spec(&self, node_spec: &NodeSpec) -> Result<bool> {
        match node_spec {
            NodeSpec::All => Ok(true),
            NodeSpec::Nodes(v) => self
                .current_node()
                .map(|cn| v.contains(&cn.node_catalog_id)),
        }
    }

    pub async fn background_update(&self, duration: Duration, shutdown_token: ShutdownToken) {
        loop {
            let last_check_time = *self.enterprise.last_catalog_check_time.read();
            let next_check_time = last_check_time + duration;
            tokio::select! {
                _ = self.time_provider.sleep_until(next_check_time) => {
                    if self.state.lock().is_shutdown() {
                        break;
                    }
                    // may have been updated since we last slept, if so don't need to check again
                    if last_check_time != *self.enterprise.last_catalog_check_time.read() {
                        continue;
                    }
                    let next_sequence = self.sequence_number().next();
                    match self.store.load_catalog_sequenced_log(next_sequence).await {
                        Ok(Some(_ordered_catalog_batch)) => {
                            if shutdown_token.is_cancelled() {
                                // Do not recursively update the catalog if shutdown triggered
                                break;
                            }
                            if let Err(err) = self.try_update_catalog().await {
                                error!(?err, "failed background update to catalog");
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            error!(?err, "failed to check object store for new catalog updates");
                        }
                    };
                    self.update_last_check_time();
                }
                _ = shutdown_token.wait_for_shutdown() => {
                    break;
                }
            }
        }
    }

    async fn try_update_catalog(&self) -> Result<()> {
        let mut permit = CATALOG_WRITE_PERMIT.lock().await;
        self.load_and_update_from_object_store(self.sequence_number().next(), None, &permit)
            .await?;
        *permit = self.sequence_number().next();
        self.update_last_check_time();
        Ok(())
    }

    pub(crate) fn update_last_check_time(&self) {
        let mut last_check_time = self.enterprise.last_catalog_check_time.write();
        *last_check_time = self.time_provider.now();
    }

    pub fn get_permission(
        &self,
        db_name: &str,
        token_hash: Vec<u8>,
    ) -> Option<PermissionAttributes> {
        let inner = self.inner.read();
        let token_id = inner.tokens.hash_to_id(token_hash)?;
        let resource_id = self.db_name_to_id(db_name)?;
        inner.token_permissions.get_permission(
            ResourceType::Database,
            TokenPermissionResourceIdentifier::Database(resource_id),
            &token_id,
        )
    }

    pub async fn create_token_with_permission(
        &self,
        all_permissions: Vec<PermissionDetailsSpec>,
        token_name: String,
        expiry_secs: Option<u64>,
    ) -> Result<(Arc<TokenInfo>, String)> {
        let (token, hash) = create_token_and_hash();
        self.catalog_update_with_retry(|| {
            if self.inner.read().tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            let (token_id, created_at, expiry) = {
                let mut inner = self.inner.write();
                let token_id = inner.tokens.get_and_increment_next_id();
                let created_at = self.time_provider.now();
                let expiry = expiry_secs.map(|secs| {
                    created_at
                        .checked_add(Duration::from_secs(secs))
                        .expect("duration not to overflow")
                        .timestamp_millis()
                });
                (token_id, created_at.timestamp_millis(), expiry)
            };

            // NB: the validation happens here for parsing but the parsed types aren't used in
            //     `CreateTokenDetails` currently. This will be addressed in
            //     https://github.com/influxdata/influxdb_pro/issues/745
            let all_perms = all_permissions.iter().try_fold(
                Vec::with_capacity(all_permissions.len()),
                |mut permission_details, api_permission| {
                    let resource_type = ResourceType::from_str(&api_permission.resource_type)
                        .map(|res_type| {
                            if matches!(res_type, ResourceType::Wildcard) {
                                Err(CatalogError::CannotParsePermissionForToken(
                                    "* resource type can only be set for admin token".to_string(),
                                ))
                            } else {
                                Ok(res_type)
                            }
                        })
                        .map_err(|err| {
                            CatalogError::CannotParsePermissionForToken(err.to_string())
                        })??;

                    let _ = ResourceIdentifier::build_resource_ids_for_type(
                        resource_type,
                        // todo: avoid this cloning
                        Arc::from(self.clone_inner()),
                        &api_permission.resource_identifier,
                    )
                    .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

                    let _ = Actions::build_actions_for_type(resource_type, &api_permission.actions)
                        .map_err(|err| {
                            CatalogError::CannotParsePermissionForToken(err.to_string())
                        })?;

                    // at this point everything for permission has been parsed
                    permission_details.push(PermissionDetails {
                        resource_type: api_permission.resource_type.clone(),
                        resource_identifier: api_permission.resource_identifier.clone(),
                        actions: api_permission.actions.clone(),
                    });

                    Ok::<Vec<PermissionDetails>, CatalogError>(permission_details)
                },
            )?;

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateResourceScopedToken(
                    CreateTokenDetails {
                        token_id,
                        name: Arc::from(token_name.as_str()),
                        hash: hash.clone(),
                        created_at,
                        expiry,
                        permissions: all_perms,
                    },
                )],
            }))
        })
        .await?;

        let token_info = {
            self.inner
                .read()
                .tokens
                .repo()
                .get_by_name(&token_name)
                .expect("token info must be present after token creation by name")
        };

        // we need to pass these details back, especially this token as this is what user should
        // send in subsequent requests
        Ok((token_info, token))
    }

    pub fn total_active_cores_in_use_by_all_nodes(&self) -> u64 {
        self.list_nodes().iter().fold(0, |mut acc, node| {
            if node.is_running() {
                acc += node.core_count();
            }
            acc
        })
    }

    pub fn total_active_cores_in_use_by_other_nodes(&self) -> u64 {
        self.list_nodes()
            .iter()
            .filter(|node| {
                self.current_node()
                    .is_ok_and(|current| current.node_catalog_id != node.node_catalog_id)
            })
            .fold(0, |mut acc, node| {
                if node.is_running() {
                    acc += node.core_count();
                }

                acc
            })
    }

    pub async fn set_retention_period_for_table(
        &self,
        db_name: &str,
        table_name: &str,
        duration: Duration,
    ) -> Result<OrderedCatalogBatch> {
        info!(
            db_name,
            table_name,
            duration_ns = duration.as_nanos(),
            "set table-level retention policy"
        );
        let (db, table) = self.active_table(db_name, table_name)?;

        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::SetRetentionPeriod(
                    SetRetentionPeriodLog {
                        database_name: db.name(),
                        database_id: db.id,
                        table: Some((Arc::clone(&table.table_name), table.table_id)),
                        retention_period: RetentionPeriod::Duration(duration),
                    },
                )],
            ))
        })
        .await
    }

    pub async fn clear_retention_period_for_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<OrderedCatalogBatch> {
        info!(db_name, table_name, "clear table-level retention policy");
        let (db, table) = self.active_table(db_name, table_name)?;

        self.catalog_update_with_retry(|| {
            Ok(CatalogBatch::database(
                self.time_provider.now().timestamp_nanos(),
                db.id,
                db.name(),
                vec![DatabaseCatalogOp::ClearRetentionPeriod(
                    ClearRetentionPeriodLog {
                        database_name: db.name(),
                        database_id: db.id,
                        table: Some((Arc::clone(&table.table_name), table.table_id)),
                    },
                )],
            ))
        })
        .await
    }

    pub fn commercial_license_file_path(&self) -> Path {
        let id = self.catalog_id();
        format!("{id}/commercial_license").into()
    }

    pub fn trial_or_home_license_file_path(&self) -> Path {
        let catalog_id = self.catalog_id();
        format!("{catalog_id}/trial_or_home_license").into()
    }

    /// Returns the active (not deleted) database schema and table definition for the
    /// given database and table names.
    fn active_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<(Arc<DatabaseSchema>, Arc<TableDefinition>)> {
        let Some(db) = self.db_schema(db_name) else {
            return Err(CatalogError::NotFound(db_name.to_string()));
        };

        // Checking `deleted` is sufficient here. We include both `deleted` and `hard_delete_time`
        // checks to prevent future regressions.
        if db.deleted || db.hard_delete_time.is_some() {
            return Err(CatalogError::AlreadyDeleted(db_name.to_string()));
        }

        let Some(table) = db.table_definition(table_name) else {
            return Err(CatalogError::NotFound(table_name.to_string()));
        };
        if table.deleted || table.hard_delete_time.is_some() {
            return Err(CatalogError::AlreadyDeleted(table_name.to_string()));
        }

        Ok((db, table))
    }

    /// Returns gen2+ durations sorted by level, if any exist in the catalog.
    pub fn compacted_generation_durations(&self) -> Option<Vec<Duration>> {
        let mut durations: Vec<(u8, Duration)> = self
            .list_generation_durations()
            .into_iter()
            .filter(|(level, _)| *level >= 2)
            .collect();
        if durations.is_empty() {
            return None;
        }
        durations.sort_by_key(|(level, _)| *level);
        Some(durations.into_iter().map(|(_, d)| d).collect())
    }
}

/// Create a token with permissions using a pre-computed hash
pub(super) async fn create_token_with_permission_and_hash(
    catalog: &Catalog,
    all_permissions: Vec<PermissionDetailsSpec>,
    token_name: String,
    hash: Vec<u8>,
    expiry_millis: Option<i64>,
) -> Result<()> {
    // Create the token - will fail if referenced databases don't exist
    catalog
        .catalog_update_with_retry(|| {
            // Lock the entire update so that nothing changes between reading and
            // writing
            let mut inner = catalog.inner.write();

            if inner.tokens.repo().contains_name(&token_name) {
                return Err(CatalogError::TokenNameAlreadyExists(token_name.clone()));
            }

            // Validate permissions and convert to the catalog format
            let mut permission_details = Vec::new();
            for perm in &all_permissions {
                // Validate resource type
                let resource_type = ResourceType::from_str(&perm.resource_type)
                    .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

                // Validate that resources exist (for databases)
                if resource_type == ResourceType::Database {
                    for db_name in &perm.resource_identifier {
                        if inner.databases.get_by_name(db_name).is_none() {
                            return Err(CatalogError::DatabaseNotFound {
                                db_name: Arc::from(db_name.clone()),
                            });
                        }
                    }
                }

                // Validate actions are valid for the resource type
                let _ = Actions::build_actions_for_type(resource_type, &perm.actions)
                    .map_err(|err| CatalogError::CannotParsePermissionForToken(err.to_string()))?;

                // Create permission details using the same string format
                let perm_detail = PermissionDetails {
                    resource_type: perm.resource_type.clone(),
                    resource_identifier: perm.resource_identifier.clone(),
                    actions: perm.actions.clone(),
                };
                permission_details.push(perm_detail);
            }

            let token_id = inner.tokens.get_and_increment_next_id();
            let created_at = catalog.time_provider.now().timestamp_millis();

            Ok(CatalogBatch::Token(TokenBatch {
                time_ns: created_at,
                ops: vec![TokenCatalogOp::CreateResourceScopedToken(
                    CreateTokenDetails {
                        token_id,
                        name: Arc::from(token_name.as_str()),
                        hash: hash.clone(),
                        created_at,
                        expiry: expiry_millis,
                        permissions: permission_details,
                    },
                )],
            }))
        })
        .await?;

    Ok(())
}

impl TokenPermissionProvider for Catalog {
    fn is_token_allowed_access(
        &self,
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Option<bool> {
        self.inner
            .read()
            .token_permissions
            .is_allowed_access(token_id, access_request)
    }
}

impl IdProvider for Catalog {
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.db_name_to_id(db_name)
    }

    fn validate_db_id(&self, db_id: &str) -> Option<DbId> {
        let db_id = db_id.parse().ok()?;
        self.db_exists(db_id).then_some(db_id)
    }
}

impl CatalogProvider for Catalog {}

impl InnerCatalog {
    pub fn apply_token_batch_enterprise(&mut self, token_batch: &TokenBatch) -> Result<bool> {
        let mut is_updated = false;
        for op in &token_batch.ops {
            is_updated |= match op {
                TokenCatalogOp::CreateAdminToken(create_admin_token_details) => {
                    // add an entry into permissions
                    self.token_permissions.add_permission(
                        ResourceType::Wildcard,
                        TokenPermissionResourceIdentifier::Wildcard,
                        create_admin_token_details.token_id,
                        ActionsBitmap::MAX,
                    );
                    true
                }
                TokenCatalogOp::RegenerateAdminToken(_) => false,
                TokenCatalogOp::CreateResourceScopedToken(create_token_details) => {
                    self.handle_token_creation(create_token_details)?;
                    true
                }
                TokenCatalogOp::DeleteToken(delete_token_details) => {
                    let token_id = self
                        .tokens
                        .delete_token(delete_token_details.token_name.to_owned())?;
                    self.token_permissions.remove(&token_id);
                    true
                }
            };
        }
        Ok(is_updated)
    }

    fn handle_token_creation(
        &mut self,
        create_token_details: &CreateTokenDetails,
    ) -> Result<(), crate::CatalogError> {
        let mut token_info = TokenInfo::new(
            create_token_details.token_id,
            Arc::clone(&create_token_details.name),
            create_token_details.hash.clone(),
            create_token_details.created_at,
            create_token_details.expiry,
        );
        let mut all_permissions = Vec::new();
        // NB: the validation has already happened when coming to this point so it's safe
        //     ignore the errors here and use `expect`. This will be tidied up when addressing
        //     issue, https://github.com/influxdata/influxdb_pro/issues/745
        for permission in &create_token_details.permissions {
            let resource_type = ResourceType::from_str(&permission.resource_type)
                .expect("resource type should be parseable");
            let allowed_actions =
                Actions::build_actions_for_type(resource_type, &permission.actions)
                    .expect("resource actions to be parseable");

            let resource_identifier = {
                let name_to_id_provider =
                    Arc::from(self.clone()) as Arc<dyn ResourceNameToIdProvider>;
                ResourceIdentifier::build_resource_ids_for_type(
                    resource_type,
                    name_to_id_provider,
                    &permission.resource_identifier,
                )
                .expect("resource identifier to be parseable")
            };

            match resource_identifier {
                ResourceIdentifier::Database(ref db_ids) => {
                    for db_id in db_ids {
                        self.token_permissions.add_permission(
                            resource_type,
                            TokenPermissionResourceIdentifier::Database(*db_id),
                            create_token_details.token_id,
                            allowed_actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifier::System(ref system_resource_identifiers) => {
                    for system_id in system_resource_identifiers {
                        self.token_permissions.add_permission(
                            resource_type,
                            TokenPermissionResourceIdentifier::System(*system_id),
                            create_token_details.token_id,
                            allowed_actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifier::Wildcard => {
                    self.token_permissions.add_permission(
                        resource_type,
                        TokenPermissionResourceIdentifier::Wildcard,
                        create_token_details.token_id,
                        allowed_actions.to_bitmap(),
                    );
                }
                _ => {
                    error!(?resource_identifier, "cannot map resource identifier");
                    panic!("unexpected resource identifier mapping");
                }
            };

            // Capture resource names for Database resources
            let resource_names = match (&resource_type, &resource_identifier) {
                (ResourceType::Database, ResourceIdentifier::Database(db_ids)) => {
                    let names_map: IndexMap<_, _> = db_ids
                        .iter()
                        .filter_map(|db_id| {
                            self.databases.id_to_name(db_id).map(|name| {
                                (
                                    db_id.to_string(),
                                    influxdb3_authz::ResourceMetadata {
                                        name: name.to_string(),
                                        deleted: false,
                                    },
                                )
                            })
                        })
                        .collect();

                    (!names_map.is_empty()).then_some(names_map)
                }
                _ => None,
            };

            all_permissions.push(Permission {
                resource_type,
                resource_identifier,
                actions: allowed_actions,
                resource_names,
            });
        }
        token_info.set_permissions(all_permissions);
        self.tokens
            .add_token(create_token_details.token_id, token_info)?;
        Ok(())
    }
}

impl ResourceNameToIdProvider for InnerCatalog {
    fn resource_name_to_id(
        &self,
        resource_type: ResourceType,
        names: &[String],
    ) -> Result<influxdb3_authz::ResourceIdentifier, ResourceMappingError> {
        let mut contains_all_resource_indicator = false;
        for name in names {
            if name == "*" {
                contains_all_resource_indicator = true;
                break;
            }
        }

        if contains_all_resource_indicator && names.len() > 1 {
            return Err(ResourceMappingError::MixedWildcardAndRegularResourceName);
        }

        if contains_all_resource_indicator {
            Ok(ResourceIdentifier::Wildcard)
        } else {
            match resource_type {
                ResourceType::Database => {
                    let ids = names.iter().try_fold(
                        Vec::with_capacity(names.len()),
                        |mut ids, name| {
                            let id = self.databases.name_to_id(name).ok_or_else(|| {
                                ResourceMappingError::InvalidResourceName(name.to_string())
                            })?;
                            ids.push(id);
                            Ok(ids)
                        },
                    )?;
                    Ok(ResourceIdentifier::Database(ids))
                }
                ResourceType::System => {
                    let ids = names.iter().try_fold(
                        Vec::with_capacity(names.len()),
                        |mut ids, name| {
                            let id = match name.to_lowercase().trim() {
                                "health" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH)
                                }
                                "ping" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::PING)
                                }
                                "metrics" => SystemResourceIdentifier::from(
                                    SystemResourceIdentifier::METRICS,
                                ),
                                "ready" => {
                                    SystemResourceIdentifier::from(SystemResourceIdentifier::READY)
                                }
                                _ => {
                                    return Err(ResourceMappingError::InvalidResourceName(
                                        name.to_string(),
                                    ));
                                }
                            };
                            ids.push(id);
                            Ok(ids)
                        },
                    )?;
                    Ok(ResourceIdentifier::System(ids))
                }
                ResourceType::Wildcard => Ok(ResourceIdentifier::Wildcard),
                ResourceType::Token => unimplemented!(),
            }
        }
    }
}

impl ResourceIdToNameProvider for InnerCatalog {
    fn resource_id_to_name(
        &self,
        identifier: &ResourceIdentifier,
    ) -> Vec<std::result::Result<String, ResourceMappingError>> {
        match identifier {
            ResourceIdentifier::Database(db_ids) => db_ids
                .iter()
                .map(|id| {
                    self.databases
                        .id_to_name(id)
                        .ok_or_else(|| ResourceMappingError::MissingResourceId(id.to_string()))
                        .map(|name| name.to_string())
                })
                .collect(),
            ResourceIdentifier::System(system_resource_identifiers) => system_resource_identifiers
                .iter()
                .map(|id| Ok(id.to_string()))
                .collect(),
            ResourceIdentifier::Wildcard => vec![Ok("*".to_string())],
            _ => unimplemented!(),
        }
    }
}

pub(crate) fn hydrate_token_permissions(
    snap: &RepositorySnapshot<TokenId, TokenInfoSnapshot>,
) -> TokenPermissions {
    // token_permissions is an in memory repr, this is used for quick lookups when checking
    // perms for a token, so it is not saved to catalog and instead we hydrate this field when
    // loading a snapshot.
    let mut token_perms = TokenPermissions::new();
    snap.repo.iter().for_each(|(id, token)| {
        token.permissions.iter().for_each(|permission| {
            let actions = Actions::from_snapshot(permission.actions.clone());
            match &permission.resource_identifier {
                ResourceIdentifierSnapshot::Database(db_ids) => {
                    for resource_id in db_ids {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::Database(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::Token(token_ids) => {
                    for resource_id in token_ids {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::Token(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::System(system_resource_identifiers) => {
                    for resource_id in system_resource_identifiers {
                        token_perms.add_permission(
                            ResourceType::from_snapshot(permission.resource_type),
                            TokenPermissionResourceIdentifier::System(*resource_id),
                            *id,
                            actions.to_bitmap(),
                        );
                    }
                }
                ResourceIdentifierSnapshot::Wildcard => {
                    token_perms.add_permission(
                        ResourceType::from_snapshot(permission.resource_type),
                        TokenPermissionResourceIdentifier::Wildcard,
                        *id,
                        actions.to_bitmap(),
                    );
                }
            };
        });
    });
    token_perms
}

impl NodeModes {
    pub fn is_compactor(&self) -> bool {
        self.0.contains(&NodeMode::Compact) || self.0.contains(&NodeMode::All)
    }

    pub fn is_ingester(&self) -> bool {
        self.0.contains(&NodeMode::Ingest) || self.0.contains(&NodeMode::All)
    }

    pub fn is_querier(&self) -> bool {
        self.0.contains(&NodeMode::Query)
            || self.0.contains(&NodeMode::All)
            || self.0.contains(&NodeMode::Process)
    }
    pub fn is_processor(&self) -> bool {
        self.0.contains(&NodeMode::Process) || self.0.contains(&NodeMode::All)
    }

    pub fn contains(&self, mode: &NodeMode) -> bool {
        self.0.contains(mode)
    }

    pub fn contains_only(&self, mode: &NodeMode) -> bool {
        self.0.len() == 1 && self.0.contains(mode)
    }

    pub fn into_iter(&self) -> btree_set::IntoIter<NodeMode> {
        self.0.clone().into_iter()
    }
}

#[cfg(test)]
mod tests;
