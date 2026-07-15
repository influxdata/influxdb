//! v3 in-memory catalog state.
//!
//! [`InnerCatalog`] holds the authoritative in-memory representation of the
//! catalog. Records are applied to it via [`CatalogRecord::apply`][crate::format::CatalogRecord::apply].

use std::sync::Arc;

use influxdb3_authz::permissions::TokenPermissions;
use influxdb3_id::{DbId, NodeId, QueryGroupId};
use uuid::Uuid;

use bytes::Bytes;

use crate::catalog::{CatalogSequenceNumber, Repository};
use crate::format::Record;
use crate::format::apply::serialize_snapshot_file;

use super::schema::{
    database::DatabaseSchema,
    node::NodeDefinition,
    query_group::QueryGroupDefinition,
    role::RoleRepository,
    storage::{GenerationConfig, StorageMode},
    tokens::TokenRepository,
    user::UserRepository,
};
use crate::format::{FeatureLevel, derive_feature_level};

/// The authoritative in-memory catalog state for v3.
///
/// This struct is the target of [`CatalogRecord::apply`][crate::format::CatalogRecord::apply] — each record type
/// mutates the catalog through this struct's public fields and methods.
#[derive(Debug, Clone)]
pub struct InnerCatalog {
    /// Monotonically increasing sequence number.
    pub(crate) sequence: CatalogSequenceNumber,
    /// User-provided catalog identifier (object store prefix).
    pub(crate) catalog_id: Arc<str>,
    /// Unique identifier for this catalog instantiation.
    pub(crate) catalog_uuid: Uuid,
    /// Global storage mode.
    pub(crate) storage_mode: StorageMode,
    /// Generation duration configuration.
    pub(crate) generation_config: GenerationConfig,
    /// Cluster-wide committed feature level. Records with sequence
    /// numbers above this value (in their respective partition) are
    /// rejected on the write path unless flagged `UPGRADE_SAFE`.
    pub(crate) committed_feature_level: crate::format::FeatureLevel,
    /// Registered cluster nodes.
    pub(crate) nodes: Repository<NodeId, NodeDefinition>,
    /// Databases and their contents.
    pub(crate) databases: Repository<DbId, DatabaseSchema>,
    /// Authentication tokens.
    pub(crate) tokens: TokenRepository,
    /// Token-permission lookup index.
    pub(crate) token_permissions: TokenPermissions,
    /// User accounts.
    pub(crate) users: UserRepository,
    /// Access control roles.
    pub(crate) roles: RoleRepository,
    /// Operator-defined query groups.
    pub(crate) query_groups: Repository<QueryGroupId, QueryGroupDefinition>,
    /// Records applied to the catalog, in application order. Used as the
    /// payload when writing a snapshot.
    ///
    /// The function responsible for appending records and making sure that they're all validated
    /// and such is [`apply_records`]. That function replaces hard-delete records with their
    /// respective [`SetNextId`] records and ensures that no records referencing hard-deleted
    /// resources exist in this Vec.
    ///
    /// [`apply_records`]: crate::format::apply::apply_records
    /// [`SetNextId`]: crate::format::records::SetNextId
    pub(crate) ordered_records: Vec<Record>,
}

impl InnerCatalog {
    /// Create a new, empty catalog.
    pub fn new(catalog_id: Arc<str>, catalog_uuid: Uuid) -> Self {
        Self {
            sequence: CatalogSequenceNumber::new(0),
            catalog_id,
            catalog_uuid,
            storage_mode: StorageMode::default(),
            generation_config: GenerationConfig::default(),
            committed_feature_level: crate::format::derive_feature_level(),
            nodes: Repository::default(),
            databases: Repository::default(),
            tokens: TokenRepository::default(),
            token_permissions: TokenPermissions::default(),
            users: UserRepository::default(),
            roles: RoleRepository::default(),
            query_groups: Repository::default(),
            ordered_records: Vec::new(),
        }
    }

    /// Current sequence number.
    pub fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }

    pub fn create_snapshot(&mut self) -> Bytes {
        #[cfg(feature = "true_deletion")]
        {
            use crate::format::record_ids;

            // These records were pushed into ordered_records so that they could then be processed and
            // cause the rest of the system to delete these databases and tables, but we don't actually
            // need them anymore 'cause we should've already replaced their creation events with
            // `SetNextId` records. So we can remove them here so we don't have to worry about them anymore.
            self.ordered_records.retain(|rec| {
                ![record_ids::DELETE_DATABASE, record_ids::DELETE_TABLE].contains(&rec.id())
            });
        }

        serialize_snapshot_file(
            self.catalog_uuid,
            self.sequence.get(),
            &self.ordered_records,
        )
    }

    /// Point-in-time catalog usage counts — non-deleted, non-internal
    /// databases and their non-deleted tables. Fed to
    /// [`CatalogLimiter`][super::usage::CatalogLimiter] implementations when
    /// resolving limits for a new transaction.
    ///
    /// TODO: replace this O(databases × tables) walk with cached counters
    /// maintained by a v2-style catalog event subscriber once that lands
    /// for v3.
    pub(crate) fn current_usage(&self) -> super::usage::CurrentCatalogUsage {
        use crate::catalog::INTERNAL_DB_NAME;

        let mut total_db_count = 0;
        let mut total_table_count = 0;
        let mut total_column_count = 0;

        for db in self.databases.resource_iter() {
            if db.deleted || db.name().as_ref() == INTERNAL_DB_NAME {
                continue;
            }
            total_db_count += 1;
            for table in db.tables.resource_iter() {
                if table.deleted {
                    continue;
                }
                total_table_count += 1;
                total_column_count += table.num_field_columns() + table.num_tag_columns();
            }
        }

        super::usage::CurrentCatalogUsage::new(
            total_db_count,
            total_table_count,
            total_column_count,
        )
    }

    /// Propose the new feature level for the catalog cluster based on
    /// a consensus accross running nodes in the cluster.
    ///
    /// Returns `None` if the proposed feature level is not greater than
    /// the current catalog feature level.
    pub(crate) fn propose_new_feature_level(
        &self,
        registering_node_id: &str,
    ) -> Option<FeatureLevel> {
        let proposed = self.feature_level_consensus_of_running_nodes(registering_node_id);
        (proposed > self.committed_feature_level).then_some(proposed)
    }

    /// Determine the lowest common denominator of feature level for both
    /// Core and Enterprise across all _running_ nodes in the cluster.
    fn feature_level_consensus_of_running_nodes(&self, registering_node_id: &str) -> FeatureLevel {
        let mut min_level = derive_feature_level();
        for node in self.nodes.resource_iter() {
            // ignore the registering node, as its feature level is the
            // derived level from the local catalog record registry of
            // the running process:
            if node.node_id.as_ref() == registering_node_id {
                continue;
            }
            if !node.is_running() {
                continue;
            }
            min_level = min_level.min(&node.feature_level);
        }
        min_level
    }

    /// Counts of triggers by spec variant: (single-table-wal, all-tables-wal,
    /// schedule, request). Mirrors v2's `num_triggers` shape used by
    /// `ProcessingEngineMetrics`.
    pub fn num_triggers(&self) -> (u64, u64, u64, u64) {
        use super::schema::trigger::TriggerSpecificationDefinition as Spec;
        self.databases
            .resource_iter()
            .map(|db| {
                db.processing_engine_triggers.iter().fold(
                    (0u64, 0u64, 0u64, 0u64),
                    |(mut wal, mut all_wal, mut sched, mut req), (_, trigger)| {
                        match trigger.trigger {
                            Spec::SingleTableWalWrite { .. } => wal += 1,
                            Spec::AllTablesWalWrite => all_wal += 1,
                            Spec::Schedule { .. } | Spec::Every { .. } => sched += 1,
                            Spec::RequestPath { .. } => req += 1,
                        }
                        (wal, all_wal, sched, req)
                    },
                )
            })
            .fold(
                (0u64, 0u64, 0u64, 0u64),
                |(a1, a2, a3, a4), (b1, b2, b3, b4)| (a1 + b1, a2 + b2, a3 + b3, a4 + b4),
            )
    }
}

mod enterprise;
