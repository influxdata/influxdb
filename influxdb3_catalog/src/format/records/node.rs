//! Node operations (record_ids 2-3, 27-30).

use std::sync::Arc;

use super::impl_bitcode_encoding;
use super::types::NodeMode;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::{
    NodeDefinition, NodeMode as SchemaNodeMode, NodeState,
};
use crate::format::FeatureLevel;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};
use influxdb3_id::NodeId;
use influxdb3_wal::SnapshotSequenceNumber;
use uuid::Uuid;

/// Register a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RegisterNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Instance identifier for this node.
    pub instance_id: String,
    /// Registration timestamp in nanoseconds.
    pub registered_time_ns: i64,
    /// Number of CPU cores available.
    pub core_count: u64,
    /// Operating modes for this node.
    pub mode: Vec<NodeMode>,
    /// Process UUID as bytes.
    pub process_uuid: [u8; 16],
    /// Optional connection information.
    pub conn_info: Option<String>,
    /// Optional CLI parameters.
    pub cli_params: Option<String>,
    /// Lowest row-delete-predicate version this node understands. Carried
    /// through from the v2 catalog; the v3 runtime sets `0`.
    pub row_delete_predicate_version: u64,
    /// Feature level of the process that registered the node.
    pub feature_level: FeatureLevel,
}

impl CatalogRecord for RegisterNode {
    const ID: RecordId = record_ids::REGISTER_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RegisterNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        let node_id: Arc<str> = Arc::from(self.node_id.as_str());
        let instance_id: Arc<str> = Arc::from(self.instance_id.as_str());
        let mode: Vec<SchemaNodeMode> = self
            .mode
            .iter()
            .copied()
            .map(SchemaNodeMode::from)
            .collect();

        if let Some(mut node) = catalog.nodes.get_by_name(&node_id) {
            // Idempotent re-registration policy is centralized on
            // `NodeDefinition::can_re_register`. This redundant apply-time
            // check exists as a defense-in-depth backstop; the authoritative
            // validation runs in the CatalogOp preparation step (#2782).
            if !node.can_re_register(&instance_id) {
                return Err(ApplyError(format!(
                    "{}: invalid node re-registration: node '{}' is {} with instance_id '{}' \
                     but received registration with instance_id '{}'",
                    Self::NAME,
                    self.node_id,
                    node.state.as_str(),
                    node.instance_id,
                    self.instance_id,
                )));
            }
            let n = Arc::make_mut(&mut node);
            n.instance_id = Arc::clone(&instance_id);
            n.mode = mode;
            n.core_count = self.core_count;
            n.state = NodeState::Running {
                registered_time_ns: self.registered_time_ns,
            };
            n.conn_info = self.conn_info.as_deref().map(Arc::from);
            n.cli_params = self.cli_params.as_deref().map(Arc::from);
            n.row_delete_predicate_version = self.row_delete_predicate_version;
            n.feature_level = self.feature_level;
            catalog.nodes.update(node_catalog_id, node)?;
        } else {
            let new_node = Arc::new(NodeDefinition {
                node_id: Arc::clone(&node_id),
                node_catalog_id,
                instance_id,
                mode,
                core_count: self.core_count,
                state: NodeState::Running {
                    registered_time_ns: self.registered_time_ns,
                },
                conn_info: self.conn_info.as_deref().map(Arc::from),
                cli_params: self.cli_params.as_deref().map(Arc::from),
                row_delete_predicate_version: self.row_delete_predicate_version,
                feature_level: self.feature_level,
            });
            catalog.nodes.insert(node_catalog_id, new_node)?;
        }
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeRegistered {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
            process_uuid: Uuid::from_bytes(self.process_uuid),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RegisterNode>()
}

/// Stop a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct StopNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Stop timestamp in nanoseconds.
    pub stopped_time_ns: i64,
    /// Process UUID as bytes.
    pub process_uuid: [u8; 16],
}

impl CatalogRecord for StopNode {
    const ID: RecordId = record_ids::STOP_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "StopNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        catalog.nodes.modify_by_id(&node_catalog_id, |n| {
            // Legacy StopNode carries no ack or final-snapshot information; use
            // `stopped_time_ns` as a synthetic ack timestamp so system tables don't
            // show epoch dates, and leave `final_snapshot_sequence` unset.
            n.state = NodeState::Stopped {
                stopped_time_ns: self.stopped_time_ns,
                ack_time_ns: self.stopped_time_ns,
                final_snapshot_sequence: None,
            };
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeStopped {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
            process_uuid: Uuid::from_bytes(self.process_uuid),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<StopNode>()
}

/// Request a graceful stop for a node. The node continues running until it
/// acknowledges the stop via [`AckStopNode`] with its final snapshot sequence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RequestStopNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Stop-request timestamp in nanoseconds.
    pub stopped_time_ns: i64,
    /// Process UUID of the issuer (for attribution / audit).
    pub process_uuid: [u8; 16],
}

impl CatalogRecord for RequestStopNode {
    const ID: RecordId = record_ids::REQUEST_STOP_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RequestStopNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        catalog.nodes.modify_by_id(&node_catalog_id, |n| {
            // Already past Running: leave the existing terminal/transitional
            // state in place rather than rewinding timestamps.
            if let NodeState::Running { .. } = n.state {
                n.state = NodeState::Stopping {
                    stopped_time_ns: self.stopped_time_ns,
                };
            }
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeRequestStop {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
            process_uuid: Uuid::from_bytes(self.process_uuid),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RequestStopNode>()
}

/// Acknowledge that a node has fully stopped (final snapshot persisted).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct AckStopNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Ack timestamp in nanoseconds.
    pub ack_time_ns: i64,
    /// Process UUID as bytes.
    pub process_uuid: [u8; 16],
    /// Final snapshot sequence persisted before the node stopped (`None`
    /// if the node never produced a snapshot).
    pub final_snapshot_sequence: Option<u64>,
}

impl CatalogRecord for AckStopNode {
    const ID: RecordId = record_ids::ACK_STOP_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "AckStopNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        catalog.nodes.modify_by_id(&node_catalog_id, |n| {
            if let NodeState::Stopping { stopped_time_ns } = n.state {
                n.state = NodeState::Stopped {
                    stopped_time_ns,
                    ack_time_ns: self.ack_time_ns,
                    final_snapshot_sequence: self
                        .final_snapshot_sequence
                        .map(SnapshotSequenceNumber::new),
                };
            }
            Ok(())
        })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeAckedStop {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
            process_uuid: Uuid::from_bytes(self.process_uuid),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<AckStopNode>()
}

/// Mark a node for permanent removal from the cluster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RemoveNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Removal-request timestamp in nanoseconds.
    pub requested_time_ns: i64,
    /// Reserved for future use (e.g., issuer attribution); writers must set to nil.
    pub process_uuid: [u8; 16],
}

impl CatalogRecord for RemoveNode {
    const ID: RecordId = record_ids::REMOVE_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RemoveNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        catalog
            .nodes
            .modify_by_id(&node_catalog_id, |n| match n.state {
                NodeState::Stopped {
                    final_snapshot_sequence,
                    ..
                } => {
                    n.state = NodeState::Removing {
                        requested_time_ns: self.requested_time_ns,
                        final_snapshot_sequence,
                    };
                    Ok(())
                }
                NodeState::Removing { .. } => Ok(()),
                _ => Err(ApplyError(format!(
                    "RemoveNode: node '{}' is not in Stopped state",
                    self.node_id
                ))),
            })
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeRemoveRequested {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RemoveNode>()
}

/// Permanently remove a node from the catalog.
///
/// Unlike sibling node records, this carries no `process_uuid`: it is emitted
/// by the compactor's `NodeRemovalDriver` after object-store cleanup, never
/// by the node being unregistered, and no watcher filters on issuer identity
/// for this terminal transition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct UnregisterNode {
    /// The node's catalog ID.
    pub node_catalog_id: u32,
    /// The node's string identifier.
    pub node_id: String,
    /// Unregister timestamp in nanoseconds.
    pub unregistered_time_ns: i64,
}

impl CatalogRecord for UnregisterNode {
    const ID: RecordId = record_ids::UNREGISTER_NODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "UnregisterNode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        let node_catalog_id = NodeId::new(self.node_catalog_id);
        if let Some(node) = catalog.nodes.get_by_id(&node_catalog_id)
            && !matches!(node.state, NodeState::Removing { .. })
        {
            return Err(ApplyError(format!(
                "UnregisterNode: node '{}' is not in Removing state",
                self.node_id
            )));
        }
        // Already-gone path is a no-op; remove() on a missing id is safe.
        catalog.nodes.remove(&node_catalog_id);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::NodeUnregistered {
            node_id: Arc::from(self.node_id.as_str()),
            node_catalog_id: NodeId::new(self.node_catalog_id),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<UnregisterNode>()
}

impl From<NodeMode> for SchemaNodeMode {
    fn from(value: NodeMode) -> Self {
        match value {
            NodeMode::Core => Self::Core,
            NodeMode::Query => Self::Query,
            NodeMode::Ingest => Self::Ingest,
            NodeMode::Compact => Self::Compact,
            NodeMode::Process => Self::Process,
            NodeMode::All => Self::All,
        }
    }
}

impl From<&SchemaNodeMode> for NodeMode {
    fn from(value: &SchemaNodeMode) -> Self {
        match value {
            SchemaNodeMode::Core => Self::Core,
            SchemaNodeMode::Query => Self::Query,
            SchemaNodeMode::Ingest => Self::Ingest,
            SchemaNodeMode::Compact => Self::Compact,
            SchemaNodeMode::Process => Self::Process,
            SchemaNodeMode::All => Self::All,
        }
    }
}

impl_bitcode_encoding!(
    RegisterNode,
    StopNode,
    RequestStopNode,
    AckStopNode,
    RemoveNode,
    UnregisterNode
);

#[cfg(test)]
mod tests;
