//! Node operations: RegisterNodeOp, StopNodeOp, RequestStopNodeOp,
//! AckStopNodeOp, RemoveNodeOp, UnregisterNodeOp.

use std::sync::Arc;

use influxdb3_wal::SnapshotSequenceNumber;
use iox_time::Time;
use uuid::Uuid;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::node::{NodeDefinition, NodeMode, NodeModes, NodeState};
use crate::format::records::{
    AckStopNode, AdvanceFeatureLevel, RegisterNode, RemoveNode, RequestStopNode, StopNode,
    UnregisterNode,
};
use crate::format::{RecordBatch, derive_feature_level};
use crate::resource::CatalogResource;

// ---------------------------------------------------------------------------
// RegisterNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RegisterNodeArgs {
    pub node_id: Arc<str>,
    pub core_count: u64,
    pub mode: Vec<NodeMode>,
    pub process_uuid: Uuid,
    pub instance_id: Arc<str>,
    pub conn_info: Option<String>,
    pub cli_params: Option<String>,
    pub registered_time: Time,
    pub row_delete_predicate_version: usize,
}

pub(crate) struct RegisterNodeOp {
    node_id: Arc<str>,
}

impl CatalogOp for RegisterNodeOp {
    type Input = RegisterNodeArgs;
    type Output = Arc<NodeDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let existing = catalog.nodes.get_by_name(&args.node_id);
        if let Some(node) = &existing
            && !node.can_re_register(&args.instance_id)
        {
            return Err(CatalogError::InvalidNodeRegistration);
        }
        let node_catalog_id = existing
            .map(|n| n.node_catalog_id())
            .unwrap_or_else(|| catalog.nodes.next_id());

        records.push(&RegisterNode {
            node_catalog_id: node_catalog_id.get(),
            node_id: args.node_id.to_string(),
            instance_id: args.instance_id.to_string(),
            registered_time_ns: args.registered_time.timestamp_nanos(),
            core_count: args.core_count,
            mode: args.mode.iter().map(Into::into).collect(),
            process_uuid: args.process_uuid.into_bytes(),
            conn_info: args.conn_info.clone(),
            cli_params: args.cli_params.clone(),
            row_delete_predicate_version: u64::try_from(args.row_delete_predicate_version)
                .expect("row_delete_predicate_version exceeds u64"),
            feature_level: derive_feature_level(),
        });

        if let Some(target) = catalog.propose_new_feature_level(&args.node_id) {
            records.push(&AdvanceFeatureLevel { committed: target });
        }

        Ok(Self {
            node_id: Arc::clone(&args.node_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .nodes
            .get_by_name(&self.node_id)
            .expect("node should exist after apply")
    }
}

// ---------------------------------------------------------------------------
// StopNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct StopNodeArgs {
    pub node_id: Arc<str>,
    pub stopped_time: Time,
    pub process_uuid: Uuid,
}

pub(crate) struct StopNodeOp {
    node_id: Arc<str>,
}

impl CatalogOp for StopNodeOp {
    type Input = StopNodeArgs;
    type Output = Arc<NodeDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let node = catalog
            .nodes
            .get_by_name(&args.node_id)
            .ok_or_else(|| CatalogError::NotFound(format!("node '{}'", args.node_id)))?;

        if !node.state().is_running() {
            return Err(CatalogError::NodeAlreadyStopped {
                node_id: Arc::clone(&args.node_id),
            });
        }

        records.push(&StopNode {
            node_catalog_id: node.node_catalog_id().get(),
            node_id: args.node_id.to_string(),
            stopped_time_ns: args.stopped_time.timestamp_nanos(),
            process_uuid: args.process_uuid.into_bytes(),
        });

        Ok(Self {
            node_id: Arc::clone(&args.node_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .nodes
            .get_by_name(&self.node_id)
            .expect("node should exist after stop")
    }
}

// ---------------------------------------------------------------------------
// RequestStopNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RequestStopNodeArgs {
    pub node_id: Arc<str>,
    pub stopped_time: Time,
    pub process_uuid: Uuid,
}

pub(crate) struct RequestStopNodeOp {
    node_id: Arc<str>,
}

impl CatalogOp for RequestStopNodeOp {
    type Input = RequestStopNodeArgs;
    type Output = Arc<NodeDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let node = catalog
            .nodes
            .get_by_name(&args.node_id)
            .ok_or_else(|| CatalogError::NotFound(format!("node '{}'", args.node_id)))?;

        // Stopping, Stopped, and Removing all mean a stop is already in
        // effect: report IdempotentNoOp (HTTP 200) like RemoveNodeOp does for
        // Removing, so controllers can retry a stop without treating it as a
        // conflict.
        if !node.state().is_running() {
            return Err(CatalogError::IdempotentNoOp);
        }

        records.push(&RequestStopNode {
            node_catalog_id: node.node_catalog_id().get(),
            node_id: args.node_id.to_string(),
            stopped_time_ns: args.stopped_time.timestamp_nanos(),
            process_uuid: args.process_uuid.into_bytes(),
        });

        Ok(Self {
            node_id: Arc::clone(&args.node_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .nodes
            .get_by_name(&self.node_id)
            .expect("node should exist after request-stop")
    }
}

// ---------------------------------------------------------------------------
// AckStopNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct AckStopNodeArgs {
    pub node_id: Arc<str>,
    pub ack_time: Time,
    pub process_uuid: Uuid,
    pub final_snapshot_sequence: Option<SnapshotSequenceNumber>,
}

pub(crate) struct AckStopNodeOp {
    node_id: Arc<str>,
}

impl CatalogOp for AckStopNodeOp {
    type Input = AckStopNodeArgs;
    type Output = Arc<NodeDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let node = catalog
            .nodes
            .get_by_name(&args.node_id)
            .ok_or_else(|| CatalogError::NotFound(format!("node '{}'", args.node_id)))?;

        if !matches!(node.state(), NodeState::Stopping { .. }) {
            return Err(CatalogError::InvalidStopAck {
                node_id: Arc::clone(&args.node_id),
                current_state: node.state().as_str(),
            });
        }

        records.push(&AckStopNode {
            node_catalog_id: node.node_catalog_id().get(),
            node_id: args.node_id.to_string(),
            ack_time_ns: args.ack_time.timestamp_nanos(),
            process_uuid: args.process_uuid.into_bytes(),
            final_snapshot_sequence: args.final_snapshot_sequence.map(|s| s.as_u64()),
        });

        Ok(Self {
            node_id: Arc::clone(&args.node_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .nodes
            .get_by_name(&self.node_id)
            .expect("node should exist after ack-stop")
    }
}

// ---------------------------------------------------------------------------
// RemoveNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct RemoveNodeArgs {
    pub node_id: Arc<str>,
    pub requested_time: Time,
}

pub(crate) struct RemoveNodeOp {
    node_id: Arc<str>,
}

impl CatalogOp for RemoveNodeOp {
    type Input = RemoveNodeArgs;
    type Output = Arc<NodeDefinition>;

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let node = catalog
            .nodes
            .get_by_name(&args.node_id)
            .ok_or_else(|| CatalogError::NotFound(format!("node '{}'", args.node_id)))?;

        // Compactor mode is the single-writer primary-lease holder; removing it
        // requires more design.
        let modes = NodeModes::from(node.modes().clone());
        if modes.is_compactor() {
            return Err(CatalogError::NodeModeNotRemovable {
                node_id: Arc::clone(&args.node_id),
            });
        }

        match node.state() {
            NodeState::Stopped { .. } => {}
            NodeState::Removing { .. } => return Err(CatalogError::IdempotentNoOp),
            other => {
                return Err(CatalogError::NodeNotFullyStopped {
                    node_id: Arc::clone(&args.node_id),
                    current_state: other.as_str(),
                });
            }
        }

        if let Some(group) = catalog
            .query_groups
            .resource_iter()
            .find(|g| g.members().contains(&node.node_catalog_id()))
        {
            // The node is still a member of a query group, so it cannot be removed.
            return Err(CatalogError::NodeInQueryGroup {
                node_id: Arc::clone(&args.node_id),
                query_group_name: group.name(),
                query_group_id: group.id(),
            });
        }

        records.push(&RemoveNode {
            node_catalog_id: node.node_catalog_id().get(),
            node_id: args.node_id.to_string(),
            requested_time_ns: args.requested_time.timestamp_nanos(),
            process_uuid: [0u8; 16],
        });

        Ok(Self {
            node_id: Arc::clone(&args.node_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        catalog
            .nodes
            .get_by_name(&self.node_id)
            .expect("node should exist after remove")
    }
}

// ---------------------------------------------------------------------------
// UnregisterNode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct UnregisterNodeArgs {
    pub node_id: Arc<str>,
    pub unregistered_time: Time,
}

pub(crate) struct UnregisterNodeOp;

impl CatalogOp for UnregisterNodeOp {
    type Input = UnregisterNodeArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        let node = catalog
            .nodes
            .get_by_name(&args.node_id)
            .ok_or_else(|| CatalogError::NotFound(format!("node '{}'", args.node_id)))?;

        if !node.is_removing() {
            return Err(CatalogError::InvalidUnregister {
                node_id: Arc::clone(&args.node_id),
                current_state: node.state().as_str(),
            });
        }

        records.push(&UnregisterNode {
            node_catalog_id: node.node_catalog_id().get(),
            node_id: args.node_id.to_string(),
            unregistered_time_ns: args.unregistered_time.timestamp_nanos(),
        });

        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

#[cfg(test)]
mod tests;
