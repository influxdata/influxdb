use std::{collections::BTreeSet, sync::Arc};

use influxdb3_id::NodeId;
use influxdb3_wal::SnapshotSequenceNumber;
use serde::{Deserialize, Serialize};

use crate::{format::FeatureLevel, resource::CatalogResource};

/// The definition of a node in the catalog
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct NodeDefinition {
    /// User-provided, unique name for the node
    ///
    /// # Note
    ///
    /// The naming may be a bit confusing for this. This may be more aptly named `node_name`;
    /// however, it is `node_id`, because this corresponds to the user-provided `--node-id` that is
    /// used to identify the node on server start. The unique and automatically generated catalog
    /// identifier for the node is stored in `node_catalog_id`.
    pub(crate) node_id: Arc<str>,
    /// Unique identifier for the node in the catalog
    pub(crate) node_catalog_id: NodeId,
    /// A UUID generated when the node is first registered into the catalog
    pub(crate) instance_id: Arc<str>,
    /// The mode the node is operating in
    pub(crate) mode: Vec<NodeMode>,
    /// The number of cores this node is using
    pub(crate) core_count: u64,
    /// The state of the node
    pub(crate) state: NodeState,
    /// Connection information for this node (format: host:port)
    /// Used by other nodes in the cluster to connect to this node
    pub(crate) conn_info: Option<Arc<str>>,
    /// CLI parameters used to start this node (non-sensitive params only, JSON format)
    pub(crate) cli_params: Option<Arc<str>>,
    /// Lowest row-delete-predicate version this node understands. Carried
    /// through from the v2 catalog; not yet acted on by the v3 runtime.
    pub(crate) row_delete_predicate_version: u64,
    /// Feature level of the process that registered the node. `FeatureLevel::ZERO` implies that
    /// the node was registered prior to the availability of this feature.
    pub(crate) feature_level: FeatureLevel,
}

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

impl NodeDefinition {
    pub fn instance_id(&self) -> Arc<str> {
        Arc::clone(&self.instance_id)
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    pub fn node_catalog_id(&self) -> NodeId {
        self.node_catalog_id
    }

    pub fn modes(&self) -> &Vec<NodeMode> {
        &self.mode
    }

    pub fn is_running(&self) -> bool {
        self.state.is_running()
    }

    pub fn is_removing(&self) -> bool {
        self.state.is_removing()
    }

    /// Whether an incoming `RegisterNode` for this node may be accepted.
    ///
    /// Per-state policy:
    /// - `Stopped`: any `instance_id` may take over (new process replacing the old one).
    /// - `Running` or `Stopping`: only the same `instance_id` (covers idempotent
    ///   retry and ungraceful shutdown).
    /// - `Removing`: terminal; re-registration is rejected.
    pub fn can_re_register(&self, incoming_instance_id: &str) -> bool {
        match &self.state {
            NodeState::Stopped { .. } => true,
            NodeState::Running { .. } | NodeState::Stopping { .. } => {
                self.instance_id.as_ref() == incoming_instance_id
            }
            NodeState::Removing { .. } => false,
        }
    }

    pub fn core_count(&self) -> u64 {
        self.core_count
    }

    pub fn state(&self) -> NodeState {
        self.state
    }

    pub fn is_ingest(&self) -> bool {
        self.mode
            .iter()
            .any(|mode| matches!(mode, NodeMode::All | NodeMode::Ingest))
    }

    pub fn is_query(&self) -> bool {
        self.mode
            .iter()
            .any(|mode| matches!(mode, NodeMode::All | NodeMode::Query))
    }

    pub fn conn_info(&self) -> Option<Arc<str>> {
        self.conn_info.clone()
    }

    pub fn cli_params(&self) -> Option<Arc<str>> {
        self.cli_params.clone()
    }

    pub fn row_delete_predicate_version(&self) -> u64 {
        self.row_delete_predicate_version
    }
}

/// The state of a node in an InfluxDB 3 cluster
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum NodeState {
    /// A node is set to `Running` when first started and registered into the catalog
    Running { registered_time_ns: i64 },
    /// A node is in `Stopping` when a graceful shutdown has been initiated but not yet acknowledged
    Stopping { stopped_time_ns: i64 },
    /// A node is set to `Stopped` once it has acknowledged its final snapshot during shutdown
    Stopped {
        stopped_time_ns: i64,
        ack_time_ns: i64,
        final_snapshot_sequence: Option<SnapshotSequenceNumber>,
    },
    /// A node is in `Removing` when it has been marked for permanent removal from the cluster
    Removing {
        requested_time_ns: i64,
        final_snapshot_sequence: Option<SnapshotSequenceNumber>,
    },
}

impl NodeState {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    /// True only for the terminal `Stopped` variant. Use `!self.is_running()`
    /// for the broader "not Running" question.
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }

    pub fn is_removing(&self) -> bool {
        matches!(self, Self::Removing { .. })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running { .. } => "running",
            Self::Stopping { .. } => "stopping",
            Self::Stopped { .. } => "stopped",
            Self::Removing { .. } => "removing",
        }
    }

    pub fn updated_at_ns(&self) -> i64 {
        match self {
            Self::Running { registered_time_ns } => *registered_time_ns,
            Self::Stopping { stopped_time_ns } => *stopped_time_ns,
            Self::Stopped { ack_time_ns, .. } => *ack_time_ns,
            Self::Removing {
                requested_time_ns, ..
            } => *requested_time_ns,
        }
    }

    pub fn final_snapshot_sequence(&self) -> Option<SnapshotSequenceNumber> {
        match self {
            Self::Stopped {
                final_snapshot_sequence,
                ..
            }
            | Self::Removing {
                final_snapshot_sequence,
                ..
            } => *final_snapshot_sequence,
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum NodeMode {
    Core,
    // Enterprise Only:
    Query,
    Ingest,
    Compact,
    Process,
    All,
}

impl NodeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeMode::Core => "core",
            NodeMode::Query => "query",
            NodeMode::Ingest => "ingest",
            NodeMode::Compact => "compact",
            NodeMode::Process => "process",
            NodeMode::All => "all",
        }
    }
}

impl std::fmt::Display for NodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct NodeModes(pub(crate) BTreeSet<NodeMode>);

impl From<Vec<NodeMode>> for NodeModes {
    fn from(ns: Vec<NodeMode>) -> Self {
        let mut s = BTreeSet::new();

        for n in ns {
            s.insert(n);
        }

        NodeModes(s)
    }
}

impl NodeModes {
    pub fn is_ingester(&self) -> bool {
        self.0.contains(&NodeMode::Ingest) || self.0.contains(&NodeMode::All)
    }

    pub fn is_querier(&self) -> bool {
        self.0.contains(&NodeMode::Query) || self.0.contains(&NodeMode::All)
    }

    pub fn is_compactor(&self) -> bool {
        self.0.contains(&NodeMode::Compact) || self.0.contains(&NodeMode::All)
    }

    pub fn is_processor(&self) -> bool {
        self.0.contains(&NodeMode::Process) || self.0.contains(&NodeMode::All)
    }

    pub fn is_all(&self) -> bool {
        self.0.contains(&NodeMode::All)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum NodeSpec {
    #[default]
    All,
    // Enterprise-only
    Nodes(Vec<NodeId>),
}

impl NodeSpec {
    pub fn matches_node(&self, node: &NodeDefinition) -> bool {
        match self {
            Self::All => true,
            Self::Nodes(v) => v.contains(&node.node_catalog_id()),
        }
    }
}

impl From<&NodeSpec> for crate::log::NodeSpec {
    fn from(value: &NodeSpec) -> Self {
        match value {
            NodeSpec::All => Self::All,
            NodeSpec::Nodes(ids) => Self::Nodes(ids.clone()),
        }
    }
}

impl From<NodeMode> for crate::log::NodeMode {
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

impl From<crate::log::NodeMode> for NodeMode {
    fn from(value: crate::log::NodeMode) -> Self {
        match value {
            crate::log::NodeMode::Core => Self::Core,
            crate::log::NodeMode::Query => Self::Query,
            crate::log::NodeMode::Ingest => Self::Ingest,
            crate::log::NodeMode::Compact => Self::Compact,
            crate::log::NodeMode::Process => Self::Process,
            crate::log::NodeMode::All => Self::All,
        }
    }
}

#[cfg(test)]
mod node_definition_tests;
#[cfg(test)]
mod node_state_tests;
