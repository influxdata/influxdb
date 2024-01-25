use crate::data_types::State;
use k8s_openapi::schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};
use std::time::{SystemTime, UNIX_EPOCH};

use super::state::InstanceState;

/// Response body for keyspace request.
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyspaceResponseBody {
    /// Complete list of nodes for the hashring assignment of keyspace.
    pub nodes: Vec<ServiceNode>,
}

/// Identifier used by data cache node.
///
/// This identifier should remain consistent for any nodes being cycled (e.g. k8s),
/// as it determines the location in the hashring.
pub type ServiceNodeId = u64;

/// Hostname data cache node.
pub type ServiceNodeHostname = String;

/// Data cache service node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceNode {
    /// Id of data cache service node.
    pub id: ServiceNodeId,
    /// Hostname.
    pub hostname: ServiceNodeHostname,
}

/// The set of instances that form a parquet cache group.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ParquetCacheInstanceSet {
    /// The revision number of the cache instance set.
    pub revision: i64,

    /// The set of instances that form the cache set.
    pub instances: Vec<String>,
}

impl ParquetCacheInstanceSet {
    /// Returns true if the instance set is empty.
    pub fn contains(&self, node_hostname: &ServiceNodeHostname) -> bool {
        self.instances.contains(node_hostname)
    }
}

// TODO: make on-disc and in-mem representations match!
/// Converts on on-disc representation of the keyspace from the controller
/// into the keyspace respresentation consumed by the cache client & server.
impl From<&ParquetCacheInstanceSet> for KeyspaceResponseBody {
    fn from(value: &ParquetCacheInstanceSet) -> Self {
        Self {
            nodes: value
                .clone()
                .instances
                .into_iter()
                .enumerate()
                .map(|(id, hostname)| ServiceNode {
                    id: id as u64,
                    hostname,
                })
                .collect(),
        }
    }
}

impl From<&KeyspaceVersion> for InstanceState {
    fn from(value: &KeyspaceVersion) -> Self {
        match (&value.current, &value.next) {
            (Some(current), Some(next)) => {
                match (
                    current.contains(&value.self_node),
                    next.contains(&value.self_node),
                ) {
                    (false, true) => Self::Warming,
                    (true, true) => Self::Running,
                    (true, false) => Self::Cooling,
                    (false, false) => Self::Cooling,
                }
            }
            (None, Some(next)) if next.contains(&value.self_node) => Self::Warming,
            (Some(_), None) => unreachable!("next should always be set, if curr exists"),
            _ => Self::Pending,
        }
    }
}

/// Tracker of Keyspace version changes.
///
/// The response of `GET /state` is the serialized version of this struct.
#[derive(Clone, Debug)]
pub struct KeyspaceVersion {
    /// Hostname of node, in order to identify self in [`ParquetCacheInstanceSet`].
    ///
    /// Does not change.
    self_node: ServiceNodeHostname,
    /// current ParquetCacheInstanceSet
    pub current: Option<ParquetCacheInstanceSet>,
    /// next ParquetCacheInstanceSet
    pub next: Option<ParquetCacheInstanceSet>,
    /// time that the service was last updated
    pub changed: SystemTime,
}

impl Serialize for KeyspaceVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let state = State {
            state: InstanceState::from(self),
            state_changed: self
                .changed
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            current_node_set_revision: self.current.as_ref().map(|pcis| pcis.revision).unwrap_or(0),
            next_node_set_revision: self.next.as_ref().map(|pcis| pcis.revision).unwrap_or(0),
        };
        state.serialize(serializer)
    }
}

impl KeyspaceVersion {
    /// Initialize the KeyspaceVersion with only the hostname (config option) known.
    pub fn new(self_node: ServiceNodeHostname) -> Self {
        Self {
            self_node,
            current: None,
            next: None,
            changed: SystemTime::now(),
        }
    }

    /// Get hostname.
    pub fn hostname(&self) -> &ServiceNodeHostname {
        &self.self_node
    }

    /// Duplicate the `next` to `current`.
    ///
    /// This method is tightly coupled to the definition of InstanceState::from(KeyspaceVersion).
    pub fn clone_next_to_curr(&self) -> Self {
        Self {
            self_node: self.self_node.clone(),
            current: self.next.clone(),
            next: self.next.clone(),
            changed: SystemTime::now(),
        }
    }

    /// Set next.
    pub fn set_next(&self, next: ParquetCacheInstanceSet) -> Self {
        Self {
            self_node: self.self_node.clone(),
            current: self.next.clone(), // increment forward
            next: Some(next),
            changed: SystemTime::now(),
        }
    }
}
