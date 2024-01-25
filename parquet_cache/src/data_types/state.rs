use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Default, Clone, PartialEq, Deserialize, Copy, Serialize, JsonSchema)]
pub struct State {
    /// The current state of the cache node.
    pub state: InstanceState,

    /// Timestamp (seconds from unix epoch) that the state last changed.
    pub state_changed: i64,

    /// The revision number of the current node set known to the cache node.
    pub current_node_set_revision: i64,

    /// The revision number of the next node set known to the cache node.
    pub next_node_set_revision: i64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Default, Copy, Clone, JsonSchema)]
pub enum InstanceState {
    #[default]
    /// Default state, prior to loading any configmap keyspace.
    #[serde(rename = "pending")]
    Pending,
    /// Have configmap, are warming, and not receiving traffic.
    ///
    /// Can still respond to `GET /state` requests (from controller).
    #[serde(rename = "warming")]
    Warming,
    /// Ready for traffic.
    ///
    /// Includes own host in `GET /keyspace` responses.
    #[serde(rename = "running")]
    Running,
    /// Response to `GET /keyspace` requests are now directing traffic elsewhere.
    ///
    /// May still have ongoing requests.
    #[serde(rename = "cooling")]
    Cooling,
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Warming => write!(f, "warming"),
            Self::Running => write!(f, "running"),
            Self::Cooling => write!(f, "cooling"),
        }
    }
}
