use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Permission {
    #[serde(rename = "action")]
    pub action: Action,
    #[serde(rename = "resource")]
    pub resource: crate::models::Resource,
}

impl Permission {
    pub fn new(action: Action, resource: crate::models::Resource) -> Permission {
        Permission { action, resource }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Action {
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "write")]
    Write,
}
