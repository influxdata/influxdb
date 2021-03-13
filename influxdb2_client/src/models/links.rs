use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Links {
    /// URI of resource.
    #[serde(rename = "next", skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// URI of resource.
    #[serde(rename = "self")]
    pub _self: String,
    /// URI of resource.
    #[serde(rename = "prev", skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
}

impl Links {
    pub fn new(_self: String) -> Links {
        Links {
            next: None,
            _self,
            prev: None,
        }
    }
}
