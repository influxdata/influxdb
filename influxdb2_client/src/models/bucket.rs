use serde::{Deserialize, Serialize};

/// Bucket Schema
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::BucketLinks>,
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub _type: Option<Type>,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    #[serde(rename = "rp", skip_serializing_if = "Option::is_none")]
    pub rp: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// Rules to expire or retain data.  No rules means data never expires.
    #[serde(rename = "retentionRules")]
    pub retention_rules: Vec<crate::models::RetentionRule>,
    #[serde(rename = "labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<crate::models::Label>>,
}

impl Bucket {
    pub fn new(name: String, retention_rules: Vec<crate::models::RetentionRule>) -> Bucket {
        Bucket {
            links: None,
            id: None,
            _type: None,
            name,
            description: None,
            org_id: None,
            rp: None,
            created_at: None,
            updated_at: None,
            retention_rules,
            labels: None,
        }
    }
}

/// Bucket Type
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "user")]
    User,
    #[serde(rename = "system")]
    System,
}

///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketLinks {
    /// URI of resource.
    #[serde(rename = "labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// URI of resource.
    #[serde(rename = "members", skip_serializing_if = "Option::is_none")]
    pub members: Option<String>,
    /// URI of resource.
    #[serde(rename = "org", skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    /// URI of resource.
    #[serde(rename = "owners", skip_serializing_if = "Option::is_none")]
    pub owners: Option<String>,
    /// URI of resource.
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub _self: Option<String>,
    /// URI of resource.
    #[serde(rename = "write", skip_serializing_if = "Option::is_none")]
    pub write: Option<String>,
}

impl BucketLinks {
    pub fn new() -> BucketLinks {
        BucketLinks {
            labels: None,
            members: None,
            org: None,
            owners: None,
            _self: None,
            write: None,
        }
    }
}

/// List all buckets
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Buckets {
    #[serde(rename = "links", skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
    #[serde(rename = "buckets", skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<crate::models::Bucket>>,
}

impl Buckets {
    pub fn new() -> Buckets {
        Buckets {
            links: None,
            buckets: None,
        }
    }
}
