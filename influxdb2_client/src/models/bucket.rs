//! Bucket

use serde::{Deserialize, Serialize};

/// Bucket Schema
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Bucket {
    /// BucketLinks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::BucketLinks>,
    /// Bucket ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Bucket Type
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<Type>,
    /// Bucket name
    pub name: String,
    /// Bucket description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Organization ID of bucket
    #[serde(rename = "orgID", skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
    /// RP
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rp: Option<String>,
    /// Created At
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Updated At
    #[serde(rename = "updatedAt", skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// Rules to expire or retain data. No rules means data never expires.
    #[serde(rename = "retentionRules")]
    pub retention_rules: Vec<crate::models::RetentionRule>,
    /// Bucket labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<crate::models::Label>>,
}

impl Bucket {
    /// Returns instance of Bucket
    pub fn new(name: String, retention_rules: Vec<crate::models::RetentionRule>) -> Self {
        Self {
            name,
            retention_rules,
            ..Default::default()
        }
    }
}

/// Bucket Type
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Type {
    /// User
    #[serde(rename = "user")]
    User,
    /// System
    #[serde(rename = "system")]
    System,
}

/// Bucket links
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct BucketLinks {
    /// Labels
    #[serde(rename = "labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// Members
    #[serde(rename = "members", skip_serializing_if = "Option::is_none")]
    pub members: Option<String>,
    /// Organization
    #[serde(rename = "org", skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    /// Owners
    #[serde(rename = "owners", skip_serializing_if = "Option::is_none")]
    pub owners: Option<String>,
    /// Self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
    /// Write
    #[serde(rename = "write", skip_serializing_if = "Option::is_none")]
    pub write: Option<String>,
}

impl BucketLinks {
    /// Returns instance of BucketLinks
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

/// List all buckets
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Buckets {
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
    /// Buckets
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<crate::models::Bucket>>,
}

impl Buckets {
    /// Returns list of buckets
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}
