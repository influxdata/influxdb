//! Bucket

use serde::{Deserialize, Serialize};

/// Bucket Schema
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Updated At
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// Rules to expire or retain data. No rules means data never expires.
    pub retention_rules: Vec<crate::models::RetentionRule>,
    /// Bucket labels
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<crate::models::Label>,
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
#[serde(rename_all = "camelCase")]
pub enum Type {
    /// User
    User,
    /// System
    System,
}

/// Bucket links
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketLinks {
    /// Labels
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// Members
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<String>,
    /// Organization
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    /// Owners
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owners: Option<String>,
    /// Self
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_: Option<String>,
    /// Write
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write: Option<String>,
}

impl BucketLinks {
    /// Returns instance of BucketLinks
    pub fn new() -> Self {
        Self::default()
    }
}

/// List all buckets
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Buckets {
    /// Links
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<crate::models::Links>,
    /// Buckets
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<crate::models::Bucket>,
}

impl Buckets {
    /// Returns list of buckets
    pub fn new() -> Self {
        Self::default()
    }
}

/// PostBucketRequest
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostBucketRequest {
    /// Organization ID
    #[serde(rename = "orgID")]
    pub org_id: String,
    /// Bucket name
    pub name: String,
    /// Bucket Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// RP
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rp: Option<String>,
    /// Rules to expire or retain data.  No rules means data never expires.
    #[serde(default)]
    pub retention_rules: Vec<crate::models::RetentionRule>,
}

impl PostBucketRequest {
    /// Returns instance of PostBucketRequest
    pub fn new(org_id: String, name: String) -> Self {
        Self {
            org_id,
            name,
            ..Default::default()
        }
    }
}
