//! # Onboarding
//!
//! Initial setup of InfluxDB instance

use serde::{Deserialize, Serialize};

/// Check if database has default user, org, bucket created, returns true if
/// not.
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct IsOnboarding {
    /// True if onboarding has already been completed otherwise false
    #[serde(default)]
    pub allowed: Option<bool>,
}

impl IsOnboarding {
    /// Return instance of IsOnboarding
    pub fn new() -> Self {
        Self::default()
    }
}

/// Post onboarding request, to setup initial user, org and bucket.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnboardingRequest {
    /// Initial username
    pub username: String,
    /// Initial organization name
    pub org: String,
    /// Initial bucket name
    pub bucket: String,
    /// Initial password of user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Retention period in nanoseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_period_seconds: Option<i32>,
    /// Retention period *in nanoseconds* for the new bucket. This key's name
    /// has been misleading since OSS 2.0 GA, please transition to use
    /// `retentionPeriodSeconds`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_period_hrs: Option<i32>,
}

impl OnboardingRequest {
    /// Return instance of OnboardingRequest
    pub fn new(username: String, org: String, bucket: String) -> Self {
        Self {
            username,
            org,
            bucket,
            ..Default::default()
        }
    }
}

/// OnboardingResponse
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct OnboardingResponse {
    /// User
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<crate::models::User>,
    /// Organization
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<crate::models::Organization>,
    /// Bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<crate::models::Bucket>,
    /// Auth token
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<crate::models::Authorization>,
}

impl OnboardingResponse {
    /// Return instance of OnboardingResponse
    pub fn new() -> Self {
        Self::default()
    }
}
