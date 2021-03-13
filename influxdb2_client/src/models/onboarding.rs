use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IsOnboarding {
    /// True means that the influxdb instance has NOT had initial setup; false
    /// means that the database has been setup.
    #[serde(rename = "allowed", skip_serializing_if = "Option::is_none")]
    pub allowed: Option<bool>,
}

impl IsOnboarding {
    pub fn new() -> IsOnboarding {
        IsOnboarding { allowed: None }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OnboardingRequest {
    #[serde(rename = "username")]
    pub username: String,
    #[serde(rename = "password", skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(rename = "org")]
    pub org: String,
    #[serde(rename = "bucket")]
    pub bucket: String,
    #[serde(
        rename = "retentionPeriodSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub retention_period_seconds: Option<i32>,
    /// Retention period *in nanoseconds* for the new bucket. This key's name
    /// has been misleading since OSS 2.0 GA, please transition to use
    /// `retentionPeriodSeconds`
    #[serde(rename = "retentionPeriodHrs", skip_serializing_if = "Option::is_none")]
    pub retention_period_hrs: Option<i32>,
}

impl OnboardingRequest {
    pub fn new(username: String, org: String, bucket: String) -> OnboardingRequest {
        OnboardingRequest {
            username,
            password: None,
            org,
            bucket,
            retention_period_seconds: None,
            retention_period_hrs: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OnboardingResponse {
    #[serde(rename = "user", skip_serializing_if = "Option::is_none")]
    pub user: Option<crate::models::User>,
    #[serde(rename = "org", skip_serializing_if = "Option::is_none")]
    pub org: Option<crate::models::Organization>,
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<crate::models::Bucket>,
    #[serde(rename = "auth", skip_serializing_if = "Option::is_none")]
    pub auth: Option<crate::models::Authorization>,
}

impl OnboardingResponse {
    pub fn new() -> OnboardingResponse {
        OnboardingResponse {
            user: None,
            org: None,
            bucket: None,
            auth: None,
        }
    }
}
