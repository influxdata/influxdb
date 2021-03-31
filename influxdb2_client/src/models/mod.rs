//! InfluxDB Models
//!
//! Roughly follows the OpenAPI specification

pub mod user;
pub use self::user::{User, UserLinks, Users, UsersLinks};
pub mod organization;
pub use self::organization::{Organization, OrganizationLinks, Organizations};
pub mod bucket;
pub use self::bucket::{Bucket, BucketLinks, Buckets};
pub mod onboarding;
pub use self::onboarding::{IsOnboarding, OnboardingRequest, OnboardingResponse};
pub mod links;
pub use self::links::Links;
pub mod permission;
pub use self::permission::Permission;
pub mod label;
pub use self::label::Label;
pub mod authorization;
pub use self::authorization::{Authorization, AuthorizationAllOfLinks};
pub mod resource;
pub use self::resource::Resource;
pub mod retention_rule;
pub use self::retention_rule::RetentionRule;
