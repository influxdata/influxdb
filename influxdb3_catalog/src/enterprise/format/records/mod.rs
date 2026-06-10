//! Enterprise record type definitions.
//!
//! # Enterprise Records (RecordId::enterprise)
//!
//! - **Token (e1)**: CreateResourceScopedToken
//! - **Table retention (e2-e3)**: SetTableRetentionPeriod, ClearTableRetentionPeriod
//! - **OAuth login identity (e4-e5)**: CreateLoginIdentityOAuth, DeleteLoginIdentityOAuth

mod login_identity_oauth;
mod retention;
mod token;

pub use login_identity_oauth::{CreateLoginIdentityOAuth, DeleteLoginIdentityOAuth};
pub use retention::{ClearTableRetentionPeriod, SetTableRetentionPeriod};
pub use token::CreateResourceScopedToken;
