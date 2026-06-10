use std::sync::Arc;

use influxdb3_id::TokenId;
use iox_time::{MockProvider, Time};
use sha2::Digest;

use crate::{
    Actions, AuthProvider, AuthenticatorError, Permission, ResourceIdentifier, ResourceType,
    TokenAuthenticator, TokenInfo, TokenProvider,
};

#[derive(Debug)]
struct MockTokenProvider {
    hashed_token: Vec<u8>,
    expired: bool,
}

impl MockTokenProvider {
    fn new(token: &str, expired: bool) -> Self {
        let hash = sha2::Sha512::digest(token);
        Self {
            hashed_token: hash.to_vec(),
            expired,
        }
    }
}

impl TokenProvider for MockTokenProvider {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<std::sync::Arc<crate::TokenInfo>> {
        if token_hash == self.hashed_token {
            if self.expired {
                Some(Arc::new(TokenInfo::new(
                    TokenId::from(0),
                    "admin-token".into(),
                    self.hashed_token.clone(),
                    1000,
                    Some(1743320379000),
                )))
            } else {
                Some(Arc::new(TokenInfo::new(
                    TokenId::from(0),
                    "admin-token".into(),
                    self.hashed_token.clone(),
                    1000,
                    Some(i64::MAX),
                )))
            }
        } else {
            None
        }
    }
}

fn token_info(permissions: Vec<Permission>) -> TokenInfo {
    let mut token = TokenInfo::new(
        TokenId::from(0),
        "a-token".into(),
        vec![],
        1000,
        Some(i64::MAX),
    );
    token.set_permissions(permissions);
    token
}

#[test]
fn admin_token_is_admin() {
    // The `*:*:*` permission shape, as stored for admin tokens.
    let token = token_info(vec![Permission {
        resource_type: ResourceType::Wildcard,
        resource_identifier: ResourceIdentifier::Wildcard,
        actions: Actions::Wildcard,
        resource_names: None,
    }]);
    assert!(token.is_admin());
}

#[test]
fn resource_scoped_token_is_not_admin() {
    // A non-wildcard resource type is never an admin token, even if the other
    // dimensions are wildcards.
    let scoped = token_info(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Wildcard,
        actions: Actions::Wildcard,
        resource_names: None,
    }]);
    assert!(!scoped.is_admin());

    // A token with no permissions is not an admin token.
    assert!(!token_info(vec![]).is_admin());
}

#[test_log::test(tokio::test)]
async fn test_authenticator_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let token_id = authenticator
        .authenticate(Some(token.as_bytes().to_vec()))
        .await
        .expect("to get token id after successful auth");
    assert_eq!(TokenId::from(0), token_id);
}

#[test_log::test(tokio::test)]
async fn test_authenticator_missing_token() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let result = authenticator
        .authenticate(Some("not-matching-token".as_bytes().to_vec()))
        .await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AuthenticatorError::InvalidToken
    ));
}

#[test_log::test(tokio::test)]
async fn test_authenticator_expired_token() {
    let time_provider = MockProvider::new(Time::from_timestamp_millis(1743420379000).unwrap());
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, true);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let result = authenticator
        .authenticate(Some("sample-token".as_bytes().to_vec()))
        .await;
    assert!(result.is_err());
    if let AuthenticatorError::ExpiredToken(expiry_time_str) = result.unwrap_err() {
        assert_eq!("2025-03-30T07:39:39+00:00", expiry_time_str);
    } else {
        panic!("not the right type of authentication error");
    }
}
