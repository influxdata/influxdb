use std::sync::Arc;

use influxdb3_authz::TokenInfo;
use influxdb3_id::TokenId;

use crate::CatalogError;
use crate::catalog::TokenRepository;

/// Regression test: if the underlying repository rejects an insert (e.g. because an entry
/// with the same id and name already exists), the hash lookup map must not be left
/// pointing at a hash that does not correspond to any stored token.
#[test]
fn add_token_does_not_update_hash_lookup_when_repo_insert_fails() {
    let mut tokens = TokenRepository::default();

    let token_id = TokenId::new(1);
    let token_name: Arc<str> = Arc::from("token-1");
    let original_hash = b"original-hash".to_vec();
    let new_hash = b"new-hash".to_vec();

    let original = TokenInfo::new(
        token_id,
        Arc::clone(&token_name),
        original_hash.clone(),
        0,
        None,
    );
    tokens.add_token(token_id, original).unwrap();

    // Attempt to add a different token re-using the same id and name. The underlying
    // repository rejects this as a duplicate, so the hash lookup map must remain
    // consistent with the repository.
    let duplicate = TokenInfo::new(token_id, Arc::clone(&token_name), new_hash.clone(), 1, None);
    let err = tokens.add_token(token_id, duplicate).unwrap_err();
    assert!(
        matches!(err, CatalogError::AlreadyExists),
        "expected AlreadyExists, got {err:?}",
    );

    // The hash lookup map must still resolve to the original token, and must not
    // resolve the rejected hash to anything.
    assert_eq!(tokens.hash_to_id(original_hash), Some(token_id));
    assert_eq!(tokens.hash_to_id(new_hash), None);
}
