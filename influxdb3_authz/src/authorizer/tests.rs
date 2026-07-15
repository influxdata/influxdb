use std::sync::Arc;

use authz::{Authorizer, Error as IoxError, Permission as IoxPermission, Resource};
use influxdb3_id::{DbId, TokenId};
use iox_time::{MockProvider, Time};
use sha2::Digest;

use crate::{
    AccessRequest, AuthProvider, CatalogProvider, DatabaseActions, IdProvider,
    ResourceAuthorizationError, TokenAuthenticator, TokenAuthenticatorAndAuthorizer, TokenInfo,
    TokenPermissionProvider, TokenProvider,
};

#[derive(Debug)]
struct MockTokenProvider {
    hashed_token: Vec<u8>,
    expired: bool,
}

impl MockTokenProvider {
    pub(crate) fn new(token: &str, expired: bool) -> Self {
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

#[derive(Debug)]
struct MockCatalogProvider(bool);

impl IdProvider for MockCatalogProvider {
    fn db_name_to_id(&self, _db_name: &str) -> Option<influxdb3_id::DbId> {
        Some(DbId::new(0))
    }

    fn validate_db_id(&self, _db_id: &str) -> Option<DbId> {
        None
    }
}

impl TokenPermissionProvider for MockCatalogProvider {
    fn is_token_allowed_access(
        &self,
        _token_id: &TokenId,
        _access_request: crate::AccessRequest,
    ) -> Option<bool> {
        Some(self.0)
    }
}

impl CatalogProvider for MockCatalogProvider {}

#[derive(Debug)]
struct MockCatalogProviderWithAccess {
    allowed_access: u16,
    db_id: Option<DbId>,
}

impl IdProvider for MockCatalogProviderWithAccess {
    fn db_name_to_id(&self, _db_name: &str) -> Option<influxdb3_id::DbId> {
        self.db_id
    }

    fn validate_db_id(&self, _db_id: &str) -> Option<DbId> {
        None
    }
}

impl TokenPermissionProvider for MockCatalogProviderWithAccess {
    fn is_token_allowed_access(
        &self,
        _token_id: &TokenId,
        access_request: crate::AccessRequest,
    ) -> Option<bool> {
        match access_request {
            AccessRequest::MaybeDatabase(_db_id, database_actions) => {
                Some(database_actions.contains(self.allowed_access))
            }
            AccessRequest::Database(_db_id, database_actions) => {
                Some(database_actions.contains(self.allowed_access))
            }
            AccessRequest::AnyDatabase(database_actions) => {
                Some(database_actions.contains(self.allowed_access))
            }
            AccessRequest::System(_system_resource_identifier, system_actions) => {
                Some(system_actions.contains(self.allowed_access))
            }
            AccessRequest::User(_)
            | AccessRequest::Role(_)
            | AccessRequest::AdminToken(_)
            | AccessRequest::ResourceToken(_)
            | AccessRequest::Token(_, _) => {
                unimplemented!()
            }
            AccessRequest::Admin => unimplemented!(),
        }
    }
}

impl CatalogProvider for MockCatalogProviderWithAccess {}

#[test_log::test(tokio::test)]
async fn test_authorizer_authorization_failed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProvider(false);
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );
    let err = authorizer
        .authorize_action(
            Some(&crate::Subject::Token(TokenId::from(0))),
            crate::AccessRequest::Admin,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ResourceAuthorizationError::Unauthorized));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_for_read_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProvider(true);
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Read,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed and get permissions");
    assert_eq!(1, authorization.permissions().len());
    match authorization
        .permissions()
        .first()
        .expect("permission to be present")
    {
        IoxPermission::ResourceAction(resource, action) => {
            matches!(resource, Resource::Database(_));
            matches!(action, authz::Action::Read);
        }
    };
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_for_read_write_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProvider(true);
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
            authz::Action::Write,
        ),
    ];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed and get permissions");

    assert_eq!(2, authorization.permissions().len());
    match authorization
        .permissions()
        .first()
        .expect("permission to be present")
    {
        IoxPermission::ResourceAction(resource, action) => {
            matches!(resource, Resource::Database(_));
            matches!(action, authz::Action::Read);
        }
    };
    match authorization
        .permissions()
        .get(1)
        .expect("permission to be present")
    {
        IoxPermission::ResourceAction(resource, action) => {
            matches!(resource, Resource::Database(_));
            matches!(action, authz::Action::Write);
        }
    };
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_write_failed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::READ,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Write,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(
        authorization.unwrap_err(),
        IoxError::Forbidden { .. }
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_read_failed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::WRITE,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Read,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(
        authorization.unwrap_err(),
        IoxError::Forbidden { .. }
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_describe_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::DESCRIBE,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Describe,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("describe permission should be granted");

    assert_eq!(1, authorization.permissions().len());
    assert!(matches!(
        authorization.permissions().first(),
        Some(IoxPermission::ResourceAction(_, action)) if matches!(action, authz::Action::Describe)
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_describe_failed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::READ,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Describe,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(
        authorization.unwrap_err(),
        IoxError::Forbidden { .. }
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_delete_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::DELETE,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Delete,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("delete permission should be granted");

    assert_eq!(1, authorization.permissions().len());
    assert!(matches!(
        authorization.permissions().first(),
        Some(IoxPermission::ResourceAction(_, action)) if matches!(action, authz::Action::Delete)
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_delete_failed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::READ,
        db_id: Some(DbId::new(1)),
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Delete,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(
        authorization.unwrap_err(),
        IoxError::Forbidden { .. }
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_missing_db_with_perm_mismatch() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::WRITE,
        db_id: None,
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Read,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(
        authorization.unwrap_err(),
        IoxError::Forbidden { .. }
    ));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_iox_authorization_missing_db_write() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let catalog_provider = MockCatalogProviderWithAccess {
        allowed_access: DatabaseActions::WRITE,
        db_id: None,
    };
    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[IoxPermission::ResourceAction(
        Resource::Database(authz::Target::ResourceName("some-db".to_owned())),
        authz::Action::Write,
    )];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .unwrap();
    match authorization
        .permissions()
        .first()
        .expect("permission to be present")
    {
        IoxPermission::ResourceAction(resource, action) => {
            matches!(resource, Resource::Database(_));
            matches!(action, authz::Action::Write);
        }
    };
}

// Multi-database authorization tests
#[derive(Debug)]
struct MockMultiDbCatalogProvider {
    db_permissions: hashbrown::HashMap<String, (Option<DbId>, DatabaseActions)>,
}

impl MockMultiDbCatalogProvider {
    fn new() -> Self {
        Self {
            db_permissions: hashbrown::HashMap::new(),
        }
    }

    fn with_db(mut self, name: &str, db_id: Option<DbId>, actions: DatabaseActions) -> Self {
        self.db_permissions
            .insert(name.to_string(), (db_id, actions));
        self
    }
}

impl IdProvider for MockMultiDbCatalogProvider {
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.db_permissions.get(db_name).and_then(|(id, _)| *id)
    }

    fn validate_db_id(&self, _db_id: &str) -> Option<DbId> {
        None
    }
}

impl TokenPermissionProvider for MockMultiDbCatalogProvider {
    fn is_token_allowed_access(
        &self,
        _token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Option<bool> {
        match access_request {
            AccessRequest::MaybeDatabase(Some(db_id), requested_actions) => {
                // Find the database with this ID
                for (_, (stored_id, allowed_actions)) in &self.db_permissions {
                    if *stored_id == Some(db_id) {
                        return Some(allowed_actions.is_allowed(&requested_actions));
                    }
                }
                None
            }
            AccessRequest::MaybeDatabase(None, _) => None,
            AccessRequest::AnyDatabase(action) => {
                // Check if any database has the requested action
                let has_permission = self
                    .db_permissions
                    .iter()
                    .any(|(_, (_, allowed_actions))| allowed_actions.is_allowed(&action));
                Some(has_permission)
            }
            _ => None,
        }
    }
}

impl CatalogProvider for MockMultiDbCatalogProvider {}

#[test_log::test(tokio::test)]
async fn test_authorizer_multi_db_partial_access() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));

    // Set up permissions: READ for db1, no access to db2, READ for db3
    let catalog_provider = MockMultiDbCatalogProvider::new()
        .with_db(
            "db1",
            Some(DbId::new(1)),
            DatabaseActions::from(DatabaseActions::READ),
        )
        .with_db("db2", Some(DbId::new(2)), DatabaseActions::from(0)) // No permissions
        .with_db(
            "db3",
            Some(DbId::new(3)),
            DatabaseActions::from(DatabaseActions::READ),
        );

    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db2".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db3".to_owned())),
            authz::Action::Read,
        ),
    ];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed with partial permissions");

    // Should only return db1 and db3 permissions
    assert_eq!(2, authorization.permissions().len());

    // Verify db1 permission
    let IoxPermission::ResourceAction(resource, action) = &authorization.permissions()[0];
    let Resource::Database(authz::Target::ResourceName(name)) = resource else {
        panic!("expected Database resource with ResourceName");
    };
    assert_eq!("db1", name);
    assert!(matches!(action, authz::Action::Read));

    // Verify db3 permission
    let IoxPermission::ResourceAction(resource, action) = &authorization.permissions()[1];
    let Resource::Database(authz::Target::ResourceName(name)) = resource else {
        panic!("expected Database resource with ResourceName");
    };
    assert_eq!("db3", name);
    assert!(matches!(action, authz::Action::Read));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_multi_db_all_denied() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));

    // No permissions for any database
    let catalog_provider = MockMultiDbCatalogProvider::new()
        // NOTE(tjh): `0` is _no_ permissions
        .with_db("db1", Some(DbId::new(1)), DatabaseActions::from(0))
        .with_db("db2", Some(DbId::new(2)), DatabaseActions::from(0));

    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db2".to_owned())),
            authz::Action::Read,
        ),
    ];

    let result = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await;

    assert!(matches!(result.unwrap_err(), IoxError::Forbidden { .. }));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_multi_db_all_allowed() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));

    // READ permissions for all databases
    let catalog_provider = MockMultiDbCatalogProvider::new()
        .with_db(
            "db1",
            Some(DbId::new(1)),
            DatabaseActions::from(DatabaseActions::READ),
        )
        .with_db(
            "db2",
            Some(DbId::new(2)),
            DatabaseActions::from(DatabaseActions::READ),
        )
        .with_db(
            "db3",
            Some(DbId::new(3)),
            DatabaseActions::from(DatabaseActions::READ),
        );

    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db2".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db3".to_owned())),
            authz::Action::Read,
        ),
    ];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed with all permissions");

    // All permissions should be returned
    assert_eq!(3, authorization.permissions().len());
}

#[test_log::test(tokio::test)]
async fn test_authorizer_multi_db_mixed_read_write() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));

    // db1: READ only, db2: WRITE only
    let catalog_provider = MockMultiDbCatalogProvider::new()
        .with_db(
            "db1",
            Some(DbId::new(1)),
            DatabaseActions::from(DatabaseActions::READ),
        )
        .with_db(
            "db2",
            Some(DbId::new(2)),
            DatabaseActions::from(DatabaseActions::WRITE),
        );

    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Write, // Should be denied
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db2".to_owned())),
            authz::Action::Read, // Should be denied
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db2".to_owned())),
            authz::Action::Write,
        ),
    ];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed with partial permissions");

    // Should return only db1:Read and db2:Write
    assert_eq!(2, authorization.permissions().len());

    // Verify db1:Read permission
    let IoxPermission::ResourceAction(resource, action) = &authorization.permissions()[0];
    let Resource::Database(authz::Target::ResourceName(name)) = resource else {
        panic!("expected Database resource with ResourceName");
    };
    assert_eq!("db1", name);
    assert!(matches!(action, authz::Action::Read));

    // Verify db2:Write permission
    let IoxPermission::ResourceAction(resource, action) = &authorization.permissions()[1];
    let Resource::Database(authz::Target::ResourceName(name)) = resource else {
        panic!("expected Database resource with ResourceName");
    };
    assert_eq!("db2", name);
    assert!(matches!(action, authz::Action::Write));
}

#[test_log::test(tokio::test)]
async fn test_authorizer_multi_db_nonexistent_database() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));

    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));

    // Only db1 exists
    let catalog_provider = MockMultiDbCatalogProvider::new().with_db(
        "db1",
        Some(DbId::new(1)),
        DatabaseActions::from(DatabaseActions::READ),
    );

    let authorizer = TokenAuthenticatorAndAuthorizer::new(
        Arc::new(authenticator),
        Arc::new(catalog_provider) as _,
    );

    let perms = &[
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("db1".to_owned())),
            authz::Action::Read,
        ),
        IoxPermission::ResourceAction(
            Resource::Database(authz::Target::ResourceName("nonexistent".to_owned())),
            authz::Action::Read,
        ),
    ];

    let authorization = authorizer
        .authorize(Some(token.as_bytes().to_vec()), perms)
        .await
        .expect("should succeed with only existing db");

    // Should only return db1 permission
    assert_eq!(1, authorization.permissions().len());
    let IoxPermission::ResourceAction(resource, _) = &authorization.permissions()[0];
    let Resource::Database(authz::Target::ResourceName(name)) = resource else {
        panic!("expected Database resource with ResourceName");
    };
    assert_eq!("db1", name);
}

#[test]
fn user_describe_grant_confers_database_visibility_but_not_read() {
    use crate::role::{
        DatabaseAction, Permission, Permissions, ResourceIdentifier,
        role_permissions::DatabasePermission,
    };

    let perms = Permissions::new(vec![Permission::Database(DatabasePermission::new(
        DatabaseAction::Describe,
        ResourceIdentifier::All,
    ))]);

    let db_id = DbId::new(1);

    assert!(super::check_user_database_access(
        &perms,
        Some(db_id),
        DatabaseActions::from(DatabaseActions::DESCRIBE),
    ));

    assert!(!super::check_user_database_access(
        &perms,
        Some(db_id),
        DatabaseActions::from(DatabaseActions::READ),
    ));
}

#[test]
fn user_system_all_read_grant_allows_specific_resources() {
    use crate::role::{
        Permission, Permissions, ResourceIdentifier, SystemAction,
        role_permissions::SystemPermission,
    };
    use crate::{SystemActions, SystemResourceIdentifier};

    let perms = Permissions::new(vec![Permission::System(SystemPermission::new(
        SystemAction::Read,
        ResourceIdentifier::All,
    ))]);

    for resource in [
        SystemResourceIdentifier::HEALTH,
        SystemResourceIdentifier::METRICS,
        SystemResourceIdentifier::PING,
        SystemResourceIdentifier::READY,
    ] {
        assert!(super::check_user_system_access(
            &perms,
            SystemResourceIdentifier::from(resource),
            SystemActions::from(SystemActions::READ),
        ));
    }
}

#[test]
fn user_system_specific_grant_only_allows_that_resource() {
    use crate::role::{
        Permission, Permissions, ResourceIdentifier, SystemAction, SystemResource,
        role_permissions::SystemPermission,
    };
    use crate::{SystemActions, SystemResourceIdentifier};

    let perms = Permissions::new(vec![Permission::System(SystemPermission::new(
        SystemAction::Read,
        ResourceIdentifier::Identifier(SystemResource::Health),
    ))]);

    assert!(super::check_user_system_access(
        &perms,
        SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH),
        SystemActions::from(SystemActions::READ),
    ));
    assert!(!super::check_user_system_access(
        &perms,
        SystemResourceIdentifier::from(SystemResourceIdentifier::METRICS),
        SystemActions::from(SystemActions::READ),
    ));
}

#[test]
fn user_without_system_grant_denied() {
    use crate::role::{Permissions, UserAction, role_permissions::UserPermission};
    use crate::{SystemActions, SystemResourceIdentifier};

    let perms = Permissions::new(vec![crate::role::Permission::User(UserPermission::new(
        UserAction::Read,
    ))]);

    assert!(!super::check_user_system_access(
        &perms,
        SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH),
        SystemActions::from(SystemActions::READ),
    ));
}

#[test]
fn user_system_unknown_resource_id_denied() {
    use crate::role::{
        Permission, Permissions, ResourceIdentifier, SystemAction,
        role_permissions::SystemPermission,
    };
    use crate::{SystemActions, SystemResourceIdentifier};

    let perms = Permissions::new(vec![Permission::System(SystemPermission::new(
        SystemAction::Read,
        ResourceIdentifier::All,
    ))]);

    assert!(!super::check_user_system_access(
        &perms,
        SystemResourceIdentifier::from(u16::MAX),
        SystemActions::from(SystemActions::READ),
    ));
}

#[test]
fn user_system_admin_all_covers_system_access() {
    use crate::role::{Permission, Permissions};
    use crate::{SystemActions, SystemResourceIdentifier};

    let perms = Permissions::new(vec![Permission::AccountAdminAll]);

    assert!(super::check_user_system_access(
        &perms,
        SystemResourceIdentifier::from(SystemResourceIdentifier::HEALTH),
        SystemActions::from(SystemActions::READ),
    ));
}
