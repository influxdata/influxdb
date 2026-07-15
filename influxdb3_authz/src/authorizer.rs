use std::{fmt::Debug, sync::Arc};

use crate::role::{
    self, ResourceIdentifier as RoleResourceIdentifier, RoleAction, SystemAction, SystemResource,
    role_permissions::{DatabasePermission, SystemPermission},
};
use async_trait::async_trait;
use authz::{
    Authorization as IoxAuthorization, Authorizer as IoxAuthorizer, Error as IoxError,
    Permission as IoxPermission, Target,
};
use influxdb3_id::{DbId, TokenId};
use observability_deps::tracing::{error, trace};

use crate::{
    AccessRequest, AuthProvider, AuthenticatorError, DatabaseActions, ResourceAuthorizationError,
    Subject, SystemActions, SystemResourceIdentifier,
};

pub const _INTERNAL_DB_ID: u32 = 0;

pub trait IdProvider: Send + Sync + Debug + 'static {
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId>;
    fn validate_db_id(&self, db_id: &str) -> Option<DbId>;
}

pub trait TokenPermissionProvider: Send + Sync + Debug + 'static {
    /// This internally checks if the token has fine grained permission that allows access to the
    /// resource in AccessRequest _and_ also checks if token has admin privileges in which case the
    /// token naturally has access to _any_ AccessRequest
    fn is_token_allowed_access(
        &self,
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Option<bool>;
}

pub trait CatalogProvider:
    Send + Sync + Debug + 'static + TokenPermissionProvider + IdProvider
{
}

#[derive(Clone, Debug)]
pub struct TokenAuthenticatorAndAuthorizer {
    authenticator: Arc<dyn AuthProvider>,
    catalog_provider: Arc<dyn CatalogProvider>,
}

impl TokenAuthenticatorAndAuthorizer {
    pub fn new(
        authenticator: Arc<dyn AuthProvider>,
        catalog_provider: Arc<dyn CatalogProvider>,
    ) -> Self {
        Self {
            authenticator,
            catalog_provider,
        }
    }
}

#[async_trait]
impl AuthProvider for TokenAuthenticatorAndAuthorizer {
    async fn authenticate(
        &self,
        unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError> {
        self.authenticator.authenticate(unhashed_token).await
    }

    async fn authorize_action(
        &self,
        subject: Option<&Subject>,
        access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError> {
        let subject = subject.ok_or(ResourceAuthorizationError::Unauthorized)?;
        match subject {
            Subject::Token(token_id) => {
                trace!(?token_id, ?access_request, "checking token access");
                if let AccessRequest::MaybeDatabase(Some(db_id), _database_actions) = access_request
                {
                    // _internal db requires admin perm
                    if db_id == DbId::from(_INTERNAL_DB_ID) {
                        return self
                            .authorize_action(Some(subject), AccessRequest::Admin)
                            .await;
                    }
                }

                let is_authorized = self
                    .catalog_provider
                    .is_token_allowed_access(token_id, access_request);
                match is_authorized {
                    Some(allowed) => {
                        trace!(?token_id, ?allowed, "token allowed access");
                        if !allowed {
                            return Err(ResourceAuthorizationError::Unauthorized);
                        }
                    }
                    None => {
                        trace!(?token_id, "token not present, not allowed access");
                        return Err(ResourceAuthorizationError::Unauthorized);
                    }
                };
                Ok(())
            }
            Subject::User {
                user_id,
                permissions,
            } => {
                trace!(?user_id, ?access_request, "checking user access");
                let authorized = match access_request {
                    AccessRequest::Admin => permissions
                        .as_slice()
                        .contains(&role::role_permissions::Permission::AccountAdminAll),
                    AccessRequest::Database(db_id, actions) => {
                        check_user_database_access(permissions, Some(db_id), actions)
                    }
                    AccessRequest::MaybeDatabase(db_id, actions) => {
                        if let Some(id) = db_id
                            && id == DbId::from(_INTERNAL_DB_ID)
                        {
                            return self
                                .authorize_action(Some(subject), AccessRequest::Admin)
                                .await;
                        }
                        check_user_database_access(permissions, db_id, actions)
                    }
                    AccessRequest::User(action) => {
                        let required = role::role_permissions::UserPermission::new(action);
                        permissions.has_permission(&required)
                    }
                    AccessRequest::Role(action) => match &action {
                        // All authenticated users have access to read all roles in the instance
                        RoleAction::Read => true,
                        _ => {
                            let required = role::role_permissions::RolePermission::new(action);
                            permissions.has_permission(&required)
                        }
                    },
                    AccessRequest::AdminToken(action) => {
                        let required = role::role_permissions::AdminTokenPermission::new(action);
                        permissions.has_permission(&required)
                    }
                    AccessRequest::ResourceToken(action) => {
                        let required = role::role_permissions::TokenPermission::new(action);
                        permissions.has_permission(&required)
                    }
                    AccessRequest::System(resource_id, actions) => {
                        check_user_system_access(permissions, resource_id, actions)
                    }
                    AccessRequest::AnyDatabase(actions) => {
                        check_user_database_access(permissions, None, actions)
                    }
                    AccessRequest::Token(_token_id, _crud_actions) => unimplemented!(),
                };
                if authorized {
                    Ok(())
                } else {
                    Err(ResourceAuthorizationError::Unauthorized)
                }
            }
        }
    }

    fn should_check_token(&self) -> bool {
        true
    }

    fn upcast(&self) -> Arc<dyn IoxAuthorizer> {
        let cloned_self = (*self).clone();
        Arc::new(cloned_self) as _
    }
}

#[derive(Debug)]
struct NotImplementedError;

impl std::fmt::Display for NotImplementedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not implemented")
    }
}

impl std::error::Error for NotImplementedError {}

#[async_trait]
impl IoxAuthorizer for TokenAuthenticatorAndAuthorizer {
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<IoxAuthorization, IoxError> {
        match self.authenticate(token).await {
            Ok(token_id) => {
                let mut authorized_perms = Vec::new();
                let mut all_failed = true;

                // Check each permission individually
                for perm in perms {
                    match perm {
                        IoxPermission::ResourceAction(resource, action) => {
                            // Extract database target
                            let authz::Resource::Database(target) = resource;

                            let db_id = match target {
                                Target::ResourceName(name) => {
                                    self.catalog_provider.db_name_to_id(name)
                                }
                                Target::ResourceId(id) => self.catalog_provider.validate_db_id(id),
                            };

                            let action_bits = match action {
                                authz::Action::Create | authz::Action::ReadSchema => {
                                    return Err(IoxError::verification(
                                        "create and read_schema actions not supported",
                                        Box::new(NotImplementedError),
                                    ));
                                }
                                authz::Action::Read => DatabaseActions::READ,
                                authz::Action::Write => DatabaseActions::WRITE,
                                authz::Action::Describe => DatabaseActions::DESCRIBE,
                                authz::Action::Delete => DatabaseActions::DELETE,
                            };

                            let res = self
                                .authorize_action(
                                    Some(&Subject::Token(token_id)),
                                    AccessRequest::MaybeDatabase(
                                        db_id,
                                        DatabaseActions::from(action_bits),
                                    ),
                                )
                                .await;

                            match res {
                                Ok(()) => {
                                    trace!(?perm, "permission authorized");
                                    authorized_perms.push(perm.clone());
                                    all_failed = false;
                                }
                                Err(err) => {
                                    trace!(?perm, ?err, "permission denied");
                                }
                            }
                        }
                    }
                }

                // If all permissions failed, return Forbidden
                if all_failed {
                    let authorization = IoxAuthorization::new(None, perms.to_vec());
                    error!("all permissions denied");
                    return Err(IoxError::Forbidden { authorization });
                }

                // Return authorization with only the permitted subset
                let authorization = IoxAuthorization::new(None, authorized_perms);
                trace!(
                    authorized_count = authorization.permissions().len(),
                    total_requested = perms.len(),
                    "authorization complete"
                );
                Ok(authorization)
            }
            Err(err) => {
                let iox_err = err.into();
                Err(iox_err)
            }
        }
    }
}

fn check_user_database_access(
    permissions: &role::Permissions,
    db_id: Option<DbId>,
    actions: DatabaseActions,
) -> bool {
    let resource = match db_id {
        Some(id) => RoleResourceIdentifier::Identifier(id),
        None => RoleResourceIdentifier::All,
    };

    actions
        .to_database_actions()
        .iter()
        .all(|action| permissions.has_permission(&DatabasePermission::new(*action, resource)))
}

fn check_user_system_access(
    permissions: &role::Permissions,
    resource_id: SystemResourceIdentifier,
    actions: SystemActions,
) -> bool {
    let Ok(resource) = SystemResource::try_from(resource_id) else {
        return false;
    };

    let requested_actions = SystemAction::from_bitmap(actions);
    if requested_actions.is_empty() {
        return false;
    }

    requested_actions.into_iter().all(|action| {
        permissions.has_permission(&SystemPermission::new(
            action,
            RoleResourceIdentifier::Identifier(resource),
        ))
    })
}

#[cfg(test)]
mod tests;
