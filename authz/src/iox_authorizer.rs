use async_trait::async_trait;
use generated_types::influxdata::iox::authz::v1::{self as proto, AuthorizeResponse};
use observability_deps::tracing::warn;
use snafu::Snafu;
use tonic::Response;

use super::{Authorizer, Permission};

/// Authorizer implementation using influxdata.iox.authz.v1 protocol.
#[derive(Clone, Debug)]
pub struct IoxAuthorizer {
    client:
        proto::iox_authorizer_service_client::IoxAuthorizerServiceClient<tonic::transport::Channel>,
}

impl IoxAuthorizer {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub fn connect_lazy<D>(dst: D) -> Result<Self, Box<dyn std::error::Error>>
    where
        D: TryInto<tonic::transport::Endpoint> + Send,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let ep = tonic::transport::Endpoint::new(dst)?;
        let client = proto::iox_authorizer_service_client::IoxAuthorizerServiceClient::new(
            ep.connect_lazy(),
        );
        Ok(Self { client })
    }

    async fn request(
        &self,
        token: Vec<u8>,
        requested_perms: &[Permission],
    ) -> Result<Response<AuthorizeResponse>, tonic::Status> {
        let req = proto::AuthorizeRequest {
            token,
            permissions: requested_perms
                .iter()
                .filter_map(|p| p.clone().try_into().ok())
                .collect(),
        };
        let mut client = self.client.clone();
        client.authorize(req).await
    }
}

#[async_trait]
impl Authorizer for IoxAuthorizer {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        requested_perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        let authz_rpc_result = self
            .request(token.ok_or(Error::NoToken)?, requested_perms)
            .await
            .map_err(|status| Error::Verification {
                msg: status.message().to_string(),
                source: Box::new(status),
            })?
            .into_inner();

        if !authz_rpc_result.valid {
            return Err(Error::InvalidToken);
        }

        let intersected_perms: Vec<Permission> = authz_rpc_result
            .permissions
            .into_iter()
            .filter_map(|p| match p.try_into() {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error=%e, "authz service returned incompatible permission");
                    None
                }
            })
            .collect();

        if intersected_perms.is_empty() {
            return Err(Error::Forbidden);
        }
        Ok(intersected_perms)
    }
}

/// Authorization related error.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Communication error when verifying a token.
    #[snafu(display("token verification not possible: {msg}"))]
    Verification {
        /// Message describing the error.
        msg: String,
        /// Source of the error.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Token is invalid.
    #[snafu(display("invalid token"))]
    InvalidToken,

    /// The token's permissions do not allow the operation.
    #[snafu(display("forbidden"))]
    Forbidden,

    /// No token has been supplied, but is required.
    #[snafu(display("no token"))]
    NoToken,
}

impl Error {
    /// Create new Error::Verification.
    pub fn verification(
        msg: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Verification {
            msg: msg.into(),
            source: source.into(),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::verification(value.message(), value.clone())
    }
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use test_helpers_end_to_end::Authorizer as AuthorizerServer;

    use super::*;
    use crate::{Action, Authorizer, Permission, Resource};

    const NAMESPACE: &str = "bananas";

    macro_rules! test_iox_authorizer {
        (
            $name:ident,
            token_permissions = $token_permissions:expr,
            permissions_required = $permissions_required:expr,
            want = $want:pat
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_iox_authorizer_ $name>]() {
                    let mut authz_server = AuthorizerServer::create().await;
                    let authz = IoxAuthorizer::connect_lazy(authz_server.addr())
                            .expect("Failed to create IoxAuthorizer client.");

                    let token = authz_server.create_token_for(NAMESPACE, $token_permissions);

                    let got = authz.permissions(
                        Some(token.as_bytes().to_vec()),
                        $permissions_required
                    ).await;

                    assert_matches!(got, $want);
                }
            }
        };
    }

    test_iox_authorizer!(
        ok,
        token_permissions = &["ACTION_WRITE"],
        permissions_required = &[Permission::ResourceAction(
            Resource::Database(NAMESPACE.to_string()),
            Action::Write,
        )],
        want = Ok(_)
    );

    test_iox_authorizer!(
        insufficient_perms,
        token_permissions = &["ACTION_READ"],
        permissions_required = &[Permission::ResourceAction(
            Resource::Database(NAMESPACE.to_string()),
            Action::Write,
        )],
        want = Err(Error::Forbidden)
    );

    test_iox_authorizer!(
        any_of_required_perms,
        token_permissions = &["ACTION_WRITE"],
        permissions_required = &[
            Permission::ResourceAction(Resource::Database(NAMESPACE.to_string()), Action::Write,),
            Permission::ResourceAction(Resource::Database(NAMESPACE.to_string()), Action::Create,)
        ],
        want = Ok(_)
    );

    #[tokio::test]
    async fn test_invalid_token() {
        let authz_server = AuthorizerServer::create().await;
        let authz = IoxAuthorizer::connect_lazy(authz_server.addr())
            .expect("Failed to create IoxAuthorizer client.");

        let invalid_token = b"UGLY";

        let got = authz
            .permissions(
                Some(invalid_token.to_vec()),
                &[Permission::ResourceAction(
                    Resource::Database(NAMESPACE.to_string()),
                    Action::Read,
                )],
            )
            .await;

        assert_matches!(got, Err(Error::InvalidToken));
    }
}
