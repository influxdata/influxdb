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
            .await?;

        let intersected_perms: Vec<Permission> = authz_rpc_result
            .into_inner()
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

        match (requested_perms, &intersected_perms[..]) {
            // used in connection `Authorizer::probe()`
            ([], _) => Ok(vec![]),
            // token does not have any of the requested_perms
            (_, []) => Err(Error::Forbidden),
            // if token has `any_of` the requested_perms => return ok
            _ => Ok(intersected_perms),
        }
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
