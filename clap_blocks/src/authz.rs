//! CLI config for request authorization.

use ::authz::{Authorizer, IoxAuthorizer};
use snafu::Snafu;
use std::{boxed::Box, sync::Arc};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Invalid authz service address {addr}: {source}"))]
    BadServiceAddress {
        addr: String,
        source: Box<dyn std::error::Error>,
    },
}

/// Configuration for optional request authorization.
#[derive(Clone, Debug, Default, clap::Parser)]
pub struct AuthzConfig {
    #[clap(long = "authz-addr", env = "INFLUXDB_IOX_AUTHZ_ADDR")]
    pub(crate) authz_addr: Option<String>,
}

impl AuthzConfig {
    /// Authorizer from the configuration.
    ///
    /// An authorizer is optional so will only be created if configured.
    /// An error will only occur when the authorizer configuration is
    /// invalid.
    pub fn authorizer(&self) -> Result<Option<Arc<dyn Authorizer>>, Error> {
        if let Some(s) = &self.authz_addr {
            IoxAuthorizer::connect_lazy(s.clone())
                .map(|c| Some(Arc::new(c) as Arc<dyn Authorizer>))
                .map_err(|e| Error::BadServiceAddress {
                    addr: s.clone(),
                    source: e,
                })
        } else {
            Ok(None)
        }
    }
}
