//! gRPC service implementations for `compactor`.

use crate::handler::CompactorHandler;
use std::sync::Arc;
use thiserror::Error;

/// This type is responsible for managing all gRPC services exposed by
/// `compactor`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<C: CompactorHandler> {
    compactor_handler: Arc<C>,
}

impl<C: CompactorHandler + Send + Sync + 'static> GrpcDelegate<C> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `compactor_handler`.
    pub fn new(compactor_handler: Arc<C>) -> Self {
        Self { compactor_handler }
    }

    // TODO: add grpc service here, and register it with a add_(gated_)service call in
    //       server_type/compactor.rs:server_grpc().
}

/// Errors returnable by the Compactor gRPC service
#[derive(Clone, Copy, Debug, Error)]
pub enum Error {}

impl From<Error> for tonic::Status {
    /// Logs and converts a result from the business logic into the appropriate tonic status
    fn from(err: Error) -> Self {
        // An explicit match on the Error enum will ensure appropriate logging is handled for any
        // new error variants.
        // TODO see equivalent in ingester grpc server once we have some error types
        //match err {
        //    _ => {
        //        todo!();
        //    }
        //}
        err.to_status()
    }
}

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    // LB: this allow is to satisfy clippy; unsure why the ingester doesn't get this clippy error
    // but since this is clearly a placeholder, this note should suffice as a reminder to remove
    // this when this function is implemented for the compactor
    #[allow(clippy::wrong_self_convention)]
    fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        // TODO see equivalent in ingester grpc server once we have some error types
        Status::internal(self.to_string())
    }
}
