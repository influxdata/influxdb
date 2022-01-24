//! gRPC service implementations for `ingester`.

use crate::handler::IngestHandler;
use std::sync::Arc;

/// This type is responsible for managing all gRPC services exposed by
/// `ingester`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: IngestHandler> {
    #[allow(dead_code)]
    ingest_handler: Arc<I>,
}

impl<I: IngestHandler> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `ingest_handler`.
    pub fn new(ingest_handler: Arc<I>) -> Self {
        Self { ingest_handler }
    }
}
