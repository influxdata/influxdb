/// A gRPC request error.
///
/// This is a non-application level error returned when a gRPC request to the
/// IOx server has failed.
#[derive(Debug)]
pub struct GrpcError(tonic::transport::Error);

impl std::fmt::Display for GrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for GrpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Convert errors from the underlying gRPC client into `GrpcError` instances.
impl From<tonic::transport::Error> for GrpcError {
    fn from(v: tonic::transport::Error) -> Self {
        Self(v)
    }
}
