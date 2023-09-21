#![warn(unused_crate_dependencies)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use generated_types::i_ox_testing_server::{IOxTesting, IOxTestingServer};
use generated_types::{TestErrorRequest, TestErrorResponse};
use observability_deps::tracing::warn;

/// Concrete implementation of the gRPC IOx testing service API
struct IOxTestingService {}

#[tonic::async_trait]
impl IOxTesting for IOxTestingService {
    async fn test_error(
        &self,
        _req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, tonic::Status> {
        warn!("Got a test_error request. About to panic");
        // Purposely do not use a static string (so that the panic
        // code has to deal with aribtrary payloads). See
        // https://github.com/influxdata/influxdb_iox/issues/1953
        panic!("This {}", "is a test panic");
    }
}

pub fn make_server() -> IOxTestingServer<impl IOxTesting> {
    IOxTestingServer::new(IOxTestingService {})
}
