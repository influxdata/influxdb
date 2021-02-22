use generated_types::i_ox_testing_server::{IOxTesting, IOxTestingServer};
use generated_types::{TestErrorRequest, TestErrorResponse};
use tracing::warn;

/// Concrete implementation of the gRPC IOx testing service API
struct IOxTestingService {}

#[tonic::async_trait]
impl IOxTesting for IOxTestingService {
    async fn test_error(
        &self,
        _req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, tonic::Status> {
        warn!("Got a test_error request. About to panic");
        panic!("This is a test panic");
    }
}

pub fn make_server() -> IOxTestingServer<impl IOxTesting> {
    IOxTestingServer::new(IOxTestingService {})
}
