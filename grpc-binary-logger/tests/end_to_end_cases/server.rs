use futures::Stream;
use std::pin::Pin;
use tonic::{metadata::MetadataValue, Request, Response, Status};

use grpc_binary_logger_test_proto::{test_server, TestRequest, TestResponse};

#[derive(Debug, Clone, Copy)]
pub struct TestService;

type PinnedStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl test_server::Test for TestService {
    async fn test_unary(
        &self,
        request: Request<TestRequest>,
    ) -> Result<Response<TestResponse>, Status> {
        let request = request.into_inner();

        if request.question == 42 {
            return Err(Status::invalid_argument("The Answer is not a question"));
        }

        let mut res = tonic::Response::new(TestResponse {
            answer: request.question + 1,
        });
        res.metadata_mut().insert(
            "my-server-header",
            MetadataValue::from_static("my-server-header-value"),
        );
        Ok(res)
    }

    type TestStreamStream = PinnedStream<TestResponse>;

    async fn test_stream(
        &self,
        request: Request<TestRequest>,
    ) -> Result<Response<Self::TestStreamStream>, Status> {
        let request = request.into_inner();
        let it = (0..=request.question).map(|answer| Ok(TestResponse { answer }));
        Ok(tonic::Response::new(Box::pin(futures::stream::iter(it))))
    }
}
