use futures::Stream;
use grpc_binary_logger::{BinaryLoggerLayer, FileSink, NoReflection};
use std::pin::Pin;
use tonic::{metadata::MetadataValue, transport::Server, Request, Response, Status};

use grpc_binary_logger_test_proto::{
    test_server::{self, TestServer},
    TestRequest, TestResponse,
};

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = TestService;

    println!("TestService listening on {addr}");

    // Create a binary log sink that writes length delimited binary log entries.
    let file = std::fs::File::create("/tmp/grpcgo_binarylog.bin")?;
    let sink = FileSink::new(file);
    // Create a binary logger with a given sink.
    let binlog_layer = BinaryLoggerLayer::new(sink);
    // You can provide a custom predicate that selects requests that you want to be logged.
    // The default NoReflection predicate filters out all gRPC reflection chatter.
    let binlog_layer = binlog_layer.with_predicate(NoReflection);
    // You can provide a custom logger.
    let binlog_layer = binlog_layer.with_error_logger(|e| eprintln!("grpc binlog error: {e:?}"));

    Server::builder()
        .layer(binlog_layer)
        .add_service(TestServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
