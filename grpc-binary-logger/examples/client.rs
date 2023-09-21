use grpc_binary_logger_test_proto::{test_client::TestClient, TestRequest};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TestClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(TestRequest { question: 41 });
    let response = client.test_unary(request).await?;
    println!("RESPONSE={response:?}");

    Ok(())
}
