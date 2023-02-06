use bytes::Buf;
use futures::FutureExt;
use http::{HeaderValue, StatusCode};
use test_helpers_end_to_end::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};
use tonic::codegen::Body;

/// The error response data structure returned by the HTTP API as a JSON-encoded
/// payload.
#[derive(Debug, serde::Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[tokio::test]
pub async fn test_json_errors() {
    let database_url = maybe_skip_integration!();

    let test_config = TestConfig::new_all_in_one(Some(database_url));
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![Step::Custom(Box::new(|state: &mut StepTestState| {
            async {
                let response = state.cluster().write_to_router("bananas").await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                assert_eq!(
                    response
                        .headers()
                        .get("content-type")
                        .expect("no content type in HTTP error response"),
                    HeaderValue::from_str("application/json").unwrap()
                );

                let body = read_body(response.into_body()).await;
                let err = serde_json::from_slice::<ErrorBody>(&body).expect("invalid JSON payload");
                assert!(!err.code.is_empty());
                assert!(!err.message.is_empty());
            }
            .boxed()
        }))],
    )
    .run()
    .await;
}

async fn read_body<T, E>(mut body: T) -> Vec<u8>
where
    T: Body<Data = bytes::Bytes, Error = E> + Unpin,
    E: std::fmt::Debug,
{
    let mut bufs = vec![];
    while let Some(buf) = body.data().await {
        let buf = buf.expect("failed to read response body");
        if buf.has_remaining() {
            bufs.extend(buf.to_vec());
        }
    }

    bufs
}
