use bytes::Buf;
use futures::FutureExt;
use http::{HeaderValue, StatusCode};
use test_helpers_end_to_end::{
    maybe_skip_integration, Authorizer, MiniCluster, Step, StepTest, StepTestState, TestConfig,
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
                let response = state.cluster().write_to_router("bananas", None).await;
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

#[tokio::test]
async fn authz() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";

    // Set up the authorizer  =================================
    let mut authz = Authorizer::create().await;

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_non_shared_with_authz(database_url, authz.addr()).await;

    let write_token = authz.create_token_for(cluster.namespace(), &["ACTION_WRITE"]);
    let read_token = authz.create_token_for(cluster.namespace(), &["ACTION_READ"]);

    let line_protocol = format!(
        "{table_name},tag1=A,tag2=B val=42i 123456\n\
         {table_name},tag1=A,tag2=C val=43i 123457"
    );

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocolExpectingError {
                line_protocol: line_protocol.clone(),
                expected_error_code: http::StatusCode::UNAUTHORIZED,
            },
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let token = read_token.clone();
                async move {
                    let cluster = state.cluster();
                    let authorization = format!("Token {}", token.clone());
                    let response = cluster
                        .write_to_router(
                            format!(
                                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                                 {table_name},tag1=A,tag2=C val=43i 123457"
                            ),
                            Some(authorization.as_str()),
                        )
                        .await;
                    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
                }
                .boxed()
            })),
            Step::WriteLineProtocolWithAuthorization {
                line_protocol: line_protocol.clone(),
                authorization: format!("Token {write_token}"),
            },
        ],
    )
    .run()
    .await;

    authz.close().await;
}
