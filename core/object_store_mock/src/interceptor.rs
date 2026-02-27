use async_trait::async_trait;
use futures::future::BoxFuture;
use object_store::{
    ClientOptions,
    client::{HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService},
};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

pub type ResponseGenerator =
    Arc<dyn Fn(&HttpRequest) -> BoxFuture<'static, HttpResponse> + Send + Sync>;

/// For use when testing or mocking client-server interactions with a remote store
/// supported by [`object_store`](https://docs.rs/object_store).
///
/// This enables us to test store-specific behaviors which are not
/// captured by the generic object_store mock implementations, without introducing
/// external dependencies (e.g. such as an actual S3 server) into our test suite.
///
/// # Example
/// ```rust,no_run
/// use object_store_mock::interceptor::{HttpInterceptor, ResponseGenerator};
/// use object_store::aws::AmazonS3Builder;
/// use futures::future::BoxFuture;
/// use futures::FutureExt;
/// use http::StatusCode;
/// use bytes::Bytes;
/// use std::sync::Arc;
///
/// // Create a response generator that validates URIs and returns custom responses
/// let response_generator: ResponseGenerator = Arc::new(|req| {
///     let uri = req.uri().to_string();
///     async move {
///         if uri.contains("valid-bucket") {
///             let mut resp = object_store::client::HttpResponse::new(Bytes::new().into());
///             *resp.status_mut() = StatusCode::OK;
///             resp
///         } else {
///             let mut resp = object_store::client::HttpResponse::new(Bytes::from("Access Denied").into());
///             *resp.status_mut() = StatusCode::FORBIDDEN;
///             resp
///         }
///     }.boxed()
/// });
///
/// let interceptor = HttpInterceptor::new(response_generator);
///
/// let store = AmazonS3Builder::new()
///     .with_bucket_name("valid-bucket")
///     .with_region("us-east-1")
///     .with_access_key_id("test-key")
///     .with_secret_access_key("test-secret")
///     .with_http_connector(interceptor.clone())
///     .with_allow_http(true)
///     .build()
///     .unwrap();
///
/// // Use the interceptor to inspect recorded requests
/// let recorded = interceptor.get_recorded_requests();
///```
#[derive(Clone)]
pub struct HttpInterceptor {
    requests: Arc<Mutex<Vec<String>>>,
    response_generator: ResponseGenerator,
}

impl Debug for HttpInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HttpInterceptor")
    }
}

impl HttpInterceptor {
    pub fn new(response_generator: ResponseGenerator) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            response_generator,
        }
    }

    pub fn get_recorded_requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl HttpConnector for HttpInterceptor {
    fn connect(&self, _options: &ClientOptions) -> crate::Result<HttpClient> {
        let service = RequestRecorder {
            requests: Arc::clone(&self.requests),
            response_generator: Arc::clone(&self.response_generator),
        };
        Ok(HttpClient::new(service))
    }
}

/// Custom HttpService that records all requests
struct RequestRecorder {
    requests: Arc<Mutex<Vec<String>>>,
    response_generator: ResponseGenerator,
}

impl Debug for RequestRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RequestRecorder")
    }
}

#[async_trait]
impl HttpService for RequestRecorder {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Record the request URL
        let url = req.uri().to_string();
        self.requests.lock().unwrap().push(url);

        // Generate a response using the provided response generator (which is async)
        let response = (self.response_generator)(&req).await;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::FutureExt;
    use http::StatusCode;
    use http_body_util::BodyExt;
    use object_store::{
        ObjectStore, PutPayload, aws::AmazonS3Builder, memory::InMemory, path::Path,
    };

    /// A response generator that validates URIs and delegates to an inner store.
    fn response_generator(
        store: Arc<dyn ObjectStore>,
        expected_uri_prefix: String,
    ) -> ResponseGenerator {
        Arc::new(move |req: &HttpRequest| {
            let store = Arc::clone(&store);
            let expected_uri_prefix = expected_uri_prefix.clone();
            let uri = req.uri().to_string();
            let method = req.method().clone();
            // Try to get bytes if the body is contiguous, otherwise clone the entire body
            let body_for_put = req.body().clone();

            async move {
                // Check if the URI matches the expected pattern
                if !uri.starts_with(&expected_uri_prefix) {
                    // Return 403 Forbidden for invalid URIs
                    let error_body = format!(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Invalid URI: {}</Message>
    <RequestId>test-request-id</RequestId>
</Error>"#,
                        uri
                    );
                    let mut resp = HttpResponse::new(error_body.into());
                    *resp.status_mut() = StatusCode::FORBIDDEN;
                    resp.headers_mut()
                        .insert("content-type", "application/xml".parse().unwrap());
                    return resp;
                }

                // For valid URIs, delegate to the store
                let test_path = Path::from(uri.split('/').next_back().unwrap());
                match method.as_str() {
                    "PUT" => {
                        let body_bytes = body_for_put.collect().await.unwrap().to_bytes();
                        // Put the test content into the inner store
                        store
                            .put(&test_path, PutPayload::from(body_bytes))
                            .await
                            .expect("Failed to put test data");

                        let mut resp = HttpResponse::new(Bytes::new().into());
                        *resp.status_mut() = StatusCode::OK;
                        resp.headers_mut()
                            .insert("etag", "\"test-etag\"".parse().unwrap());
                        resp
                    }
                    "GET" => {
                        // Return the test content
                        match store.get(&test_path).await {
                            Ok(get_result) => {
                                let test_data = get_result
                                    .bytes()
                                    .await
                                    .expect("Failed to get test data bytes");

                                let test_data_len = test_data.len();
                                let mut resp = HttpResponse::new(test_data.into());
                                *resp.status_mut() = StatusCode::OK;
                                resp.headers_mut().insert(
                                    "content-length",
                                    test_data_len.to_string().parse().unwrap(),
                                );
                                resp.headers_mut()
                                    .insert("etag", "\"test-etag\"".parse().unwrap());
                                resp
                            }
                            Err(_) => {
                                // Return 404 for not found
                                let error_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>The specified key does not exist.</Message>
</Error>"#
                                    .to_string();
                                let mut resp = HttpResponse::new(error_body.into());
                                *resp.status_mut() = StatusCode::NOT_FOUND;
                                resp.headers_mut()
                                    .insert("content-type", "application/xml".parse().unwrap());
                                resp
                            }
                        }
                    }
                    _ => {
                        // Default response
                        unimplemented!()
                    }
                }
            }
            .boxed()
        })
    }

    #[tokio::test]
    async fn test_http_interceptor_pass_thru() {
        // Create an in-memory store to use for valid requests
        let memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Put some test data in the memory store
        let test_path = Path::from("test-folder/test-file.txt");
        let test_data = Bytes::from("test content");

        // Define the expected URI pattern (note: with allow_http, bucket is in the path)
        let expected_uri_prefix = "https://s3.us-east-1.amazonaws.com/valid-bucket".to_string();

        // Create a response generator that validates the URI
        let response_generator = response_generator(memory_store, expected_uri_prefix.clone());

        // Create the interceptor
        let interceptor = HttpInterceptor::new(response_generator);

        // Create an S3 store with the interceptor
        let valid_store = AmazonS3Builder::new()
            .with_bucket_name("valid-bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-key")
            .with_secret_access_key("test-secret")
            .with_http_connector(interceptor.clone())
            .with_allow_http(true)
            .build()
            .expect("Failed to build S3 store");

        // Test: Valid URI should succeed
        let put_result = valid_store
            .put(&test_path, PutPayload::from(test_data.clone()))
            .await;
        assert!(
            put_result.is_ok(),
            "PUT request with valid URI should succeed, got: {:?}",
            put_result
        );

        // Test: should be able to get the data back
        let get_result = valid_store.get(&test_path).await;
        assert!(
            get_result.is_ok(),
            "GET request with valid URI should succeed, got: {:?}",
            get_result
        );
        let got_data = get_result
            .unwrap()
            .bytes()
            .await
            .expect("Failed to get bytes");
        assert_eq!(
            got_data, test_data,
            "GET response data should match put data"
        );

        // Check that requests were recorded
        let recorded = interceptor.get_recorded_requests();
        assert!(!recorded.is_empty(), "Should have recorded requests");
        // Verify all recorded requests start with the expected URI prefix
        for req_uri in &recorded {
            assert!(
                req_uri.starts_with(&expected_uri_prefix),
                "All requests should start with expected URI prefix, got: {}",
                req_uri
            );
        }
    }

    #[tokio::test]
    async fn test_http_interceptor_rejects_invalid_uri() {
        // Create response generator that only accepts requests to "allowed-bucket"
        let allowed_uri = "https://allowed-bucket.s3.us-west-2.amazonaws.com".to_string();

        // Create a response generator that validates the URI
        let memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let response_generator = response_generator(memory_store, allowed_uri);

        let interceptor = HttpInterceptor::new(response_generator);

        // Try to create a store with a different bucket name (should be rejected)
        let invalid_store = AmazonS3Builder::new()
            .with_bucket_name("wrong-bucket")
            .with_region("us-west-2")
            .with_access_key_id("test-key")
            .with_secret_access_key("test-secret")
            .with_http_connector(interceptor.clone())
            .with_allow_http(true)
            .build()
            .expect("Failed to build S3 store");

        // This request should fail because the URI doesn't match
        let test_path = Path::from("test.txt");
        let result = invalid_store.get(&test_path).await;

        assert!(
            result.is_err(),
            "Request to wrong bucket should fail with 403"
        );

        // Verify the error is a 403
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("403")
                || err_msg.contains("Forbidden")
                || err_msg.contains("AccessDenied"),
            "Error should indicate access denied, got: {}",
            err_msg
        );

        // Verify requests were recorded
        let recorded = interceptor.get_recorded_requests();
        assert!(
            !recorded.is_empty(),
            "Should have recorded the rejected request"
        );
        assert!(
            recorded.iter().any(|r| r.contains("wrong-bucket")),
            "Should have recorded request to wrong-bucket"
        );
    }
}
