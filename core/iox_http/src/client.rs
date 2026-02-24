use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use iox_http_util::RequestBody;

pub type ClientError = hyper_util::client::legacy::Error;

/// Return a hyper 0.x style client that uses the tokio runtime
pub fn hyper0_client() -> Client<HttpConnector, RequestBody> {
    Client::builder(TokioExecutor::new()).build_http()
}
