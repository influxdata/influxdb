use super::{request::Request, Result};
use http::{HeaderMap, Response};
use hyper::Body;
use kube_core::ApiResource;
use std::fmt::Debug;
use std::sync::Arc;

pub trait Handler: Debug {
    fn api_resource(&self) -> ApiResource;

    fn handle(&self, req: Request, header: HeaderMap, body: Vec<u8>) -> Result<Response<Body>>;
}

pub trait AsHandler {
    fn as_handler(self: &Arc<Self>) -> Arc<dyn Handler + Send + Sync>;
}

impl<T> AsHandler for T
where
    T: Handler + Send + Sync + 'static,
{
    fn as_handler(self: &Arc<Self>) -> Arc<dyn Handler + Send + Sync> {
        Arc::clone(self) as Arc<dyn Handler + Send + Sync>
    }
}
