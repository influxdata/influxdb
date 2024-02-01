use super::{request::ApiPlural, Call, Handler, Result};
use http::{Request, Response};
use hyper::Body;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Service provides a [tower::Service] that acts like a kubernetes API server.
#[derive(Debug)]
pub struct Service {
    handlers: Arc<Mutex<HashMap<ApiPlural, Arc<dyn Handler + Send + Sync>>>>,
}

impl Service {
    pub fn new() -> Self {
        let handlers = Arc::new(Mutex::new(HashMap::new()));
        Self { handlers }
    }

    pub fn add_handler(&self, handler: Arc<dyn Handler + Send + Sync>) {
        let key = handler.api_resource().into();
        self.handlers
            .lock()
            .unwrap()
            .deref_mut()
            .insert(key, handler);
    }
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

impl tower::Service<Request<Body>> for Service {
    type Response = Response<Body>;
    type Error = super::Error;
    type Future = Call;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let req = super::request::Request::parse(&parts);
        match self.handlers.lock().unwrap().get(&req.api_plural()) {
            Some(handler) => Call::new(Some(Arc::clone(handler)), req, parts.headers, body),
            None => Call::new(None, req, parts.headers, body),
        }
    }
}
