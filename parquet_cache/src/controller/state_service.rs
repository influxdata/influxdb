use super::{Error, Result};
use crate::data_types::State;
use hyper::service::Service;
use k8s_openapi::api::core::v1::Pod;
use observability_deps::tracing::debug;
use std::fmt::{Debug, Formatter};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::buffer::Buffer;
use tower::util::BoxService;
use tower::{BoxError, ServiceExt};

#[derive(Debug, Clone)]
pub struct Request {
    pub pod: Pod,
    pub port: Option<String>,
}

#[derive(Clone)]
pub struct Client {
    inner: Buffer<BoxService<Request, Option<State>, BoxError>, Request>,
}

impl Client {
    pub fn new<S>(svc: S) -> Self
    where
        S: Service<Request, Response = Option<State>> + Clone + Send + 'static,
        S::Error: Into<BoxError> + Send + Sync,
        S::Future: Future<Output = Result<Option<State>, S::Error>> + Send + 'static,
    {
        Self {
            inner: Buffer::new(BoxService::new(svc.map_err(|e| e.into())), 1024),
        }
    }

    pub async fn state(&mut self, pod: &Pod, port: &Option<String>) -> Result<Option<State>> {
        let request = Request {
            pod: pod.clone(),
            port: port.clone(),
        };
        poll_fn(|cx| (self.inner.poll_ready(cx)))
            .await
            .map_err(Error::NodeStateError)?;
        self.inner
            .call(request)
            .await
            .map_err(Error::NodeStateError)
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "pod state service client")
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new(ReqwestClient {})
    }
}

#[derive(Debug, Clone)]
struct ReqwestClient {}

impl Service<Request> for ReqwestClient {
    type Response = Option<State>;
    type Error = reqwest::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let fut = async {
            let url = req
                .pod
                .status
                .and_then(|status| status.pod_ip)
                .map(|ip_addr| match req.port {
                    Some(port) => format!("http://{ip_addr}:{port}/state"),
                    None => format!("http://{ip_addr}/state"),
                });
            debug!(url, "Getting pod state");
            if let Some(url) = url {
                let response = match reqwest::get(url).await {
                    Ok(response) => Some(response.json().await?),
                    Err(error) => {
                        debug!(
                            error = &error as &dyn std::error::Error,
                            "Error getting state"
                        );
                        if error.is_connect() {
                            None
                        } else {
                            return Err(error);
                        }
                    }
                };
                Ok(response)
            } else {
                Ok(None)
            }
        };
        Box::pin(fut)
    }
}
