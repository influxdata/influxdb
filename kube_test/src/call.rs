use super::{request::Request, Handler, Result};
use http::{HeaderMap, Response, StatusCode};
use hyper::body::HttpBody;
use hyper::Body;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

#[derive(Debug)]
pub struct Call {
    handler: Option<Arc<dyn Handler + Send + Sync>>,
    request: Request,
    header: HeaderMap,
    body: Body,
    buf: Vec<u8>,
}

impl Call {
    pub(crate) fn new(
        handler: Option<Arc<dyn Handler + Send + Sync>>,
        request: Request,
        header: HeaderMap,
        body: Body,
    ) -> Self {
        Self {
            handler,
            request,
            header,
            body,
            buf: vec![],
        }
    }
}

impl Future for Call {
    type Output = Result<Response<Body>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &this.handler {
            None => {
                let data = serde_json::to_vec(&super::status::resource_not_found(
                    &this.request.api_plural(),
                ))
                .unwrap();
                Poll::Ready(
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(data.into())
                        .map_err(super::Error::from),
                )
            }
            Some(handler) => {
                while !&this.body.is_end_stream() {
                    match ready!(Pin::new(&mut this.body).poll_data(cx)).transpose()? {
                        Some(buf) => this.buf.extend_from_slice(buf.as_ref()),
                        None => break,
                    }
                }
                Poll::Ready(handler.handle(
                    mem::take(&mut this.request),
                    mem::take(&mut this.header),
                    mem::take(&mut this.buf),
                ))
            }
        }
    }
}
