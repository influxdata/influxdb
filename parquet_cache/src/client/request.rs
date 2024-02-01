use std::{collections::HashMap, pin::Pin};

use futures::Future;
use http::uri::Scheme;
use hyper::{header::HeaderValue, Body, Method, Request, Uri};

pub type PinnedFuture<R, E> = Pin<Box<dyn Future<Output = Result<R, E>> + Send>>;

#[derive(Debug, Default)]
pub struct RawRequest {
    pub headers: HashMap<&'static str, String>,
    pub body: Body,
    pub uri_parts: http::uri::Parts,
    pub method: Method,
    pub key: Option<String>,
}

impl TryFrom<RawRequest> for Request<Body> {
    type Error = http::Error;

    fn try_from(value: RawRequest) -> Result<Self, Self::Error> {
        let RawRequest {
            headers: req_headers,
            body,
            mut uri_parts,
            method,
            key: _,
        } = value;

        // reduce unnecessary (within cluster) overhead from https
        uri_parts.scheme = Some(Scheme::HTTP);

        let mut req = Request::builder()
            .method(method)
            .uri(Uri::from_parts(uri_parts)?);

        for (k, v) in req_headers.into_iter() {
            req = req.header(
                k,
                HeaderValue::from_str(v.as_str()).map_err(http::Error::from)?,
            );
        }

        req.body(body)
    }
}
