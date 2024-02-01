use std::{fmt::Debug, pin::Pin};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{stream::BoxStream, Future};
use http::StatusCode;
use hyper::Body;

use crate::data_types::{
    GetObjectMetaResponse, KeyspaceResponseBody, KeyspaceVersion, ServiceNode,
};

pub type PinnedFuture = Pin<Box<dyn Future<Output = Result<Response, super::error::Error>> + Send>>;

pub enum Response {
    /// Internal-only response used during pre-warming, for `PATCH /warmed`
    Ready,
    /// For `GET /keyspace`
    Keyspace(Vec<ServiceNode>),
    /// For `GET /state`
    KeyspaceVersion(KeyspaceVersion),
    /// For `GET /metadata`
    Head(GetObjectMetaResponse),
    /// For `GET /object`
    Data(BoxStream<'static, object_store::Result<Bytes>>),
    /// For `POST /write-hint`
    Written,
}

impl Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready => write!(f, "Response::Ready"),
            Self::Keyspace(k) => write!(f, "Response::Keyspace({:?})", k),
            Self::KeyspaceVersion(v) => write!(f, "Response::KeyspaceVersion({:?})", v),
            Self::Head(h) => write!(f, "Response::Head({:?})", h),
            Self::Data(_) => write!(f, "Response::Data"),
            Self::Written => write!(f, "Response::Written"),
        }
    }
}

impl Response {
    pub fn code(&self) -> StatusCode {
        match self {
            Self::Ready => {
                unreachable!("should be an internal-only Response, and not sent across the wire")
            }
            Self::Keyspace(_) | Self::KeyspaceVersion(_) | Self::Head(_) | Self::Data(_) => {
                StatusCode::OK
            }
            Self::Written => StatusCode::CREATED,
        }
    }
}

impl From<Response> for Body {
    fn from(value: Response) -> Self {
        match value {
            Response::Ready => {
                unreachable!("should be an internal-only Response, and not sent across the wire")
            }
            Response::Keyspace(nodes) => {
                Self::from(build_resp_body(&KeyspaceResponseBody { nodes }))
            }
            Response::KeyspaceVersion(version) => {
                Self::from(serde_json::json!(version).to_string())
            }
            Response::Head(data) => Self::from(build_resp_body(&data)),
            Response::Data(stream) => Self::wrap_stream(stream),
            Response::Written => Self::empty(),
        }
    }
}

fn build_resp_body<T>(body: &T) -> Bytes
where
    T: Sized + serde::Serialize,
{
    let mut buf = BytesMut::new().writer();
    serde_json::to_writer(&mut buf, body).expect("should write response body");

    buf.into_inner().freeze()
}
