//! Interface for all implementation of the client interface.
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::{fmt::Debug, sync::Arc};

use crate::error::DynError;

/// Query response.
pub struct QueryResponse<ResponseMetadata, ResponsePayload>
where
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    /// Metadata.
    pub metadata: ResponseMetadata,

    /// Payload.
    pub payload: BoxStream<'static, Result<ResponsePayload, DynError>>,
}

impl<ResponseMetadata, ResponsePayload> Debug for QueryResponse<ResponseMetadata, ResponsePayload>
where
    ResponseMetadata: Clone + Debug + Send + Sync + 'static,
    ResponsePayload: Clone + Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponse")
            .field("metadata", &self.metadata)
            .field("payload", &"<STREAM>")
            .finish()
    }
}

/// Abstract query client.
#[async_trait]
pub trait Layer: Debug + Send + Sync + 'static {
    /// Request type.
    type Request: Clone + Debug + Send + Sync + 'static;

    /// Response metdata type.
    type ResponseMetadata: Clone + Debug + Send + Sync + 'static;

    /// Response payload type.
    type ResponsePayload: Clone + Debug + Send + Sync + 'static;

    /// Perform request.
    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError>;
}

#[async_trait]
impl<T> Layer for Arc<T>
where
    T: Layer,
{
    type Request = T::Request;
    type ResponseMetadata = T::ResponseMetadata;
    type ResponsePayload = T::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        self.as_ref().query(request).await
    }
}
