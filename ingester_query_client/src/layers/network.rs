//! Network layer.
use std::fmt::Debug;

use async_trait::async_trait;
use client_util::tower::SetRequestHeadersService;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderName, HeaderValue, Uri};
use ingester_query_grpc::influxdata::iox::ingester::v2 as proto;
use tonic::transport::Channel;

use crate::{
    error::DynError,
    layer::{Layer, QueryResponse},
};

/// Network layer.
#[derive(Debug)]
pub struct NetworkLayer {
    /// Lazy-connect network channel.
    ///
    /// This can be cloned, all clones share the same connection pool.
    channel: Channel,
}

impl NetworkLayer {
    /// Create new network layer
    pub fn new(uri: Uri) -> Self {
        // connect lazy / on-demand to avoid thunder herding during start-up
        let channel = Channel::builder(uri).connect_lazy();

        Self { channel }
    }
}

#[async_trait]
impl Layer for NetworkLayer {
    type Request = (proto::QueryRequest, Vec<(HeaderName, HeaderValue)>);
    type ResponseMetadata = ();
    type ResponsePayload = proto::QueryResponse;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        let (request, headers) = request;

        let mut client = proto::ingester_query_service_client::IngesterQueryServiceClient::new(
            SetRequestHeadersService::new(self.channel.clone(), headers),
        );
        client
            .query(request)
            .await
            .map(|resp| QueryResponse {
                metadata: (),
                payload: resp.into_inner().map_err(DynError::new).boxed(),
            })
            .map_err(DynError::new)
    }
}
