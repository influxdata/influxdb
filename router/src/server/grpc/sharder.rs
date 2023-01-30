//! A gRPC service to provide shard mappings to external clients.

use std::sync::Arc;

use data_types::{NamespaceName, ShardId, ShardIndex, TopicMetadata};
use generated_types::influxdata::iox::sharder::v1::{
    shard_service_server, MapToShardRequest, MapToShardResponse,
};
use hashbrown::HashMap;
use iox_catalog::interface::Catalog;
use sharder::Sharder;
use tonic::{Request, Response};

use crate::shard::Shard;

/// A [`ShardService`] exposes a [gRPC endpoint] for external systems to discover the shard mapping
/// for specific tables.
///
/// The [`ShardService`] builds a cached mapping of Kafka partition index numbers ([`ShardIndex`])
/// to [`Catalog`] row IDs ([`ShardId`]) in order to handle requests without generating Catalog
/// queries. This mapping is expected to be unchanged over the lifetime of a router instance.
///
/// This service MUST be initialised with the same sharder instance as the
/// [`ShardedWriteBuffer`] for the outputs to be correct.
///
/// [gRPC endpoint]: generated_types::influxdata::iox::sharder::v1::shard_service_server::ShardService
/// [`ShardedWriteBuffer`]: crate::dml_handlers::ShardedWriteBuffer
#[derive(Debug, Clone)]
pub struct ShardService<S> {
    sharder: S,

    // A pre-loaded mapping of all Kafka partition (shard) indexes for the in-use Kafka
    // topic, to their respective catalog row shard ID.
    mapping: HashMap<ShardIndex, ShardId>,
}

impl<S> ShardService<S>
where
    S: Send + Sync,
{
    /// Initialise a gRPC [`ShardService`] handler, building a cached mapping
    /// from the catalog.
    ///
    /// [`ShardService`]: generated_types::influxdata::iox::sharder::v1::shard_service_server::ShardService
    pub async fn new(
        sharder: S,
        topic: TopicMetadata,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, iox_catalog::interface::Error> {
        // Build the mapping of Kafka partition (shard) index -> Catalog shard ID
        let mapping = catalog
            .repositories()
            .await
            .shards()
            .list_by_topic(&topic)
            .await?
            .into_iter()
            .map(|s| (s.shard_index, s.id))
            .collect();

        Ok(Self { sharder, mapping })
    }
}

#[tonic::async_trait]
impl<S> shard_service_server::ShardService for ShardService<S>
where
    S: Sharder<(), Item = Arc<Shard>> + 'static,
{
    async fn map_to_shard(
        &self,
        request: Request<MapToShardRequest>,
    ) -> Result<Response<MapToShardResponse>, tonic::Status> {
        let req = request.into_inner();

        // Validate the namespace.
        let ns = NamespaceName::try_from(req.namespace_name)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        // Map the (table, namespace) tuple to the Shard for it.
        let shard = self.sharder.shard(&req.table_name, &ns, &());

        // Look up the shard index in the cached mapping, to extract the catalog ID associated with
        // the Shard.
        let shard_id = self
            .mapping
            .get(&shard.shard_index())
            .expect("in-use shard maps to non-existant catalog entry");

        Ok(Response::new(MapToShardResponse {
            shard_id: shard_id.get(),
            shard_index: shard.shard_index().get(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, sync::Arc};

    use futures::stream::{FuturesUnordered, StreamExt};
    use generated_types::influxdata::iox::sharder::v1::shard_service_server::ShardService as _;
    use iox_catalog::mem::MemCatalog;
    use sharder::JumpHash;
    use write_buffer::{
        core::WriteBufferWriting,
        mock::{MockBufferForWriting, MockBufferSharedState},
    };

    use super::*;

    const N_SHARDS: i32 = 10;

    #[tokio::test]
    async fn test_mapping() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let write_buffer: Arc<dyn WriteBufferWriting> = Arc::new(init_write_buffer());

        let topic = catalog
            .repositories()
            .await
            .topics()
            .create_or_get("test")
            .await
            .expect("topic create");

        let actual_mapping = (0..N_SHARDS)
            .map(|idx| {
                let catalog = Arc::clone(&catalog);
                let topic = topic.clone();
                async move {
                    let shard_index = ShardIndex::new(idx);
                    let row = catalog
                        .repositories()
                        .await
                        .shards()
                        .create_or_get(&topic, shard_index)
                        .await
                        .expect("failed to create shard");
                    (shard_index, row.id)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<HashMap<ShardIndex, ShardId>>()
            .await;

        let sharder = JumpHash::new(
            actual_mapping
                .clone()
                .into_iter()
                .map(|(idx, _id)| Shard::new(idx, Arc::clone(&write_buffer), &metrics))
                .map(Arc::new),
        );

        let svc = ShardService::new(sharder, topic, catalog)
            .await
            .expect("failed to init service");

        // Validate the correct mapping was constructed.
        assert_eq!(svc.mapping, actual_mapping);

        // Validate calling the RPC service returns correct mapping data.
        for i in 0..100 {
            let resp = svc
                .map_to_shard(Request::new(MapToShardRequest {
                    table_name: format!("{i}"),
                    namespace_name: "bananas".to_string(),
                }))
                .await
                .expect("rpc call should succeed")
                .into_inner();

            let actual = actual_mapping
                .get(&ShardIndex::new(resp.shard_index))
                .expect("returned shard index must exist in mapping");
            assert_eq!(actual.get(), resp.shard_id);
        }
    }

    // Init a mock write buffer with the given number of shards.
    fn init_write_buffer() -> MockBufferForWriting {
        let time = iox_time::MockProvider::new(
            iox_time::Time::from_timestamp_millis(668563200000).unwrap(),
        );
        MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_shards(NonZeroU32::new(N_SHARDS as _).unwrap()),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer")
    }
}
