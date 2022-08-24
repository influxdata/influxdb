//! A gRPC service to provide shard mappings to external clients.

use std::sync::Arc;

use data_types::{DatabaseName, KafkaPartition, KafkaTopic, SequencerId};
use generated_types::influxdata::iox::sharder::v1::{
    shard_service_server, ShardToSequencerIdRequest, ShardToSequencerIdResponse,
};
use hashbrown::HashMap;
use iox_catalog::interface::Catalog;
use sharder::Sharder;
use tonic::{Request, Response};

use crate::sequencer::Sequencer;

/// A [`ShardService`] exposes a [gRPC endpoint] for external systems to
/// discover the shard mapping for specific tables.
///
/// The [`ShardService`] builds a cached mapping of Kafka partition index
/// numbers to [`Catalog`] row IDs in order to handle requests without
/// generating Catalog queries. This mapping is expected to be unchanged over
/// the lifetime of a router instance.
///
/// This service MUST be initialised with the same sharder instance as the
/// [`ShardedWriteBuffer`] for the outputs to be correct.
///
/// [gRPC endpoint]: generated_types::influxdata::iox::sharder::v1::shard_service_server::ShardService
/// [`ShardedWriteBuffer`]: crate::dml_handlers::ShardedWriteBuffer
#[derive(Debug)]
pub struct ShardService<S> {
    sharder: S,

    // A pre-loaded mapping of all Kafka partition indexes for the in-use Kafka
    // topic, to their respective catalog row ID.
    mapping: HashMap<KafkaPartition, SequencerId>,
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
        topic: KafkaTopic,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, iox_catalog::interface::Error> {
        // Build the mapping of Kafka partition index -> Catalog ID
        let mapping = catalog
            .repositories()
            .await
            .sequencers()
            .list_by_kafka_topic(&topic)
            .await?
            .into_iter()
            .map(|s| (s.kafka_partition, s.id))
            .collect();

        Ok(Self { sharder, mapping })
    }
}

#[tonic::async_trait]
impl<S> shard_service_server::ShardService for ShardService<S>
where
    S: Sharder<(), Item = Arc<Sequencer>> + 'static,
{
    async fn shard_to_sequencer_id(
        &self,
        request: Request<ShardToSequencerIdRequest>,
    ) -> Result<Response<ShardToSequencerIdResponse>, tonic::Status> {
        let req = request.into_inner();

        // Validate the namespace.
        let ns = DatabaseName::try_from(req.namespace_name)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        // Map the (table, namespace) tuple to the Sequencer for it.
        let sequencer = self.sharder.shard(&req.table_name, &ns, &());

        // Lookup the Kafka partition index in the cached mapping, to extract
        // the catalog ID associated with the Sequencer.
        let catalog_id = self
            .mapping
            .get(&sequencer.kafka_index())
            .expect("in-use shard maps to non-existant catalog entry");

        Ok(Response::new(ShardToSequencerIdResponse {
            sequencer_id: catalog_id.get(),
            sequencer_index: sequencer.kafka_index().get(),
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

    const N_SEQUENCERS: i32 = 10;

    #[tokio::test]
    async fn test_mapping() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let write_buffer: Arc<dyn WriteBufferWriting> = Arc::new(init_write_buffer());

        let topic = catalog
            .repositories()
            .await
            .kafka_topics()
            .create_or_get("test")
            .await
            .expect("topic create");

        let actual_mapping = (0..N_SEQUENCERS)
            .map(|idx| {
                let catalog = Arc::clone(&catalog);
                let topic = topic.clone();
                async move {
                    let partition_idx = KafkaPartition::new(idx);
                    let row = catalog
                        .repositories()
                        .await
                        .sequencers()
                        .create_or_get(&topic, partition_idx)
                        .await
                        .expect("failed to create sequencer");
                    (partition_idx, row.id)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<HashMap<KafkaPartition, SequencerId>>()
            .await;

        let sharder = JumpHash::new(
            actual_mapping
                .clone()
                .into_iter()
                .map(|(idx, _id)| Sequencer::new(idx, Arc::clone(&write_buffer), &*metrics))
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
                .shard_to_sequencer_id(Request::new(ShardToSequencerIdRequest {
                    table_name: format!("{}", i),
                    namespace_name: "bananas".to_string(),
                }))
                .await
                .expect("rpc call should succeed")
                .into_inner();

            let actual = actual_mapping
                .get(&KafkaPartition::new(resp.sequencer_index))
                .expect("returned kafka partition index must exist in mapping");
            assert_eq!(actual.get(), resp.sequencer_id);
        }
    }

    // Init a mock write buffer with the given number of sequencers.
    fn init_write_buffer() -> MockBufferForWriting {
        let time = iox_time::MockProvider::new(iox_time::Time::from_timestamp_millis(668563200000));
        MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_sequencers(
                NonZeroU32::new(N_SEQUENCERS as _).unwrap(),
            ),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer")
    }
}
