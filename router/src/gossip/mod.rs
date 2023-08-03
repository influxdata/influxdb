//! Gossip event dispatcher & handler implementations for routers.
//!
//! This sub-system is composed of the following primary components:
//!
//! * [`gossip`] crate: provides the gossip transport, the [`GossipHandle`], and
//!   the [`Dispatcher`]. This crate operates on raw bytes.
//!
//! * The outgoing [`SchemaChangeObserver`]: a router-specific wrapper over the
//!   underlying [`GossipHandle`]. This type translates the application calls
//!   into protobuf [`Msg`], and serialises them into bytes, sending them over
//!   the underlying [`gossip`] impl.
//!
//! * The incoming [`GossipMessageDispatcher`]: deserialises the incoming bytes
//!   from the gossip [`Dispatcher`] into [`Msg`] and passes them off to the
//!   [`NamespaceSchemaGossip`] implementation for processing.
//!
//! * The incoming [`NamespaceSchemaGossip`]: processes [`Msg`] received from
//!   peers, applying them to the local cache state if necessary.
//!
//! ```text
//!         ┌────────────────────────────────────────────────────┐
//!         │                   NamespaceCache                   │
//!         └────────────────────────────────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                   diff                        diff
//!                     │                           │
//!                     │              ┌─────────────────────────┐
//!                     │              │  NamespaceSchemaGossip  │
//!                     │              └─────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                     │     Application types     │
//!                     │                           │
//!                     ▼                           │
//!         ┌──────────────────────┐   ┌─────────────────────────┐
//!         │ SchemaChangeObserver │   │ GossipMessageDispatcher │
//!         └──────────────────────┘   └─────────────────────────┘
//!                     │                           ▲
//!                     │                           │
//!                     │   Encoded Protobuf bytes  │
//!                     │                           │
//!                     │                           │
//!        ┌ Gossip  ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─
//!                     ▼                           │             │
//!        │    ┌──────────────┐          ┌──────────────────┐
//!             │ GossipHandle │          │    Dispatcher    │    │
//!        │    └──────────────┘          └──────────────────┘
//!                                                               │
//!        └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
//! ```
//!
//! [`GossipHandle`]: gossip::GossipHandle
//! [`Dispatcher`]: gossip::Dispatcher
//! [`SchemaChangeObserver`]: schema_change_observer::SchemaChangeObserver
//! [`Msg`]: generated_types::influxdata::iox::gossip::v1::gossip_message::Msg
//! [`GossipMessageDispatcher`]: dispatcher::GossipMessageDispatcher
//! [`NamespaceSchemaGossip`]: namespace_cache::NamespaceSchemaGossip

pub mod dispatcher;
pub mod namespace_cache;
pub mod schema_change_observer;
pub mod traits;

#[cfg(test)]
mod mock_schema_broadcast;

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use data_types::{
        partition_template::{
            test_table_partition_override, NamespacePartitionTemplateOverride,
            TablePartitionTemplateOverride, PARTITION_BY_DAY_PROTO,
        },
        Column, ColumnId, ColumnsByName, NamespaceId, NamespaceName, NamespaceSchema, TableId,
        TableSchema,
    };
    use gossip::Dispatcher;
    use test_helpers::timeout::FutureTimeout;

    use crate::namespace_cache::{MemoryNamespaceCache, NamespaceCache};

    use super::{
        dispatcher::GossipMessageDispatcher, namespace_cache::NamespaceSchemaGossip,
        schema_change_observer::SchemaChangeObserver, traits::SchemaBroadcast,
    };

    #[derive(Debug)]
    struct GossipPipe {
        dispatcher: GossipMessageDispatcher,
    }

    impl GossipPipe {
        fn new(dispatcher: GossipMessageDispatcher) -> Self {
            Self { dispatcher }
        }
    }

    #[async_trait]
    impl SchemaBroadcast for Arc<GossipPipe> {
        async fn broadcast(&self, payload: Vec<u8>) {
            self.dispatcher
                .dispatch(payload.into())
                .with_timeout_panic(Duration::from_secs(5))
                .await;
        }
    }

    /// Return a pair of "nodes" (independent caches) layered in the various
    /// gossip components, with a mock gossip communication layer.
    fn new_node_pair() -> (impl NamespaceCache, impl NamespaceCache) {
        // Setup a cache for node A and wrap it in the gossip layer.
        let node_a_cache = Arc::new(MemoryNamespaceCache::default());
        let dispatcher_a = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&node_a_cache)));
        let dispatcher_a = GossipMessageDispatcher::new(dispatcher_a, 100);
        let gossip_a = Arc::new(GossipPipe::new(dispatcher_a));

        // Setup a cache for node B.
        let node_b_cache = Arc::new(MemoryNamespaceCache::default());
        let dispatcher_b = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&node_b_cache)));
        let dispatcher_b = GossipMessageDispatcher::new(dispatcher_b, 100);
        let gossip_b = Arc::new(GossipPipe::new(dispatcher_b));

        // Connect the two nodes via adaptors that will plug one "node" into the
        // other.
        let node_a = SchemaChangeObserver::new(Arc::clone(&node_a_cache), Arc::clone(&gossip_b));
        let node_b = SchemaChangeObserver::new(Arc::clone(&node_b_cache), Arc::clone(&gossip_a));

        (node_a, node_b)
    }

    // Place a new namespace with a table and column into node A, and check it
    // becomes readable on node B.
    //
    // This is an integration test of the various schema gossip components.
    #[tokio::test]
    async fn test_integration() {
        let (node_a, node_b) = new_node_pair();

        // Fill in a table with a column to insert into A
        let mut tables = BTreeMap::new();
        tables.insert(
            "platanos".to_string(),
            TableSchema {
                id: TableId::new(4242),
                partition_template: test_table_partition_override(vec![
                    data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                ]),
                columns: ColumnsByName::new([Column {
                    id: ColumnId::new(1234),
                    table_id: TableId::new(4242),
                    name: "c1".to_string(),
                    column_type: data_types::ColumnType::U64,
                }]),
            },
        );

        // Wrap the tables into a schema
        let namespace_name = NamespaceName::try_from("bananas").unwrap();
        let schema = NamespaceSchema {
            id: NamespaceId::new(4242),
            tables,
            max_columns_per_table: 1,
            max_tables: 2,
            retention_period_ns: Some(1234),
            partition_template: NamespacePartitionTemplateOverride::try_from(
                (**PARTITION_BY_DAY_PROTO).clone(),
            )
            .unwrap(),
        };

        // Put the new schema into A's cache
        node_a.put_schema(namespace_name.clone(), schema.clone());

        // And read it back in B
        let got = async {
            loop {
                if let Ok(v) = node_b.get_schema(&namespace_name).await {
                    return v;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Ensuring the content is identical
        assert_eq!(*got, schema);
    }

    // As above, but ensuring default partition templates propagate correctly.
    #[tokio::test]
    async fn test_integration_default_partition_templates() {
        let (node_a, node_b) = new_node_pair();

        // Fill in a table with a column to insert into A
        let mut tables = BTreeMap::new();
        tables.insert(
            "platanos".to_string(),
            TableSchema {
                id: TableId::new(4242),
                partition_template: TablePartitionTemplateOverride::try_new(
                    None,
                    &NamespacePartitionTemplateOverride::default(),
                )
                .unwrap(),
                columns: ColumnsByName::new([Column {
                    id: ColumnId::new(1234),
                    table_id: TableId::new(4242),
                    name: "c1".to_string(),
                    column_type: data_types::ColumnType::U64,
                }]),
            },
        );

        // Wrap the tables into a schema
        let namespace_name = NamespaceName::try_from("bananas").unwrap();
        let schema = NamespaceSchema {
            id: NamespaceId::new(4242),
            tables,
            max_columns_per_table: 1,
            max_tables: 2,
            retention_period_ns: Some(1234),
            partition_template: NamespacePartitionTemplateOverride::default(),
        };

        // Put the new schema into A's cache
        node_a.put_schema(namespace_name.clone(), schema.clone());

        // And read it back in B
        let got = async {
            loop {
                if let Ok(v) = node_b.get_schema(&namespace_name).await {
                    return v;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Ensuring the content is identical
        assert_eq!(*got, schema);
    }
}
