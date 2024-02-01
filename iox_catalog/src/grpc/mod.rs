//! gRPC catalog tunnel.
//!
//! This tunnels catalog requests over gRPC.

pub mod client;
mod serialization;
pub mod server;

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use data_types::NamespaceName;
    use iox_time::SystemProvider;
    use metric::{Attributes, Metric, U64Counter};
    use test_helpers::maybe_start_logging;
    use tokio::{net::TcpListener, task::JoinSet};
    use tonic::transport::{server::TcpIncoming, Server, Uri};

    use crate::{interface::Catalog, interface_tests::TestCatalog, mem::MemCatalog};

    use super::*;

    #[tokio::test]
    async fn test_catalog() {
        maybe_start_logging();

        crate::interface_tests::test_catalog(|| async {
            let metrics = Arc::new(metric::Registry::default());
            let time_provider = Arc::new(SystemProvider::new()) as _;
            let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
            let test_server = TestServer::new(backing_catalog).await;
            let uri = test_server.uri();

            // create new metrics for client so that they don't overlap w/ server
            let metrics = Arc::new(metric::Registry::default());
            let client = Arc::new(client::GrpcCatalogClient::new(
                uri,
                metrics,
                Arc::clone(&time_provider),
            ));

            let test_catalog = TestCatalog::new(client);
            test_catalog.hold_onto(test_server);

            Arc::new(test_catalog) as _
        })
        .await;
    }

    #[tokio::test]
    async fn test_catalog_metrics() {
        maybe_start_logging();

        let time_provider = Arc::new(SystemProvider::new()) as _;
        let metrics = Arc::new(metric::Registry::default());
        let backing_catalog = Arc::new(MemCatalog::new(metrics, Arc::clone(&time_provider)));
        let test_server = TestServer::new(backing_catalog).await;
        let uri = test_server.uri();

        // create new metrics for client so that they don't overlap w/ server
        let metrics = Arc::new(metric::Registry::default());
        let client = Arc::new(client::GrpcCatalogClient::new(
            uri,
            Arc::clone(&metrics),
            Arc::clone(&time_provider),
        ));

        let ns = client
            .repositories()
            .namespaces()
            .create(&NamespaceName::new("testns").unwrap(), None, None, None)
            .await
            .expect("namespace failed to create");

        let _ = client
            .repositories()
            .tables()
            .list_by_namespace_id(ns.id)
            .await
            .expect("failed to list namespaces");

        let metric = metrics
            .get_instrument::<Metric<U64Counter>>("grpc_client_requests")
            .expect("failed to get metric");

        let count = metric
            .get_observer(&Attributes::from(&[
                (
                    "path",
                    "/influxdata.iox.catalog.v2.CatalogService/NamespaceCreate",
                ),
                ("status", "ok"),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(count, 1);

        let count = metric
            .get_observer(&Attributes::from(&[
                (
                    "path",
                    "/influxdata.iox.catalog.v2.CatalogService/TableListByNamespaceId",
                ),
                ("status", "ok"),
            ]))
            .unwrap()
            .fetch();

        assert_eq!(count, 1);
    }

    struct TestServer {
        addr: SocketAddr,
        #[allow(dead_code)]
        task: JoinSet<()>,
    }

    impl TestServer {
        async fn new(catalog: Arc<dyn Catalog>) -> Self {
            let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
            let mut task = JoinSet::new();
            task.spawn(async move {
                Server::builder()
                    .add_service(server::GrpcCatalogServer::new(catalog).service())
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });

            Self { addr, task }
        }

        fn uri(&self) -> Uri {
            format!("http://{}:{}", self.addr.ip(), self.addr.port())
                .parse()
                .unwrap()
        }
    }
}
