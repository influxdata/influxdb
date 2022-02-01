use std::{collections::BTreeMap, sync::Arc};

use data_types::{router::Router as RouterConfig, server_id::ServerId};
use metric::Registry as MetricRegistry;
use parking_lot::RwLock;
use snafu::Snafu;
use time::TimeProvider;
use trace::TraceCollector;
use write_buffer::config::WriteBufferConfigFactory;

use crate::{
    connection_pool::ConnectionPool,
    resolver::{RemoteTemplate, Resolver},
    router::Router,
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum SetServerIdError {
    #[snafu(display("id already set: {}", server_id))]
    AlreadySet { server_id: ServerId },
}

/// Main entry point to manage a router node.
#[derive(Debug)]
pub struct RouterServer {
    server_id: RwLock<Option<ServerId>>,
    metric_registry: Arc<MetricRegistry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    routers: RwLock<BTreeMap<String, Arc<Router>>>,
    resolver: Arc<Resolver>,
    connection_pool: Arc<ConnectionPool>,
}

impl RouterServer {
    pub async fn new(
        remote_template: Option<RemoteTemplate>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self::new_inner(remote_template, trace_collector, time_provider, None, false).await
    }

    pub async fn for_testing(
        remote_template: Option<RemoteTemplate>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        time_provider: Arc<dyn TimeProvider>,
        wb_factory: Option<Arc<WriteBufferConfigFactory>>,
    ) -> Self {
        Self::new_inner(
            remote_template,
            trace_collector,
            time_provider,
            wb_factory,
            true,
        )
        .await
    }

    async fn new_inner(
        remote_template: Option<RemoteTemplate>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
        time_provider: Arc<dyn TimeProvider>,
        wb_factory: Option<Arc<WriteBufferConfigFactory>>,
        use_mock_grpc: bool,
    ) -> Self {
        let metric_registry = Arc::new(metric::Registry::new());
        let wb_factory = wb_factory.unwrap_or_else(|| {
            Arc::new(WriteBufferConfigFactory::new(
                time_provider,
                Arc::clone(&metric_registry),
            ))
        });
        let connection_pool =
            Arc::new(ConnectionPool::new(use_mock_grpc, wb_factory, trace_collector.clone()).await);

        Self {
            server_id: RwLock::new(None),
            metric_registry,
            trace_collector,
            routers: Default::default(),
            resolver: Arc::new(Resolver::new(remote_template)),
            connection_pool,
        }
    }

    /// Get server ID, if any.
    pub fn server_id(&self) -> Option<ServerId> {
        *self.server_id.read()
    }

    /// Set server ID.
    ///
    /// # Error
    /// This will fail when an ID is already set to a different ID.
    pub fn set_server_id(&self, server_id: ServerId) -> Result<(), SetServerIdError> {
        let mut guard = self.server_id.write();
        match *guard {
            Some(existing) if existing == server_id => Ok(()),
            Some(existing) => Err(SetServerIdError::AlreadySet {
                server_id: existing,
            }),
            None => {
                *guard = Some(server_id);
                Ok(())
            }
        }
    }

    /// Metric registry associated with this server.
    pub fn metric_registry(&self) -> &Arc<MetricRegistry> {
        &self.metric_registry
    }

    /// Trace collector associated with this server.
    pub fn trace_collector(&self) -> &Option<Arc<dyn TraceCollector>> {
        &self.trace_collector
    }

    /// List all routers, sorted by name,
    pub fn routers(&self) -> Vec<Arc<Router>> {
        self.routers.read().values().cloned().collect()
    }

    /// Update or create router.
    ///
    /// Returns `true` if the router already existed.
    pub fn update_router(&self, config: RouterConfig) -> bool {
        let router = Router::new(
            config,
            Arc::clone(&self.resolver),
            Arc::clone(&self.connection_pool),
        );
        self.routers
            .write()
            .insert(router.name().to_string(), Arc::new(router))
            .is_some()
    }

    /// Delete router.
    ///
    /// Returns `true` if the router existed.
    pub fn delete_router(&self, router_name: &str) -> bool {
        self.routers.write().remove(router_name).is_some()
    }

    /// Get registered router, if any.
    ///
    /// The router name is identical to the database for which this router handles data.
    pub fn router(&self, router_name: &str) -> Option<Arc<Router>> {
        self.routers.read().get(router_name).cloned()
    }

    /// Resolver associated with this server.
    pub fn resolver(&self) -> &Arc<Resolver> {
        &self.resolver
    }

    /// Connection pool associated with this server.
    pub fn connection_pool(&self) -> &Arc<ConnectionPool> {
        &self.connection_pool
    }
}

pub mod test_utils {
    use std::sync::Arc;

    use time::SystemProvider;

    use super::RouterServer;

    pub async fn make_router_server() -> RouterServer {
        RouterServer::new(None, None, Arc::new(SystemProvider::new())).await
    }
}

#[cfg(test)]
mod tests {
    use data_types::router::QuerySinks;

    use crate::server::test_utils::make_router_server;

    use super::*;

    #[tokio::test]
    async fn test_server_id() {
        let id13 = ServerId::try_from(13).unwrap();
        let id42 = ServerId::try_from(42).unwrap();

        // server starts w/o any ID
        let server = make_router_server().await;
        assert_eq!(server.server_id(), None);

        // setting ID
        server.set_server_id(id13).unwrap();
        assert_eq!(server.server_id(), Some(id13));

        // setting a 2nd time to the same value should work
        server.set_server_id(id13).unwrap();
        assert_eq!(server.server_id(), Some(id13));

        // chaning the ID fails
        let err = server.set_server_id(id42).unwrap_err();
        assert!(matches!(err, SetServerIdError::AlreadySet { .. }));
    }

    #[tokio::test]
    async fn test_router_crud() {
        let server = make_router_server().await;

        let cfg_foo_1 = RouterConfig {
            name: String::from("foo"),
            write_sharder: Default::default(),
            write_sinks: Default::default(),
            query_sinks: Default::default(),
        };
        let cfg_foo_2 = RouterConfig {
            query_sinks: QuerySinks {
                grpc_remotes: vec![ServerId::try_from(1).unwrap()],
            },
            ..cfg_foo_1.clone()
        };
        assert_ne!(cfg_foo_1, cfg_foo_2);

        let cfg_bar = RouterConfig {
            name: String::from("bar"),
            write_sharder: Default::default(),
            write_sinks: Default::default(),
            query_sinks: Default::default(),
        };

        // no routers
        assert_eq!(server.routers().len(), 0);
        assert!(!server.delete_router("foo"));

        // add routers
        assert!(!server.update_router(cfg_foo_1.clone()));
        assert!(!server.update_router(cfg_bar.clone()));
        let routers = server.routers();
        assert_eq!(routers.len(), 2);
        assert_eq!(routers[0].config(), &cfg_bar);
        assert_eq!(routers[1].config(), &cfg_foo_1);
        assert_eq!(server.router("bar").unwrap().config(), &cfg_bar);
        assert_eq!(server.router("foo").unwrap().config(), &cfg_foo_1);

        // update router
        assert!(server.update_router(cfg_foo_2.clone()));
        let routers = server.routers();
        assert_eq!(routers.len(), 2);
        assert_eq!(routers[0].config(), &cfg_bar);
        assert_eq!(routers[1].config(), &cfg_foo_2);
        assert_eq!(server.router("bar").unwrap().config(), &cfg_bar);
        assert_eq!(server.router("foo").unwrap().config(), &cfg_foo_2);

        // delete routers
        assert!(server.delete_router("foo"));
        let routers = server.routers();
        assert_eq!(routers.len(), 1);
        assert_eq!(routers[0].config(), &cfg_bar);
        assert_eq!(server.router("bar").unwrap().config(), &cfg_bar);
        assert!(server.router("foo").is_none());

        // deleting router a 2nd time works
        assert!(!server.delete_router("foo"));
    }
}
