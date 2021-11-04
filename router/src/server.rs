use std::{collections::BTreeMap, sync::Arc};

use data_types::{router::Router as RouterConfig, server_id::ServerId};
use metric::Registry as MetricRegistry;
use parking_lot::RwLock;
use snafu::Snafu;
use trace::TraceCollector;

use crate::router::Router;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum SetServerIdError {
    #[snafu(display("Server ID already set: {}", server_id))]
    AlreadySet { server_id: ServerId },
}

/// Main entry point to manage a router node.
#[derive(Debug)]
pub struct RouterServer {
    server_id: RwLock<Option<ServerId>>,
    metric_registry: Arc<MetricRegistry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    routers: RwLock<BTreeMap<String, Arc<Router>>>,
}

impl RouterServer {
    pub fn new(trace_collector: Option<Arc<dyn TraceCollector>>) -> Self {
        let metric_registry = Arc::new(metric::Registry::new());

        Self {
            server_id: RwLock::new(None),
            metric_registry,
            trace_collector,
            routers: Default::default(),
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
        let router = Router::new(config);
        self.routers
            .write()
            .insert(router.name().to_string(), Arc::new(router))
            .is_some()
    }

    /// Delete router.
    ///
    /// Returns `true` if the router existed.
    pub fn delete_router(&self, name: &str) -> bool {
        self.routers.write().remove(name).is_some()
    }
}

pub mod test_utils {
    use super::RouterServer;

    pub fn make_router_server() -> RouterServer {
        RouterServer::new(None)
    }
}

#[cfg(test)]
mod tests {
    use data_types::router::QuerySinks;

    use crate::server::test_utils::make_router_server;

    use super::*;

    #[test]
    fn test_server_id() {
        let id13 = ServerId::try_from(13).unwrap();
        let id42 = ServerId::try_from(42).unwrap();

        // server starts w/o any ID
        let server = make_router_server();
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

    #[test]
    fn test_router_crud() {
        let server = make_router_server();

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

        // update router
        assert!(server.update_router(cfg_foo_2.clone()));
        let routers = server.routers();
        assert_eq!(routers.len(), 2);
        assert_eq!(routers[0].config(), &cfg_bar);
        assert_eq!(routers[1].config(), &cfg_foo_2);

        // delete routers
        assert!(server.delete_router("foo"));
        let routers = server.routers();
        assert_eq!(routers.len(), 1);
        assert_eq!(routers[0].config(), &cfg_bar);
        assert!(!server.delete_router("foo"));
    }
}
