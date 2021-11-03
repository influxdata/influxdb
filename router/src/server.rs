use std::sync::Arc;

use data_types::server_id::ServerId;
use metric::Registry as MetricRegistry;
use parking_lot::RwLock;
use snafu::Snafu;
use trace::TraceCollector;

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
}

impl RouterServer {
    pub fn new(trace_collector: Option<Arc<dyn TraceCollector>>) -> Self {
        let metric_registry = Arc::new(metric::Registry::new());

        Self {
            server_id: RwLock::new(None),
            metric_registry,
            trace_collector,
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

    pub fn metric_registry(&self) -> &Arc<MetricRegistry> {
        &self.metric_registry
    }

    pub fn trace_collector(&self) -> &Option<Arc<dyn TraceCollector>> {
        &self.trace_collector
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
}
