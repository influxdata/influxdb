//! Helpers to ensure service links are respected during shutdown.
//!
//! This does NOT affect correctness of the tests but often speeds them up because clients (like the ingester
//! communicating with the catalog) no longer get stuck on retries during the shutdown phase (and would be killed after
//! a timeout).
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

/// An abstract service that can be linked in a client-server relationship
pub(crate) trait LinkableService: std::fmt::Debug + Send + Sync {
    /// Add new known client.
    ///
    /// **NOTE: This does NOT perform the opposite operation ([`add_link_server`](Self::add_link_server)) for the
    /// client. Use [`link_services`] instead.**
    fn add_link_client(&self, client: Weak<dyn LinkableService>);

    /// Unlink all clients from this service.
    ///
    /// **NOTE: This does NOT perform the opposite operation ([`remove_link_server`](Self::remove_link_server)) for the
    /// returned clients. Use [`unlink_services`] instead.**
    fn remove_link_clients(&self) -> Vec<Arc<dyn LinkableService>>;

    /// Add new known server that should be kept alive until the client is gone.
    ///
    /// **NOTE: This does NOT perform the opposite operation ([`add_link_client`](Self::add_link_client)) for the
    /// client. Use [`link_services`] instead.**
    fn add_link_server(&self, server: Arc<dyn LinkableService>);

    /// Remove given server.
    ///
    /// The server will no longer kept alive. This is a no-op if the server is unknown.
    ///
    /// **NOTE: This does NOT perform the opposite operation ([`remove_link_clients`](Self::remove_link_clients)) for the
    /// server. Use [`unlink_services`] instead.**
    fn remove_link_server(&self, server: Arc<dyn LinkableService>);
}

/// Simple implementation of [`LinkableService`] that can be used as a struct member.
///
/// Using this as a struct member and NOT directly is important so that the tracked [`Arc`]s use the actual service
/// struct, not this helper.
#[derive(Debug, Default)]
pub(crate) struct LinkableServiceImpl {
    clients: Mutex<Vec<Weak<dyn LinkableService>>>,
    servers: Mutex<Vec<Arc<dyn LinkableService>>>,
}

impl LinkableService for LinkableServiceImpl {
    fn add_link_client(&self, client: Weak<dyn LinkableService>) {
        self.clients.lock().push(client);
    }

    fn remove_link_clients(&self) -> Vec<Arc<dyn LinkableService>> {
        let mut guard = self.clients.lock();
        guard
            .drain(..)
            .filter_map(|client| client.upgrade())
            .collect()
    }

    fn add_link_server(&self, server: Arc<dyn LinkableService>) {
        self.servers.lock().push(server);
    }

    fn remove_link_server(&self, server: Arc<dyn LinkableService>) {
        self.servers
            .lock()
            .retain(|server2| !Arc::ptr_eq(&server, server2));
    }
}

impl Clone for LinkableServiceImpl {
    fn clone(&self) -> Self {
        let clients = self.clients.lock();
        let server = self.servers.lock();
        Self {
            clients: Mutex::new(clients.clone()),
            servers: Mutex::new(server.clone()),
        }
    }
}

/// Cross-link server and client.
pub(crate) fn link_services(server: Arc<dyn LinkableService>, client: Arc<dyn LinkableService>) {
    server.add_link_client(Arc::downgrade(&client));
    client.add_link_server(server);
}

/// Unlink clients from a given server so it is no longer kept alive.
///
/// The known clients are returned so they can potentially be re-linked.
pub(crate) fn unlink_services(server: Arc<dyn LinkableService>) -> Vec<Arc<dyn LinkableService>> {
    let clients = server.remove_link_clients();
    for client in &clients {
        client.remove_link_server(Arc::clone(&server));
    }
    clients
}
