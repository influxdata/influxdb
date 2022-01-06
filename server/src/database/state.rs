//! Database initialization states
use crate::ApplicationState;
use data_types::{server_id::ServerId, DatabaseName};
use internal_types::freezable::Freezable;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::init::DatabaseState;

#[derive(Debug, Clone)]
/// Information about where a database is located on object store,
/// and how to perform startup activities.
pub struct DatabaseConfig {
    pub name: DatabaseName<'static>,
    pub location: String,
    pub server_id: ServerId,
    pub wipe_catalog_on_error: bool,
    pub skip_replay: bool,
}

/// State shared with the `Database` background worker
#[derive(Debug)]
pub(crate) struct DatabaseShared {
    /// Configuration provided to the database at startup
    pub(crate) config: RwLock<DatabaseConfig>,

    /// A token that is used to trigger shutdown of the background worker
    pub(crate) shutdown: CancellationToken,

    /// Application-global state
    pub(crate) application: Arc<ApplicationState>,

    /// The initialization state of the `Database`, wrapped in a
    /// `Freezable` to ensure there is only one task with an
    /// outstanding intent to write at any time.
    pub(crate) state: RwLock<Freezable<DatabaseState>>,

    /// Notify that the database state has changed
    pub(crate) state_notify: Notify,
}
