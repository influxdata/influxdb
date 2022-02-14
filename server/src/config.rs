use crate::ProvidedDatabaseRules;
use async_trait::async_trait;
use data_types::server_id::ServerId;
use generated_types::influxdata::iox::management::v1::OwnerInfo;
use uuid::Uuid;

pub mod object_store;
mod owner;

/// A generic opaque error returned by [`ConfigProvider`]
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Result type returned by [`ConfigProvider`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A generic trait for interacting with the configuration
/// of a database server
#[async_trait]
pub trait ConfigProvider: std::fmt::Debug + Send + Sync {
    /// Returns a list of database name and UUID pairs
    async fn fetch_server_config(&self, server_id: ServerId) -> Result<Vec<(String, Uuid)>>;

    /// Persists a list of database names and UUID pairs overwriting any
    /// pre-existing persisted server configuration
    async fn store_server_config(
        &self,
        server_id: ServerId,
        config: &[(String, Uuid)],
    ) -> Result<()>;

    /// Returns the configuration for the database with the given `uuid`
    async fn fetch_rules(&self, uuid: Uuid) -> Result<ProvidedDatabaseRules>;

    /// Persists the configuration for the database with the given `uuid`
    async fn store_rules(&self, uuid: Uuid, rules: &ProvidedDatabaseRules) -> Result<()>;

    /// Returns the owner information for the database with the given `uuid`
    async fn fetch_owner_info(&self, server_id: ServerId, uuid: Uuid) -> Result<OwnerInfo>;

    /// Updates the owner information for the database with the given `uuid`
    /// and records it as owned by `server_id`
    async fn update_owner_info(&self, server_id: Option<ServerId>, uuid: Uuid) -> Result<()>;

    /// Updates the owner information for the database with the given `uuid`
    /// and records it as owned by `server_id`
    async fn create_owner_info(&self, server_id: ServerId, uuid: Uuid) -> Result<()>;
}
