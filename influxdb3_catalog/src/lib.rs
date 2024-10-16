use std::sync::Arc;

use catalog::DatabaseSchema;
use influxdb3_id::DbId;

pub mod catalog;
pub(crate) mod serialize;

/// Provide [`DatabaseSchema`] and there derivatives where needed.
///
/// This trait captures the read-only behaviour of the [`catalog::Catalog`] as where it is used to
/// serve queries and provide schema-related info of the underlying database.
pub trait DatabaseSchemaProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Convert a database name to its corresponding [`DbId`]
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId>;

    /// Convert a [`DbId`] into its corresponding database name
    fn db_id_to_name(&self, db_id: DbId) -> Option<Arc<str>>;

    /// Get the [`DatabaseSchema`] for the given name, or `None` otherwise
    fn db_schema(&self, db_name: &str) -> Option<Arc<DatabaseSchema>>;

    /// Get the [`DatabaseSchema`] for the given [`DbId`], or `None` otherwise
    fn db_schema_by_id(&self, db_id: DbId) -> Option<Arc<DatabaseSchema>>;

    /// Get the [`DatabaseSchema`] as well as the corresponding [`DbId`] for the given name
    fn db_schema_and_id(&self, db_name: &str) -> Option<(DbId, Arc<DatabaseSchema>)>;

    /// Get a list of the database names in the underlying catalog'd instance
    fn db_names(&self) -> Vec<String>;

    /// List out all [`DatabaseSchema`] in the database
    fn list_db_schema(&self) -> Vec<Arc<DatabaseSchema>>;
}
