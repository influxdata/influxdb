use crate::DatabaseName;
use uuid::Uuid;

/// Detailed metadata about an active database.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveDatabase {
    /// The name of the database
    pub name: DatabaseName<'static>,
    /// The UUID of the database
    pub uuid: Uuid,
}
