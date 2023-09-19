//! Types protecting production by implementing limits on customer data.

use generated_types::influxdata::iox::namespace::{
    v1 as namespace_proto, v1::update_namespace_service_protection_limit_request::LimitUpdate,
};
use thiserror::Error;

/// Max tables allowed in a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct MaxTables(i32);

#[allow(missing_docs)]
impl MaxTables {
    pub const fn new(v: i32) -> Self {
        Self(v)
    }

    pub fn get(&self) -> i32 {
        self.0
    }

    /// Default per-namespace table count service protection limit.
    pub const fn const_default() -> Self {
        Self(500)
    }
}

impl Default for MaxTables {
    fn default() -> Self {
        Self::const_default()
    }
}

impl std::fmt::Display for MaxTables {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Max columns per table allowed in a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct MaxColumnsPerTable(i32);

#[allow(missing_docs)]
impl MaxColumnsPerTable {
    pub const fn new(v: i32) -> Self {
        Self(v)
    }

    pub fn get(&self) -> i32 {
        self.0
    }

    /// Default per-table column count service protection limit.
    pub const fn const_default() -> Self {
        Self(200)
    }
}

impl Default for MaxColumnsPerTable {
    fn default() -> Self {
        Self::const_default()
    }
}

impl std::fmt::Display for MaxColumnsPerTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Overrides for service protection limits.
#[derive(Debug, Copy, Clone)]
pub struct NamespaceServiceProtectionLimitsOverride {
    /// The maximum number of tables that can exist in this namespace
    pub max_tables: Option<MaxTables>,
    /// The maximum number of columns per table in this namespace
    pub max_columns_per_table: Option<MaxColumnsPerTable>,
}

impl From<namespace_proto::ServiceProtectionLimits> for NamespaceServiceProtectionLimitsOverride {
    fn from(value: namespace_proto::ServiceProtectionLimits) -> Self {
        let namespace_proto::ServiceProtectionLimits {
            max_tables,
            max_columns_per_table,
        } = value;
        Self {
            max_tables: max_tables.map(MaxTables::new),
            max_columns_per_table: max_columns_per_table.map(MaxColumnsPerTable::new),
        }
    }
}

/// Updating one, but not both, of the limits is what the UpdateNamespaceServiceProtectionLimit
/// gRPC request supports, so match that encoding on the Rust side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLimitUpdate {
    /// Requesting an update to the maximum number of tables allowed in this namespace
    MaxTables(MaxTables),
    /// Requesting an update to the maximum number of columns allowed in each table in this
    /// namespace
    MaxColumnsPerTable(MaxColumnsPerTable),
}

/// Errors converting from raw values to the service limits
#[derive(Error, Debug, Clone, Copy)]
pub enum ServiceLimitError {
    /// A negative or 0 value was specified; those aren't allowed
    #[error("service limit values must be greater than 0")]
    MustBeGreaterThanZero,

    /// No value was provided so we can't update anything
    #[error("a supported service limit value is required")]
    NoValueSpecified,
}

impl TryFrom<Option<LimitUpdate>> for ServiceLimitUpdate {
    type Error = ServiceLimitError;

    fn try_from(limit_update: Option<LimitUpdate>) -> Result<Self, Self::Error> {
        match limit_update {
            Some(LimitUpdate::MaxTables(n)) => {
                if n == 0 {
                    return Err(ServiceLimitError::MustBeGreaterThanZero);
                }
                Ok(ServiceLimitUpdate::MaxTables(MaxTables::new(n)))
            }
            Some(LimitUpdate::MaxColumnsPerTable(n)) => {
                if n == 0 {
                    return Err(ServiceLimitError::MustBeGreaterThanZero);
                }
                Ok(ServiceLimitUpdate::MaxColumnsPerTable(
                    MaxColumnsPerTable::new(n),
                ))
            }
            None => Err(ServiceLimitError::NoValueSpecified),
        }
    }
}
