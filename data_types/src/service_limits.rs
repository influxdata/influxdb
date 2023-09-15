//! Types protecting production by implementing limits on customer data.

use generated_types::influxdata::iox::namespace::v1 as namespace_proto;

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
    pub max_tables: Option<i32>,
    /// The maximum number of columns per table in this namespace
    pub max_columns_per_table: Option<i32>,
}

impl From<namespace_proto::ServiceProtectionLimits> for NamespaceServiceProtectionLimitsOverride {
    fn from(value: namespace_proto::ServiceProtectionLimits) -> Self {
        let namespace_proto::ServiceProtectionLimits {
            max_tables,
            max_columns_per_table,
        } = value;
        Self {
            max_tables,
            max_columns_per_table,
        }
    }
}
