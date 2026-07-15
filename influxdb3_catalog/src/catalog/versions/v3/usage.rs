//! Catalog usage snapshot and limit interface.
//!
//! [`CatalogLimiter`] is the trait enterprise plugs into for dynamic limits
//! that can vary by current usage (e.g. per-tier caps). [`CatalogLimits`] is
//! the default concrete implementation with per-field overrides.

use super::{NUM_TAG_COLUMNS_LIMIT, catalog::Catalog};

/// Point-in-time counts read from [`InnerCatalog`] on `update` or
/// transaction begin. Passed to [`CatalogLimiter`] so implementations can
/// return limits that depend on current usage.
///
/// [`InnerCatalog`]: super::inner::InnerCatalog
#[derive(Debug, Clone, Copy, Default)]
pub struct CurrentCatalogUsage {
    total_db_count: usize,
    total_table_count: usize,
    total_column_count: usize,
}

impl CurrentCatalogUsage {
    pub(crate) fn new(
        total_db_count: usize,
        total_table_count: usize,
        total_column_count: usize,
    ) -> Self {
        Self {
            total_db_count,
            total_table_count,
            total_column_count,
        }
    }

    pub fn total_db_count(&self) -> usize {
        self.total_db_count
    }

    pub fn total_table_count(&self) -> usize {
        self.total_table_count
    }

    pub fn total_column_count(&self) -> usize {
        self.total_column_count
    }
}

/// Returns the catalog's resource limits. Enterprise provides implementations
/// that vary limits by current usage; the default [`CatalogLimits`] ignores
/// `current` and returns its stored values.
pub trait CatalogLimiter: Send + Sync + 'static {
    fn database_count_limit(&self, current: &CurrentCatalogUsage) -> usize;
    fn table_count_limit(&self, current: &CurrentCatalogUsage) -> usize;
    fn column_per_table_limit(&self, current: &CurrentCatalogUsage) -> usize;
    fn tag_column_per_table_limit(&self, current: &CurrentCatalogUsage) -> usize;
}

/// Default [`CatalogLimiter`] with static per-field values.
#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    num_dbs: usize,
    num_tables: usize,
    num_columns_per_table: usize,
    num_tag_columns_per_table: usize,
}

impl CatalogLimits {
    pub fn new(num_dbs: usize, num_tables: usize, num_columns_per_table: usize) -> Self {
        Self {
            num_dbs,
            num_tables,
            num_columns_per_table,
            num_tag_columns_per_table: NUM_TAG_COLUMNS_LIMIT,
        }
    }

    pub fn with_tag_columns_per_table_limit(mut self, num_tag_columns_per_table: usize) -> Self {
        self.num_tag_columns_per_table = num_tag_columns_per_table;
        self
    }

    /// Limits high enough to be effectively unenforced.
    ///
    /// Intended for tests, and for internal paths (e.g. migrating an existing
    /// catalog) that must not reject resources for exceeding a per-product
    /// limit. Per-product limits are configured by each binary via [`new`].
    ///
    /// [`new`]: Self::new
    pub fn none() -> Self {
        Self::new(usize::MAX, usize::MAX, usize::MAX)
    }
}

impl CatalogLimiter for CatalogLimits {
    fn database_count_limit(&self, _current: &CurrentCatalogUsage) -> usize {
        self.num_dbs
    }

    fn table_count_limit(&self, _current: &CurrentCatalogUsage) -> usize {
        self.num_tables
    }

    fn column_per_table_limit(&self, _current: &CurrentCatalogUsage) -> usize {
        self.num_columns_per_table
    }

    fn tag_column_per_table_limit(&self, _current: &CurrentCatalogUsage) -> usize {
        self.num_tag_columns_per_table
    }
}

/// [`CatalogLimiter`] that caps the catalog by total column count across all
/// tables. Database and table limits are derived headroom: as the column
/// budget is consumed, available db/table slots shrink.
#[derive(Clone, Copy, Debug)]
pub struct MaximumColumnCountLimiter {
    max_total_columns: u32,
}

impl MaximumColumnCountLimiter {
    pub fn new(max_total_columns: u32) -> Self {
        Self { max_total_columns }
    }
}

impl Default for MaximumColumnCountLimiter {
    fn default() -> Self {
        Self {
            max_total_columns: Catalog::MAX_TOTAL_COLUMNS,
        }
    }
}

impl CatalogLimiter for MaximumColumnCountLimiter {
    fn database_count_limit(&self, current: &CurrentCatalogUsage) -> usize {
        let total_column_count = current.total_column_count();
        let max_total_columns = self.max_total_columns as usize;
        if total_column_count < max_total_columns {
            current
                .total_db_count()
                .saturating_add(max_total_columns - total_column_count)
        } else {
            current.total_db_count()
        }
    }

    fn table_count_limit(&self, current: &CurrentCatalogUsage) -> usize {
        let total_column_count = current.total_column_count();
        let max_total_columns = self.max_total_columns as usize;
        if total_column_count < max_total_columns {
            current
                .total_table_count()
                .saturating_add(max_total_columns - total_column_count)
        } else {
            current.total_table_count()
        }
    }

    fn column_per_table_limit(&self, current: &CurrentCatalogUsage) -> usize {
        (self.max_total_columns as usize).saturating_sub(current.total_column_count())
    }

    fn tag_column_per_table_limit(&self, _current: &CurrentCatalogUsage) -> usize {
        NUM_TAG_COLUMNS_LIMIT
    }
}

#[cfg(test)]
mod tests;
