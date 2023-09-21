use std::sync::Arc;

use data_types::{partition_template::TablePartitionTemplateOverride, Table};

/// Metadata from the catalog for a table
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMetadata {
    name: TableName,
    partition_template: TablePartitionTemplateOverride,
}

impl TableMetadata {
    #[cfg(test)]
    pub fn new_for_testing(
        name: TableName,
        partition_template: TablePartitionTemplateOverride,
    ) -> Self {
        Self {
            name,
            partition_template,
        }
    }

    pub(crate) fn name(&self) -> &TableName {
        &self.name
    }

    pub(crate) fn partition_template(&self) -> &TablePartitionTemplateOverride {
        &self.partition_template
    }
}

impl From<Table> for TableMetadata {
    fn from(t: Table) -> Self {
        Self {
            name: t.name.into(),
            partition_template: t.partition_template,
        }
    }
}

impl std::fmt::Display for TableMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.name, f)
    }
}

/// The string name / identifier of a Table.
///
/// A reference-counted, cheap clone-able string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TableName(Arc<str>);

impl<T> From<T> for TableName
where
    T: AsRef<str>,
{
    fn from(v: T) -> Self {
        Self(Arc::from(v.as_ref()))
    }
}

impl From<TableName> for Arc<str> {
    fn from(v: TableName) -> Self {
        v.0
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for TableName {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}
