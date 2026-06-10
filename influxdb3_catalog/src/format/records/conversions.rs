//! Shared conversions between wire and schema representations of
//! cross-cutting types.

use std::sync::Arc;
use std::time::Duration;

use iox_time::Time;

use super::types::{DeletionScope, NodeSpec, RetentionPeriod};
use crate::catalog::versions::v3::deletes::DeletionScope as SchemaDeletionScope;
use crate::catalog::versions::v3::schema::node::NodeSpec as SchemaNodeSpec;
use crate::catalog::versions::v3::schema::retention::RetentionPeriod as SchemaRetentionPeriod;
use influxdb3_id::NodeId;

/// Format string for soft-deleted resource name suffixes.
pub(crate) const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

/// Build the renamed name for a soft-deleted resource as `{base}-{deletion_time}`.
/// If that stamped name already exists (per `has_name_collision`), the resource's
/// unique `id` is appended so the deleted name stays unique within the repository.
pub(crate) fn soft_deleted_name(
    base: &str,
    deletion_time: Time,
    id: impl std::fmt::Display,
    has_name_collision: impl Fn(&str) -> bool,
) -> Arc<str> {
    let stamped = format!(
        "{base}-{}",
        deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT)
    );
    if has_name_collision(&stamped) {
        Arc::from(format!("{stamped}-{id}"))
    } else {
        Arc::from(stamped)
    }
}

impl From<&RetentionPeriod> for SchemaRetentionPeriod {
    fn from(value: &RetentionPeriod) -> Self {
        match value {
            RetentionPeriod::Indefinite => Self::Indefinite,
            RetentionPeriod::Duration { duration_secs } => {
                Self::Duration(std::time::Duration::from_secs(*duration_secs))
            }
        }
    }
}

impl From<&DeletionScope> for SchemaDeletionScope {
    fn from(value: &DeletionScope) -> Self {
        match value {
            DeletionScope::DataAndCatalog => Self::DataAndCatalog,
            DeletionScope::DataOnlyRemoveTables => Self::DataOnlyRemoveTables,
            DeletionScope::DataOnlyKeepResources => Self::DataOnlyKeepResources,
        }
    }
}

impl From<&NodeSpec> for SchemaNodeSpec {
    fn from(value: &NodeSpec) -> Self {
        match value {
            NodeSpec::All => Self::All,
            NodeSpec::Nodes(ids) => Self::Nodes(ids.iter().map(|id| NodeId::new(*id)).collect()),
        }
    }
}

impl From<Option<Duration>> for RetentionPeriod {
    fn from(value: Option<Duration>) -> Self {
        match value {
            None => Self::Indefinite,
            Some(d) => Self::Duration {
                duration_secs: d.as_secs(),
            },
        }
    }
}

impl From<&SchemaDeletionScope> for DeletionScope {
    fn from(value: &SchemaDeletionScope) -> Self {
        match value {
            SchemaDeletionScope::DataAndCatalog => Self::DataAndCatalog,
            SchemaDeletionScope::DataOnlyRemoveTables => Self::DataOnlyRemoveTables,
            SchemaDeletionScope::DataOnlyKeepResources => Self::DataOnlyKeepResources,
        }
    }
}

impl From<&SchemaNodeSpec> for NodeSpec {
    fn from(value: &SchemaNodeSpec) -> Self {
        match value {
            SchemaNodeSpec::All => Self::All,
            SchemaNodeSpec::Nodes(ids) => Self::Nodes(ids.iter().map(|id| id.get()).collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::soft_deleted_name;
    use iox_time::Time;

    #[test]
    fn keeps_stamped_name_when_free() {
        let name = soft_deleted_name("cpu", Time::from_timestamp_nanos(0), 7, |_| false);
        // no collision: plain `{name}-{stamp}`, no id suffix
        assert_eq!(name.as_ref(), "cpu-19700101T000000");
    }

    #[test]
    fn appends_id_when_stamped_name_taken() {
        let name = soft_deleted_name("cpu", Time::from_timestamp_nanos(0), 7, |n| {
            n == "cpu-19700101T000000"
        });
        // collision: id appended to keep the deleted name unique
        assert_eq!(name.as_ref(), "cpu-19700101T000000-7");
    }
}
