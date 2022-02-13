use data_types::{database_rules::DatabaseRules, DatabaseName};
use generated_types::{
    google::{FieldViolation, FieldViolationExt},
    influxdata::iox::management,
};
use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};
use uuid::Uuid;

/// The configuration ([`DatabaseRules`]) used to create and update
/// databases, both in original and "materialized" (with defaults filled in) form.
///
/// The rationale for storing both the rules as they were provided
/// *and* materialized form is provide the property that if the same
/// rules are sent to a database that were previously sent the
/// database will still be runing the same configuration.  If the
/// materialized configuration was stored, and then the defaults were
/// changed in a new version of the software, the required property
/// would not hold.
///
/// While this may sound like an esoteric corner case with little real
/// world impact, it has non trivial real world implications for
/// keeping the configurations of fleets of IOx servers in sync. See
/// <https://github.com/influxdata/influxdb_iox/issues/2409> for
/// further gory details.
///
/// A design goal is to keep the notion of "what user provided" as
/// isolated as much as possible so only the server crate worries
/// about what the user actually provided and the rest of the system
/// can use `data_types::database_rules::PersistedDatabaseRules` in
/// blissful ignorance of such subtlties
#[derive(Debug, Clone)]
pub struct ProvidedDatabaseRules {
    /// Full database rules, with all fields set. Derived from
    /// `original` by applying default values.
    full: Arc<DatabaseRules>,

    /// Encoded database rules, as provided by the user and as stored
    /// in the object store (may not have all fields set).
    original: management::v1::DatabaseRules,
}

impl ProvidedDatabaseRules {
    // Create a new database with a default database
    pub fn new_empty(db_name: DatabaseName<'static>) -> Self {
        let original = management::v1::DatabaseRules {
            name: db_name.to_string(),
            ..Default::default()
        };

        // Should always be able to create a DBRules with default values
        let full = Arc::new(original.clone().try_into().expect("creating empty rules"));

        Self { full, original }
    }

    pub fn new_rules(original: management::v1::DatabaseRules) -> Result<Self, FieldViolation> {
        let full = Arc::new(original.clone().try_into()?);

        Ok(Self { full, original })
    }

    /// returns the name of the database in the rules
    pub fn db_name(&self) -> &DatabaseName<'static> {
        &self.full.name
    }

    /// Return the full database rules
    pub fn rules(&self) -> &Arc<DatabaseRules> {
        &self.full
    }

    /// Return the original rules provided to this
    pub fn original(&self) -> &management::v1::DatabaseRules {
        &self.original
    }
}

#[derive(Debug, Clone)]
pub struct PersistedDatabaseRules {
    uuid: Uuid,
    provided: ProvidedDatabaseRules,
}

impl PersistedDatabaseRules {
    pub fn new(uuid: Uuid, provided: ProvidedDatabaseRules) -> Self {
        Self { uuid, provided }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn db_name(&self) -> &DatabaseName<'static> {
        &self.provided.full.name
    }

    /// Return the full database rules
    pub fn rules(&self) -> &Arc<DatabaseRules> {
        &self.provided.full
    }

    /// Return the original rules provided to this
    pub fn original(&self) -> &management::v1::DatabaseRules {
        &self.provided.original
    }

    /// Convert to its inner representation
    pub fn into_inner(self) -> (Uuid, ProvidedDatabaseRules) {
        (self.uuid, self.provided)
    }
}

impl TryFrom<management::v1::PersistedDatabaseRules> for PersistedDatabaseRules {
    type Error = FieldViolation;

    /// Create a new PersistedDatabaseRules from a grpc message
    fn try_from(proto: management::v1::PersistedDatabaseRules) -> Result<Self, Self::Error> {
        let original: management::v1::DatabaseRules = proto
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let full = Arc::new(original.clone().try_into()?);

        let uuid = Uuid::from_slice(&proto.uuid).scope("uuid")?;

        Ok(Self {
            uuid,
            provided: ProvidedDatabaseRules { full, original },
        })
    }
}
