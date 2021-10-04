use data_types::{database_rules::DatabaseRules, DatabaseName};
use generated_types::{
    database_rules::encode_persisted_database_rules, google::FieldViolation,
    influxdata::iox::management,
};
use iox_object_store::IoxObjectStore;
use snafu::{ResultExt, Snafu};
use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error saving rules for {}: {}", db_name, source))]
    ObjectStore {
        db_name: String,
        source: object_store::Error,
    },

    #[snafu(display("error deserializing database rules: {}", source))]
    Deserialization {
        source: generated_types::database_rules::ProstError,
    },

    #[snafu(display("error serializing database rules: {}", source))]
    Serialization {
        source: generated_types::database_rules::ProstError,
    },

    #[snafu(display("error fetching rules: {}", source))]
    RulesFetch { source: object_store::Error },

    #[snafu(display("error converting grpc to database rules: {}", source))]
    ConvertingRules { source: FieldViolation },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Load `ProvidedDatabaseRules` from object storage
    pub async fn load(iox_object_store: &IoxObjectStore) -> Result<Self> {
        // TODO: Retry this
        let bytes = iox_object_store
            .get_database_rules_file()
            .await
            .context(RulesFetch)?;

        let new_self = generated_types::database_rules::decode_persisted_database_rules(bytes)
            .context(Deserialization)?
            .try_into()
            .context(ConvertingRules)?;

        Ok(new_self)
    }

    /// Persist the the `ProvidedDatabaseRules` given the database object storage
    pub async fn persist(&self, uuid: Uuid, iox_object_store: &IoxObjectStore) -> Result<()> {
        let persisted_database_rules = management::v1::PersistedDatabaseRules {
            uuid: uuid.to_string(),
            // Note we save the original version
            rules: Some(self.original.clone()),
        };

        let mut data = bytes::BytesMut::new();
        encode_persisted_database_rules(&persisted_database_rules, &mut data)
            .context(Serialization)?;

        iox_object_store
            .put_database_rules_file(data.freeze())
            .await
            .context(ObjectStore {
                db_name: self.db_name(),
            })?;

        Ok(())
    }
}

impl TryFrom<management::v1::PersistedDatabaseRules> for ProvidedDatabaseRules {
    type Error = FieldViolation;

    /// Create a new ProvidedDatabaseRules from a grpc message
    fn try_from(proto: management::v1::PersistedDatabaseRules) -> Result<Self, Self::Error> {
        let original: management::v1::DatabaseRules = proto
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let full = Arc::new(original.clone().try_into()?);

        Ok(Self { full, original })
    }
}
