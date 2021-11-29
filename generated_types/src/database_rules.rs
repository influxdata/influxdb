use crate::{
    google::{FieldViolation, FieldViolationExt, FromOptionalField},
    influxdata::iox::management::v1 as management,
    DecodeError, EncodeError,
};
use data_types::{database_rules::DatabaseRules, DatabaseName};
use std::{
    convert::{TryFrom, TryInto},
    time::Duration,
};

mod lifecycle;
mod partition;

impl From<DatabaseRules> for management::DatabaseRules {
    fn from(rules: DatabaseRules) -> Self {
        Self {
            name: rules.name.into(),
            partition_template: Some(rules.partition_template.into()),
            lifecycle_rules: Some(rules.lifecycle_rules.into()),
            worker_cleanup_avg_sleep: Some(rules.worker_cleanup_avg_sleep.into()),
            write_buffer_connection: rules.write_buffer_connection.map(Into::into),
        }
    }
}

impl TryFrom<management::DatabaseRules> for DatabaseRules {
    type Error = FieldViolation;

    fn try_from(proto: management::DatabaseRules) -> Result<Self, Self::Error> {
        let name = DatabaseName::new(proto.name.clone()).scope("name")?;

        let lifecycle_rules = proto
            .lifecycle_rules
            .optional("lifecycle_rules")?
            .unwrap_or_default();

        let partition_template = proto
            .partition_template
            .optional("partition_template")?
            .unwrap_or_default();

        let worker_cleanup_avg_sleep = match proto.worker_cleanup_avg_sleep {
            Some(d) => d.try_into().scope("worker_cleanup_avg_sleep")?,
            None => Duration::from_secs(500),
        };

        let write_buffer_connection = match proto.write_buffer_connection {
            Some(c) => Some(c.try_into().scope("write_buffer_connection")?),
            None => None,
        };

        Ok(Self {
            name,
            partition_template,
            lifecycle_rules,
            worker_cleanup_avg_sleep,
            write_buffer_connection,
        })
    }
}

/// Decode database rules that were encoded using `encode_persisted_database_rules`
pub fn decode_persisted_database_rules(
    bytes: prost::bytes::Bytes,
) -> Result<management::PersistedDatabaseRules, DecodeError> {
    prost::Message::decode(bytes)
}

/// Encode database rules into a serialized format suitable for storage in object store
pub fn encode_persisted_database_rules(
    rules: &management::PersistedDatabaseRules,
    bytes: &mut prost::bytes::BytesMut,
) -> Result<(), EncodeError> {
    prost::Message::encode(rules, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::database_rules::LifecycleRules;

    #[test]
    fn test_database_rules_defaults() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.name.as_str(), protobuf.name.as_str());
        assert_eq!(protobuf.name, back.name);

        assert_eq!(rules.partition_template.parts.len(), 0);

        // These will be defaulted as optionality not preserved on non-protobuf
        // DatabaseRules
        assert_eq!(back.partition_template, Some(Default::default()));
        assert_eq!(back.lifecycle_rules, Some(LifecycleRules::default().into()));
    }
}
