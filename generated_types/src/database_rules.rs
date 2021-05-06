use std::convert::{TryFrom, TryInto};

use thiserror::Error;

use data_types::database_rules::{ColumnType, ColumnValue, DatabaseRules, Order};
use data_types::DatabaseName;

use crate::google::{FieldViolation, FieldViolationExt, FromFieldOpt};
use crate::influxdata::iox::management::v1 as management;

mod lifecycle;
mod partition;
mod shard;
mod write_buffer;

impl From<DatabaseRules> for management::DatabaseRules {
    fn from(rules: DatabaseRules) -> Self {
        Self {
            name: rules.name.into(),
            partition_template: Some(rules.partition_template.into()),
            write_buffer_config: rules.write_buffer_config.map(Into::into),
            lifecycle_rules: Some(rules.lifecycle_rules.into()),
            shard_config: rules.shard_config.map(Into::into),
        }
    }
}

impl TryFrom<management::DatabaseRules> for DatabaseRules {
    type Error = FieldViolation;

    fn try_from(proto: management::DatabaseRules) -> Result<Self, Self::Error> {
        let name = DatabaseName::new(proto.name.clone()).field("name")?;

        let write_buffer_config = proto.write_buffer_config.optional("write_buffer_config")?;

        let lifecycle_rules = proto
            .lifecycle_rules
            .optional("lifecycle_rules")?
            .unwrap_or_default();

        let partition_template = proto
            .partition_template
            .optional("partition_template")?
            .unwrap_or_default();

        let shard_config = proto
            .shard_config
            .optional("shard_config")
            .unwrap_or_default();

        Ok(Self {
            name,
            partition_template,
            write_buffer_config,
            lifecycle_rules,
            shard_config,
        })
    }
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("failed to decode protobuf: {0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("validation failed: {0}")]
    ValidationError(#[from] FieldViolation),
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("failed to encode protobuf: {0}")]
    EncodeError(#[from] prost::EncodeError),
}

pub fn decode_database_rules(bytes: prost::bytes::Bytes) -> Result<DatabaseRules, DecodeError> {
    let message: management::DatabaseRules = prost::Message::decode(bytes)?;
    Ok(message.try_into()?)
}

pub fn encode_database_rules(
    rules: DatabaseRules,
    bytes: &mut prost::bytes::BytesMut,
) -> Result<(), EncodeError> {
    let encoded: management::DatabaseRules = rules.into();
    Ok(prost::Message::encode(&encoded, bytes)?)
}

impl From<Order> for management::Order {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => Self::Asc,
            Order::Desc => Self::Desc,
        }
    }
}

impl TryFrom<management::Order> for Order {
    type Error = FieldViolation;

    fn try_from(proto: management::Order) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::Order::Unspecified => Self::default(),
            management::Order::Asc => Self::Asc,
            management::Order::Desc => Self::Desc,
        })
    }
}

impl From<ColumnType> for management::ColumnType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::I64 => Self::I64,
            ColumnType::U64 => Self::U64,
            ColumnType::F64 => Self::F64,
            ColumnType::String => Self::String,
            ColumnType::Bool => Self::Bool,
        }
    }
}

impl TryFrom<management::ColumnType> for ColumnType {
    type Error = FieldViolation;

    fn try_from(proto: management::ColumnType) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::ColumnType::Unspecified => return Err(FieldViolation::required("")),
            management::ColumnType::I64 => Self::I64,
            management::ColumnType::U64 => Self::U64,
            management::ColumnType::F64 => Self::F64,
            management::ColumnType::String => Self::String,
            management::ColumnType::Bool => Self::Bool,
        })
    }
}

impl From<ColumnValue> for management::Aggregate {
    fn from(v: ColumnValue) -> Self {
        match v {
            ColumnValue::Min => Self::Min,
            ColumnValue::Max => Self::Max,
        }
    }
}

impl TryFrom<management::Aggregate> for ColumnValue {
    type Error = FieldViolation;

    fn try_from(proto: management::Aggregate) -> Result<Self, Self::Error> {
        use management::Aggregate;

        Ok(match proto {
            Aggregate::Unspecified => return Err(FieldViolation::required("")),
            Aggregate::Min => Self::Min,
            Aggregate::Max => Self::Max,
        })
    }
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

        // These should be none as preserved on non-protobuf DatabaseRules
        assert!(back.write_buffer_config.is_none());
        assert!(back.shard_config.is_none());
    }
}
