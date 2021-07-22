use std::convert::{TryFrom, TryInto};
use std::time::Duration;

use thiserror::Error;

use data_types::database_rules::{
    DatabaseRules, RoutingConfig, RoutingRules, Sink, WriteBufferConnection,
};
use data_types::DatabaseName;

use crate::google::{FieldViolation, FieldViolationExt, FromFieldOpt};
use crate::influxdata::iox::management::v1 as management;

mod lifecycle;
mod partition;
mod shard;
mod sink;

impl From<DatabaseRules> for management::DatabaseRules {
    fn from(rules: DatabaseRules) -> Self {
        Self {
            name: rules.name.into(),
            partition_template: Some(rules.partition_template.into()),
            lifecycle_rules: Some(rules.lifecycle_rules.into()),
            routing_rules: rules.routing_rules.map(Into::into),
            worker_cleanup_avg_sleep: Some(rules.worker_cleanup_avg_sleep.into()),
            write_buffer_connection: rules.write_buffer_connection.map(Into::into),
        }
    }
}

impl TryFrom<management::DatabaseRules> for DatabaseRules {
    type Error = FieldViolation;

    fn try_from(proto: management::DatabaseRules) -> Result<Self, Self::Error> {
        let name = DatabaseName::new(proto.name.clone()).field("name")?;

        let lifecycle_rules = proto
            .lifecycle_rules
            .optional("lifecycle_rules")?
            .unwrap_or_default();

        let partition_template = proto
            .partition_template
            .optional("partition_template")?
            .unwrap_or_default();

        let routing_rules = proto
            .routing_rules
            .optional("routing_rules")
            .unwrap_or_default();

        let worker_cleanup_avg_sleep = match proto.worker_cleanup_avg_sleep {
            Some(d) => d.try_into().field("worker_cleanup_avg_sleep")?,
            None => Duration::from_secs(500),
        };

        let write_buffer_connection = match proto.write_buffer_connection {
            Some(c) => Some(c.try_into().field("write_buffer_connection")?),
            None => None,
        };

        Ok(Self {
            name,
            partition_template,
            lifecycle_rules,
            routing_rules,
            worker_cleanup_avg_sleep,
            write_buffer_connection,
        })
    }
}

impl From<RoutingRules> for management::database_rules::RoutingRules {
    fn from(routing_rules: RoutingRules) -> Self {
        match routing_rules {
            RoutingRules::RoutingConfig(cfg) => {
                management::database_rules::RoutingRules::RoutingConfig(cfg.into())
            }
            RoutingRules::ShardConfig(cfg) => {
                management::database_rules::RoutingRules::ShardConfig(cfg.into())
            }
        }
    }
}

impl TryFrom<management::database_rules::RoutingRules> for RoutingRules {
    type Error = FieldViolation;

    fn try_from(proto: management::database_rules::RoutingRules) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::database_rules::RoutingRules::ShardConfig(cfg) => {
                RoutingRules::ShardConfig(cfg.try_into()?)
            }
            management::database_rules::RoutingRules::RoutingConfig(cfg) => {
                RoutingRules::RoutingConfig(cfg.try_into()?)
            }
        })
    }
}

impl From<RoutingConfig> for management::RoutingConfig {
    fn from(routing_config: RoutingConfig) -> Self {
        #[allow(deprecated)]
        Self {
            target: None,
            sink: Some(routing_config.sink.into()),
        }
    }
}

impl TryFrom<management::RoutingConfig> for RoutingConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::RoutingConfig) -> Result<Self, Self::Error> {
        #[allow(deprecated)]
        Ok(Self {
            sink: if proto.target.is_some() {
                Sink::Iox(proto.target.required("target")?)
            } else {
                proto.sink.required("sink")?
            },
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

impl From<WriteBufferConnection> for management::database_rules::WriteBufferConnection {
    fn from(v: WriteBufferConnection) -> Self {
        match v {
            WriteBufferConnection::Writing(c) => Self::Writing(c),
            WriteBufferConnection::Reading(c) => Self::Reading(c),
        }
    }
}

impl TryFrom<management::database_rules::WriteBufferConnection> for WriteBufferConnection {
    type Error = FieldViolation;

    fn try_from(
        proto: management::database_rules::WriteBufferConnection,
    ) -> Result<Self, Self::Error> {
        use management::database_rules::WriteBufferConnection;

        Ok(match proto {
            WriteBufferConnection::Writing(c) => Self::Writing(c),
            WriteBufferConnection::Reading(c) => Self::Reading(c),
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
        assert!(back.routing_rules.is_none());
    }

    #[test]
    fn test_routing_rules_conversion() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            routing_rules: None,
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.try_into().unwrap();
        let back: management::DatabaseRules = rules.into();

        assert!(back.routing_rules.is_none());

        #[allow(deprecated)]
        let routing_config = management::RoutingConfig {
            target: Some(management::NodeGroup {
                nodes: vec![management::node_group::Node { id: 1234 }],
            }),
            sink: None,
        };

        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            routing_rules: Some(management::database_rules::RoutingRules::RoutingConfig(
                routing_config,
            )),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.try_into().unwrap();
        let back: management::DatabaseRules = rules.into();

        assert!(back.routing_rules.is_some());

        #[allow(deprecated)]
        let routing_config = management::RoutingConfig {
            target: None,
            sink: Some(management::Sink {
                sink: Some(management::sink::Sink::Iox(management::NodeGroup {
                    nodes: vec![management::node_group::Node { id: 1234 }],
                })),
            }),
        };

        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            routing_rules: Some(management::database_rules::RoutingRules::RoutingConfig(
                routing_config,
            )),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.try_into().unwrap();
        let back: management::DatabaseRules = rules.into();

        assert!(back.routing_rules.is_some());
    }
}
