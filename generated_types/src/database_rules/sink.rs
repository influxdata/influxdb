use std::convert::TryFrom;

use data_types::database_rules::{KafkaProducer, Sink};

use crate::google::{FieldViolation, FromField};
use crate::influxdata::iox::management::v1 as management;

impl From<Sink> for management::Sink {
    fn from(shard: Sink) -> Self {
        let sink = match shard {
            Sink::Iox(node_group) => management::sink::Sink::Iox(node_group.into()),
            Sink::Kafka(kafka) => management::sink::Sink::Kafka(kafka.into()),
            Sink::DevNull => management::sink::Sink::DevNull(Default::default()),
        };
        management::Sink { sink: Some(sink) }
    }
}

impl TryFrom<management::Sink> for Sink {
    type Error = FieldViolation;

    fn try_from(proto: management::Sink) -> Result<Self, Self::Error> {
        let sink = proto.sink.ok_or_else(|| FieldViolation::required(""))?;
        Ok(match sink {
            management::sink::Sink::Iox(node_group) => Sink::Iox(node_group.scope("node_group")?),
            management::sink::Sink::Kafka(kafka) => Sink::Kafka(kafka.scope("kafka")?),
            management::sink::Sink::DevNull(_) => Sink::DevNull,
        })
    }
}

impl From<KafkaProducer> for management::KafkaProducer {
    fn from(_: KafkaProducer) -> Self {
        Self {}
    }
}

impl TryFrom<management::KafkaProducer> for KafkaProducer {
    type Error = FieldViolation;

    fn try_from(_: management::KafkaProducer) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
