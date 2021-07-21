use std::convert::TryFrom;

use data_types::database_rules::Sink;

use crate::google::{FieldViolation, FromField};
use crate::influxdata::iox::management::v1 as management;

impl From<Sink> for management::Sink {
    fn from(shard: Sink) -> Self {
        let sink = match shard {
            Sink::Iox(node_group) => management::sink::Sink::Iox(node_group.into()),
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
        })
    }
}
