use std::convert::{TryFrom, TryInto};

use data_types::database_rules::{WriteBufferConfig, WriteBufferRollover};

use crate::google::{FieldViolation, FromField};
use crate::influxdata::iox::management::v1 as management;

impl From<WriteBufferConfig> for management::WriteBufferConfig {
    fn from(rollover: WriteBufferConfig) -> Self {
        let buffer_rollover: management::write_buffer_config::Rollover =
            rollover.buffer_rollover.into();

        Self {
            buffer_size: rollover.buffer_size as u64,
            segment_size: rollover.segment_size as u64,
            buffer_rollover: buffer_rollover as _,
            persist_segments: rollover.store_segments,
            close_segment_after: rollover.close_segment_after.map(Into::into),
        }
    }
}

impl TryFrom<management::WriteBufferConfig> for WriteBufferConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::WriteBufferConfig) -> Result<Self, Self::Error> {
        let buffer_rollover = proto.buffer_rollover().scope("buffer_rollover")?;
        let close_segment_after = proto
            .close_segment_after
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "closeSegmentAfter".to_string(),
                description: "Duration must be positive".to_string(),
            })?;

        Ok(Self {
            buffer_size: proto.buffer_size as usize,
            segment_size: proto.segment_size as usize,
            buffer_rollover,
            store_segments: proto.persist_segments,
            close_segment_after,
        })
    }
}

impl From<WriteBufferRollover> for management::write_buffer_config::Rollover {
    fn from(rollover: WriteBufferRollover) -> Self {
        match rollover {
            WriteBufferRollover::DropOldSegment => Self::DropOldSegment,
            WriteBufferRollover::DropIncoming => Self::DropIncoming,
            WriteBufferRollover::ReturnError => Self::ReturnError,
        }
    }
}

impl TryFrom<management::write_buffer_config::Rollover> for WriteBufferRollover {
    type Error = FieldViolation;

    fn try_from(proto: management::write_buffer_config::Rollover) -> Result<Self, Self::Error> {
        use management::write_buffer_config::Rollover;
        Ok(match proto {
            Rollover::Unspecified => return Err(FieldViolation::required("")),
            Rollover::DropOldSegment => Self::DropOldSegment,
            Rollover::DropIncoming => Self::DropIncoming,
            Rollover::ReturnError => Self::ReturnError,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_buffer_config_default() {
        let protobuf: management::WriteBufferConfig = Default::default();

        let res: Result<WriteBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "buffer_rollover");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_write_buffer_config_rollover() {
        let protobuf = management::WriteBufferConfig {
            buffer_rollover: management::write_buffer_config::Rollover::DropIncoming as _,
            ..Default::default()
        };

        let config: WriteBufferConfig = protobuf.clone().try_into().unwrap();
        let back: management::WriteBufferConfig = config.clone().into();

        assert_eq!(config.buffer_rollover, WriteBufferRollover::DropIncoming);
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_write_buffer_config_negative_duration() {
        use crate::google::protobuf::Duration;

        let protobuf = management::WriteBufferConfig {
            buffer_rollover: management::write_buffer_config::Rollover::DropOldSegment as _,
            close_segment_after: Some(Duration {
                seconds: -1,
                nanos: -40,
            }),
            ..Default::default()
        };

        let res: Result<WriteBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "closeSegmentAfter");
        assert_eq!(&err.description, "Duration must be positive");
    }
}
