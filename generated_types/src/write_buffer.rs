use crate::{
    google::{FieldViolation, FromOptionalField},
    influxdata::iox::write_buffer::v1 as write_buffer,
};
use data_types::write_buffer::{
    WriteBufferConnection, WriteBufferCreationConfig, DEFAULT_N_SEQUENCERS,
};
use std::{convert::TryFrom, num::NonZeroU32};

impl From<WriteBufferConnection> for write_buffer::WriteBufferConnection {
    fn from(v: WriteBufferConnection) -> Self {
        Self {
            r#type: v.type_,
            connection: v.connection,
            connection_config: v.connection_config.into_iter().collect(),
            creation_config: v.creation_config.map(|x| x.into()),
        }
    }
}

impl From<WriteBufferCreationConfig> for write_buffer::WriteBufferCreationConfig {
    fn from(v: WriteBufferCreationConfig) -> Self {
        Self {
            n_sequencers: v.n_sequencers.get(),
            options: v.options.into_iter().collect(),
        }
    }
}

impl TryFrom<write_buffer::WriteBufferConnection> for WriteBufferConnection {
    type Error = FieldViolation;

    fn try_from(proto: write_buffer::WriteBufferConnection) -> Result<Self, Self::Error> {
        Ok(Self {
            type_: proto.r#type,
            connection: proto.connection,
            connection_config: proto.connection_config.into_iter().collect(),
            creation_config: proto.creation_config.optional("creation_config")?,
        })
    }
}

impl TryFrom<write_buffer::WriteBufferCreationConfig> for WriteBufferCreationConfig {
    type Error = FieldViolation;

    fn try_from(proto: write_buffer::WriteBufferCreationConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            n_sequencers: NonZeroU32::try_from(proto.n_sequencers)
                .unwrap_or_else(|_| NonZeroU32::try_from(DEFAULT_N_SEQUENCERS).unwrap()),
            options: proto.options.into_iter().collect(),
        })
    }
}
