use crate::{influxdata::iox::management::v1 as management, DecodeError, EncodeError};

/// Decode server config that was encoded using `encode_persisted_server_config`
pub fn decode_persisted_server_config(
    bytes: prost::bytes::Bytes,
) -> Result<management::ServerConfig, DecodeError> {
    prost::Message::decode(bytes)
}

/// Encode server config into a serialized format suitable for storage in object store
pub fn encode_persisted_server_config(
    server_config: &management::ServerConfig,
    bytes: &mut prost::bytes::BytesMut,
) -> Result<(), EncodeError> {
    prost::Message::encode(server_config, bytes)
}
