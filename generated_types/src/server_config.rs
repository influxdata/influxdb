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

/// Encode server information to be serialized into a database's object store directory and used to
/// identify that database's owning server
pub fn encode_database_owner_info(
    owner_info: &management::OwnerInfo,
    bytes: &mut prost::bytes::BytesMut,
) -> Result<(), EncodeError> {
    prost::Message::encode(owner_info, bytes)
}

/// Encode server information that was encoded using `encode_database_owner_info` to compare
/// with the currently-running server
pub fn decode_database_owner_info(
    bytes: prost::bytes::Bytes,
) -> Result<management::OwnerInfo, DecodeError> {
    prost::Message::decode(bytes)
}
