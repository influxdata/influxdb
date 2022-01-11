use bytes::Bytes;
use generated_types::influxdata::iox::preserved_catalog::v1 as proto;
use iox_object_store::{IoxObjectStore, TransactionFilePath};
use object_store::{ObjectStore, ObjectStoreApi};
use prost::Message;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error during protobuf serialization: {}", source))]
    Serialization { source: prost::EncodeError },

    #[snafu(display("Error during protobuf deserialization: {}", source))]
    Deserialization { source: prost::DecodeError },

    #[snafu(display("Error during store write operation: {}", source))]
    Write {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Error during store read operation: {}", source))]
    Read {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Serialize and store protobuf-encoded transaction.
pub async fn store_transaction_proto(
    iox_object_store: &IoxObjectStore,
    path: &TransactionFilePath,
    proto: &proto::Transaction,
) -> Result<()> {
    let mut data = Vec::new();
    proto.encode(&mut data).context(SerializationSnafu {})?;
    let data = Bytes::from(data);

    iox_object_store
        .put_catalog_transaction_file(path, data)
        .await
        .context(WriteSnafu {})?;

    Ok(())
}

/// Load and deserialize protobuf-encoded transaction from store.
pub async fn load_transaction_proto(
    iox_object_store: &IoxObjectStore,
    path: &TransactionFilePath,
) -> Result<proto::Transaction> {
    let data = iox_object_store
        .get_catalog_transaction_file(path)
        .await
        .context(ReadSnafu {})?
        .bytes()
        .await
        .context(ReadSnafu {})?;
    let proto = proto::Transaction::decode(&data[..]).context(DeserializationSnafu {})?;
    Ok(proto)
}
