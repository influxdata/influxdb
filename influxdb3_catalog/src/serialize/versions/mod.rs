use crate::object_store::ObjectStoreCatalogError;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use std::io::Cursor;

pub mod v1;
pub mod v2;

fn verify_checksum(checksum: &[u8], data: &[u8]) -> Result<(), ObjectStoreCatalogError> {
    let mut cursor = Cursor::new(checksum);
    let crc32_checksum = cursor
        .read_u32::<BigEndian>()
        .expect("read big endian u32 checksum");
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    let checksum = hasher.finalize();
    if checksum != crc32_checksum {
        return Err(ObjectStoreCatalogError::unexpected(
            "crc 32 checksum mismatch when deserializing catalog log file",
        ));
    }
    Ok(())
}

fn hash_and_freeze(mut buf: BytesMut, data: Vec<u8>) -> Bytes {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data);
    let checksum = hasher.finalize();

    buf.extend_from_slice(&checksum.to_be_bytes());
    buf.extend_from_slice(&data);

    buf.freeze()
}

#[cfg(test)]
pub(crate) mod test_util {
    use serde::{Serialize, de::DeserializeOwned};

    /// Deserialize `value` after reshaping its JSON to match what InfluxDB 3 Core wrote
    /// before the catalog codebases were unified: `drop` removes fields Core did not
    /// write; `add` inserts fields only Core wrote.
    pub(crate) fn from_core_shape<T>(
        value: &T,
        drop: &[&str],
        add: &[(&str, serde_json::Value)],
    ) -> serde_json::Result<T>
    where
        T: Serialize + DeserializeOwned,
    {
        let mut json = serde_json::to_value(value)?;
        let obj = json
            .as_object_mut()
            .expect("value serializes to a JSON object");
        for key in drop {
            obj.remove(*key);
        }
        for (key, val) in add {
            obj.insert((*key).to_string(), val.clone());
        }
        serde_json::from_value(json)
    }
}
