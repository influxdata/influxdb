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
