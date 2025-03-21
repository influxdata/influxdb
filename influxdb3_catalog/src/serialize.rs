use std::io::Cursor;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

use crate::{
    CatalogError, Result,
    log::{self, OrderedCatalogBatch},
    snapshot::{self, CatalogSnapshot},
};

const CHECKSUM_LEN: usize = size_of::<u32>();

pub fn verify_and_deserialize_catalog_file(bytes: Bytes) -> Result<OrderedCatalogBatch> {
    if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER_V1) {
        // V1 Deserialization:
        let id_len = LOG_FILE_TYPE_IDENTIFIER_V1.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = bitcode::deserialize::<log::versions::v1::OrderedCatalogBatch>(&data)
            .context("failed to deserialize v1 catalog log file contents")?;
        Ok(log.into())
    } else if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER_V2) {
        // V2 Deserialization:
        let id_len = LOG_FILE_TYPE_IDENTIFIER_V2.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = serde_json::from_slice::<OrderedCatalogBatch>(&data)
            .context("failed to deserialize v1 catalog log file contents")?;
        Ok(log)
    } else {
        Err(CatalogError::unexpected("unrecognized catalog file format"))
    }
}

pub fn verify_and_deserialize_catalog_checkpoint_file(bytes: Bytes) -> Result<CatalogSnapshot> {
    if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER_V1) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER_V1.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot = bitcode::deserialize::<snapshot::versions::v1::CatalogSnapshot>(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        Ok(snapshot.into())
    } else if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER_V2) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER_V2.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot = serde_json::from_slice(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        Ok(snapshot)
    } else {
        Err(CatalogError::unexpected(
            "unrecognized catalog checkpoint file format",
        ))
    }
}

fn verify_checksum(checksum: &[u8], data: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(checksum);
    let crc32_checksum = cursor
        .read_u32::<BigEndian>()
        .expect("read big endian u32 checksum");
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    let checksum = hasher.finalize();
    if checksum != crc32_checksum {
        return Err(CatalogError::unexpected(
            "crc 32 checksum mismatch when deserializing catalog log file",
        ));
    }
    Ok(())
}

const LOG_FILE_TYPE_IDENTIFIER_V1: &[u8] = b"idb3.001.l";
const LOG_FILE_TYPE_IDENTIFIER_V2: &[u8] = b"idb3.002.l";

pub fn serialize_catalog_log(log: &OrderedCatalogBatch) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(LOG_FILE_TYPE_IDENTIFIER_V2);

    let data = serde_json::to_vec(log).context("failed to serialize catalog log file")?;

    Ok(hash_and_freeze(buf, data))
}

const SNAPSHOT_FILE_TYPE_IDENTIFIER_V1: &[u8] = b"idb3.001.s";
const SNAPSHOT_FILE_TYPE_IDENTIFIER_V2: &[u8] = b"idb3.002.s";

pub fn serialize_catalog_snapshot(snapshot: &CatalogSnapshot) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(SNAPSHOT_FILE_TYPE_IDENTIFIER_V2);

    let data = serde_json::to_vec(snapshot).context("failed to serialize catalog snapshot file")?;

    Ok(hash_and_freeze(buf, data))
}

fn hash_and_freeze(mut buf: BytesMut, data: Vec<u8>) -> Bytes {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&data);
    let checksum = hasher.finalize();

    buf.extend_from_slice(&checksum.to_be_bytes());
    buf.extend_from_slice(&data);

    buf.freeze()
}
