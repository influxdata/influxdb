use std::io::Cursor;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

use crate::{
    CatalogError, Result, catalog::CatalogSequenceNumber, log::OrderedCatalogBatch,
    snapshot::CatalogSnapshot,
};

#[derive(Debug)]
pub enum CatalogFile {
    Log(OrderedCatalogBatch),
    Snapshot(CatalogSnapshot),
}

impl CatalogFile {
    pub fn sequence(&self) -> CatalogSequenceNumber {
        match self {
            CatalogFile::Log(batch) => batch.sequence_number(),
            CatalogFile::Snapshot(snap) => snap.sequence_number(),
        }
    }
}

const CHECKSUM_LEN: usize = size_of::<u32>();

pub fn verify_and_deserialize_catalog(bytes: Bytes) -> Result<CatalogFile> {
    if bytes.starts_with(LOG_FILE_TYPE_IDENTIFIER) {
        let id_len = LOG_FILE_TYPE_IDENTIFIER.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let log = bitcode::deserialize(&data)
            .context("failed to deserialize catalog log file contents")?;
        Ok(CatalogFile::Log(log))
    } else if bytes.starts_with(SNAPSHOT_FILE_TYPE_IDENTIFIER) {
        let id_len = SNAPSHOT_FILE_TYPE_IDENTIFIER.len();
        let checksum = bytes.slice(id_len..id_len + CHECKSUM_LEN);
        let data = bytes.slice(id_len + CHECKSUM_LEN..);
        verify_checksum(&checksum, &data)?;
        let snapshot = bitcode::deserialize(&data)
            .context("failed to deserialize catalog snapshot file contents")?;
        Ok(CatalogFile::Snapshot(snapshot))
    } else {
        Err(CatalogError::unexpected("unrecognized catalog file format"))
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

const LOG_FILE_TYPE_IDENTIFIER: &[u8] = b"idb3.001.l";

pub fn serialize_catalog_log(log: &OrderedCatalogBatch) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(LOG_FILE_TYPE_IDENTIFIER);

    let data = bitcode::serialize(log).context("failed to serialize catalog log file")?;

    Ok(hash_and_freeze(buf, data))
}

const SNAPSHOT_FILE_TYPE_IDENTIFIER: &[u8] = b"idb3.001.s";

pub fn serialize_catalog_snapshot(snapshot: &CatalogSnapshot) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(SNAPSHOT_FILE_TYPE_IDENTIFIER);

    let data = bitcode::serialize(snapshot).context("failed to serialize catalog snapshot file")?;

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
