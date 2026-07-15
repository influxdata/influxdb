//! Serde-serializable views over a parsed catalog file, for inspection tooling.

use serde::Serialize;

use crate::format::{CatalogFile, Header, REGISTRY, Record, RecordId};

/// View of a file header.
#[derive(Debug, Clone, Serialize)]
pub struct HeaderView {
    pub format_version: u32,
    pub snapshot: bool,
    pub catalog_uuid: String,
    pub sequence_number: u64,
    pub record_count: u32,
    pub payload_len: u64,
    pub payload_crc: u32,
}

impl From<&Header> for HeaderView {
    fn from(h: &Header) -> Self {
        Self {
            format_version: h.format_version,
            snapshot: h.is_snapshot(),
            catalog_uuid: h.catalog_uuid().to_string(),
            sequence_number: h.sequence_number,
            record_count: h.record_count,
            payload_len: h.payload_len,
            payload_crc: h.payload_crc,
        }
    }
}

/// View of a record header (no decoded body).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RecordHeaderView {
    pub id: RecordId,
    pub name: Option<&'static str>,
    pub upgrade_safe: bool,
    pub sequence: u64,
    pub length: u32,
}

impl RecordHeaderView {
    pub fn from_record(record: &Record) -> Self {
        Self {
            id: record.id(),
            name: REGISTRY.get(record.id()).map(|e| e.name),
            upgrade_safe: record.is_upgrade_safe(),
            sequence: record.sequence(),
            length: record.header.length,
        }
    }
}

/// Decoded body, or a hex dump when the record id is unknown or undecodable.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum RecordBodyView {
    Decoded(serde_json::Value),
    Unknown { unknown: bool, hex: String },
}

impl RecordBodyView {
    fn from_record(record: &Record) -> Self {
        match REGISTRY.get(record.id()) {
            Some(entry) => match (entry.decode_to_value)(&record.data) {
                Ok(value) => RecordBodyView::Decoded(value),
                Err(_) => RecordBodyView::Unknown {
                    unknown: true,
                    hex: hex_encode(&record.data),
                },
            },
            None => RecordBodyView::Unknown {
                unknown: true,
                hex: hex_encode(&record.data),
            },
        }
    }
}

/// View of a record header plus its decoded body.
#[derive(Debug, Clone, Serialize)]
pub struct RecordView {
    #[serde(flatten)]
    pub header: RecordHeaderView,
    pub body: RecordBodyView,
}

impl RecordView {
    pub fn from_record(record: &Record) -> Self {
        Self {
            header: RecordHeaderView::from_record(record),
            body: RecordBodyView::from_record(record),
        }
    }
}

/// Count of records of a given type.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RecordTypeCount {
    pub id: RecordId,
    pub name: Option<&'static str>,
    pub count: u64,
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The header view for a parsed file.
pub fn header_view(file: &CatalogFile) -> HeaderView {
    HeaderView::from(&file.header)
}

/// All records in the file, in write order.
pub fn all_records(file: &CatalogFile) -> Vec<&Record> {
    file.records.iter().collect()
}

/// Full decoded record views for every record in the file.
pub fn record_views(file: &CatalogFile) -> Vec<RecordView> {
    all_records(file)
        .into_iter()
        .map(RecordView::from_record)
        .collect()
}

/// Histogram of record types over a set of records, sorted by id.
pub fn histogram(records: &[&Record]) -> Vec<RecordTypeCount> {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<RecordId, u64> = BTreeMap::new();
    for r in records {
        *counts.entry(r.id()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(id, count)| RecordTypeCount {
            id,
            name: REGISTRY.get(id).map(|e| e.name),
            count,
        })
        .collect()
}

#[cfg(test)]
mod tests;
