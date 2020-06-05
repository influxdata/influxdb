#![allow(
    unused_imports,
    clippy::redundant_static_lifetimes,
    clippy::redundant_closure
)]

include!(concat!(env!("OUT_DIR"), "/influxdata.platform.storage.rs"));
include!(concat!(env!("OUT_DIR"), "/wal_generated.rs"));

// Can't implement `Default` because `prost::Message` implements `Default`
impl TimestampRange {
    pub fn max() -> Self {
        TimestampRange {
            start: std::i64::MIN,
            end: std::i64::MAX,
        }
    }
}
