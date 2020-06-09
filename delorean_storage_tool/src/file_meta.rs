#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use std::collections::{BTreeMap, BTreeSet};

use delorean::storage::tsm::{IndexEntry, InfluxID, TSMReader};
use delorean::storage::StorageError;

use log::{debug, info};

use crate::error::{Error, Result};
use crate::input::{FileType, InputReader};

pub fn dump_meta(input_filename: &str) -> Result<()> {
    info!("dstool meta starting");
    debug!("Reading from input file {}", input_filename);

    let input_reader = InputReader::new(input_filename)?;

    match input_reader.file_type() {
        FileType::LineProtocol => Err(Error::NotImplemented {
            operation_name: String::from("Line protocol metadata dump"),
        }),
        FileType::TSM => {
            let len = input_reader.len();
            let mut reader = TSMReader::new(input_reader, len);
            let index = reader.index().map_err(|e| Error::TSM { source: e })?;

            let mut stats_builder = TSMMetadataBuilder::new();

            for mut entry in index {
                stats_builder.process_entry(&mut entry)?;
            }
            stats_builder.print_report();
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
struct MeasurementMetadata {
    /// tag name --> list of seen tag values
    tags: BTreeMap<String, BTreeSet<String>>,
    /// List of field names seen
    fields: BTreeSet<String>,
}

impl MeasurementMetadata {
    fn update_for_entry(&mut self, index_entry: &mut IndexEntry) -> Result<()> {
        let tagset = index_entry.tagset().map_err(|e| Error::TSM { source: e })?;
        for (tag_name, tag_value) in tagset {
            let tag_entry = self.tags.entry(tag_name).or_default();
            tag_entry.insert(tag_value);
        }
        let field_name = index_entry
            .field_key()
            .map_err(|e| Error::TSM { source: e })?;
        self.fields.insert(field_name);
        Ok(())
    }

    fn print_report(&self, prefix: &str) {
        for (tag_name, tag_values) in &self.tags {
            println!("{} tag {} = {:?}", prefix, tag_name, tag_values);
        }
        for field_name in &self.fields {
            println!("{} field {}", prefix, field_name);
        }
    }
}

/// Represents stats for a single bucket
#[derive(Debug, Default)]
struct BucketMetadata {
    /// How many index entries have been seen
    count: u64,

    // Total 'records' (aka sum of the lengths of all timeseries)
    total_records: u64,

    // measurement ->
    measurements: BTreeMap<String, MeasurementMetadata>,
}

impl BucketMetadata {
    fn update_for_entry(&mut self, index_entry: &mut IndexEntry) -> Result<()> {
        self.count += 1;
        self.total_records += u64::from(index_entry.count);
        let measurement = index_entry
            .measurement()
            .map_err(|e| Error::TSM { source: e })?;
        let meta = self.measurements.entry(measurement).or_default();
        meta.update_for_entry(index_entry)?;
        Ok(())
    }

    fn print_report(&self, prefix: &str) {
        for (measurement, meta) in &self.measurements {
            println!("{}{}", prefix, measurement);
            let indent = format!("{}  ", prefix);
            meta.print_report(&indent);
        }
    }
}

#[derive(Debug, Default)]
struct TSMMetadataBuilder {
    num_entries: u32,

    // (org_id, bucket_id) --> Bucket Metadata
    bucket_stats: BTreeMap<(InfluxID, InfluxID), BucketMetadata>,
}

impl TSMMetadataBuilder {
    fn new() -> TSMMetadataBuilder {
        Self::default()
    }

    fn process_entry(&mut self, entry: &mut Result<IndexEntry, StorageError>) -> Result<()> {
        match entry {
            Ok(index_entry) => {
                self.num_entries += 1;
                let key = (index_entry.org_id(), index_entry.bucket_id());
                let stats = self.bucket_stats.entry(key).or_default();
                stats.update_for_entry(index_entry)?;
                Ok(())
            }
            Err(e) => Err(Error::TSM { source: e.clone() }),
        }
    }

    fn print_report(&self) {
        println!("TSM Metadata Report:");
        println!("  Valid Index Entries: {}", self.num_entries);
        println!("  Organizations/Bucket Stats:");
        for (k, stats) in &self.bucket_stats {
            let (org_id, bucket_id) = k;
            println!(
                "    ({}, {}) {} index entries, {} total records",
                org_id, bucket_id, stats.count, stats.total_records
            );
            println!("    Measurements:");
            stats.print_report("      ");
        }
    }
}
