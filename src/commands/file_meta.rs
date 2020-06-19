#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;

use delorean::storage::tsm::{IndexEntry, InfluxID, TSMReader};
use delorean::storage::StorageError;

use delorean_parquet::metadata::print_parquet_metadata;
use log::{debug, info};

use crate::commands::error::{Error, Result};
use crate::commands::input::{FileType, InputReader};

pub fn dump_meta(input_filename: &str) -> Result<()> {
    info!("meta starting");
    debug!("Reading from input file {}", input_filename);

    let input_reader = InputReader::new(input_filename)?;

    match input_reader.file_type() {
        FileType::LineProtocol => Err(Error::NotImplemented {
            operation_name: String::from("Line protocol metadata dump"),
        }),
        FileType::TSM => {
            let len = input_reader
                .len()
                .try_into()
                .expect("File size more than usize");
            let reader =
                TSMReader::try_new(input_reader, len).map_err(|e| Error::TSM { source: e })?;

            let mut stats_builder = TSMMetadataBuilder::new();

            for mut entry in reader {
                stats_builder.process_entry(&mut entry)?;
            }
            stats_builder.print_report();
            Ok(())
        }
        FileType::Parquet => {
            let input_len = input_reader.len();
            print_parquet_metadata(input_reader, input_len)
                .map_err(|e| Error::UnableDumpToParquetMetadata { source: e })?;
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
        let key = index_entry
            .parse_key()
            .map_err(|e| Error::TSM { source: e })?;

        for (tag_name, tag_value) in key.tagset {
            let tag_entry = self.tags.entry(tag_name).or_default();
            tag_entry.insert(tag_value);
        }
        self.fields.insert(key.field_key);
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
        let key = index_entry
            .parse_key()
            .map_err(|e| Error::TSM { source: e })?;

        let meta = self.measurements.entry(key.measurement).or_default();
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
