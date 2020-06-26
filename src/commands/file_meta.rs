use delorean_parquet::{error::Error as DeloreanParquetError, metadata::print_parquet_metadata};
use delorean_tsm::{reader::IndexEntry, reader::TSMIndexReader, InfluxID, TSMError};
use log::{debug, info};
use snafu::{ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
};

use crate::commands::input::{FileType, InputReader};

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn dump_meta(input_filename: &str) -> Result<()> {
    info!("meta starting");
    debug!("Reading from input file {}", input_filename);

    let input_reader = InputReader::new(input_filename).context(OpenInput)?;

    match input_reader.file_type() {
        FileType::LineProtocol => NotImplemented {
            operation_name: "Line protocol metadata dump",
        }
        .fail(),
        FileType::TSM => {
            let len = input_reader
                .len()
                .try_into()
                .expect("File size more than usize");
            let reader = TSMIndexReader::try_new(input_reader, len).context(TSM)?;

            let mut stats_builder = TSMMetadataBuilder::new();

            for entry in reader {
                let entry = entry.context(TSM)?;
                stats_builder.process_entry(entry)?;
            }
            stats_builder.print_report();
            Ok(())
        }
        FileType::Parquet => {
            print_parquet_metadata(input_reader).context(UnableDumpToParquetMetadata)
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
        let key = index_entry.parse_key().context(TSM)?;

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
        let key = index_entry.parse_key().context(TSM)?;

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
    fn new() -> Self {
        Self::default()
    }

    fn process_entry(&mut self, mut index_entry: IndexEntry) -> Result<()> {
        self.num_entries += 1;
        let key = (index_entry.org_id(), index_entry.bucket_id());
        let stats = self.bucket_stats.entry(key).or_default();
        stats.update_for_entry(&mut index_entry)?;
        Ok(())
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening input {}", source))]
    OpenInput { source: super::input::Error },

    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String },

    #[snafu(display("Unable to dump parquet file metadata: {}", source))]
    UnableDumpToParquetMetadata { source: DeloreanParquetError },

    #[snafu(display(r#"Error reading TSM data: {}"#, source))]
    TSM { source: TSMError },
}
