use chrono::Utc;
use delorean_arrow::arrow::record_batch::RecordBatch;
use delorean_generated_types::wal as wb;
use delorean_wal::{Entry as WalEntry, Result as WalResult};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_storage::TimestampRange;

use crate::column::{ColumnValue, Value};
use crate::dictionary::Dictionary;
use crate::table::{Table, TimestampPredicate};
use crate::wal::WalEntryBuilder;

use chrono::offset::TimeZone;
use snafu::{OptionExt, ResultExt, Snafu};

pub const TIME_COLUMN_NAME: &str = "time";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not read WAL entry: {}", source))]
    WalEntryRead { source: delorean_wal::Error },

    #[snafu(display("Partition {} not found", partition))]
    PartitionNotFound { partition: String },

    #[snafu(display(
        "Column name {} not found in dictionary of partition {}",
        column,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column: String,
        partition: u32,
        source: crate::dictionary::Error,
    },

    #[snafu(display("Error restoring table '{}': {}", table_name, source))]
    TableRestoration {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table Error:: {}", source))]
    TableError { source: crate::table::Error },

    #[snafu(display("Table Error in '{}': {}", table_name, source))]
    NamedTableError {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display(
        "Table name {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableNameNotFoundInDictionary {
        table: String,
        partition: u32,
        source: crate::dictionary::Error,
    },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: u32 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    pub key: String,
    pub generation: u32,
    /// `dictionary` maps &str -> u32. The u32s are used in place of String or str to avoid slow
    /// string operations. The same dictionary is used for table names, tag names, tag values, and
    /// field names.
    // TODO: intern string field values too?
    pub dictionary: Dictionary,
    /// tables is a map of the dictionary ID for the table name to the table
    pub tables: HashMap<u32, Table>,
    pub is_open: bool,
}

impl Partition {
    pub fn new(generation: u32, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            generation,
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            is_open: true,
        }
    }

    pub fn add_wal_row(
        &mut self,
        table_name: &str,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let table_id = self.dictionary.lookup_value_or_insert(table_name);

        let t = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id));

        t.add_wal_row(&mut self.dictionary, values)
            .context(TableRestoration { table_name })
    }

    pub fn write_line(
        &mut self,
        line: &ParsedLine<'_>,
        builder: &mut Option<WalEntryBuilder<'_>>,
    ) -> Result<()> {
        if let Some(b) = builder.as_mut() {
            b.ensure_partition_exists(self.generation, &self.key);
        }

        let table_name = line.series.measurement.as_str();
        let table_id = self.dictionary.lookup_value_or_insert(table_name);

        let column_count = line.column_count();
        let mut values: Vec<ColumnValue<'_>> = Vec::with_capacity(column_count);

        // Make sure the time, tag and field names exist in the dictionary
        if let Some(tags) = &line.series.tag_set {
            for (column, value) in tags {
                let tag_column_id = self.dictionary.lookup_value_or_insert(column.as_str());
                let tag_value_id = self.dictionary.lookup_value_or_insert(value.as_str());

                values.push(ColumnValue {
                    id: tag_column_id,
                    column,
                    value: Value::TagValue(tag_value_id, value),
                });
            }
        }

        for (column, value) in &line.field_set {
            let field_column_id = self.dictionary.lookup_value_or_insert(column.as_str());
            values.push(ColumnValue {
                id: field_column_id,
                column,
                value: Value::FieldValue(value),
            });
        }

        let time_column_id = self.dictionary.lookup_value_or_insert(TIME_COLUMN_NAME);
        // TODO: shouldn't the default for timestamp be the current time, not 0?
        let time = line.timestamp.unwrap_or(0);
        let time_value = FieldValue::I64(time);
        values.push(ColumnValue {
            id: time_column_id,
            column: TIME_COLUMN_NAME,
            value: Value::FieldValue(&time_value),
        });

        let table = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id));

        table
            .add_row(&values, &self.dictionary, builder)
            .context(TableError)?;

        Ok(())
    }

    /// Create a predicate suitable for passing to
    /// `matches_timestamp_predicate` on a table within this partition
    /// from the input timestamp range.
    pub fn make_timestamp_predicate(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<Option<TimestampPredicate>> {
        match range {
            None => Ok(None),
            Some(range) => {
                let time_column_id = self.dictionary.lookup_value(TIME_COLUMN_NAME).context(
                    ColumnNameNotFoundInDictionary {
                        column: TIME_COLUMN_NAME,
                        partition: self.generation,
                    },
                )?;

                Ok(Some(TimestampPredicate {
                    range,
                    time_column_id,
                }))
            }
        }
    }

    /// returns true if data with partition key `key` should be
    /// written to this partition,
    pub fn should_write(&self, key: &str) -> bool {
        self.key.starts_with(key) && self.is_open
    }

    /// Convert the table specified in this partition into an arrow record batch
    pub fn table_to_arrow(&self, table_name: &str, columns: &[&str]) -> Result<RecordBatch> {
        let table_id =
            self.dictionary
                .lookup_value(table_name)
                .context(TableNameNotFoundInDictionary {
                    table: table_name,
                    partition: self.generation,
                })?;

        let table = self
            .tables
            .get(&table_id)
            .context(TableNotFoundInPartition {
                table: table_id,
                partition: self.generation,
            })?;
        table
            .to_arrow(&self, columns)
            .context(NamedTableError { table_name })
    }
}

/// Computes the partition key from a row being restored from the WAL.
/// TODO: This can't live on `Db`, because when we're restoring from the WAL, we don't have a `Db`
/// yet. Where do the partitioning rules come from?
pub fn partition_key(
    row: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
) -> String {
    // TODO - wire this up to use partitioning rules, for now just partition by day
    let ts = row
        .iter()
        .find(|v| v.column().expect("restored row values must have column") == TIME_COLUMN_NAME)
        .expect("restored rows must have timestamp")
        .value_as_i64value()
        .expect("restored timestamp rows must be i64")
        .value();
    let dt = Utc.timestamp_nanos(ts);
    dt.format("%Y-%m-%dT%H").to_string()
}

#[derive(Default, Debug)]
pub struct RestorationStats {
    pub row_count: usize,
    pub tables: BTreeSet<String>,
}

/// Given a set of WAL entries, restore them into a set of Partitions.
pub fn restore_partitions_from_wal(
    wal_entries: impl Iterator<Item = WalResult<WalEntry>>,
) -> Result<(Vec<Partition>, u32, RestorationStats)> {
    let mut stats = RestorationStats::default();
    let mut max_partition_generation = 0;

    let mut partitions = BTreeMap::new();

    for wal_entry in wal_entries {
        let wal_entry = wal_entry.context(WalEntryRead)?;
        let bytes = wal_entry.as_data();

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&bytes);

        if let Some(entries) = batch.entries() {
            for entry in entries {
                if let Some(po) = entry.partition_open() {
                    let generation = po.generation();
                    let key = po
                        .key()
                        .expect("restored partitions should have keys")
                        .to_string();

                    if generation > max_partition_generation {
                        max_partition_generation = generation;
                    }

                    // raw entry opportunity
                    if !partitions.contains_key(&key) {
                        partitions.insert(key.clone(), Partition::new(generation, key));
                    }
                } else if let Some(_ps) = entry.partition_snapshot_started() {
                    todo!("handle partition snapshot");
                } else if let Some(_pf) = entry.partition_snapshot_finished() {
                    todo!("handle partition snapshot finished")
                } else if let Some(row) = entry.write() {
                    let values = row.values().expect("restored rows should have values");
                    let table = row.table().expect("restored rows should have table");
                    let partition_key = partition_key(&values);

                    let partition =
                        partitions
                            .get_mut(&partition_key)
                            .context(PartitionNotFound {
                                partition: partition_key,
                            })?;

                    stats.row_count += 1;
                    // Avoid allocating if we don't need to
                    if !stats.tables.contains(table) {
                        stats.tables.insert(table.to_string());
                    }

                    partition.add_wal_row(table, &values)?;
                }
            }
        }
    }
    let partitions = partitions.into_iter().map(|(_, p)| p).collect();

    Ok((partitions, max_partition_generation, stats))
}
