use delorean_arrow::arrow::record_batch::RecordBatch;
use delorean_generated_types::wal as wb;
use delorean_wal::{Entry as WalEntry, Result as WalResult};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use delorean_storage::TimestampRange;

use crate::dictionary::Dictionary;
use crate::table::{Table, TimestampPredicate};

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
        partition: String,
        source: crate::dictionary::Error,
    },

    #[snafu(display("Error writing table '{}': {}", table_name, source))]
    TableWrite {
        table_name: String,
        source: crate::table::Error,
    },

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
        partition: String,
        source: crate::dictionary::Error,
    },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: String },

    #[snafu(display("Attempt to write table batch without a name"))]
    TableWriteWithoutName,

    #[snafu(display("Error restoring WAL entry, missing partition key"))]
    MissingPartitionKey,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    pub key: String,
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
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            is_open: true,
        }
    }

    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        if let Some(table_batches) = entry.table_batches() {
            for batch in table_batches {
                self.write_table_batch(&batch)?;
            }
        }

        Ok(())
    }

    fn write_table_batch(&mut self, batch: &wb::TableWriteBatch<'_>) -> Result<()> {
        let table_name = batch.name().context(TableWriteWithoutName)?;
        let table_id = self.dictionary.lookup_value_or_insert(table_name);

        let table = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id));

        if let Some(rows) = batch.rows() {
            table
                .append_rows(&mut self.dictionary, &rows)
                .context(TableWrite { table_name })?;
        }

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
                        partition: &self.key,
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
                    partition: &self.key,
                })?;

        let table = self
            .tables
            .get(&table_id)
            .context(TableNotFoundInPartition {
                table: table_id,
                partition: &self.key,
            })?;
        table
            .to_arrow(&self, columns)
            .context(NamedTableError { table_name })
    }
}

#[derive(Default, Debug)]
pub struct RestorationStats {
    pub row_count: usize,
    pub tables: BTreeSet<String>,
}

/// Given a set of WAL entries, restore them into a set of Partitions.
pub fn restore_partitions_from_wal(
    wal_entries: impl Iterator<Item = WalResult<WalEntry>>,
) -> Result<(Vec<Partition>, RestorationStats)> {
    let mut stats = RestorationStats::default();

    let mut partitions = BTreeMap::new();

    for wal_entry in wal_entries {
        let wal_entry = wal_entry.context(WalEntryRead)?;
        let bytes = wal_entry.as_data();

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&bytes);

        if let Some(entries) = batch.entries() {
            for entry in entries {
                let partition_key = entry.partition_key().context(MissingPartitionKey)?;

                if !partitions.contains_key(partition_key) {
                    partitions.insert(
                        partition_key.to_string(),
                        Partition::new(partition_key.to_string()),
                    );
                }

                let partition = partitions
                    .get_mut(partition_key)
                    .context(PartitionNotFound {
                        partition: partition_key,
                    })?;

                partition.write_entry(&entry)?;
            }
        }
    }
    let partitions = partitions
        .into_iter()
        .map(|(_, p)| p)
        .collect::<Vec<Partition>>();

    // compute the stats
    for p in &partitions {
        for (id, table) in &p.tables {
            let name = p
                .dictionary
                .lookup_id(*id)
                .expect("table id wasn't inserted into dictionary on restore");
            if !stats.tables.contains(name) {
                stats.tables.insert(name.to_string());
            }

            stats.row_count += table.row_count();
        }
    }

    Ok((partitions, stats))
}
