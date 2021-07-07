use super::partition::Partition;
use crate::db::catalog::metrics::TableMetrics;
use data_types::partition_metadata::PartitionSummary;
use hashbrown::HashMap;
use internal_types::schema::{
    builder::SchemaBuilder,
    merge::{Error as SchemaMergerError, SchemaMerger},
    Schema,
};
use std::{ops::Deref, result::Result, sync::Arc};
use tracker::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Table-wide schema.
type TableSchema = RwLock<Schema>;

/// A `Table` is a collection of `Partition` each of which is a collection of `Chunk`
#[derive(Debug)]
pub struct Table {
    /// Database name
    db_name: Arc<str>,

    /// Table name
    table_name: Arc<str>,

    /// key is partition key
    partitions: HashMap<Arc<str>, Arc<RwLock<Partition>>>,

    /// Table metrics
    metrics: TableMetrics,

    /// Table-wide schema.
    schema: TableSchema,
}

impl Table {
    /// Create a new table catalog object.
    ///
    /// This function is not pub because `Table`s should be
    /// created using the interfaces on [`Catalog`](crate::db::catalog::Catalog) and not
    /// instantiated directly.
    pub(super) fn new(db_name: Arc<str>, table_name: Arc<str>, metrics: TableMetrics) -> Self {
        // build empty schema for this table
        let mut builder = SchemaBuilder::new();
        builder.measurement(db_name.as_ref());
        let schema = builder.build().expect("cannot build empty schema");
        let schema = metrics.new_table_lock(schema);

        Self {
            db_name,
            table_name,
            partitions: Default::default(),
            metrics,
            schema,
        }
    }

    pub fn partition(&self, partition_key: impl AsRef<str>) -> Option<&Arc<RwLock<Partition>>> {
        self.partitions.get(partition_key.as_ref())
    }

    pub fn partitions(&self) -> impl Iterator<Item = &Arc<RwLock<Partition>>> + '_ {
        self.partitions.values()
    }

    pub fn get_or_create_partition(
        &mut self,
        partition_key: impl AsRef<str>,
    ) -> &Arc<RwLock<Partition>> {
        let metrics = &self.metrics;
        let db_name = &self.db_name;
        let table_name = &self.table_name;
        let (_, partition) = self
            .partitions
            .raw_entry_mut()
            .from_key(partition_key.as_ref())
            .or_insert_with(|| {
                let partition_key = Arc::from(partition_key.as_ref());
                let partition_metrics = metrics.new_partition_metrics();
                let partition = Partition::new(
                    Arc::clone(&db_name),
                    Arc::clone(&partition_key),
                    Arc::clone(&table_name),
                    partition_metrics,
                );
                let partition = Arc::new(metrics.new_partition_lock(partition));
                (partition_key, partition)
            });
        partition
    }

    pub fn partition_keys(&self) -> impl Iterator<Item = &Arc<str>> + '_ {
        self.partitions.keys()
    }

    pub fn partition_summaries(&self) -> impl Iterator<Item = PartitionSummary> + '_ {
        self.partitions.values().map(|x| x.read().summary())
    }

    pub fn schema_upsert_handle(
        &self,
        new_schema: &Schema,
    ) -> Result<TableSchemaUpsertHandle<'_>, SchemaMergerError> {
        TableSchemaUpsertHandle::new(&self.schema, new_schema)
    }
}

/// Inner state of [`TableSchemaUpsertHandle`] that depends if the schema will be changed during the write operation or
/// not.
enum TableSchemaUpsertHandleInner<'a> {
    /// Schema will not be changed.
    NoChange {
        table_schema_read: RwLockReadGuard<'a, Schema>,
    },

    /// Schema might change (if write to mutable buffer is successfull).
    MightChange {
        table_schema_write: RwLockWriteGuard<'a, Schema>,
        merged_schema: Schema,
    },
}

/// Handle that can be used to modify the table-wide [schema](Schema) during new writes.
pub struct TableSchemaUpsertHandle<'a> {
    inner: TableSchemaUpsertHandleInner<'a>,
}

impl<'a> TableSchemaUpsertHandle<'a> {
    fn new(table_schema: &'a TableSchema, new_schema: &Schema) -> Result<Self, SchemaMergerError> {
        // Be optimistic and only get a read lock. It is rather rate that the schema will change when new data arrives
        // and we do NOT want to serialize all writes on a single lock.
        let table_schema_read = table_schema.read();

        // Let's see if we can merge the new schema with the existing one (this may or may not result in any schema
        // change).
        let merged_schema = Self::try_merge(&table_schema_read, new_schema)?;

        // Now check if this would actually change the schema:
        if &merged_schema == table_schema_read.deref() {
            // Optimism payed off and we get away we the read lock.
            Ok(Self {
                inner: TableSchemaUpsertHandleInner::NoChange { table_schema_read },
            })
        } else {
            // Schema changed, so we need a real write lock. To do that, we must first drop the read lock.
            drop(table_schema_read);

            // !!! Here we have a lock-gap !!!

            // Re-lock with write permissions.
            let table_schema_write = table_schema.write();

            // During the above write lock, the schema might have changed again, so we need to perform the merge again.
            // This may also lead to a failure now, e.g. when adding a column that was added with a different type
            // during the lock gap.
            let merged_schema = Self::try_merge(&table_schema_write, new_schema)?;

            Ok(Self {
                inner: TableSchemaUpsertHandleInner::MightChange {
                    table_schema_write,
                    merged_schema,
                },
            })
        }
    }

    /// Try to merge schema.
    ///
    /// This will also sort the columns!
    fn try_merge(schema1: &Schema, schema2: &Schema) -> Result<Schema, SchemaMergerError> {
        Ok(SchemaMerger::new().merge(schema1)?.merge(schema2)?.build())
    }

    /// Commit potential schema change.
    fn commit(self) {
        match self.inner {
            TableSchemaUpsertHandleInner::NoChange { table_schema_read } => {
                // Nothing to do since there was no schema changed queued. Just drop the read guard.
                drop(table_schema_read);
            }
            TableSchemaUpsertHandleInner::MightChange {
                mut table_schema_write,
                merged_schema,
            } => {
                // Commit new schema and drop write guard;
                *table_schema_write = merged_schema;
                drop(table_schema_write);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use internal_types::schema::{InfluxColumnType, InfluxFieldType};
    use tracker::LockTracker;

    use super::*;

    #[test]
    fn test_handle_no_change() {
        let lock_tracker = LockTracker::default();
        let table_schema_orig = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let table_schema = lock_tracker.new_lock(table_schema_orig.clone());

        // writing with the same schema must not trigger a change
        let schema1 = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let handle = TableSchemaUpsertHandle::new(&table_schema, &schema1).unwrap();
        assert!(matches!(
            handle.inner,
            TableSchemaUpsertHandleInner::NoChange { .. }
        ));
        assert_eq!(table_schema.read().deref(), &table_schema_orig);
        handle.commit();
        assert_eq!(table_schema.read().deref(), &table_schema_orig);

        // writing with different column order must not trigger a change
        let schema2 = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag2", InfluxColumnType::Tag)
            .influx_column("tag1", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let handle = TableSchemaUpsertHandle::new(&table_schema, &schema2).unwrap();
        assert!(matches!(
            handle.inner,
            TableSchemaUpsertHandleInner::NoChange { .. }
        ));
        assert_eq!(table_schema.read().deref(), &table_schema_orig);
        handle.commit();
        assert_eq!(table_schema.read().deref(), &table_schema_orig);

        // writing with a column subset must not trigger a change
        let schema3 = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let handle = TableSchemaUpsertHandle::new(&table_schema, &schema3).unwrap();
        assert!(matches!(
            handle.inner,
            TableSchemaUpsertHandleInner::NoChange { .. }
        ));
        assert_eq!(table_schema.read().deref(), &table_schema_orig);
        handle.commit();
        assert_eq!(table_schema.read().deref(), &table_schema_orig);
    }

    #[test]
    fn test_handle_might_change() {
        let lock_tracker = LockTracker::default();
        let table_schema_orig = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let table_schema = lock_tracker.new_lock(table_schema_orig.clone());

        let new_schema = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag3", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let handle = TableSchemaUpsertHandle::new(&table_schema, &new_schema).unwrap();
        assert!(matches!(
            handle.inner,
            TableSchemaUpsertHandleInner::MightChange { .. }
        ));

        // cannot read while lock is held
        assert!(table_schema.try_read().is_none());

        handle.commit();
        let table_schema_expected = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Tag)
            .influx_column("tag3", InfluxColumnType::Tag)
            .build()
            .unwrap();
        assert_eq!(table_schema.read().deref(), &table_schema_expected);
    }

    #[test]
    fn test_handle_error() {
        let lock_tracker = LockTracker::default();
        let table_schema_orig = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let table_schema = lock_tracker.new_lock(table_schema_orig.clone());

        let schema1 = SchemaBuilder::new()
            .measurement("m1")
            .influx_column("tag1", InfluxColumnType::Tag)
            .influx_column("tag2", InfluxColumnType::Field(InfluxFieldType::String))
            .build()
            .unwrap();
        assert!(TableSchemaUpsertHandle::new(&table_schema, &schema1).is_err());

        // schema did not change
        assert_eq!(table_schema.read().deref(), &table_schema_orig);
    }
}
