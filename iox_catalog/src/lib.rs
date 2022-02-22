//! The IOx catalog which keeps track of what namespaces, tables, columns, parquet files,
//! and deletes are in the system. Configuration information for distributing ingest, query
//! and compaction is also stored here.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use crate::interface::{
    ColumnType, Error, KafkaPartition, KafkaTopic, NamespaceSchema, QueryPool, Result, Sequencer,
    SequencerId, TableSchema, Transaction,
};

use interface::{ParquetFile, ProcessedTombstone, RepoCollection, Tombstone};
use mutable_batch::MutableBatch;
use std::{borrow::Cow, collections::BTreeMap};

#[allow(dead_code)]
const SHARED_KAFKA_TOPIC: &str = "iox-shared";
const SHARED_QUERY_POOL: &str = SHARED_KAFKA_TOPIC;
const TIME_COLUMN: &str = "time";

/// A string value representing an infinite retention policy.
pub const INFINITE_RETENTION_POLICY: &str = "inf";

pub mod interface;
pub mod mem;
pub mod postgres;

/// Given an iterator of `(table_name, batch)` to validate, this function
/// ensures all the columns within `batch` match the existing schema for
/// `table_name` in `schema`. If the column does not already exist in `schema`,
/// it is created and an updated [`NamespaceSchema`] is returned.
///
/// This function pushes schema additions through to the backend catalog, and
/// relies on the catalog to serialise concurrent additions of a given column,
/// ensuring only one type is ever accepted per column.
pub async fn validate_or_insert_schema<'a, T, U, R>(
    tables: T,
    schema: &NamespaceSchema,
    repos: &mut R,
) -> Result<Option<NamespaceSchema>>
where
    T: IntoIterator<IntoIter = U, Item = (&'a str, &'a MutableBatch)> + Send + Sync,
    U: Iterator<Item = T::Item> + Send,
    R: RepoCollection + ?Sized,
{
    let tables = tables.into_iter();

    // The (potentially updated) NamespaceSchema to return to the caller.
    let mut schema = Cow::Borrowed(schema);

    for (table_name, batch) in tables {
        validate_mutable_batch(batch, table_name, &mut schema, repos).await?;
    }

    match schema {
        Cow::Owned(v) => Ok(Some(v)),
        Cow::Borrowed(_) => Ok(None),
    }
}

async fn validate_mutable_batch<R>(
    mb: &MutableBatch,
    table_name: &str,
    schema: &mut Cow<'_, NamespaceSchema>,
    repos: &mut R,
) -> Result<()>
where
    R: RepoCollection + ?Sized,
{
    // Check if the table exists in the schema.
    //
    // Because the entry API requires &mut it is not used to avoid a premature
    // clone of the Cow.
    let mut table = match schema.tables.get(table_name) {
        Some(t) => Cow::Borrowed(t),
        None => {
            // The table does not exist in the cached schema.
            //
            // Attempt to create the table in the catalog, or load an existing
            // table from the catalog to populate the cache.
            let mut table = repos
                .tables()
                .create_or_get(table_name, schema.id)
                .await
                .map(|t| TableSchema::new(t.id))?;

            // Always add a time column to all new tables.
            let time_col = repos
                .columns()
                .create_or_get(TIME_COLUMN, table.id, ColumnType::Time)
                .await?;

            table.add_column(&time_col);

            assert!(schema
                .to_mut()
                .tables
                .insert(table_name.to_string(), table)
                .is_none());

            Cow::Borrowed(schema.tables.get(table_name).unwrap())
        }
    };

    // The table is now in the schema (either by virtue of it already existing,
    // or through adding it above).
    //
    // If the table itself needs to be updated during column validation it
    // becomes a Cow::owned() copy and the modified copy should be inserted into
    // the schema before returning.

    for (name, col) in mb.columns() {
        // Check if the column exists in the cached schema.
        //
        // If it does, validate it. If it does not exist, create it and insert
        // it into the cached schema.
        match table.columns.get(name.as_str()) {
            Some(existing) if existing.matches_type(col) => {
                // No action is needed as the column matches the existing column
                // schema.
            }
            Some(existing) => {
                // The column schema, and the column in the mutable batch are of
                // different types.
                return Err(Error::ColumnTypeMismatch {
                    name: name.to_string(),
                    existing: existing.column_type.to_string(),
                    new: col.influx_type().to_string(),
                });
            }
            None => {
                // The column does not exist in the cache, create/get it from
                // the catalog, and add it to the table.
                let column = repos
                    .columns()
                    .create_or_get(name.as_str(), table.id, ColumnType::from(col.influx_type()))
                    .await?;

                table.to_mut().add_column(&column);
            }
        };
    }

    if let Cow::Owned(table) = table {
        // The table schema was mutated and needs inserting into the namespace
        // schema to make the changes visible to the caller.
        assert!(schema
            .to_mut()
            .tables
            .insert(table_name.to_string(), table)
            .is_some());
    }

    Ok(())
}

/// Creates or gets records in the catalog for the shared kafka topic, query pool, and sequencers for
/// each of the partitions.
pub async fn create_or_get_default_records(
    kafka_partition_count: i32,
    txn: &mut dyn Transaction,
) -> Result<(KafkaTopic, QueryPool, BTreeMap<SequencerId, Sequencer>)> {
    let kafka_topic = txn.kafka_topics().create_or_get(SHARED_KAFKA_TOPIC).await?;
    let query_pool = txn.query_pools().create_or_get(SHARED_QUERY_POOL).await?;

    let mut sequencers = BTreeMap::new();
    for partition in 1..=kafka_partition_count {
        let sequencer = txn
            .sequencers()
            .create_or_get(&kafka_topic, KafkaPartition::new(partition))
            .await?;
        sequencers.insert(sequencer.id, sequencer);
    }

    Ok((kafka_topic, query_pool, sequencers))
}

// TODO: this function is no longer needed in the ingester. It might be needed by the Compactor
/// Insert the conpacted parquet file and its tombstones
pub async fn add_parquet_file_with_tombstones(
    parquet_file: &ParquetFile,
    tombstones: &[Tombstone],
    txn: &mut dyn Transaction,
) -> Result<(ParquetFile, Vec<ProcessedTombstone>), Error> {
    // create a parquet file in the catalog first
    let parquet = txn
        .parquet_files()
        .create(
            parquet_file.sequencer_id,
            parquet_file.table_id,
            parquet_file.partition_id,
            parquet_file.object_store_id,
            parquet_file.min_sequence_number,
            parquet_file.max_sequence_number,
            parquet_file.min_time,
            parquet_file.max_time,
        )
        .await?;

    // Now the parquet available, create its processed tombstones
    let mut processed_tombstones = Vec::with_capacity(tombstones.len());
    for tombstone in tombstones {
        processed_tombstones.push(
            txn.processed_tombstones()
                .create(parquet.id, tombstone.id)
                .await?,
        );
    }

    Ok((parquet, processed_tombstones))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::get_schema_by_name;
    use crate::mem::MemCatalog;

    // Generate a test that simulates multiple, sequential writes in `lp` and
    // asserts the resulting schema.
    //
    // This test asserts the cached schema and the database entry are always in
    // sync.
    macro_rules! test_validate_schema {
        (
            $name:ident,
            lp = [$($lp:literal,)+],                                // An array of multi-line LP writes
            want_observe_conflict = $want_observe_conflict:literal, // true if a schema validation error should be observed at some point
            want_schema = {$($want_schema:tt) +}                    // The expected resulting schema after all writes complete.
        ) => {
            paste::paste! {
                #[allow(clippy::bool_assert_comparison)]
                #[tokio::test]
                async fn [<test_validate_schema_ $name>]() {
                    use crate::interface::Catalog;
                    use std::ops::DerefMut;
                    use pretty_assertions::assert_eq;
                    const NAMESPACE_NAME: &str = "bananas";

                    let repo = MemCatalog::new();
                    let mut txn = repo.start_transaction().await.unwrap();
                    let (kafka_topic, query_pool, _) = create_or_get_default_records(2, txn.deref_mut()).await.unwrap();

                    let namespace = txn
                        .namespaces()
                        .create(NAMESPACE_NAME, "inf", kafka_topic.id, query_pool.id)
                        .await
                        .unwrap();

                    let schema = NamespaceSchema::new(
                        namespace.id,
                        namespace.kafka_topic_id,
                        namespace.query_pool_id,
                    );

                    // Apply all the lp literals as individual writes, feeding
                    // the result of one validation into the next to drive
                    // incremental construction of the schemas.
                    let mut observed_conflict = false;
                    $(
                        let schema = {
                            let lp: String = $lp.to_string();

                            let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp.as_str(), 42)
                                .expect("failed to build test writes from LP");

                            let got = validate_or_insert_schema(writes.iter().map(|(k, v)| (k.as_str(), v)), &schema, txn.deref_mut())
                                .await;

                            match got {
                                Err(Error::ColumnTypeMismatch{ .. }) => {
                                    observed_conflict = true;
                                    schema
                                },
                                Err(e) => panic!("unexpected error: {}", e),
                                Ok(Some(new_schema)) => new_schema,
                                Ok(None) => schema,
                            }
                        };
                    )+

                    assert_eq!($want_observe_conflict, observed_conflict, "should error mismatch");

                    // Invariant: in absence of concurrency, the schema within
                    // the database must always match the incrementally built
                    // cached schema.
                    let db_schema = get_schema_by_name(NAMESPACE_NAME, txn.deref_mut())
                        .await
                        .expect("database failed to query for namespace schema");
                    assert_eq!(schema, db_schema, "schema in DB and cached schema differ");

                    // Generate the map of tables => desired column types
                    let want_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = test_validate_schema!(@table, $($want_schema)+);

                    // Generate a similarly structured map from the actual
                    // schema
                    let actual_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = schema
                        .tables
                        .iter()
                        .map(|(table, table_schema)| {
                            let desired_cols = table_schema
                                .columns
                                .iter()
                                .map(|(column, column_schema)| (column.clone(), column_schema.column_type))
                                .collect::<BTreeMap<_, _>>();

                            (table.clone(), desired_cols)
                        })
                        .collect();

                    // Assert the actual namespace contents matches the desired
                    // table schemas in the test args.
                    assert_eq!(want_tables, actual_tables, "cached schema and desired schema differ");
                }
            }
        };
        // Generate a map of table names => column map (below)
        //
        // out: BTreeMap<String, BTreeMap<ColName, ColumnType>>
        (@table, $($table_name:literal: [$($columns:tt) +],)*) => {{
            let mut tables = BTreeMap::new();
            $(
                let want_cols = test_validate_schema!(@column, $($columns)+);
                assert!(tables.insert($table_name.to_string(), want_cols).is_none());
            )*
            tables
        }};
        // Generate a map of column names => ColumnType
        //
        // out: BTreeMap<ColName, ColumnType>
        (@column, $($col_name:literal => $col_type:expr,)+) => {{
            let mut cols = BTreeMap::new();
            $(
                assert!(cols.insert($col_name.to_string(), $col_type).is_none());
            )*
            cols
        }};
    }

    test_validate_schema!(
        one_write_multiple_tables,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
            ],
        }
    );

    // test that a new table will be created
    test_validate_schema!(
        two_writes_incremental_new_table,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
            "
                m1,t1=c f1=1i 2\n\
                new_measurement,t9=a f10=true 1\n\
            ",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
                ],
            "new_measurement": [
                "t9" => ColumnType::Tag,
                "f10" => ColumnType::Bool,
                "time" => ColumnType::Time,
            ],
        }
    );

    // test that a new column for an existing table will be created
    test_validate_schema!(
        two_writes_incremental_new_column,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
            "m1,new_tag=c new_field=1i 2",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
                // These are the incremental additions:
                "new_tag" => ColumnType::Tag,
                "new_field" => ColumnType::I64,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        table_always_has_time_column,
        lp = [
            "m1,t1=a f1=2i",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        two_writes_conflicting_column_types,
        lp = [
            "m1,t1=a f1=2i",
            // Second write has conflicting type for f1.
            "m1,t1=a f1=2.0",
        ],
        want_observe_conflict = true,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        two_writes_tag_field_transposition,
        lp = [
            // x is a tag
            "m1,t1=a,x=t f1=2i",
            // x is a field
            "m1,t1=a x=t,f1=2i",
        ],
        want_observe_conflict = true,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "x" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );
}
