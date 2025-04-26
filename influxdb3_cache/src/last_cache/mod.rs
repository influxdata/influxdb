//! The Last-N-Value cache holds the most N recent values for a column or set of columns on a table

use influxdb3_id::ColumnId;

mod cache;
pub use cache::CreateLastCacheArgs;
mod provider;
pub use provider::LastCacheProvider;
mod table_function;
use schema::InfluxColumnType;
pub use table_function::{LAST_CACHE_UDTF_NAME, LastCacheFunction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
    #[error(
        "last cache already exists for database and table, but it was configured differently: {reason}"
    )]
    CacheAlreadyExists { reason: String },
    #[error("specified column (name: {column_name}) does not exist in the table definition")]
    ColumnDoesNotExistByName { column_name: String },
    #[error("specified column (id: {column_id}) does not exist in the table definition")]
    ColumnDoesNotExistById { column_id: ColumnId },
    #[error("specified key column (id: {column_id}) does not exist in the table schema")]
    KeyColumnDoesNotExist { column_id: ColumnId },
    #[error("specified key column (name: {column_name}) does not exist in the table schema")]
    KeyColumnDoesNotExistByName { column_name: String },
    #[error("key column must be string, int, uint, or bool types, got: {column_type}")]
    InvalidKeyColumn { column_type: InfluxColumnType },
    #[error("specified value column ({column_id}) does not exist in the table schema")]
    ValueColumnDoesNotExist { column_id: ColumnId },
    #[error("requested last cache does not exist")]
    CacheDoesNotExist,
}

impl Error {
    fn cache_already_exists(reason: impl Into<String>) -> Self {
        Self::CacheAlreadyExists {
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use arrow::array::AsArray;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use datafusion::prelude::SessionContext;
    use indexmap::IndexMap;
    use influxdb3_catalog::{
        catalog::Catalog,
        log::{FieldDataType, LastCacheSize, LastCacheTtl},
    };
    use influxdb3_id::ColumnId;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;

    use crate::{
        last_cache::{
            CreateLastCacheArgs, LAST_CACHE_UDTF_NAME, LastCacheFunction, LastCacheProvider,
            cache::{
                KeyValue, LastCache, LastCacheKeyColumnsArg, LastCacheValueColumnsArg, Predicate,
            },
        },
        test_helpers::{TestWriter, column_ids_for_names},
    };

    fn predicates(
        preds: impl IntoIterator<Item = (ColumnId, Predicate)>,
    ) -> IndexMap<ColumnId, Predicate> {
        preds.into_iter().collect()
    }

    #[tokio::test]
    async fn pick_up_latest_write() {
        let writer = TestWriter::new().await;
        // Do a write to update the catalog with a database and table:
        let _ = writer
            .write_lp_to_rows("cpu,host=a,region=us usage=120", 1_000)
            .await;

        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        let key_columns = column_ids_for_names(["host"], &table_def);
        let col_id = table_def.column_name_to_id("host").unwrap();

        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::default(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::Explicit(key_columns),
            value_columns: LastCacheValueColumnsArg::default(),
        })
        .unwrap();

        // Do a write to update the last cache:
        let rows = writer
            .write_lp_to_rows("cpu,host=a,region=us usage=99", 2_000)
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        let predicates = predicates([(col_id, Predicate::new_in([KeyValue::string("a")]))]);

        // Check what is in the last cache:
        let batch = cache
            .to_record_batches(Arc::clone(&table_def), &predicates)
            .unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000002Z | 99.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &batch
        );

        // Do another write and see that the cache only holds the latest value:
        let rows = writer
            .write_lp_to_rows("cpu,host=a,region=us usage=88", 3_000)
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        let batch = cache.to_record_batches(table_def, &predicates).unwrap();

        assert_batches_eq!(
            [
                "+------+--------+-----------------------------+-------+",
                "| host | region | time                        | usage |",
                "+------+--------+-----------------------------+-------+",
                "| a    | us     | 1970-01-01T00:00:00.000003Z | 88.0  |",
                "+------+--------+-----------------------------+-------+",
            ],
            &batch
        );
    }

    /// Test to ensure that predicates on caches that contain multiple
    /// key columns work as expected.
    ///
    /// When a cache contains multiple key columns, if only a subset, or none of those key columns
    /// are used as predicates, then the remaining key columns, along with their respective values,
    /// will be returned in the query output.
    ///
    /// For example, give the key columns 'region' and 'host', along with the following query:
    ///
    /// ```sql
    /// SELECT * FROM last_cache('cpu') WHERE region = 'us-east';
    /// ```
    ///
    /// We expect that the query result will include a `host` column, to delineate rows associated
    /// with different host values in the cache.
    #[tokio::test]
    async fn cache_key_column_predicates() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows("cpu,region=us,host=a usage=1", 500)
            .await;

        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        let host_col_id = table_def.column_name_to_id("host").unwrap();
        let region_col_id = table_def.column_name_to_id("region").unwrap();

        // Create the last cache with keys on all tag columns:
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::default(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill multiple keys in the cache:
        let rows = writer
            .write_lp_to_rows(
                "\
            cpu,region=us,host=a usage=100\n\
            cpu,region=us,host=b usage=80\n\
            cpu,region=us,host=c usage=60\n\
            cpu,region=ca,host=d usage=40\n\
            cpu,region=ca,host=e usage=20\n\
            cpu,region=ca,host=f usage=30\n\
            ",
                1_000,
            )
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Predicate including both key columns only produces value columns from the cache
            TestCase {
                predicates: predicates([
                    (region_col_id, Predicate::new_in([KeyValue::string("us")])),
                    (host_col_id, Predicate::new_in([KeyValue::string("c")])),
                ]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Predicate on only region key column will have host column outputted in addition to
            // the value columns:
            TestCase {
                predicates: predicates([(
                    region_col_id,
                    Predicate::new_in([KeyValue::string("us")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Similar to previous, with a different region predicate:
            TestCase {
                predicates: predicates([(
                    region_col_id,
                    Predicate::new_in([KeyValue::string("ca")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Predicate on only host key column will have region column outputted in addition to
            // the value columns:
            TestCase {
                predicates: predicates([(host_col_id, Predicate::new_in([KeyValue::string("a")]))]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Omitting all key columns from the predicate will have all key columns included in
            // the query result:
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a non-existent key column as a predicate has no effect:
            // TODO: should this be an error?
            TestCase {
                predicates: predicates([(
                    ColumnId::new(u16::MAX),
                    Predicate::new_in([KeyValue::string("12345")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a non existent key column value yields empty result set:
            TestCase {
                predicates: predicates([(
                    region_col_id,
                    Predicate::new_in([KeyValue::string("eu")]),
                )]),
                expected: &["++", "++"],
            },
            // Using an invalid combination of key column values yields an empty result set:
            TestCase {
                predicates: predicates([
                    (region_col_id, Predicate::new_in([KeyValue::string("ca")])),
                    (host_col_id, Predicate::new_in([KeyValue::string("a")])),
                ]),
                expected: &["++", "++"],
            },
            // Using a non-existent key column value (for host column) also yields empty result set:
            TestCase {
                predicates: predicates([(host_col_id, Predicate::new_in([KeyValue::string("g")]))]),
                expected: &["++", "++"],
            },
            // Using an incorrect type for a key column value in predicate also yields empty result
            // set. TODO: should this be an error?
            TestCase {
                predicates: predicates([(host_col_id, Predicate::new_in([KeyValue::Bool(true)]))]),
                expected: &["++", "++"],
            },
            // Using a NOT IN predicate
            TestCase {
                predicates: predicates([(
                    region_col_id,
                    Predicate::new_not_in([KeyValue::string("us")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using an IN predicate:
            TestCase {
                predicates: predicates([(
                    host_col_id,
                    Predicate::new_in([KeyValue::string("a"), KeyValue::string("b")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z | 80.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
            // Using a NOT IN predicate:
            TestCase {
                predicates: predicates([(
                    host_col_id,
                    Predicate::new_not_in([KeyValue::string("a"), KeyValue::string("b")]),
                )]),
                expected: &[
                    "+--------+------+-----------------------------+-------+",
                    "| region | host | time                        | usage |",
                    "+--------+------+-----------------------------+-------+",
                    "| ca     | d    | 1970-01-01T00:00:00.000001Z | 40.0  |",
                    "| ca     | e    | 1970-01-01T00:00:00.000001Z | 20.0  |",
                    "| ca     | f    | 1970-01-01T00:00:00.000001Z | 30.0  |",
                    "| us     | c    | 1970-01-01T00:00:00.000001Z | 60.0  |",
                    "+--------+------+-----------------------------+-------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn non_default_cache_size() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows("cpu,region=us,host=a usage=1", 500)
            .await;

        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        let host_col_id = table_def.column_name_to_id("host").unwrap();
        let region_col_id = table_def.column_name_to_id("region").unwrap();

        // Create the last cache with keys on all tag columns and a count of 10:
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::new(10).unwrap(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Do several writes to populate the cache:
        struct Write {
            lp: &'static str,
            time_ns: i64,
        }

        let writes = [
            Write {
                lp: "cpu,region=us,host=a usage=100\n\
                    cpu,region=us,host=b usage=80",
                time_ns: 1_000,
            },
            Write {
                lp: "cpu,region=us,host=a usage=99\n\
                    cpu,region=us,host=b usage=88",
                time_ns: 1_500,
            },
            Write {
                lp: "cpu,region=us,host=a usage=95\n\
                    cpu,region=us,host=b usage=92",
                time_ns: 2_000,
            },
            Write {
                lp: "cpu,region=us,host=a usage=90\n\
                    cpu,region=us,host=b usage=99",
                time_ns: 2_500,
            },
        ];

        for write in writes {
            let rows = writer.write_lp_to_rows(write.lp, write.time_ns).await;
            for row in &rows {
                cache.push(row, Arc::clone(&table_def));
            }
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                predicates: predicates([
                    (region_col_id, Predicate::new_in([KeyValue::string("us")])),
                    (host_col_id, Predicate::new_in([KeyValue::string("a")])),
                ]),
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: predicates([(
                    region_col_id,
                    Predicate::new_in([KeyValue::string("us")]),
                )]),
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: predicates([(host_col_id, Predicate::new_in([KeyValue::string("a")]))]),
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: predicates([(host_col_id, Predicate::new_in([KeyValue::string("b")]))]),
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+--------+------+--------------------------------+-------+",
                    "| region | host | time                           | usage |",
                    "+--------+------+--------------------------------+-------+",
                    "| us     | a    | 1970-01-01T00:00:00.000001500Z | 99.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000001Z    | 100.0 |",
                    "| us     | a    | 1970-01-01T00:00:00.000002500Z | 90.0  |",
                    "| us     | a    | 1970-01-01T00:00:00.000002Z    | 95.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001500Z | 88.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000001Z    | 80.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002500Z | 99.0  |",
                    "| us     | b    | 1970-01-01T00:00:00.000002Z    | 92.0  |",
                    "+--------+------+--------------------------------+-------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();
            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn cache_ttl() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows("cpu,region=us,host=a usage=1", 500)
            .await;

        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        let host_col_id = table_def.column_name_to_id("host").unwrap();
        let region_col_id = table_def.column_name_to_id("region").unwrap();

        // create the last cache with default columns and a non-default TTL/count
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            // non-default count is used to ensure the TTL is doing the evicting:
            count: LastCacheSize::new(10).unwrap(),
            ttl: Duration::from_millis(1000).into(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache:
        let rows = writer
            .write_lp_to_rows(
                "\
                cpu,region=us,host=a usage=100\n\
                cpu,region=us,host=b usage=80\n\
                cpu,region=us,host=c usage=60\n\
                cpu,region=ca,host=d usage=40\n\
                cpu,region=ca,host=e usage=20\n\
                cpu,region=ca,host=f usage=30\n\
                ",
                1_000,
            )
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        // Check the cache for values:
        let p = predicates([
            (region_col_id, Predicate::new_in([KeyValue::string("us")])),
            (host_col_id, Predicate::new_in([KeyValue::string("a")])),
        ]);

        // Check what is in the last cache:
        let batches = cache.to_record_batches(Arc::clone(&table_def), &p).unwrap();

        assert_batches_sorted_eq!(
            [
                "+--------+------+-----------------------------+-------+",
                "| region | host | time                        | usage |",
                "+--------+------+-----------------------------+-------+",
                "| us     | a    | 1970-01-01T00:00:00.000001Z | 100.0 |",
                "+--------+------+-----------------------------+-------+",
            ],
            &batches
        );

        // wait for the TTL to clear the cache
        thread::sleep(Duration::from_millis(1000));

        // Check what is in the last cache:
        let batches = cache.to_record_batches(Arc::clone(&table_def), &p).unwrap();

        // The cache is completely empty after the TTL evicted data, so it will give back nothing:
        assert_batches_sorted_eq!(
            [
                "+--------+------+------+-------+",
                "| region | host | time | usage |",
                "+--------+------+------+-------+",
                "+--------+------+------+-------+",
            ],
            &batches
        );

        // Ensure that records can be written to the cache again:
        let rows = writer
            .write_lp_to_rows("cpu,region=us,host=a usage=333", 500_000_000)
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        // Check the cache for values:
        let p = predicates([(host_col_id, Predicate::new_in([KeyValue::string("a")]))]);

        // Check what is in the last cache:
        let batches = cache.to_record_batches(Arc::clone(&table_def), &p).unwrap();

        assert_batches_sorted_eq!(
            [
                "+--------+------+--------------------------+-------+",
                "| region | host | time                     | usage |",
                "+--------+------+--------------------------+-------+",
                "| us     | a    | 1970-01-01T00:00:00.500Z | 333.0 |",
                "+--------+------+--------------------------+-------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn fields_as_key_columns() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows(
                "temp,component_id=111 active=true,type=\"camera\",loc=\"port\",reading=150",
                500,
            )
            .await;

        let table_def = writer.db_schema().table_definition("temp").unwrap();
        let component_id_col_id = table_def.column_name_to_id("component_id").unwrap();
        let active_col_id = table_def.column_name_to_id("active").unwrap();
        let type_col_id = table_def.column_name_to_id("type").unwrap();
        let loc_col_id = table_def.column_name_to_id("loc").unwrap();

        // Create the last cache with keys on some field columns:
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::default(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::Explicit(vec![
                component_id_col_id,
                active_col_id,
                type_col_id,
                loc_col_id,
            ]),
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache:
        let rows = writer.write_lp_to_rows("\
                temp,component_id=111 active=true,type=\"camera\",loc=\"port\",reading=150\n\
                temp,component_id=222 active=true,type=\"camera\",loc=\"starboard\",reading=250\n\
                temp,component_id=333 active=true,type=\"camera\",loc=\"fore\",reading=145\n\
                temp,component_id=444 active=true,type=\"solar-panel\",loc=\"port\",reading=233\n\
                temp,component_id=555 active=false,type=\"solar-panel\",loc=\"huygens\",reading=200\n\
                temp,component_id=666 active=false,type=\"comms-dish\",loc=\"huygens\",reading=220\n\
                ", 1_000).await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            // No predicates gives everything:
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                    "| component_id | active | type        | loc       | reading | time                        |",
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                    "| 111          | true   | camera      | port      | 150.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 222          | true   | camera      | starboard | 250.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 333          | true   | camera      | fore      | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 444          | true   | solar-panel | port      | 233.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 555          | false  | solar-panel | huygens   | 200.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 666          | false  | comms-dish  | huygens   | 220.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+-------------+-----------+---------+-----------------------------+",
                ],
            },
            // Predicates on tag key column work as expected:
            TestCase {
                predicates: predicates([(
                    component_id_col_id,
                    Predicate::new_in([KeyValue::string("333")]),
                )]),
                expected: &[
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                    "| component_id | active | type   | loc  | reading | time                        |",
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                    "| 333          | true   | camera | fore | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+--------+------+---------+-----------------------------+",
                ],
            },
            // Predicate on a non-string field key:
            TestCase {
                predicates: predicates([(
                    active_col_id,
                    Predicate::new_in([KeyValue::Bool(false)]),
                )]),
                expected: &[
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                    "| component_id | active | type        | loc     | reading | time                        |",
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                    "| 555          | false  | solar-panel | huygens | 200.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 666          | false  | comms-dish  | huygens | 220.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+-------------+---------+---------+-----------------------------+",
                ],
            },
            // Predicate on a string field key:
            TestCase {
                predicates: predicates([(
                    type_col_id,
                    Predicate::new_in([KeyValue::string("camera")]),
                )]),
                expected: &[
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                    "| component_id | active | type   | loc       | reading | time                        |",
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                    "| 111          | true   | camera | port      | 150.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 222          | true   | camera | starboard | 250.0   | 1970-01-01T00:00:00.000001Z |",
                    "| 333          | true   | camera | fore      | 145.0   | 1970-01-01T00:00:00.000001Z |",
                    "+--------------+--------+--------+-----------+---------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();
            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn series_key_as_default() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows("wind_speed,state=ca,county=napa,farm=10-01 speed=60", 500)
            .await;

        let table_def = writer.db_schema().table_definition("wind_speed").unwrap();
        let state_col_id = table_def.column_name_to_id("state").unwrap();
        let county_col_id = table_def.column_name_to_id("county").unwrap();
        let farm_col_id = table_def.column_name_to_id("farm").unwrap();

        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::default(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache:
        let rows = writer
            .write_lp_to_rows(
                "\
                wind_speed,state=ca,county=napa,farm=10-01 speed=50\n\
                wind_speed,state=ca,county=napa,farm=10-02 speed=49\n\
                wind_speed,state=ca,county=orange,farm=20-01 speed=40\n\
                wind_speed,state=ca,county=orange,farm=20-02 speed=33\n\
                wind_speed,state=ca,county=yolo,farm=30-01 speed=62\n\
                wind_speed,state=ca,county=nevada,farm=40-01 speed=66\n\
                ",
                1_000,
            )
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            // No predicates yields everything in the cache
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on state column, which is part of the series key:
            TestCase {
                predicates: predicates([(
                    state_col_id,
                    Predicate::new_in([KeyValue::string("ca")]),
                )]),
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-01 | 40.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | orange | 20-02 | 33.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on county column, which is part of the series key:
            TestCase {
                predicates: predicates([(
                    county_col_id,
                    Predicate::new_in([KeyValue::string("napa")]),
                )]),
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | napa   | 10-01 | 50.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca    | napa   | 10-02 | 49.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on farm column, which is part of the series key:
            TestCase {
                predicates: predicates([(
                    farm_col_id,
                    Predicate::new_in([KeyValue::string("30-01")]),
                )]),
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | yolo   | 30-01 | 62.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
            // Predicate on all series key columns:
            TestCase {
                predicates: predicates([
                    (state_col_id, Predicate::new_in([KeyValue::string("ca")])),
                    (
                        county_col_id,
                        Predicate::new_in([KeyValue::string("nevada")]),
                    ),
                    (farm_col_id, Predicate::new_in([KeyValue::string("40-01")])),
                ]),
                expected: &[
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| state | county | farm  | speed | time                        |",
                    "+-------+--------+-------+-------+-----------------------------+",
                    "| ca    | nevada | 40-01 | 66.0  | 1970-01-01T00:00:00.000001Z |",
                    "+-------+--------+-------+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn null_values() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows(
                "temp,province=on,county=bruce,township=kincardine lo=15,hi=21,avg=18",
                500,
            )
            .await;

        let table_def = writer.db_schema().table_definition("temp").unwrap();

        // Create the last cache using defaults and a count of 15
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::new(10).unwrap(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache, but omit fields to produce nulls:
        let rows = writer
            .write_lp_to_rows(
                "\
                temp,province=on,county=bruce,township=kincardine hi=21,avg=18\n\
                temp,province=on,county=huron,township=goderich lo=16,hi=22\n\
                temp,province=on,county=bruce,township=culrock lo=13,avg=15\n\
                temp,province=on,county=wentworth,township=ancaster lo=18,hi=23,avg=20\n\
                temp,province=on,county=york,township=york lo=20\n\
                temp,province=on,county=welland,township=bertie avg=20\n\
                ",
                1_000,
            )
            .await;
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        let batches = cache.to_record_batches(table_def, &predicates([])).unwrap();

        assert_batches_sorted_eq!(
            [
                "+----------+-----------+------------+------+------+------+-----------------------------+",
                "| province | county    | township   | avg  | hi   | lo   | time                        |",
                "+----------+-----------+------------+------+------+------+-----------------------------+",
                "| on       | bruce     | culrock    | 15.0 |      | 13.0 | 1970-01-01T00:00:00.000001Z |",
                "| on       | bruce     | kincardine | 18.0 | 21.0 |      | 1970-01-01T00:00:00.000001Z |",
                "| on       | huron     | goderich   |      | 22.0 | 16.0 | 1970-01-01T00:00:00.000001Z |",
                "| on       | welland   | bertie     | 20.0 |      |      | 1970-01-01T00:00:00.000001Z |",
                "| on       | wentworth | ancaster   | 20.0 | 23.0 | 18.0 | 1970-01-01T00:00:00.000001Z |",
                "| on       | york      | york       |      |      | 20.0 | 1970-01-01T00:00:00.000001Z |",
                "+----------+-----------+------------+------+------+------+-----------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn new_fields_added_to_default_cache() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_rows(r#"plays,game_id=1 type="shot",player="kessel""#, 500)
            .await;

        let table_def = writer.db_schema().table_definition("plays").unwrap();
        let game_id_col_id = table_def.column_name_to_id("game_id").unwrap();

        // Create the last cache using default tags as keys
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::new(10).unwrap(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache. The last two lines include a new field "zone" which
        // should be added and appear in queries:
        let rows = writer
            .write_lp_to_rows(
                "\
                plays,game_id=1 type=\"shot\",player=\"mackinnon\"\n\
                plays,game_id=2 type=\"shot\",player=\"matthews\"\n\
                plays,game_id=3 type=\"hit\",player=\"tkachuk\",zone=\"away\"\n\
                plays,game_id=4 type=\"save\",player=\"bobrovsky\",zone=\"home\"\n\
                ",
                1_000,
            )
            .await;
        // get the table definition after the write as the catalog has changed:
        let table_def = writer.db_schema().table_definition("plays").unwrap();
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Cache that has values in the zone columns should produce them:
            TestCase {
                predicates: predicates([(
                    game_id_col_id,
                    Predicate::new_in([KeyValue::string("4")]),
                )]),
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 4       | bobrovsky | 1970-01-01T00:00:00.000001Z | save | home |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
            // Cache that does not have a zone column will produce it with nulls:
            TestCase {
                predicates: predicates([(
                    game_id_col_id,
                    Predicate::new_in([KeyValue::string("1")]),
                )]),
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 1       | mackinnon | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
            // Pulling from multiple caches will fill in with nulls:
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+---------+-----------+-----------------------------+------+------+",
                    "| game_id | player    | time                        | type | zone |",
                    "+---------+-----------+-----------------------------+------+------+",
                    "| 1       | mackinnon | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "| 2       | matthews  | 1970-01-01T00:00:00.000001Z | shot |      |",
                    "| 3       | tkachuk   | 1970-01-01T00:00:00.000001Z | hit  | away |",
                    "| 4       | bobrovsky | 1970-01-01T00:00:00.000001Z | save | home |",
                    "+---------+-----------+-----------------------------+------+------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn new_field_ordering() {
        let writer = TestWriter::new().await;
        let _ = writer.write_lp_to_rows("tbl,t1=a f1=1", 500).await;

        let table_def = writer.db_schema().table_definition("tbl").unwrap();
        let t1_col_id = table_def.column_name_to_id("t1").unwrap();

        // Create the last cache using the single `t1` tag column as key
        // and using the default for fields, so that new fields will get added
        // to the cache.
        let mut cache = LastCache::new(CreateLastCacheArgs {
            table_def: Arc::clone(&table_def),
            count: LastCacheSize::default(),
            ttl: LastCacheTtl::default(),
            key_columns: LastCacheKeyColumnsArg::SeriesKey,
            value_columns: LastCacheValueColumnsArg::AcceptNew,
        })
        .unwrap();

        // Write some lines to fill the cache. In this case, with just the existing
        // columns in the table, i.e., t1 and f1
        let rows = writer
            .write_lp_to_rows(
                "\
                tbl,t1=a f1=1
                tbl,t1=b f1=10
                tbl,t1=c f1=100
                ",
                1_000,
            )
            .await;
        let table_def = writer.db_schema().table_definition("tbl").unwrap();
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        // Write lines containing new fields f2 and f3, but with different orders for
        // each key column value, i.e., t1=a and t1=b:
        let rows = writer
            .write_lp_to_rows(
                "\
                tbl,t1=a f1=1,f2=2,f3=3,f4=4
                tbl,t1=b f1=10,f4=40,f3=30
                tbl,t1=c f1=100,f3=300,f2=200
                ",
                1_500,
            )
            .await;
        let table_def = writer.db_schema().table_definition("tbl").unwrap();
        for row in &rows {
            cache.push(row, Arc::clone(&table_def));
        }

        struct TestCase<'a> {
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        let test_cases = [
            // Can query on specific key column values:
            TestCase {
                predicates: predicates([(t1_col_id, Predicate::new_in([KeyValue::string("a")]))]),
                expected: &[
                    "+----+-----+-----+-----+-----+--------------------------------+",
                    "| t1 | f1  | f2  | f3  | f4  | time                           |",
                    "+----+-----+-----+-----+-----+--------------------------------+",
                    "| a  | 1.0 | 2.0 | 3.0 | 4.0 | 1970-01-01T00:00:00.000001500Z |",
                    "+----+-----+-----+-----+-----+--------------------------------+",
                ],
            },
            TestCase {
                predicates: predicates([(t1_col_id, Predicate::new_in([KeyValue::string("b")]))]),
                expected: &[
                    "+----+------+----+------+------+--------------------------------+",
                    "| t1 | f1   | f2 | f3   | f4   | time                           |",
                    "+----+------+----+------+------+--------------------------------+",
                    "| b  | 10.0 |    | 30.0 | 40.0 | 1970-01-01T00:00:00.000001500Z |",
                    "+----+------+----+------+------+--------------------------------+",
                ],
            },
            TestCase {
                predicates: predicates([(t1_col_id, Predicate::new_in([KeyValue::string("c")]))]),
                expected: &[
                    "+----+-------+-------+-------+----+--------------------------------+",
                    "| t1 | f1    | f2    | f3    | f4 | time                           |",
                    "+----+-------+-------+-------+----+--------------------------------+",
                    "| c  | 100.0 | 200.0 | 300.0 |    | 1970-01-01T00:00:00.000001500Z |",
                    "+----+-------+-------+-------+----+--------------------------------+",
                ],
            },
            // Can query accross key column values:
            TestCase {
                predicates: predicates([]),
                expected: &[
                    "+----+-------+-------+-------+------+--------------------------------+",
                    "| t1 | f1    | f2    | f3    | f4   | time                           |",
                    "+----+-------+-------+-------+------+--------------------------------+",
                    "| a  | 1.0   | 2.0   | 3.0   | 4.0  | 1970-01-01T00:00:00.000001500Z |",
                    "| b  | 10.0  |       | 30.0  | 40.0 | 1970-01-01T00:00:00.000001500Z |",
                    "| c  | 100.0 | 200.0 | 300.0 |      | 1970-01-01T00:00:00.000001500Z |",
                    "+----+-------+-------+-------+------+--------------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let batches = cache
                .to_record_batches(Arc::clone(&table_def), &t.predicates)
                .unwrap();

            assert_batches_sorted_eq!(t.expected, &batches);
        }
    }

    #[tokio::test]
    async fn catalog_initialization() {
        let obj_store = Arc::new(InMemory::new());
        let node_id = Arc::from("sample-host-id");
        let catalog = Arc::new(
            Catalog::new(
                node_id,
                Arc::clone(&obj_store) as _,
                Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let db_name = "test_db";
        catalog.create_database(db_name).await.unwrap();
        catalog
            .create_table(
                db_name,
                "test_table_1",
                &["t1", "t2", "t3"],
                &[("f1", FieldDataType::String), ("f2", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
            .create_last_cache(
                db_name,
                "test_table_1",
                Some("test_cache_1"),
                Some(&["t1"]),
                None as Option<&[&str]>,
                LastCacheSize::new(1).unwrap(),
                LastCacheTtl::from_secs(600),
            )
            .await
            .unwrap();
        catalog
            .create_table(
                db_name,
                "test_table_2",
                &["t1"],
                &[("f1", FieldDataType::String), ("f2", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
            .create_last_cache(
                db_name,
                "test_table_2",
                Some("test_cache_2"),
                Some(&["t1"]),
                Some(&["f1", "time"]),
                LastCacheSize::new(10).unwrap(),
                LastCacheTtl::from_secs(60),
            )
            .await
            .unwrap();
        catalog
            .create_last_cache(
                db_name,
                "test_table_2",
                Some("test_cache_3"),
                Option::<&[&str]>::Some(&[]),
                Some(&["f2", "time"]),
                LastCacheSize::new(100).unwrap(),
                LastCacheTtl::from_secs(500),
            )
            .await
            .unwrap();

        // This is the function we are testing, which initializes the LastCacheProvider from the catalog:
        let provider = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .expect("create last cache provider from catalog");
        // There should be a total of 3 caches:
        assert_eq!(3, provider.size());
        insta::assert_json_snapshot!(catalog.snapshot(), {
            ".catalog_uuid" => "[uuid]"
        });
    }

    /// This test sets up a [`LastCacheProvider`], creates a [`LastCache`] using the `region` and
    /// `host` columns as keys, and then writes row data containing several unique combinations of
    /// the key columns to the cache. It then sets up a DataFusion [`SessionContext`], registers
    /// the [`LastCacheFunction`] as a UDTF, and runs a series of test cases to verify queries made
    /// using the function.
    ///
    /// The purpose of this is to verify that the predicate pushdown by the UDTF [`TableProvider`]
    /// is working.
    ///
    /// Each test case verifies both the `RecordBatch` output, as well as the output of the `EXPLAIN`
    /// for a given query. The `EXPLAIN` contains a line for the `LastCacheExec`, which will list
    /// out any predicates that were pushed down from the provided SQL query to the cache.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn datafusion_udtf_predicate_conversion() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_write_batch("cpu,region=us-east,host=a usage=99,temp=88", 0)
            .await;

        // create a last cache provider so we can use it to create our UDTF provider:
        let provider = LastCacheProvider::new_from_catalog(writer.catalog())
            .await
            .unwrap();
        writer
            .catalog()
            .create_last_cache(
                TestWriter::DB_NAME,
                "cpu",
                None,
                Option::<&[&str]>::None,
                Option::<&[&str]>::None,
                LastCacheSize::default(),
                LastCacheTtl::default(),
            )
            .await
            .unwrap();

        // Use a short sleep to allow catalog change to be broadast. In future, the catalog
        // broadcast should be acknowledged and this would not be necessary... see
        // https://github.com/influxdata/influxdb_pro/issues/556
        tokio::time::sleep(Duration::from_millis(100)).await;

        // make some writes into the cache:
        let write_batch = writer
            .write_lp_to_write_batch(
                "\
            cpu,region=us-east,host=a usage=77,temp=66\n\
            cpu,region=us-east,host=b usage=77,temp=66\n\
            cpu,region=us-west,host=c usage=77,temp=66\n\
            cpu,region=us-west,host=d usage=77,temp=66\n\
            cpu,region=ca-east,host=e usage=77,temp=66\n\
            cpu,region=ca-cent,host=f usage=77,temp=66\n\
            cpu,region=ca-west,host=g usage=77,temp=66\n\
            cpu,region=ca-west,host=h usage=77,temp=66\n\
            cpu,region=eu-cent,host=i usage=77,temp=66\n\
            cpu,region=eu-cent,host=j usage=77,temp=66\n\
            cpu,region=eu-west,host=k usage=77,temp=66\n\
            cpu,region=eu-west,host=l usage=77,temp=66\n\
            ",
                1_000,
            )
            .await;
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        provider.write_wal_contents_to_cache(&wal_contents);

        let ctx = SessionContext::new();
        let last_cache_fn = LastCacheFunction::new(writer.db_schema().id, Arc::clone(&provider));
        ctx.register_udtf(LAST_CACHE_UDTF_NAME, Arc::new(last_cache_fn));

        struct TestCase<'a> {
            /// A short description of the test
            _desc: &'a str,
            /// A SQL expression to evaluate using the datafusion session context, should be of
            /// the form:
            /// ```sql
            /// SELECT * FROM last_cache('cpu') ...
            /// ```
            sql: &'a str,
            /// Expected record batch output
            expected: &'a [&'a str],
            /// Expected EXPLAIN output contains this.
            ///
            /// For checking the `LastCacheExec` portion of the EXPLAIN output for the given `sql`
            /// query. A "contains" is used instead of matching the whole EXPLAIN output to prevent
            /// flakyness from upstream changes to other parts of the query plan.
            explain_contains: &'a str,
        }

        let test_cases = [
            TestCase {
                _desc: "no predicates",
                sql: "SELECT * FROM last_cache('cpu')",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| ca-cent | f    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-east | e    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | g    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | h    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | i    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | j    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | k    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | l    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | b    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | d    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[12]",
            },
            TestCase {
                _desc: "eq predicate on region",
                sql: "SELECT * FROM last_cache('cpu') WHERE region = 'us-east'",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | b    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 IN ('us-east')]] inner=MemoryExec: partitions=1, partition_sizes=[2]",
            },
            TestCase {
                _desc: "not eq predicate on region",
                sql: "SELECT * FROM last_cache('cpu') WHERE region != 'us-east'",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| ca-cent | f    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-east | e    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | g    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | h    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | i    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | j    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | k    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | l    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | d    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 NOT IN ('us-east')]] inner=MemoryExec: partitions=1, partition_sizes=[10]",
            },
            TestCase {
                _desc: "double eq predicate on region",
                sql: "SELECT * FROM last_cache('cpu') \
                    WHERE region = 'us-east' \
                    OR region = 'us-west'",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | b    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | d    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 IN ('us-east','us-west')]] inner=MemoryExec: partitions=1, partition_sizes=[4]",
            },
            TestCase {
                _desc: "triple eq predicate on region",
                sql: "SELECT * FROM last_cache('cpu') \
                    WHERE region = 'us-east' \
                    OR region = 'us-west' \
                    OR region = 'ca-west'",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| ca-west | g    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | h    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | b    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | d    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 IN ('ca-west','us-east','us-west')]] inner=MemoryExec: partitions=1, partition_sizes=[6]",
            },
            TestCase {
                _desc: "eq predicate on region AND eq predicate on host",
                sql: "SELECT * FROM last_cache('cpu') \
                    WHERE (region = 'us-east' OR region = 'us-west') \
                    AND (host = 'a' OR host = 'c')",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 IN ('us-east','us-west')], [host@1 IN ('a','c')]] inner=MemoryExec: partitions=1, partition_sizes=[2]",
            },
            TestCase {
                _desc: "in predicate on region",
                sql: "SELECT * FROM last_cache('cpu') \
                    WHERE region IN ('ca-east', 'ca-west')",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| ca-east | e    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | g    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| ca-west | h    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 IN ('ca-east','ca-west')]] inner=MemoryExec: partitions=1, partition_sizes=[3]",
            },
            TestCase {
                _desc: "not in predicate on region",
                sql: "SELECT * FROM last_cache('cpu') \
                    WHERE region NOT IN ('ca-east', 'ca-west')",
                expected: &[
                    "+---------+------+------+-----------------------------+-------+",
                    "| region  | host | temp | time                        | usage |",
                    "+---------+------+------+-----------------------------+-------+",
                    "| ca-cent | f    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | i    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-cent | j    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | k    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| eu-west | l    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | a    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-east | b    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | c    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "| us-west | d    | 66.0 | 1970-01-01T00:00:00.000001Z | 77.0  |",
                    "+---------+------+------+-----------------------------+-------+",
                ],
                explain_contains: "LastCacheExec: predicates=[[region@0 NOT IN ('ca-east','ca-west')]] inner=MemoryExec: partitions=1, partition_sizes=[9]",
            },
        ];

        for tc in test_cases {
            // do the query:
            let results = ctx.sql(tc.sql).await.unwrap().collect().await.unwrap();
            println!("test case: {}", tc._desc);
            // check the result:
            assert_batches_sorted_eq!(tc.expected, &results);
            let explain = ctx
                .sql(format!("EXPLAIN {sql}", sql = tc.sql).as_str())
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert!(
                explain
                    .column_by_name("plan")
                    .unwrap()
                    .as_string::<i32>()
                    .iter()
                    .any(|plan| plan.is_some_and(|plan| plan.contains(tc.explain_contains))),
                "explain plan did not contain the expression:\n\n\
                {expected}\n\n\
                instead, the output was:\n\n\
                {actual:#?}",
                expected = tc.explain_contains,
                actual = explain.column_by_name("plan").unwrap().as_string::<i32>(),
            );
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_non_specified_key_val_cols() {
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_write_batch("cpu,region=us-east,host=a usage=99,temp=88", 0)
            .await;
        let provider = LastCacheProvider::new_from_catalog(writer.catalog())
            .await
            .unwrap();
        writer
            .catalog()
            .create_last_cache(
                TestWriter::DB_NAME,
                "cpu",
                None,
                Option::<&[&str]>::None,
                Some(&["usage"]),
                LastCacheSize::default(),
                LastCacheTtl::default(),
            )
            .await
            .unwrap();

        // Use a short sleep to allow catalog change to be broadast. In future, the catalog
        // broadcast should be acknowledged and this would not be necessary... see
        // https://github.com/influxdata/influxdb_pro/issues/556
        tokio::time::sleep(Duration::from_millis(100)).await;

        let write_batch = writer
            .write_lp_to_write_batch(
                "\
            cpu,region=us-east,host=a usage=77,temp=66\n\
            cpu,region=us-east,host=b usage=77,temp=66\n\
            cpu,region=us-west,host=c usage=77,temp=66\n\
            cpu,region=us-west,host=d usage=77,temp=66\n\
            cpu,region=ca-east,host=e usage=77,temp=66\n\
            cpu,region=ca-cent,host=f usage=77,temp=66\n\
            cpu,region=ca-west,host=g usage=77,temp=66\n\
            cpu,region=ca-west,host=h usage=77,temp=66\n\
            cpu,region=eu-cent,host=i usage=77,temp=66\n\
            cpu,region=eu-cent,host=j usage=77,temp=66\n\
            cpu,region=eu-west,host=k usage=77,temp=66\n\
            cpu,region=eu-west,host=l usage=77,temp=66\n\
            ",
                1_000,
            )
            .await;

        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        provider.write_wal_contents_to_cache(&wal_contents);

        let ctx = SessionContext::new();
        let last_cache_fn = LastCacheFunction::new(writer.db_schema().id, Arc::clone(&provider));
        ctx.register_udtf(LAST_CACHE_UDTF_NAME, Arc::new(last_cache_fn));

        struct TestCase<'a> {
            sql: &'a str,
            expected: &'a [&'a str],
        }

        let test_cases = [
            TestCase {
                sql: "select * from last_cache('cpu')",
                expected: &[
                    "+---------+------+-------+-----------------------------+",
                    "| region  | host | usage | time                        |",
                    "+---------+------+-------+-----------------------------+",
                    "| ca-cent | f    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca-east | e    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca-west | g    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| ca-west | h    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-cent | i    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-cent | j    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-west | k    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-west | l    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| us-east | a    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| us-east | b    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| us-west | c    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| us-west | d    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "+---------+------+-------+-----------------------------+",
                ],
            },
            TestCase {
                sql: "select * from last_cache('cpu') WHERE region = 'eu-west'",
                expected: &[
                    "+---------+------+-------+-----------------------------+",
                    "| region  | host | usage | time                        |",
                    "+---------+------+-------+-----------------------------+",
                    "| eu-west | k    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-west | l    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "+---------+------+-------+-----------------------------+",
                ],
            },
            TestCase {
                sql: "select * from last_cache('cpu') WHERE region IN ('eu-west', 'eu-cent')",
                expected: &[
                    "+---------+------+-------+-----------------------------+",
                    "| region  | host | usage | time                        |",
                    "+---------+------+-------+-----------------------------+",
                    "| eu-cent | i    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-cent | j    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-west | k    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "| eu-west | l    | 77.0  | 1970-01-01T00:00:00.000001Z |",
                    "+---------+------+-------+-----------------------------+",
                ],
            },
        ];

        for t in test_cases {
            let results = ctx.sql(t.sql).await.unwrap().collect().await.unwrap();
            assert_batches_sorted_eq!(t.expected, &results);
        }
    }
}
