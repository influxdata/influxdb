//! The Distinct Value Cache holds the distinct values for a column or set of columns on a table

mod cache;
pub use cache::{CacheError, CreateDistinctCacheArgs};
mod provider;
pub use provider::{DistinctCacheProvider, ProviderError};
mod table_function;
pub use table_function::DISTINCT_CACHE_UDTF_NAME;
pub use table_function::DistinctCacheFunction;

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq, prelude::SessionContext};
    use indexmap::IndexMap;
    use influxdb3_catalog::log::{FieldDataType, MaxAge, MaxCardinality};
    use influxdb3_id::ColumnId;
    use iox_time::{MockProvider, Time, TimeProvider};
    use observability_deps::tracing::debug;
    use std::{sync::Arc, time::Duration};

    use crate::{
        distinct_cache::{
            DISTINCT_CACHE_UDTF_NAME, DistinctCacheFunction, DistinctCacheProvider,
            cache::{CreateDistinctCacheArgs, DistinctCache, Predicate},
        },
        test_helpers::TestWriter,
    };

    #[tokio::test]
    async fn evaluate_predicates() {
        let writer = TestWriter::new().await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to get a set of rows destined for the WAL, and an updated catalog:
        let rows = writer
            .write_lp_to_rows(
                "\
            cpu,region=us-east,host=a usage=100\n\
            cpu,region=us-east,host=b usage=100\n\
            cpu,region=us-west,host=c usage=100\n\
            cpu,region=us-west,host=d usage=100\n\
            cpu,region=ca-east,host=e usage=100\n\
            cpu,region=ca-east,host=f usage=100\n\
            cpu,region=ca-cent,host=g usage=100\n\
            cpu,region=ca-cent,host=h usage=100\n\
            cpu,region=eu-east,host=i usage=100\n\
            cpu,region=eu-east,host=j usage=100\n\
            cpu,region=eu-cent,host=k usage=100\n\
            cpu,region=eu-cent,host=l usage=100\n\
            ",
                0,
            )
            .await;
        // grab the table definition for the table written to:
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        // use the two tags, in order, to create the cache:
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        let region_col_id = column_ids[0];
        let host_col_id = column_ids[1];
        // create the cache:
        let mut cache = DistinctCache::new(
            time_provider,
            CreateDistinctCacheArgs {
                table_def,
                max_cardinality: MaxCardinality::default(),
                max_age: MaxAge::default(),
                column_ids,
            },
        )
        .expect("create cache");
        // push the row data into the cache:
        for row in rows {
            cache.push(&row);
        }

        // run a series of test cases with varying predicates:
        struct TestCase<'a> {
            desc: &'a str,
            predicates: IndexMap<ColumnId, Predicate>,
            expected: &'a [&'a str],
        }

        fn create_predicate_map(
            predicates: &[(ColumnId, Predicate)],
        ) -> IndexMap<ColumnId, Predicate> {
            predicates.iter().cloned().collect()
        }

        let test_cases = [
            TestCase {
                desc: "no predicates",
                predicates: create_predicate_map(&[]),
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-cent | h    |",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "in predicate on region",
                predicates: create_predicate_map(&[(
                    region_col_id,
                    Predicate::new_in(["ca-cent", "ca-east"]),
                )]),
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-cent | h    |",
                    "| ca-east | e    |",
                    "| ca-east | f    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "in predicate on region and host",
                predicates: create_predicate_map(&[
                    (region_col_id, Predicate::new_in(["ca-cent", "ca-east"])),
                    (host_col_id, Predicate::new_in(["g", "e"])),
                ]),
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | g    |",
                    "| ca-east | e    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not in predicate on region",
                predicates: create_predicate_map(&[(
                    region_col_id,
                    Predicate::new_not_in(["ca-cent", "ca-east"]),
                )]),
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| eu-cent | k    |",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| eu-east | j    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
            TestCase {
                desc: "not in predicate on region and host",
                predicates: create_predicate_map(&[
                    (region_col_id, Predicate::new_not_in(["ca-cent", "ca-east"])),
                    (host_col_id, Predicate::new_not_in(["j", "k"])),
                ]),
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| eu-cent | l    |",
                    "| eu-east | i    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
            },
        ];

        for tc in test_cases {
            let records = cache
                .to_record_batch(cache.arrow_schema(), &tc.predicates, None, None)
                .expect("get record batches");
            println!("{}", tc.desc);
            assert_batches_sorted_eq!(tc.expected, &[records]);
        }
    }

    #[tokio::test]
    async fn cache_pruning() {
        let writer = TestWriter::new().await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to update the catalog:
        let _ = writer
            .write_lp_to_rows(
                "\
            cpu,region=us-east,host=a usage=100\n\
            ",
                time_provider.now().timestamp_nanos(),
            )
            .await;
        // grab the table definition for the table written to:
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        // use the two tags, in order, to create the cache:
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        // create a cache with some cardinality and age limits:
        let mut cache = DistinctCache::new(
            Arc::clone(&time_provider) as _,
            CreateDistinctCacheArgs {
                table_def,
                max_cardinality: MaxCardinality::try_from(10).unwrap(),
                max_age: MaxAge::from(Duration::from_nanos(100)),
                column_ids,
            },
        )
        .expect("create cache");
        // push a bunch of rows with incrementing times and varying tag values:
        for mult in 0..10 {
            time_provider.set(Time::from_timestamp_nanos(mult * 20));
            let rows = writer
                .write_lp_to_rows(
                    format!(
                        "\
                    cpu,region=us-east-{mult},host=a-{mult} usage=100\n\
                    cpu,region=us-west-{mult},host=b-{mult} usage=100\n\
                    cpu,region=us-cent-{mult},host=c-{mult} usage=100\n\
                    "
                    ),
                    time_provider.now().timestamp_nanos(),
                )
                .await;
            // push the initial row data into the cache:
            for row in rows {
                cache.push(&row);
            }
        }
        // check the cache before prune:
        // NOTE: this does not include entries that have surpassed the max_age of the cache, though,
        // there are still more than the cache's max cardinality, as it has not yet been pruned.
        let records = cache
            .to_record_batch(cache.arrow_schema(), &Default::default(), None, None)
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+------+",
                "| region    | host |",
                "+-----------+------+",
                "| us-cent-5 | c-5  |",
                "| us-cent-6 | c-6  |",
                "| us-cent-7 | c-7  |",
                "| us-cent-8 | c-8  |",
                "| us-cent-9 | c-9  |",
                "| us-east-5 | a-5  |",
                "| us-east-6 | a-6  |",
                "| us-east-7 | a-7  |",
                "| us-east-8 | a-8  |",
                "| us-east-9 | a-9  |",
                "| us-west-5 | b-5  |",
                "| us-west-6 | b-6  |",
                "| us-west-7 | b-7  |",
                "| us-west-8 | b-8  |",
                "| us-west-9 | b-9  |",
                "+-----------+------+",
            ],
            &[records]
        );
        cache.prune();
        let records = cache
            .to_record_batch(cache.arrow_schema(), &Default::default(), None, None)
            .unwrap();
        assert_batches_sorted_eq!(
            [
                "+-----------+------+",
                "| region    | host |",
                "+-----------+------+",
                "| us-cent-7 | c-7  |",
                "| us-cent-8 | c-8  |",
                "| us-cent-9 | c-9  |",
                "| us-east-7 | a-7  |",
                "| us-east-8 | a-8  |",
                "| us-east-9 | a-9  |",
                "| us-west-7 | b-7  |",
                "| us-west-8 | b-8  |",
                "| us-west-9 | b-9  |",
                "+-----------+------+",
            ],
            &[records]
        );
    }

    #[tokio::test]
    async fn distinct_cache_limit() {
        let writer = TestWriter::new().await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let rows = writer
            .write_lp_to_rows(
                "\
            cpu,region=us-east,host=a usage=100\n\
            cpu,region=us-east,host=b usage=100\n\
            cpu,region=us-west,host=c usage=100\n\
            cpu,region=us-west,host=d usage=100\n\
            cpu,region=ca-east,host=e usage=100\n\
            cpu,region=ca-east,host=f usage=100\n\
            cpu,region=ca-cent,host=g usage=100\n\
            cpu,region=ca-cent,host=h usage=100\n\
            cpu,region=eu-east,host=i usage=100\n\
            cpu,region=eu-east,host=j usage=100\n\
            cpu,region=eu-cent,host=k usage=100\n\
            cpu,region=eu-cent,host=l usage=100\n\
            ",
                0,
            )
            .await;
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        let mut cache = DistinctCache::new(
            time_provider,
            CreateDistinctCacheArgs {
                table_def,
                max_cardinality: MaxCardinality::default(),
                max_age: MaxAge::default(),
                column_ids,
            },
        )
        .unwrap();

        for row in rows {
            cache.push(&row);
        }

        // no limit produces all records in the cache:
        let batches = cache
            .to_record_batch(cache.arrow_schema(), &Default::default(), None, None)
            .unwrap();
        assert_batches_eq!(
            [
                "+---------+------+",
                "| region  | host |",
                "+---------+------+",
                "| ca-cent | g    |",
                "| ca-cent | h    |",
                "| ca-east | e    |",
                "| ca-east | f    |",
                "| eu-cent | k    |",
                "| eu-cent | l    |",
                "| eu-east | i    |",
                "| eu-east | j    |",
                "| us-east | a    |",
                "| us-east | b    |",
                "| us-west | c    |",
                "| us-west | d    |",
                "+---------+------+",
            ],
            &[batches]
        );

        // applying a limit only returns that number of records from the cache:
        let batches = cache
            .to_record_batch(cache.arrow_schema(), &Default::default(), None, Some(5))
            .unwrap();
        assert_batches_eq!(
            [
                "+---------+------+",
                "| region  | host |",
                "+---------+------+",
                "| ca-cent | g    |",
                "| ca-cent | h    |",
                "| ca-east | e    |",
                "| ca-east | f    |",
                "| eu-cent | k    |",
                "+---------+------+",
            ],
            &[batches]
        );
    }

    /// This test sets up a [`DistinctCacheProvider`], creates a [`DistinctCache`] using the `region` and
    /// `host` column, and then writes several different unique combinations of values into it.
    /// It then sets up a DataFusion [`SessionContext`], registers our [`DistinctCacheFunction`] as a
    /// UDTF, and then runs a series of test cases to verify queries against the function.
    ///
    /// The purpose of this is to see that the cache works as intended, and importantly, that the
    /// predicate pushdown is happening and being leveraged by the underlying [`DistinctCache`], vs.
    /// DataFusion doing it for us with a higher level FilterExec.
    ///
    /// Each test case verifies the `RecordBatch` output of the query as well as the output for
    /// EXPLAIN on the same query. The EXPLAIN output contains a line for the DistinctCacheExec, which
    /// is the custom execution plan impl for the distinct value cache that captures the predicates that
    /// are pushed down to the underlying [`DistinctCacahe::to_record_batch`] method, if any.
    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_datafusion_distinct_cache_udtf() {
        // create a test writer and do a write in to populate the catalog with a db/table:
        let writer = TestWriter::new().await;
        let _ = writer
            .write_lp_to_write_batch(
                "\
            cpu,region=us-east,host=a usage=100\n\
            ",
                0,
            )
            .await;

        // create a distinct provider and a cache on tag columns 'region' and 'host':
        debug!(catlog = ?writer.catalog(), "writer catalog");
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let distinct_provider =
            DistinctCacheProvider::new_from_catalog(time_provider, writer.catalog())
                .await
                .unwrap();
        writer
            .catalog()
            .create_distinct_cache(
                TestWriter::DB_NAME,
                "cpu",
                None,
                &["region", "host"],
                MaxCardinality::default(),
                MaxAge::default(),
            )
            .await
            .unwrap();

        // Use a short sleep to allow catalog change to be broadast. In future, the catalog
        // broadcast should be acknowledged and this would not be necessary... see
        // https://github.com/influxdata/influxdb_pro/issues/556
        tokio::time::sleep(Duration::from_millis(100)).await;

        // do some writes to generate a write batch and send it into the cache:
        let write_batch = writer
            .write_lp_to_write_batch(
                "\
            cpu,region=us-east,host=a usage=100\n\
            cpu,region=us-east,host=b usage=100\n\
            cpu,region=us-west,host=c usage=100\n\
            cpu,region=us-west,host=d usage=100\n\
            cpu,region=ca-east,host=e usage=100\n\
            cpu,region=ca-cent,host=f usage=100\n\
            cpu,region=ca-west,host=g usage=100\n\
            cpu,region=ca-west,host=h usage=100\n\
            cpu,region=eu-cent,host=i usage=100\n\
            cpu,region=eu-cent,host=j usage=100\n\
            cpu,region=eu-west,host=k usage=100\n\
            cpu,region=eu-west,host=l usage=100\n\
            ",
                0,
            )
            .await;
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        distinct_provider.write_wal_contents_to_cache(&wal_contents);

        // Spin up a DataFusion session context and add the distinct_cache function to it so we
        // can query for data in the cache we created and populated above:
        let ctx = SessionContext::new();
        let distinct_func =
            DistinctCacheFunction::new(writer.db_schema().id, Arc::clone(&distinct_provider));
        ctx.register_udtf(DISTINCT_CACHE_UDTF_NAME, Arc::new(distinct_func));

        struct TestCase<'a> {
            /// A short description of the test
            _desc: &'a str,
            /// A SQL expression to evaluate using the datafusion session context, should be of
            /// the form:
            /// ```sql
            /// SELECT * FROM distinct_cache('cpu') ...
            /// ```
            sql: &'a str,
            /// Expected record batch output
            expected: &'a [&'a str],
            /// Expected EXPLAIN output contains this.
            ///
            /// For checking the `DistinctCacheExec` portion of the EXPLAIN output for the given `sql`
            /// query. A "contains" is used instead of matching the whole EXPLAIN output to prevent
            /// flakyness from upstream changes to other parts of the query plan.
            explain_contains: &'a str,
            /// Use `assert_batches_sorted_eq!` to check the outputted record batches instead of
            /// `assert_batches_eq!`.
            ///
            /// # Note
            ///
            /// The cache should produce results in a sorted order as-is, however, some queries
            /// that process the results after they are emitted from the cache may have their order
            /// altered by DataFusion, e.g., `SELECT DISTINCT(column_name) FROM distinct_cache('table')`
            /// or queries that project columns that are not at the top level of the cache.
            use_sorted_assert: bool,
        }

        let test_cases = [
            TestCase {
                _desc: "no predicates",
                sql: "SELECT * FROM distinct_cache('cpu')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | f    |",
                    "| ca-east | e    |",
                    "| ca-west | g    |",
                    "| ca-west | h    |",
                    "| eu-cent | i    |",
                    "| eu-cent | j    |",
                    "| eu-west | k    |",
                    "| eu-west | l    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region",
                sql: "SELECT * FROM distinct_cache('cpu') WHERE region = 'us-east'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (us-east)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region and host",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region = 'us-east' AND host = 'a'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (us-east)], [host@1 IN (a)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region; in predicate on host",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region = 'us-east' AND host IN ('a', 'b')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (us-east)], [host@1 IN (a,b)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region; not in predicate on host",
                sql: "SELECT * FROM distinct_cache('cpu') \
                        WHERE region = 'us-east' AND host != 'a'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (us-east)], [host@1 NOT IN (a)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "in predicate on region",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region IN ('ca-cent', 'ca-east', 'us-east', 'us-west')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | f    |",
                    "| ca-east | e    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (ca-cent,ca-east,us-east,us-west)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "not in predicate on region",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region NOT IN ('ca-cent', 'ca-east', 'us-east', 'us-west')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-west | g    |",
                    "| ca-west | h    |",
                    "| eu-cent | i    |",
                    "| eu-cent | j    |",
                    "| eu-west | k    |",
                    "| eu-west | l    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 NOT IN (ca-cent,ca-east,us-east,us-west)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "or eq predicates on region",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region = 'us-east' OR region = 'ca-east'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-east | e    |",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (ca-east,us-east)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "or eq predicate on host",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE host = 'd' OR host = 'e'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-east | e    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[host@1 IN (d,e)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "un-grouped host conditions are not handled in predicate pushdown",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region = 'us-east' AND host = 'a' OR host = 'b'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "grouped host conditions are handled in predicate pushdown",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region = 'us-east' AND (host = 'a' OR host = 'b')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (us-east)], [host@1 IN (a,b)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "project region column",
                sql: "SELECT region FROM distinct_cache('cpu')",
                expected: &[
                    "+---------+",
                    "| region  |",
                    "+---------+",
                    "| ca-cent |",
                    "| ca-east |",
                    "| ca-west |",
                    "| eu-cent |",
                    "| eu-west |",
                    "| us-east |",
                    "| us-west |",
                    "+---------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "project region column taking distinct",
                sql: "SELECT DISTINCT(region) FROM distinct_cache('cpu')",
                expected: &[
                    "+---------+",
                    "| region  |",
                    "+---------+",
                    "| ca-cent |",
                    "| ca-east |",
                    "| ca-west |",
                    "| eu-cent |",
                    "| eu-west |",
                    "| us-east |",
                    "| us-west |",
                    "+---------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region] inner=MemoryExec: partitions=1, partition_sizes=[1",
                // it seems that DISTINCT changes around the order of results
                use_sorted_assert: true,
            },
            TestCase {
                _desc: "project host column",
                sql: "SELECT host FROM distinct_cache('cpu')",
                expected: &[
                    "+------+", // commenting for no new line
                    "| host |", // commenting for no new line
                    "+------+", // commenting for no new line
                    "| a    |", // commenting for no new line
                    "| b    |", // commenting for no new line
                    "| c    |", // commenting for no new line
                    "| d    |", // commenting for no new line
                    "| e    |", // commenting for no new line
                    "| f    |", // commenting for no new line
                    "| g    |", // commenting for no new line
                    "| h    |", // commenting for no new line
                    "| i    |", // commenting for no new line
                    "| j    |", // commenting for no new line
                    "| k    |", // commenting for no new line
                    "| l    |", // commenting for no new line
                    "+------+", // commenting for no new line
                ],
                explain_contains: "DistinctCacheExec: projection=[host] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                // this column will not be sorted since the order of elements depends on the next level
                // up in the cache, so the `region` column is iterated over in order, but the nested
                // `host` values, although sorted within `region`s, will not be globally sorted.
                use_sorted_assert: true,
            },
            TestCase {
                _desc: "project host column",
                sql: "SELECT host FROM distinct_cache('cpu') WHERE region = 'ca-cent'",
                expected: &[
                    "+------+", // commenting for no new line
                    "| host |", // commenting for no new line
                    "+------+", // commenting for no new line
                    "| f    |", // commenting for no new line
                    "+------+", // commenting for no new line
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] predicates=[[region@0 IN (ca-cent)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "limit clause",
                sql: "SELECT * FROM distinct_cache('cpu') LIMIT 8",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-cent | f    |",
                    "| ca-east | e    |",
                    "| ca-west | g    |",
                    "| ca-west | h    |",
                    "| eu-cent | i    |",
                    "| eu-cent | j    |",
                    "| eu-west | k    |",
                    "| eu-west | l    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] limit=8 inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "limit and offset",
                sql: "SELECT * FROM distinct_cache('cpu') LIMIT 8 OFFSET 8",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] limit=16 inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "like clause",
                sql: "SELECT * FROM distinct_cache('cpu') \
                    WHERE region LIKE 'u%'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "| us-west | c    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "DistinctCacheExec: projection=[region, host] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
        ];

        for tc in test_cases {
            println!("test case: {}\n{}\n", tc._desc, tc.sql);
            let results = ctx.sql(tc.sql).await.unwrap().collect().await.unwrap();
            if tc.use_sorted_assert {
                assert_batches_sorted_eq!(tc.expected, &results);
            } else {
                assert_batches_eq!(tc.expected, &results);
            }
            let explain = ctx
                .sql(format!("EXPLAIN {sql}", sql = tc.sql).as_str())
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .pop()
                .unwrap();

            // NOTE(hiltontj): this probably can be done a better way?
            // The EXPLAIN output will have two columns, the one we are interested in that contains
            // the details of the DistinctCacheExec is called `plan`...
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
    async fn test_projection_pushdown_indexing() {
        let writer = TestWriter::new().await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let _ = writer.write_lp_to_rows(
            "\
            wind_data,city=Berlin,country=Germany,county=Berlin wind_speed=14.63,wind_direction=270i\n\
            ",
            0,
        ).await;

        let distinct_provider =
            DistinctCacheProvider::new_from_catalog(time_provider, writer.catalog())
                .await
                .unwrap();
        writer
            .catalog()
            .create_distinct_cache(
                TestWriter::DB_NAME,
                "wind_data",
                None,
                &["country", "county", "city"],
                MaxCardinality::default(),
                MaxAge::default(),
            )
            .await
            .unwrap();

        // Use a short sleep to allow catalog change to be broadast. In future, the catalog
        // broadcast should be acknowledged and this would not be necessary... see
        // https://github.com/influxdata/influxdb_pro/issues/556
        tokio::time::sleep(Duration::from_millis(100)).await;

        let write_batch = writer.write_lp_to_write_batch(
            "\
            wind_data,city=Berlin,country=Germany,county=Berlin wind_speed=14.63,wind_direction=270i\n\
            wind_data,city=Hamburg,country=Germany,county=Hamburg wind_speed=19.8,wind_direction=26i\n\
            wind_data,city=Munich,country=Germany,county=Bavaria wind_speed=11.77,wind_direction=227i\n\
            wind_data,city=Cologne,country=Germany,county=North\\ Rhine-Westphalia wind_speed=12.44,wind_direction=339i\n\
            wind_data,city=Frankfurt,country=Germany,county=Hesse wind_speed=18.97,wind_direction=96i\n\
            wind_data,city=Stuttgart,country=Germany,county=Baden-Württemberg wind_speed=12.75,wind_direction=332i\n\
            wind_data,city=Dortmund,country=Germany,county=North\\ Rhine-Westphalia wind_speed=12.03,wind_direction=146i\n\
            wind_data,city=Paris,country=France,county=Île-de-France wind_speed=10.3,wind_direction=302i\n\
            wind_data,city=Marseille,country=France,county=Provence-Alpes-Côte\\ d'Azur wind_speed=24.65,wind_direction=288i\n\
            wind_data,city=Lyon,country=France,county=Auvergne-Rhône-Alpes wind_speed=17.83,wind_direction=288i\n\
            wind_data,city=Toulouse,country=France,county=Occitanie wind_speed=20.34,wind_direction=157i\n\
            wind_data,city=Madrid,country=Spain,county=Community\\ of\\ Madrid wind_speed=9.36,wind_direction=348i\n\
            wind_data,city=Barcelona,country=Spain,county=Catalonia wind_speed=16.52,wind_direction=14i\n\
            ", 100).await;
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 100, 1),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        distinct_provider.write_wal_contents_to_cache(&wal_contents);

        let ctx = SessionContext::new();
        let distinct_func =
            DistinctCacheFunction::new(writer.db_schema().id, Arc::clone(&distinct_provider));
        ctx.register_udtf(DISTINCT_CACHE_UDTF_NAME, Arc::new(distinct_func));

        let results = ctx
            .sql("select country, city from distinct_cache('wind_data') where country = 'Spain'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+---------+-----------+",
                "| country | city      |",
                "+---------+-----------+",
                "| Spain   | Barcelona |",
                "| Spain   | Madrid    |",
                "+---------+-----------+",
            ],
            &results
        );
    }

    // NB: This test was added as part of https://github.com/influxdata/influxdb/issues/25564
    // If we choose to support nulls in the distinct cache then this test will fail
    #[test_log::test(tokio::test)]
    async fn test_row_with_nulls_ignored() {
        let writer = TestWriter::new().await;
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cat = writer.catalog();
        cat.create_table(
            TestWriter::DB_NAME,
            "bar",
            &["t1", "t2", "t3"],
            &[("f1", FieldDataType::Float)],
        )
        .await
        .unwrap();

        cat.create_distinct_cache(
            TestWriter::DB_NAME,
            "bar",
            Some("cache_money"),
            &["t1", "t2", "t3"],
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();

        let distinct_cache_provider = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider) as _,
            Arc::clone(&cat),
        )
        .await
        .unwrap();

        let write_batch = writer
            .write_lp_to_write_batch(
                "\
            bar,t1=A,t2=A,t3=A f1=1\n\
            bar,t1=A,t2=A,t3=B f1=2\n\
            bar,t1=A,t2=B,t3=B f1=3\n\
            bar,t1=B,t2=A f1=3\n\
            bar,t1=B,t3=B f1=3\n\
            bar,t2=B,t3=B f1=3\n\
            ",
                100,
            )
            .await;
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 100, 1),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        distinct_cache_provider.write_wal_contents_to_cache(&wal_contents);

        let ctx = SessionContext::new();
        let distinct_func =
            DistinctCacheFunction::new(writer.db_schema().id, Arc::clone(&distinct_cache_provider));
        ctx.register_udtf(DISTINCT_CACHE_UDTF_NAME, Arc::new(distinct_func));

        let results = ctx
            .sql("select * from distinct_cache('bar')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----+----+----+",
                "| t1 | t2 | t3 |",
                "+----+----+----+",
                "| A  | A  | A  |",
                "| A  | A  | B  |",
                "| A  | B  | B  |",
                "+----+----+----+",
            ],
            &results
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_distinct_with_where_clause_bug() {
        let writer = TestWriter::new().await;
        writer
            .catalog()
            .create_table(
                TestWriter::DB_NAME,
                "bar",
                &["t1", "t2", "t3"],
                &[("f1", FieldDataType::Float), ("f2", FieldDataType::Float)],
            )
            .await
            .unwrap();
        writer
            .catalog()
            .create_distinct_cache(
                TestWriter::DB_NAME,
                "bar",
                Some("foo"),
                &["t1", "t2", "t3"],
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();

        let dvc = DistinctCacheProvider::new_from_catalog(
            writer.catalog().time_provider(),
            writer.catalog(),
        )
        .await
        .unwrap();

        let write_batch = writer
            .write_lp_to_write_batch(
                "\
            bar,t1=AA,t2=\"(1\\,\\ 2]\",t3=None f1=-0.0621216848940003,f2=160.75626873391323\n\
            bar,t1=BB,t2=\"(0\\,\\ 1]\",t3=\"(2.0\\,\\ 4.0]\" f1=0.0183506941911869,f2=60.72371267622072\n\
            ",
                100,
            )
            .await;
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 100, 1),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        dvc.write_wal_contents_to_cache(&wal_contents);

        let ctx = SessionContext::new();
        let distinct_func = DistinctCacheFunction::new(writer.db_schema().id, Arc::clone(&dvc));
        ctx.register_udtf(DISTINCT_CACHE_UDTF_NAME, Arc::new(distinct_func));

        // should be able to do a basic query to distinct cache:
        let results = ctx
            .sql("select * from distinct_cache('bar', 'foo')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----+----------+--------------+",
                "| t1 | t2       | t3           |",
                "+----+----------+--------------+",
                "| AA | \"(1, 2]\" | None         |",
                "| BB | \"(0, 1]\" | \"(2.0, 4.0]\" |",
                "+----+----------+--------------+",
            ],
            &results
        );

        // should be able to query with a WHERE clause:
        let results = ctx
            .sql("select * from distinct_cache('bar', 'foo') where t1 = 'BB'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----+----------+--------------+",
                "| t1 | t2       | t3           |",
                "+----+----------+--------------+",
                "| BB | \"(0, 1]\" | \"(2.0, 4.0]\" |",
                "+----+----------+--------------+",
            ],
            &results
        );

        // should be able to query with a projection:
        let results = ctx
            .sql("select t2 from distinct_cache('bar', 'foo')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----------+",
                "| t2       |",
                "+----------+",
                "| \"(1, 2]\" |",
                "| \"(0, 1]\" |",
                "+----------+",
            ],
            &results
        );

        // should be able to query with projection and a WHERE clause:
        let results = ctx
            .sql("select t2 from distinct_cache('bar', 'foo') where t1 = 'BB'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----------+",
                "| t2       |",
                "+----------+",
                "| \"(0, 1]\" |",
                "+----------+",
            ],
            &results
        );

        // should be able to query with projection and a WHERE clause:
        let results = ctx
            .sql("select t2, t3 from distinct_cache('bar', 'foo') where t1 = 'BB'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            [
                "+----------+--------------+",
                "| t2       | t3           |",
                "+----------+--------------+",
                "| \"(0, 1]\" | \"(2.0, 4.0]\" |",
                "+----------+--------------+",
            ],
            &results
        );
    }
}
