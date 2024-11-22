//! The Metadata Cache holds the distinct values for a column or set of columns on a table

mod cache;
pub use cache::{CreateMetaCacheArgs, MaxAge, MaxCardinality};
mod provider;
pub use provider::{MetaCacheProvider, ProviderError};
mod table_function;
pub use table_function::MetaCacheFunction;
pub use table_function::META_CACHE_UDTF_NAME;

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq, prelude::SessionContext};
    use indexmap::IndexMap;
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
    use influxdb3_id::ColumnId;
    use influxdb3_wal::{Gen1Duration, Row, WriteBatch};
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use iox_time::{MockProvider, Time, TimeProvider};
    use std::{sync::Arc, time::Duration};

    use crate::meta_cache::{
        cache::{CreateMetaCacheArgs, MaxAge, MaxCardinality, MetaCache, Predicate},
        MetaCacheFunction, MetaCacheProvider, META_CACHE_UDTF_NAME,
    };

    struct TestWriter {
        catalog: Arc<Catalog>,
    }

    impl TestWriter {
        const DB_NAME: &str = "test_db";

        fn new() -> Self {
            Self {
                catalog: Arc::new(Catalog::new("test-host".into(), "test-instance".into())),
            }
        }

        fn write_lp_to_rows(&self, lp: impl AsRef<str>, time_ns: i64) -> Vec<Row> {
            let lines_parsed = WriteValidator::initialize(
                Self::DB_NAME.try_into().unwrap(),
                Arc::clone(&self.catalog),
                time_ns,
            )
            .expect("initialize write validator")
            .v1_parse_lines_and_update_schema(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(time_ns),
                influxdb3_write::Precision::Nanosecond,
            )
            .expect("parse and validate v1 line protocol");

            lines_parsed.into_inner().to_rows()
        }

        fn write_lp_to_write_batch(&self, lp: impl AsRef<str>, time_ns: i64) -> WriteBatch {
            WriteValidator::initialize(
                Self::DB_NAME.try_into().unwrap(),
                Arc::clone(&self.catalog),
                time_ns,
            )
            .expect("initialize write validator")
            .v1_parse_lines_and_update_schema(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(time_ns),
                influxdb3_write::Precision::Nanosecond,
            )
            .expect("parse and validate v1 line protocol")
            .convert_lines_to_buffer(Gen1Duration::new_1m())
            .into()
        }

        fn catalog(&self) -> Arc<Catalog> {
            Arc::clone(&self.catalog)
        }

        fn db_schema(&self) -> Arc<DatabaseSchema> {
            self.catalog
                .db_schema(Self::DB_NAME)
                .expect("db schema should be initialized")
        }
    }

    #[test]
    fn evaluate_predicates() {
        let writer = TestWriter::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to get a set of rows destined for the WAL, and an updated catalog:
        let rows = writer.write_lp_to_rows(
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
        );
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
        let mut cache = MetaCache::new(
            time_provider,
            CreateMetaCacheArgs {
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
                .to_record_batch(&tc.predicates)
                .expect("get record batches");
            println!("{}", tc.desc);
            assert_batches_sorted_eq!(tc.expected, &[records]);
        }
    }

    #[test]
    fn cache_pruning() {
        let writer = TestWriter::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // write some data to update the catalog:
        let _ = writer.write_lp_to_rows(
            "\
            cpu,region=us-east,host=a usage=100\n\
            ",
            time_provider.now().timestamp_nanos(),
        );
        // grab the table definition for the table written to:
        let table_def = writer.db_schema().table_definition("cpu").unwrap();
        // use the two tags, in order, to create the cache:
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        // create a cache with some cardinality and age limits:
        let mut cache = MetaCache::new(
            Arc::clone(&time_provider) as _,
            CreateMetaCacheArgs {
                table_def,
                max_cardinality: MaxCardinality::try_from(10).unwrap(),
                max_age: MaxAge::from(Duration::from_nanos(100)),
                column_ids,
            },
        )
        .expect("create cache");
        // push a bunch of rows with incrementing times and varying tag values:
        (0..10).for_each(|mult| {
            time_provider.set(Time::from_timestamp_nanos(mult * 20));
            let rows = writer.write_lp_to_rows(
                format!(
                    "\
                    cpu,region=us-east-{mult},host=a-{mult} usage=100\n\
                    cpu,region=us-west-{mult},host=b-{mult} usage=100\n\
                    cpu,region=us-cent-{mult},host=c-{mult} usage=100\n\
                    "
                ),
                time_provider.now().timestamp_nanos(),
            );
            // push the initial row data into the cache:
            for row in rows {
                cache.push(&row);
            }
        });
        // check the cache before prune:
        // NOTE: this does not include entries that have surpassed the max_age of the cache, though,
        // there are still more than the cache's max cardinality, as it has not yet been pruned.
        let records = cache.to_record_batch(&Default::default()).unwrap();
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
        let records = cache.to_record_batch(&Default::default()).unwrap();
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

    /// This test sets up a [`MetaCacheProvider`], creates a [`MetaCache`] using the `region` and
    /// `host` column, and then writes several different unique combinations of values into it.
    /// It then sets up a DataFusion [`SessionContext`], registers our [`MetaCacheFunction`] as a
    /// UDTF, and then runs a series of test cases to verify queries against the function.
    ///
    /// The purpose of this is to see that the cache works as intended, and importantly, that the
    /// predicate pushdown is happening and being leveraged by the underlying [`MetaCache`], vs.
    /// DataFusion doing it for us with a higher level FilterExec.
    ///
    /// Each test case verifies the `RecordBatch` output of the query as well as the output for
    /// EXPLAIN on the same query. The EXPLAIN output contains a line for the MetaCacheExec, which
    /// is the custom execution plan impl for the metadata cache that captures the predicates that
    /// are pushed down to the underlying [`MetaCacahe::to_record_batch`] method, if any.
    #[tokio::test]
    async fn test_datafusion_meta_cache_udtf() {
        // create a test writer and do a write in to populate the catalog with a db/table:
        let writer = TestWriter::new();
        let _ = writer.write_lp_to_write_batch(
            "\
            cpu,region=us-east,host=a usage=100\n\
            ",
            0,
        );

        // create a meta provider and a cache on tag columns 'region' and 'host':
        let db_schema = writer.db_schema();
        let table_def = db_schema.table_definition("cpu").unwrap();
        let column_ids: Vec<ColumnId> = ["region", "host"]
            .into_iter()
            .map(|name| table_def.column_name_to_id_unchecked(name))
            .collect();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let meta_provider =
            MetaCacheProvider::new_from_catalog(time_provider, writer.catalog()).unwrap();
        meta_provider
            .create_cache(
                db_schema.id,
                None,
                CreateMetaCacheArgs {
                    table_def,
                    max_cardinality: MaxCardinality::default(),
                    max_age: MaxAge::default(),
                    column_ids,
                },
            )
            .unwrap();

        // do some writes to generate a write batch and send it into the cache:
        let write_batch = writer.write_lp_to_write_batch(
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
        );
        let wal_contents = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::write_batch_op(write_batch)],
        );
        meta_provider.write_wal_contents_to_cache(&wal_contents);

        // Spin up a DataFusion session context and add the meta_cache function to it so we
        // can query for data in the cache we created and populated above:
        let ctx = SessionContext::new();
        let meta_func = MetaCacheFunction::new(db_schema.id, Arc::clone(&meta_provider));
        ctx.register_udtf(META_CACHE_UDTF_NAME, Arc::new(meta_func));

        struct TestCase<'a> {
            /// A short description of the test
            _desc: &'a str,
            /// A SQL expression to evaluate using the datafusion session context, should be of
            /// the form:
            /// ```sql
            /// SELECT * FROM meta_cache('cpu') ...
            /// ```
            sql: &'a str,
            /// Expected record batch output
            expected: &'a [&'a str],
            /// Expected EXPLAIN output contains this.
            ///
            /// For checking the `MetaCacheExec` portion of the EXPLAIN output for the given `sql`
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
            /// altered by DataFusion, e.g., `SELECT DISTINCT(column_name) FROM meta_cache('table')`
            /// or queries that project columns that are not at the top level of the cache.
            use_sorted_assert: bool,
        }

        let test_cases = [
            TestCase {
                _desc: "no predicates",
                sql: "SELECT * FROM meta_cache('cpu')",
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region",
                sql: "SELECT * FROM meta_cache('cpu') WHERE region = 'us-east'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[0 IN (us-east)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region and host",
                sql: "SELECT * FROM meta_cache('cpu') \
                    WHERE region = 'us-east' AND host = 'a'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[0 IN (us-east)], [1 IN (a)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region; in predicate on host",
                sql: "SELECT * FROM meta_cache('cpu') \
                    WHERE region = 'us-east' AND host IN ('a', 'b')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[0 IN (us-east)], [1 IN (a,b)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "eq predicate on region; not in predicate on host",
                sql: "SELECT * FROM meta_cache('cpu') \
                        WHERE region = 'us-east' AND host != 'a'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[0 IN (us-east)], [1 NOT IN (a)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "in predicate on region",
                sql: "SELECT * FROM meta_cache('cpu') \
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
                explain_contains: "MetaCacheExec: predicates=[[0 IN (ca-cent,ca-east,us-east,us-west)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "not in predicate on region",
                sql: "SELECT * FROM meta_cache('cpu') \
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
                explain_contains: "MetaCacheExec: predicates=[[0 NOT IN (ca-cent,ca-east,us-east,us-west)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "or eq predicates on region",
                sql: "SELECT * FROM meta_cache('cpu') \
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
                explain_contains: "MetaCacheExec: predicates=[[0 IN (ca-east,us-east)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "or eq predicate on host",
                sql: "SELECT * FROM meta_cache('cpu') \
                    WHERE host = 'd' OR host = 'e'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| ca-east | e    |",
                    "| us-west | d    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[1 IN (d,e)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "un-grouped host conditions are not handled in predicate pushdown",
                sql: "SELECT * FROM meta_cache('cpu') \
                    WHERE region = 'us-east' AND host = 'a' OR host = 'b'",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "grouped host conditions are handled in predicate pushdown",
                sql: "SELECT * FROM meta_cache('cpu') \
                    WHERE region = 'us-east' AND (host = 'a' OR host = 'b')",
                expected: &[
                    "+---------+------+",
                    "| region  | host |",
                    "+---------+------+",
                    "| us-east | a    |",
                    "| us-east | b    |",
                    "+---------+------+",
                ],
                explain_contains: "MetaCacheExec: predicates=[[0 IN (us-east)], [1 IN (a,b)]] inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "project region column",
                sql: "SELECT region FROM meta_cache('cpu')",
                expected: &[
                    "+---------+",
                    "| region  |",
                    "+---------+",
                    "| ca-cent |",
                    "| ca-east |",
                    "| ca-west |",
                    "| ca-west |",
                    "| eu-cent |",
                    "| eu-cent |",
                    "| eu-west |",
                    "| eu-west |",
                    "| us-east |",
                    "| us-east |",
                    "| us-west |",
                    "| us-west |",
                    "+---------+",
                ],
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "project region column taking distinct",
                sql: "SELECT DISTINCT(region) FROM meta_cache('cpu')",
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                // it seems that DISTINCT changes around the order of results
                use_sorted_assert: true,
            },
            TestCase {
                _desc: "project host column",
                sql: "SELECT host FROM meta_cache('cpu')",
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                // this column will not be sorted since the order of elements depends on the next level
                // up in the cache, so the `region` column is iterated over in order, but the nested
                // `host` values, although sorted within `region`s, will not be globally sorted.
                use_sorted_assert: true,
            },
            TestCase {
                _desc: "limit clause",
                sql: "SELECT * FROM meta_cache('cpu') LIMIT 8",
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "limit and offset",
                sql: "SELECT * FROM meta_cache('cpu') LIMIT 8 OFFSET 8",
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
            TestCase {
                _desc: "like clause",
                sql: "SELECT * FROM meta_cache('cpu') \
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
                explain_contains: "MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]",
                use_sorted_assert: false,
            },
        ];

        for tc in test_cases {
            let results = ctx.sql(tc.sql).await.unwrap().collect().await.unwrap();
            // Uncommenting may make figuring out which test case is failing easier:
            println!("test case: {}", tc._desc);
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
            // the details of the MetaCacheExec is called `plan`...
            assert!(explain
                .column_by_name("plan")
                .unwrap()
                .as_string::<i32>()
                .iter()
                .any(|plan| plan.is_some_and(|plan| plan.contains(tc.explain_contains))),);
        }
    }
}
