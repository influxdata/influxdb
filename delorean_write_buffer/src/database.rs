use delorean_line_parser::ParsedLine;
use delorean_storage::{Database, Predicate, TimestampRange};
use delorean_wal::WalBuilder;
use delorean_wal_writer::{start_wal_sync_task, Error as WalWriterError, WalDetails};

use crate::column::Column;
use crate::partition::Partition;

use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use delorean_arrow::{
    arrow,
    arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch},
    datafusion::prelude::ExecutionConfig,
    datafusion::{
        datasource::MemTable, error::ExecutionError, execution::context::ExecutionContext,
    },
};

use crate::dictionary::Error as DictionaryError;
use crate::partition::restore_partitions_from_wal;

use crate::wal::WalEntryBuilder;

use async_trait::async_trait;
use chrono::{offset::TimeZone, Utc};
use snafu::{OptionExt, ResultExt, Snafu};
use sqlparser::{
    ast::{SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Dir {:?} invalid for DB", dir))]
    OpenDb { dir: PathBuf },

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    OpeningWal {
        database: String,
        source: WalWriterError,
    },

    #[snafu(display("Error writing to WAL for database {}: {}", database, source))]
    WritingWal {
        database: String,
        source: WalWriterError,
    },

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    LoadingWal {
        database: String,
        source: delorean_wal::Error,
    },

    #[snafu(display("Error recovering WAL for database {}: {}", database, source))]
    WalRecoverError {
        database: String,
        source: crate::partition::Error,
    },

    #[snafu(display("Error recovering WAL for partition {} on table {}", partition, table))]
    WalPartitionError { partition: u32, table: String },

    #[snafu(display("Error recovering write from WAL, column id {} not found", column_id))]
    WalColumnError { column_id: u16 },

    #[snafu(display("Error creating db dir for {}: {}", database, err))]
    CreatingWalDir {
        database: String,
        err: std::io::Error,
    },

    #[snafu(display("Database {} doesn't exist", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("Partition {} is full", partition))]
    PartitionFull { partition: u32 },

    #[snafu(display("Error in partition: {}", source))]
    PartitionError { source: crate::partition::Error },

    #[snafu(display("Error in table: {}", source))]
    TableError { source: crate::table::Error },

    #[snafu(display(
        "Table name {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableNameNotFoundInDictionary {
        table: String,
        partition: u32,
        source: DictionaryError,
    },

    #[snafu(display(
        "Table ID {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableIdNotFoundInDictionary {
        table: u32,
        partition: u32,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column ID {} not found in dictionary of partition {}",
        column,
        partition
    ))]
    ColumnIdNotFoundInDictionary {
        column: u32,
        partition: u32,
        source: DictionaryError,
    },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: u32 },

    #[snafu(display("Internal Error: Column {} not found", column))]
    InternalColumnNotFound { column: u32 },

    #[snafu(display("Unexpected insert error"))]
    InsertError,

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("id conversion error"))]
    IdConversionError { source: std::num::TryFromIntError },

    #[snafu(display("Invalid sql query: {} : {}", query, source))]
    InvalidSqlQuery {
        query: String,
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("error executing query {}: {}", query, source))]
    QueryError {
        query: String,
        source: ExecutionError,
    },

    #[snafu(display("Unsupported SQL statement in query {}: {}", query, statement))]
    UnsupportedStatement {
        query: String,
        statement: Box<Statement>,
    },

    #[snafu(display("query error {} on query {}", message, query))]
    GenericQueryError { message: String, query: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Db {
    pub name: String,
    // TODO: partitions need to be wrapped in an Arc if they're going to be used without this lock
    partitions: RwLock<Vec<Partition>>,
    next_partition_generation: AtomicU32,
    wal_details: Option<WalDetails>,
    dir: PathBuf,
}

impl Db {
    /// Create a new DB that will create and use the Write Ahead Log
    /// (WAL) directory `wal_dir`
    pub async fn try_with_wal(name: &str, wal_dir: &mut PathBuf) -> Result<Self> {
        wal_dir.push(&name);
        if let Err(e) = std::fs::create_dir(wal_dir.clone()) {
            match e.kind() {
                ErrorKind::AlreadyExists => (),
                _ => {
                    return CreatingWalDir {
                        database: name,
                        err: e,
                    }
                    .fail()
                }
            }
        }
        let dir = wal_dir.clone();
        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder)
            .await
            .context(OpeningWal { database: name })?;
        wal_details
            .write_metadata()
            .await
            .context(OpeningWal { database: name })?;

        Ok(Self {
            name: name.to_string(),
            dir,
            partitions: RwLock::new(vec![]),
            next_partition_generation: AtomicU32::new(1),
            wal_details: Some(wal_details),
        })
    }

    /// Create a new DB and initially restore pre-existing data in the
    /// Write Ahead Log (WAL) directory `wal_dir`
    pub async fn restore_from_wal(wal_dir: PathBuf) -> Result<Self> {
        let now = std::time::Instant::now();
        let name = wal_dir
            .iter()
            .last()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_str()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_string();

        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder.clone())
            .await
            .context(OpeningWal { database: &name })?;

        // TODO: check wal metadata format
        let entries = wal_builder
            .entries()
            .context(LoadingWal { database: &name })?;

        let (partitions, max_partition_generation, stats) =
            restore_partitions_from_wal(entries).context(WalRecoverError { database: &name })?;

        let elapsed = now.elapsed();
        info!(
            "{} database loaded {} rows in {:?} in {} tables",
            &name,
            stats.row_count,
            elapsed,
            stats.tables.len(),
        );

        info!(
            "{} database partition count: {}, max generation: {}",
            &name,
            partitions.len(),
            max_partition_generation
        );

        Ok(Self {
            name,
            dir: wal_dir,
            partitions: RwLock::new(partitions),
            next_partition_generation: AtomicU32::new(max_partition_generation + 1),
            wal_details: Some(wal_details),
        })
    }
}

#[async_trait]
impl Database for Db {
    type Error = Error;

    // TODO: writes lines creates a column named "time" for the timestmap data. If
    //       we keep this we need to validate that no tag or field has the same name.
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
        let mut partitions = self.partitions.write().await;

        let mut builder = self.wal_details.as_ref().map(|_| WalEntryBuilder::new());

        // TODO: rollback writes to partitions on validation failures
        for line in lines {
            let key = self.partition_key(line);
            // TODO: could this be a hashmap lookup instead of iteration, or no because of the
            // way partitioning rules might work?
            // TODO: or could we group lines by key and share the results of looking up the
            // partition? or could that make insertion go in an undesired order?
            match partitions.iter_mut().find(|p| p.should_write(&key)) {
                Some(p) => p.write_line(line, &mut builder).context(PartitionError)?,
                None => {
                    let generation = self
                        .next_partition_generation
                        .fetch_add(1, Ordering::Relaxed);

                    if let Some(builder) = &mut builder {
                        builder.add_partition_open(generation, &key);
                    }

                    let mut p = Partition::new(generation, key);
                    p.write_line(line, &mut builder).context(PartitionError)?;
                    partitions.push(p)
                }
            }
        }

        if let Some(wal) = &self.wal_details {
            let data = builder
                .expect("Where there's wal_details, there's a builder")
                .data();

            wal.write_and_sync(data).await.context(WritingWal {
                database: self.name.clone(),
            })?;
        }

        Ok(())
    }

    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error> {
        // TODO: Cache this information to avoid creating this each time
        let partitions = self.partitions.read().await;
        let mut table_names: BTreeSet<String> = BTreeSet::new();
        for partition in partitions.iter() {
            let timestamp_predicate = partition
                .make_timestamp_predicate(range)
                .context(PartitionError)?;
            for (table_name_symbol, table) in &partition.tables {
                if table
                    .matches_timestamp_predicate(&timestamp_predicate)
                    .context(TableError)?
                {
                    let table_name = partition.dictionary.lookup_id(*table_name_symbol).context(
                        TableIdNotFoundInDictionary {
                            table: *table_name_symbol,
                            partition: partition.generation,
                        },
                    )?;

                    if !table_names.contains(table_name) {
                        table_names.insert(table_name.to_string());
                    }
                }
            }
        }
        Ok(Arc::new(table_names))
    }

    // return all column names in this database, while applying optional predicates
    async fn tag_column_names(
        &self,
        table: Option<String>,
        range: Option<TimestampRange>,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error> {
        let partitions = self.partitions.read().await;

        let mut column_names = BTreeSet::new();

        for partition in partitions.iter() {
            // ids are relative to a specific partition
            let table_symbol = match &table {
                Some(name) => Some(partition.dictionary.lookup_value(&name).context(
                    TableNameNotFoundInDictionary {
                        table: name,
                        partition: partition.generation,
                    },
                )?),
                None => None,
            };

            let timestamp_predicate = partition
                .make_timestamp_predicate(range)
                .context(PartitionError)?;

            // Find all columns ids in all partition's tables so that we
            // can look up id --> String conversion once
            let mut partition_column_ids = BTreeSet::new();

            let table_iter = partition
                .tables
                .values()
                // filter out any tables that don't pass the predicates
                .filter(|table| table.matches_id_predicate(&table_symbol));

            for table in table_iter {
                for (column_id, column_index) in &table.column_id_to_index {
                    if let Column::Tag(col) = &table.columns[*column_index] {
                        if table
                            .column_matches_timestamp_predicate(col, &timestamp_predicate)
                            .context(TableError)?
                        {
                            partition_column_ids.insert(column_id);
                        }
                    }
                }
            }

            // convert all the partition's column_ids to Strings
            for column_id in partition_column_ids {
                let column_name = partition.dictionary.lookup_id(*column_id).context(
                    ColumnIdNotFoundInDictionary {
                        column: *column_id,
                        partition: partition.generation,
                    },
                )?;

                if !column_names.contains(column_name) {
                    column_names.insert(column_name.to_string());
                }
            }
        } // next partition

        Ok(Arc::new(column_names))
    }

    // return all column names with a predicate
    async fn tag_column_names_with_predicate(
        &self,
        _table: Option<String>,
        _range: Option<TimestampRange>,
        _predicate: Predicate,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error> {
        unimplemented!("tag_column_names_with_predicate in WriteBufferDatabase");
    }

    async fn table_to_arrow(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error> {
        let partitions = self.partitions.read().await;

        partitions
            .iter()
            .map(|p| {
                p.table_to_arrow(table_name, columns)
                    .context(PartitionError)
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
        let mut tables = vec![];

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, query).context(InvalidSqlQuery { query })?;

        for statement in ast {
            match statement {
                Statement::Query(q) => {
                    if let SetExpr::Select(q) = q.body {
                        for item in q.from {
                            if let TableFactor::Table { name, .. } = item.relation {
                                let name = name.to_string();
                                let data = self.table_to_arrow(&name, &[]).await?;
                                tables.push(ArrowTable {
                                    name,
                                    schema: data[0].schema().clone(),
                                    data,
                                });
                            }
                        }
                    }
                }
                _ => {
                    return UnsupportedStatement {
                        query: query.to_string(),
                        statement,
                    }
                    .fail()
                }
            }
        }

        let config = ExecutionConfig::new().with_batch_size(1024 * 1024);
        let mut ctx = ExecutionContext::with_config(config);

        for table in tables {
            let provider =
                MemTable::new(table.schema, vec![table.data]).context(QueryError { query })?;
            ctx.register_table(&table.name, Box::new(provider));
        }

        let plan = ctx
            .create_logical_plan(&query)
            .context(QueryError { query })?;
        let plan = ctx.optimize(&plan).context(QueryError { query })?;
        let plan = ctx
            .create_physical_plan(&plan)
            .context(QueryError { query })?;

        ctx.collect(plan).context(QueryError { query })
    }
}

impl Db {
    // partition_key returns the partition key for the given line. The key will be the prefix of a
    // partition name (multiple partitions can exist for each key). It uses the user defined
    // partitioning rules to construct this key
    fn partition_key(&self, line: &ParsedLine<'_>) -> String {
        // TODO - wire this up to use partitioning rules, for now just partition by day
        let ts = line.timestamp.unwrap();
        let dt = Utc.timestamp_nanos(ts);
        dt.format("%Y-%m-%dT%H").to_string()
    }
}

struct ArrowTable {
    name: String,
    schema: Arc<ArrowSchema>,
    data: Vec<RecordBatch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use delorean_storage::{Database, TimestampRange};

    use arrow::{
        array::{Array, StringArray, StringArrayOps},
        util::pretty::pretty_format_batches,
    };
    use delorean_line_parser::parse_lines;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
    }

    // Abstract a bit of boilerplate around table assertions and improve the failure output.
    // The default failure message uses Debug formatting, which prints newlines as `\n`.
    // This prints the pretty_format_batches using Display so it's easier to read the tables.
    fn assert_table_eq(table: &str, partitions: &[arrow::record_batch::RecordBatch]) {
        let res = pretty_format_batches(partitions).unwrap();
        assert_eq!(table, res, "\n\nleft:\n\n{}\nright:\n\n{}", table, res);
    }

    #[tokio::test(threaded_scheduler)]
    async fn list_table_names() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // no tables initially
        assert_eq!(*db.table_names(None).await?, BTreeSet::new());

        // write two different tables
        let lines: Vec<_> =
            parse_lines("cpu,region=west user=23.2 10\ndisk,region=east bytes=99i 11")
                .map(|l| l.unwrap())
                .collect();
        db.write_lines(&lines).await?;

        // Now, we should see the two tables
        assert_eq!(*db.table_names(None).await?, to_set(&["cpu", "disk"]));

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn list_table_names_timestamps() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // write two different tables at the following times:
        // cpu: 100 and 150
        // disk: 200
        let lines: Vec<_> =
            parse_lines("cpu,region=west user=23.2 100\ncpu,region=west user=21.0 150\ndisk,region=east bytes=99i 200")
                .map(|l| l.unwrap())
                .collect();
        db.write_lines(&lines).await?;

        // Cover all times
        let range = Some(TimestampRange { start: 0, end: 201 });
        assert_eq!(*db.table_names(range).await?, to_set(&["cpu", "disk"]));

        // Right before disk
        let range = Some(TimestampRange { start: 0, end: 200 });
        assert_eq!(*db.table_names(range).await?, to_set(&["cpu"]));

        // only one point of cpu
        let range = Some(TimestampRange {
            start: 50,
            end: 101,
        });
        assert_eq!(*db.table_names(range).await?, to_set(&["cpu"]));

        // no ranges
        let range = Some(TimestampRange {
            start: 250,
            end: 350,
        });
        assert_eq!(*db.table_names(range).await?, to_set(&[]));

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn missing_tags_are_null() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // Note the `region` tag is introduced in the second line, so
        // the values in prior rows for the region column are
        // null. Likewise the `core` tag is introduced in the third
        // line so the prior columns are null
        let lines: Vec<_> = parse_lines(
            "cpu,region=west user=23.2 10\n\
                         cpu, user=10.0 11\n\
                         cpu,core=one user=10.0 11\n",
        )
        .map(|l| l.unwrap())
        .collect();
        db.write_lines(&lines).await?;

        let partitions = db.table_to_arrow("cpu", &["region", "core"]).await?;
        let columns = partitions[0].columns();

        assert_eq!(
            2,
            columns.len(),
            "Got only two columns in partiton: {:#?}",
            columns
        );

        let region_col = columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get region column as a string");

        assert_eq!(region_col.len(), 3);
        assert_eq!(region_col.value(0), "west", "region_col: {:?}", region_col);
        assert!(!region_col.is_null(0), "is_null(0): {:?}", region_col);
        assert!(region_col.is_null(1), "is_null(1): {:?}", region_col);
        assert!(region_col.is_null(2), "is_null(1): {:?}", region_col);

        let host_col = columns[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get host column as a string");

        assert_eq!(host_col.len(), 3);
        assert!(host_col.is_null(0), "is_null(0): {:?}", host_col);
        assert!(host_col.is_null(1), "is_null(1): {:?}", host_col);
        assert!(!host_col.is_null(2), "is_null(2): {:?}", host_col);
        assert_eq!(host_col.value(2), "one", "host_col: {:?}", host_col);

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn write_data_and_recover() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+-------+------+---------+-----------+
| region | host | user | other | str         | b     | time | new_tag | new_field |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true  | 10   |         | 0         |
| west   | B    | 23.1 | 0     |             | false | 15   |         | 0         |
|        | A    | 0    | 0     |             | false | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
"#;
        let expected_mem_table = r#"+--------+------+-------+------+
| region | host | val   | time |
+--------+------+-------+------+
| east   | C    | 23432 | 10   |
+--------+------+-------+------+
"#;
        let expected_disk_table = r#"+--------+------+----------+--------------+------+
| region | host | bytes    | used_percent | time |
+--------+------+----------+--------------+------+
| west   | A    | 23432323 | 76.2         | 10   |
+--------+------+----------+--------------+------+
"#;

        let cpu_columns = &[
            "region",
            "host",
            "user",
            "other",
            "str",
            "b",
            "time",
            "new_tag",
            "new_field",
        ];
        let mem_columns = &["region", "host", "val", "time"];
        let disk_columns = &["region", "host", "bytes", "used_percent", "time"];

        {
            let db = Db::try_with_wal("mydb", &mut dir).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i,str=\"some string\",b=true 10\ndisk,region=west,host=A bytes=23432323i,used_percent=76.2 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=B user=23.1 15")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,host=A,new_tag=foo new_field=15.1 20")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("mem,region=east,host=C val=23432 10")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        // check that it recovers from the wal
        {
            let db = Db::restore_from_wal(dir).await?;

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn recover_partial_entries() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+-------+------+---------+-----------+
| region | host | user | other | str         | b     | time | new_tag | new_field |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true  | 10   |         | 0         |
| west   | B    | 23.1 | 0     |             | false | 15   |         | 0         |
|        | A    | 0    | 0     |             | false | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
"#;
        let expected_mem_table = r#"+--------+------+-------+------+
| region | host | val   | time |
+--------+------+-------+------+
| east   | C    | 23432 | 10   |
+--------+------+-------+------+
"#;
        let expected_disk_table = r#"+--------+------+----------+--------------+------+
| region | host | bytes    | used_percent | time |
+--------+------+----------+--------------+------+
| west   | A    | 23432323 | 76.2         | 10   |
+--------+------+----------+--------------+------+
"#;

        let cpu_columns = &[
            "region",
            "host",
            "user",
            "other",
            "str",
            "b",
            "time",
            "new_tag",
            "new_field",
        ];
        let mem_columns = &["region", "host", "val", "time"];
        let disk_columns = &["region", "host", "bytes", "used_percent", "time"];
        {
            let db = Db::try_with_wal("mydb", &mut dir).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i,str=\"some string\",b=true 10\ndisk,region=west,host=A bytes=23432323i,used_percent=76.2 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=B user=23.1 15")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,host=A,new_tag=foo new_field=15.1 20")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("mem,region=east,host=C val=23432 10")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        // check that it can recover from the last 2 self-describing entries of the wal
        {
            let name = dir.iter().last().unwrap().to_str().unwrap().to_string();

            let wal_builder = WalBuilder::new(&dir);

            let wal_entries = wal_builder
                .entries()
                .context(LoadingWal { database: &name })?;

            // Skip the first 2 entries in the wal; only restore from the last 2
            let wal_entries = wal_entries.skip(2);

            let (partitions, max_partition_generation, _stats) =
                restore_partitions_from_wal(wal_entries)?;

            let db = Db {
                name,
                dir,
                partitions: RwLock::new(partitions),
                next_partition_generation: AtomicU32::new(max_partition_generation + 1),
                wal_details: None,
            };

            // some cpu
            let smaller_cpu_table = r#"+------+---------+-----------+------+
| host | new_tag | new_field | time |
+------+---------+-----------+------+
| A    | foo     | 15.1      | 20   |
+------+---------+-----------+------+
"#;
            let smaller_cpu_columns = &["host", "new_tag", "new_field", "time"];
            let partitions = db.table_to_arrow("cpu", smaller_cpu_columns).await?;
            assert_table_eq(smaller_cpu_table, &partitions);

            // all of mem
            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            // no disk
            let nonexistent_table = db.table_to_arrow("disk", disk_columns).await;
            assert!(nonexistent_table.is_err());
            let actual_message = format!("{:?}", nonexistent_table);
            let expected_message = "TableNameNotFoundInDictionary";
            assert!(
                actual_message.contains(expected_message),
                "Did not find '{}' in '{}'",
                expected_message,
                actual_message
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn db_partition_key() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("mydb", &mut dir).await?;

        let partition_keys: Vec<_> = parse_lines(
            "\
cpu user=23.2 1600107710000000000
disk bytes=23432323i 1600136510000000000",
        )
        .map(|line| db.partition_key(&line.unwrap()))
        .collect();

        assert_eq!(partition_keys, vec!["2020-09-14T18", "2020-09-15T02"]);

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn list_column_names() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY,city=NYC,borough=Brooklyn temp=60.8 400\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        #[derive(Debug)]
        struct TestCase<'a> {
            measurement: Option<String>,
            range: Option<TimestampRange>,
            predicate: Option<Predicate>,
            expected_tag_keys: Result<Vec<&'a str>>,
        };

        let test_cases = vec![
            TestCase {
                measurement: None,
                range: None,
                predicate: None,
                expected_tag_keys: Ok(vec!["borough", "city", "county", "state"]),
            },
            TestCase {
                measurement: None,
                range: Some(TimestampRange::new(150, 201)),
                predicate: None,
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
            // TODO: figure out how to do a predicate like this:
            // -- Predicate: state="MA"
            // use node::{Value, Comparison};
            // let root = Node {
            //     value: Some(Value::Comparison(Comparison::Equal as i32)),
            //     children: vec![
            //         Node {
            //             value: Some(Value::TagRefValue("state".to_string())),
            //             children: vec![],
            //         },
            //         Node {
            //             value: Some(Value::StringValue("MA".to_string())),
            //             children: vec![],
            //         }
            //     ],
            // };

            // TODO predicates
            // TestCase {
            //     measurement: None,
            //     range: None,
            //     predicate: None, // TODO: state=MA
            //     expected_tag_keys: Ok(vec!["city", "county", "state"]),
            // },
            // TODO timestamp and predicate
            // TestCase {
            //     measurement: None,
            //     range: Some(TimestampRange::new(150, 201)),
            //     predicate: None, // TODO: state=MA
            //     expected_tag_keys: Ok(vec!["city", "state"]),
            // },
            // Cases with measurement names (restrict to o2)
            TestCase {
                measurement: Some("o2".to_string()),
                range: None,
                predicate: None,
                expected_tag_keys: Ok(vec!["borough", "city", "state"]),
            },
            TestCase {
                measurement: Some("o2".to_string()),
                range: Some(TimestampRange::new(150, 201)),
                predicate: None,
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
            // TODO predicates
            // TestCase {
            //     measurement: Some("o2".to_string()),
            //     range: None,
            //     predicate: None, // TODO: state=CA
            //     expected_tag_keys: Ok(vec!["city"]),
            // },
            // // TODO timestamp and predicate
            // TestCase {
            //     measurement: Some("o2".to_string()),
            //     range: Some(TimestampRange::new(150, 201)),
            //     predicate: None, // TODO: state=CA
            //     expected_tag_keys: Ok(vec!["city"]),
            // },
        ];

        for test_case in test_cases.into_iter() {
            let test_case_str = format!("{:#?}", test_case);
            println!("Running test case: {:?}", test_case);

            let actual_tag_keys = match test_case.predicate {
                Some(predicate) => db.tag_column_names_with_predicate(
                    test_case.measurement,
                    test_case.range,
                    predicate,
                ),
                None => db.tag_column_names(test_case.measurement, test_case.range),
            }
            .await;

            let is_match = if let Ok(expected_tag_keys) = &test_case.expected_tag_keys {
                let expected_tag_keys = to_set(expected_tag_keys);
                if let Ok(actual_tag_keys) = &actual_tag_keys {
                    **actual_tag_keys == expected_tag_keys
                } else {
                    false
                }
            } else if let Err(e) = &actual_tag_keys {
                // use string compare to compare errors to avoid having to build exact errors
                format!("{:?}", e) == format!("{:?}", test_case.expected_tag_keys)
            } else {
                false
            };

            assert!(
                is_match,
                "Mismatch\n\
                     actual_tag_keys: \n\
                     {:?}\n\
                     expected_tag_keys: \n\
                     {:?}\n\
                     Test_case: \n\
                     {}",
                actual_tag_keys, test_case.expected_tag_keys, test_case_str
            );
        }

        Ok(())
    }
}
