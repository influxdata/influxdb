//! A SQLite backed implementation of the Catalog

use crate::{
    interface::{
        self, sealed::TransactionFinalize, CasFailure, Catalog, ColumnRepo,
        ColumnTypeMismatchSnafu, Error, NamespaceRepo, ParquetFileRepo, PartitionRepo,
        ProcessedTombstoneRepo, QueryPoolRepo, RepoCollection, Result, ShardRepo, SoftDeletedRows,
        TableRepo, TombstoneRepo, TopicMetadataRepo, Transaction,
    },
    metrics::MetricDecorator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES, SHARED_TOPIC_ID, SHARED_TOPIC_NAME,
};
use async_trait::async_trait;
use data_types::{
    Column, ColumnId, ColumnSet, ColumnType, ColumnTypeCount, CompactionLevel, Namespace,
    NamespaceId, ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId,
    PartitionKey, PartitionParam, ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber,
    Shard, ShardId, ShardIndex, SkippedCompaction, Table, TableId, TablePartition, Timestamp,
    Tombstone, TombstoneId, TopicId, TopicMetadata, TRANSITION_SHARD_ID, TRANSITION_SHARD_INDEX,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{collections::HashMap, fmt::Display};

use iox_time::{SystemProvider, TimeProvider};
use metric::Registry;
use observability_deps::tracing::{debug, warn};
use parking_lot::Mutex;
use snafu::prelude::*;
use sqlx::types::Json;
use sqlx::{
    migrate::Migrator, sqlite::SqliteConnectOptions, types::Uuid, Executor, Pool, Row, Sqlite,
    SqlitePool,
};
use std::str::FromStr;
use std::sync::Arc;

static MIGRATOR: Migrator = sqlx::migrate!("sqlite/migrations");

/// Maximum number of files deleted by [`ParquetFileRepo::delete_old_ids_only].
const MAX_PARQUET_FILES_DELETED_ONCE: i64 = 1_000;

/// SQLite connection options.
#[derive(Debug, Clone)]
pub struct SqliteConnectionOptions {
    /// DSN.
    pub dsn: String,
}

/// SQLite catalog.
#[derive(Debug)]
pub struct SqliteCatalog {
    metrics: Arc<Registry>,
    pool: Pool<Sqlite>,
    time_provider: Arc<dyn TimeProvider>,
    options: SqliteConnectionOptions,
}

// struct to get return value from "select count(id) ..." query
#[derive(sqlx::FromRow)]
struct Count {
    count: i64,
}

/// transaction for [`SqliteCatalog`].
#[derive(Debug)]
pub struct SqliteTxn {
    inner: Mutex<SqliteTxnInner>,
    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum SqliteTxnInner {
    Txn(Option<sqlx::Transaction<'static, Sqlite>>),
    Oneshot(Pool<Sqlite>),
}

impl<'c> Executor<'c> for &'c mut SqliteTxnInner {
    type Database = Sqlite;

    #[allow(clippy::type_complexity)]
    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Self::Database as sqlx::Database>::QueryResult,
                <Self::Database as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            SqliteTxnInner::Txn(txn) => txn.as_mut().expect("Not yet finalized").fetch_many(query),
            SqliteTxnInner::Oneshot(pool) => pool.fetch_many(query),
        }
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: sqlx::Execute<'q, Self::Database>,
    {
        match self {
            SqliteTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .fetch_optional(query),
            SqliteTxnInner::Oneshot(pool) => pool.fetch_optional(query),
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Self::Database as sqlx::database::HasStatement<'q>>::Statement, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        match self {
            SqliteTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .prepare_with(sql, parameters),
            SqliteTxnInner::Oneshot(pool) => pool.prepare_with(sql, parameters),
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        match self {
            SqliteTxnInner::Txn(txn) => txn.as_mut().expect("Not yet finalized").describe(sql),
            SqliteTxnInner::Oneshot(pool) => pool.describe(sql),
        }
    }
}

impl Drop for SqliteTxn {
    fn drop(&mut self) {
        if let SqliteTxnInner::Txn(Some(_)) = self.inner.lock().deref() {
            warn!("Dropping SqliteTxn w/o finalizing (commit or abort)");

            // SQLx ensures that the inner transaction enqueues a rollback when it is dropped, so
            // we don't need to spawn a task here to call `rollback` manually.
        }
    }
}

#[async_trait]
impl TransactionFinalize for SqliteTxn {
    async fn commit_inplace(&mut self) -> Result<(), Error> {
        match self.inner.get_mut() {
            SqliteTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .commit()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            SqliteTxnInner::Oneshot(_) => {
                panic!("cannot commit oneshot");
            }
        }
    }

    async fn abort_inplace(&mut self) -> Result<(), Error> {
        match self.inner.get_mut() {
            SqliteTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .rollback()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            SqliteTxnInner::Oneshot(_) => {
                panic!("cannot abort oneshot");
            }
        }
    }
}

impl SqliteCatalog {
    /// Connect to the catalog store.
    pub async fn connect(options: SqliteConnectionOptions, metrics: Arc<Registry>) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(&options.dsn)
            .map_err(|e| Error::SqlxError { source: e })?
            .create_if_missing(true);

        let pool = SqlitePool::connect_with(opts)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;
        Ok(Self {
            metrics,
            pool,
            time_provider: Arc::new(SystemProvider::new()),
            options,
        })
    }
}

impl Display for SqliteCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sqlite(dsn='{}')", self.options.dsn)
    }
}

#[async_trait]
impl Catalog for SqliteCatalog {
    async fn setup(&self) -> Result<()> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e.into() })?;

        if std::env::var("INFLUXDB_IOX_RPC_MODE").is_ok() {
            // We need to manually insert the topic here so that we can create the transition shard below.
            sqlx::query(
                r#"
INSERT INTO topic (name)
VALUES ($1)
ON CONFLICT (name)
DO NOTHING;
            "#,
            )
            .bind(SHARED_TOPIC_NAME)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e })?;

            // The transition shard must exist and must have magic ID and INDEX.
            sqlx::query(
                r#"
INSERT INTO shard (id, topic_id, shard_index, min_unpersisted_sequence_number)
VALUES ($1, $2, $3, 0)
ON CONFLICT (topic_id, shard_index)
DO NOTHING;
            "#,
            )
            .bind(TRANSITION_SHARD_ID)
            .bind(SHARED_TOPIC_ID)
            .bind(TRANSITION_SHARD_INDEX)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e })?;
        }

        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Box::new(MetricDecorator::new(
            SqliteTxn {
                inner: Mutex::new(SqliteTxnInner::Txn(Some(transaction))),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        )))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            SqliteTxn {
                inner: Mutex::new(SqliteTxnInner::Oneshot(self.pool.clone())),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        ))
    }

    fn metrics(&self) -> Arc<Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[async_trait]
impl RepoCollection for SqliteTxn {
    fn topics(&mut self) -> &mut dyn TopicMetadataRepo {
        self
    }

    fn query_pools(&mut self) -> &mut dyn QueryPoolRepo {
        self
    }

    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn shards(&mut self) -> &mut dyn ShardRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn tombstones(&mut self) -> &mut dyn TombstoneRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }

    fn processed_tombstones(&mut self) -> &mut dyn ProcessedTombstoneRepo {
        self
    }
}

#[async_trait]
impl TopicMetadataRepo for SqliteTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<TopicMetadata> {
        let rec = sqlx::query_as::<_, TopicMetadata>(
            r#"
INSERT INTO topic ( name )
VALUES ( $1 )
ON CONFLICT (name)
DO UPDATE SET name = topic.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<TopicMetadata>> {
        let rec = sqlx::query_as::<_, TopicMetadata>(
            r#"
SELECT *
FROM topic
WHERE name = $1;
        "#,
        )
        .bind(name) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let topic = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(topic))
    }
}

#[async_trait]
impl QueryPoolRepo for SqliteTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT (name)
DO UPDATE SET name = query_pool.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl NamespaceRepo for SqliteTxn {
    async fn create(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
        topic_id: TopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
                INSERT INTO namespace ( name, topic_id, query_pool_id, retention_period_ns, max_tables )
                VALUES ( $1, $2, $3, $4, $5 )
                RETURNING *;
            "#,
        )
            .bind(name) // $1
            .bind(topic_id) // $2
            .bind(query_pool_id) // $3
            .bind(retention_period_ns) // $4
            .bind(DEFAULT_MAX_TABLES); // $5

        let rec = rec.fetch_one(self.inner.get_mut()).await.map_err(|e| {
            if is_unique_violation(&e) {
                Error::NameExists {
                    name: name.to_string(),
                }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        // Ensure the column default values match the code values.
        debug_assert_eq!(rec.max_tables, DEFAULT_MAX_TABLES);
        debug_assert_eq!(rec.max_columns_per_table, DEFAULT_MAX_COLUMNS_PER_TABLE);

        Ok(rec)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_id(
        &mut self,
        id: NamespaceId,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE id=$1 AND {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }

    async fn get_by_name(
        &mut self,
        name: &str,
        deleted: SoftDeletedRows,
    ) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"SELECT * FROM namespace WHERE name=$1 AND {v};"#,
                v = deleted.as_sql_predicate()
            )
            .as_str(),
        )
        .bind(name) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }

    async fn soft_delete(&mut self, name: &str) -> Result<()> {
        let flagged_at = Timestamp::from(self.time_provider.now());

        // note that there is a uniqueness constraint on the name column in the DB
        sqlx::query(r#"UPDATE namespace SET deleted_at=$1 WHERE name = $2;"#)
            .bind(flagged_at) // $1
            .bind(name) // $2
            .execute(self.inner.get_mut())
            .await
            .context(interface::CouldNotDeleteNamespaceSnafu)
            .map(|_| ())
    }

    async fn update_table_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_tables = $1
WHERE name = $2
RETURNING *;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(self.inner.get_mut())
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }

    async fn update_column_limit(&mut self, name: &str, new_max: i32) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
UPDATE namespace
SET max_columns_per_table = $1
WHERE name = $2
RETURNING *;
        "#,
        )
        .bind(new_max)
        .bind(name)
        .fetch_one(self.inner.get_mut())
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }

    async fn update_retention_period(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"UPDATE namespace SET retention_period_ns = $1 WHERE name = $2 RETURNING *;"#,
        )
        .bind(retention_period_ns) // $1
        .bind(name) // $2
        .fetch_one(self.inner.get_mut())
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFoundByName {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }
}

#[async_trait]
impl TableRepo for SqliteTxn {
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        // A simple insert statement becomes quite complicated in order to avoid checking the table
        // limits in a select and then conditionally inserting (which would be racey).
        //
        // from https://www.postgresql.org/docs/current/sql-insert.html
        //   "INSERT inserts new rows into a table. One can insert one or more rows specified by
        //   value expressions, or zero or more rows resulting from a query."
        // By using SELECT rather than VALUES it will insert zero rows if it finds a null in the
        // subquery, i.e. if count >= max_tables. fetch_one() will return a RowNotFound error if
        // nothing was inserted. Not pretty!
        let rec = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id )
SELECT $1, id FROM (
    SELECT namespace.id AS id, max_tables, COUNT(table_name.id) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    WHERE namespace.id = $2
    GROUP BY namespace.max_tables, table_name.namespace_id, namespace.id
) AS get_count WHERE count < max_tables
ON CONFLICT (namespace_id, name)
DO UPDATE SET name = table_name.name
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(namespace_id) // $2
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::TableCreateLimitError {
                table_name: name.to_string(),
                namespace_id,
            },
            _ => {
                if is_fk_violation(&e) {
                    Error::ForeignKeyViolation { source: e }
                } else {
                    Error::SqlxError { source: e }
                }
            }
        })?;

        Ok(rec)
    }

    async fn get_by_id(&mut self, table_id: TableId) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(table))
    }

    async fn get_by_namespace_and_name(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1 AND name = $2;
            "#,
        )
        .bind(namespace_id) // $1
        .bind(name) // $2
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let table = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(table))
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT *
FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>("SELECT * FROM table_name;")
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl ColumnRepo for SqliteTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT $1, table_id, $3 FROM (
    SELECT max_columns_per_table, namespace.id, table_name.id as table_id, COUNT(column_name.id) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
                   LEFT JOIN column_name ON table_name.id = column_name.table_id
    WHERE table_name.id = $2
    GROUP BY namespace.max_columns_per_table, namespace.id, table_name.id
) AS get_count WHERE count < max_columns_per_table
ON CONFLICT (table_id, name)
DO UPDATE SET name = column_name.name
RETURNING *;
        "#,
        )
            .bind(name) // $1
            .bind(table_id) // $2
            .bind(column_type) // $3
            .fetch_one(self.inner.get_mut())
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => Error::ColumnCreateLimitError {
                    column_name: name.to_string(),
                    table_id,
                },
                _ => {
                    if is_fk_violation(&e) {
                        Error::ForeignKeyViolation { source: e }
                    } else {
                        Error::SqlxError { source: e }
                    }
                }})?;

        ensure!(
            rec.column_type == column_type,
            ColumnTypeMismatchSnafu {
                name,
                existing: rec.column_type,
                new: column_type,
            }
        );

        Ok(rec)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(namespace_id)
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT * FROM column_name
WHERE table_id = $1;
            "#,
        )
        .bind(table_id)
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>("SELECT * FROM column_name;")
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn create_or_get_many_unchecked(
        &mut self,
        table_id: TableId,
        columns: HashMap<&str, ColumnType>,
    ) -> Result<Vec<Column>> {
        let num_columns = columns.len();
        #[derive(Deserialize, Serialize)]
        struct NameType<'a> {
            name: &'a str,
            column_type: i8,
        }
        impl<'a> NameType<'a> {
            fn from(value: (&&'a str, &ColumnType)) -> Self {
                Self {
                    name: value.0,
                    column_type: *value.1 as i8,
                }
            }
        }
        let cols = columns.iter().map(NameType::<'_>::from).collect::<Vec<_>>();

        // The `ORDER BY` in this statement is important to avoid deadlocks during concurrent
        // writes to the same IOx table that each add many new columns. See:
        //
        // - <https://rcoh.svbtle.com/sqlite-unique-constraints-can-cause-deadlock>
        // - <https://dba.stackexchange.com/a/195220/27897>
        // - <https://github.com/influxdata/idpe/issues/16298>
        let out = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT a.value ->> 'name' AS name, $1, a.value ->> 'column_type' AS column_type
FROM json_each($2) as a
ORDER BY name
ON CONFLICT (table_id, name)
DO UPDATE SET name = column_name.name
RETURNING *;
            "#,
        )
        .bind(table_id) // $1
        .bind(&Json(cols)) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        assert_eq!(num_columns, out.len());

        for existing in &out {
            let want = columns.get(existing.name.as_str()).unwrap();
            ensure!(
                existing.column_type == *want,
                ColumnTypeMismatchSnafu {
                    name: &existing.name,
                    existing: existing.column_type,
                    new: *want,
                }
            );
        }

        Ok(out)
    }

    async fn list_type_count_by_table_id(
        &mut self,
        table_id: TableId,
    ) -> Result<Vec<ColumnTypeCount>> {
        sqlx::query_as::<_, ColumnTypeCount>(
            r#"
select column_type as col_type, count(1) AS count from column_name where table_id = $1 group by 1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ShardRepo for SqliteTxn {
    async fn create_or_get(
        &mut self,
        topic: &TopicMetadata,
        shard_index: ShardIndex,
    ) -> Result<Shard> {
        sqlx::query_as::<_, Shard>(
            r#"
INSERT INTO shard
    ( topic_id, shard_index, min_unpersisted_sequence_number )
VALUES
    ( $1, $2, 0 )
ON CONFLICT (topic_id, shard_index)
DO UPDATE SET topic_id = shard.topic_id
RETURNING *;
        "#,
        )
        .bind(topic.id) // $1
        .bind(shard_index) // $2
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn get_by_topic_id_and_shard_index(
        &mut self,
        topic_id: TopicId,
        shard_index: ShardIndex,
    ) -> Result<Option<Shard>> {
        let rec = sqlx::query_as::<_, Shard>(
            r#"
SELECT *
FROM shard
WHERE topic_id = $1
  AND shard_index = $2;
        "#,
        )
        .bind(topic_id) // $1
        .bind(shard_index) // $2
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let shard = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(shard))
    }

    async fn list(&mut self) -> Result<Vec<Shard>> {
        sqlx::query_as::<_, Shard>(r#"SELECT * FROM shard;"#)
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_topic(&mut self, topic: &TopicMetadata) -> Result<Vec<Shard>> {
        sqlx::query_as::<_, Shard>(r#"SELECT * FROM shard WHERE topic_id = $1;"#)
            .bind(topic.id) // $1
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn update_min_unpersisted_sequence_number(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<()> {
        let _ = sqlx::query(
            r#"
UPDATE shard
SET min_unpersisted_sequence_number = $1
WHERE id = $2;
                "#,
        )
        .bind(sequence_number.get()) // $1
        .bind(shard_id) // $2
        .execute(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }
}

// We can't use [`Partition`], as uses Vec<String> which the Sqlite
// driver cannot serialise

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct PartitionPod {
    id: PartitionId,
    shard_id: ShardId,
    table_id: TableId,
    partition_key: PartitionKey,
    sort_key: Json<Vec<String>>,
    persisted_sequence_number: Option<SequenceNumber>,
    new_file_at: Option<Timestamp>,
}

impl From<PartitionPod> for Partition {
    fn from(value: PartitionPod) -> Self {
        Self {
            id: value.id,
            shard_id: value.shard_id,
            table_id: value.table_id,
            partition_key: value.partition_key,
            sort_key: value.sort_key.0,
            persisted_sequence_number: value.persisted_sequence_number,
            new_file_at: value.new_file_at,
        }
    }
}

#[async_trait]
impl PartitionRepo for SqliteTxn {
    async fn create_or_get(
        &mut self,
        key: PartitionKey,
        shard_id: ShardId,
        table_id: TableId,
    ) -> Result<Partition> {
        // Note: since sort_key is now an array, we must explicitly insert '{}' which is an empty
        // array rather than NULL which sqlx will throw `UnexpectedNullError` while is is doing
        // `ColumnDecode`

        let v = sqlx::query_as::<_, PartitionPod>(
            r#"
INSERT INTO partition
    ( partition_key, shard_id, table_id, sort_key)
VALUES
    ( $1, $2, $3, '[]')
ON CONFLICT (table_id, partition_key)
DO UPDATE SET partition_key = partition.partition_key
RETURNING *;
        "#,
        )
        .bind(key) // $1
        .bind(shard_id) // $2
        .bind(table_id) // $3
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        // If the partition_key_unique constraint was hit because there was an
        // existing record for (table_id, partition_key) ensure the partition
        // key in the DB is mapped to the same shard_id the caller
        // requested.
        assert_eq!(
            v.shard_id, shard_id,
            "attempted to overwrite partition with different shard ID"
        );

        Ok(v.into())
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, PartitionPod>(r#"SELECT * FROM partition WHERE id = $1;"#)
            .bind(partition_id) // $1
            .fetch_one(self.inner.get_mut())
            .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition.into()))
    }

    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT partition.*
FROM table_name
INNER JOIN partition on partition.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(namespace_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT *
FROM partition
WHERE table_id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            "#,
        )
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    /// Update the sort key for `partition_id` if and only if `old_sort_key`
    /// matches the current value in the database.
    ///
    /// This compare-and-swap operation is allowed to spuriously return
    /// [`CasFailure::ValueMismatch`] for performance reasons (avoiding multiple
    /// round trips to service a transaction in the happy path).
    async fn cas_sort_key(
        &mut self,
        partition_id: PartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
    ) -> Result<Partition, CasFailure<Vec<String>>> {
        let old_sort_key = old_sort_key.unwrap_or_default();
        let res = sqlx::query_as::<_, PartitionPod>(
            r#"
UPDATE partition
SET sort_key = $1
WHERE id = $2 AND sort_key = $3
RETURNING *;
        "#,
        )
        .bind(Json(new_sort_key)) // $1
        .bind(partition_id) // $2
        .bind(Json(&old_sort_key)) // $3
        .fetch_one(self.inner.get_mut())
        .await;

        let partition = match res {
            Ok(v) => v,
            Err(sqlx::Error::RowNotFound) => {
                // This update may have failed either because:
                //
                // * A row with the specified ID did not exist at query time
                //   (but may exist now!)
                // * The sort key does not match.
                //
                // To differentiate, we submit a get partition query, returning
                // the actual sort key if successful.
                //
                // NOTE: this is racy, but documented - this might return "Sort
                // key differs! Old key: <old sort key you provided>"
                return Err(CasFailure::ValueMismatch(
                    PartitionRepo::get_by_id(self, partition_id)
                        .await
                        .map_err(CasFailure::QueryError)?
                        .ok_or(CasFailure::QueryError(Error::PartitionNotFound {
                            id: partition_id,
                        }))?
                        .sort_key,
                ));
            }
            Err(e) => return Err(CasFailure::QueryError(Error::SqlxError { source: e })),
        };

        debug!(
            ?partition_id,
            ?old_sort_key,
            ?new_sort_key,
            "partition sort key cas successful"
        );

        Ok(partition.into())
    }

    async fn record_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
        reason: &str,
        num_files: usize,
        limit_num_files: usize,
        limit_num_files_first_in_partition: usize,
        estimated_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        sqlx::query(
            r#"
INSERT INTO skipped_compactions
    ( partition_id, reason, num_files, limit_num_files, limit_num_files_first_in_partition, estimated_bytes, limit_bytes, skipped_at )
VALUES
    ( $1, $2, $3, $4, $5, $6, $7, $8 )
ON CONFLICT ( partition_id )
DO UPDATE
SET
reason = EXCLUDED.reason,
num_files = EXCLUDED.num_files,
limit_num_files = EXCLUDED.limit_num_files,
limit_num_files_first_in_partition = EXCLUDED.limit_num_files_first_in_partition,
estimated_bytes = EXCLUDED.estimated_bytes,
limit_bytes = EXCLUDED.limit_bytes,
skipped_at = EXCLUDED.skipped_at;
        "#,
        )
            .bind(partition_id) // $1
            .bind(reason)
            .bind(num_files as i64)
            .bind(limit_num_files as i64)
            .bind(limit_num_files_first_in_partition as i64)
            .bind(estimated_bytes as i64)
            .bind(limit_bytes as i64)
            .bind(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)
            .execute(self.inner.get_mut())
            .await
            .context(interface::CouldNotRecordSkippedCompactionSnafu { partition_id })?;
        Ok(())
    }

    async fn get_in_skipped_compaction(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        let rec = sqlx::query_as::<_, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id = $1;"#,
        )
        .bind(partition_id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let skipped_partition_record = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(skipped_partition_record))
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
SELECT * FROM skipped_compactions
        "#,
        )
        .fetch_all(self.inner.get_mut())
        .await
        .context(interface::CouldNotListSkippedCompactionsSnafu)
    }

    async fn delete_skipped_compactions(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
DELETE FROM skipped_compactions
WHERE partition_id = $1
RETURNING *
        "#,
        )
        .bind(partition_id)
        .fetch_optional(self.inner.get_mut())
        .await
        .context(interface::CouldNotDeleteSkippedCompactionsSnafu)
    }

    async fn update_persisted_sequence_number(
        &mut self,
        partition_id: PartitionId,
        sequence_number: SequenceNumber,
    ) -> Result<()> {
        let _ = sqlx::query(
            r#"
UPDATE partition
SET persisted_sequence_number = $1
WHERE id = $2;
                "#,
        )
        .bind(sequence_number.get()) // $1
        .bind(partition_id) // $2
        .execute(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"SELECT * FROM partition ORDER BY id DESC LIMIT $1;"#,
        )
        .bind(n as i64) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn most_recent_n_in_shards(
        &mut self,
        n: usize,
        shards: &[ShardId],
    ) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"SELECT * FROM partition WHERE shard_id IN (SELECT value FROM json_each($1)) ORDER BY id DESC LIMIT $2;"#,
        )
            .bind(&Json(shards.iter().map(|v| v.get()).collect::<Vec<_>>()))
            .bind(n as i64)
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn partitions_with_recent_created_files(
        &mut self,
        time_in_the_past: Timestamp,
        max_num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id, p.table_id, t.namespace_id, p.shard_id
            FROM partition p, table_name t
            WHERE p.new_file_at > $1
                AND p.table_id = t.id
            LIMIT $2;
            "#,
        )
        .bind(time_in_the_past) // $1
        .bind(max_num_partitions as i64) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn partitions_to_compact(&mut self, recent_time: Timestamp) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            WHERE p.new_file_at > $1
            "#,
        )
        .bind(recent_time) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl TombstoneRepo for SqliteTxn {
    async fn create_or_get(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        let v = sqlx::query_as::<_, Tombstone>(
            r#"
INSERT INTO tombstone
    ( table_id, shard_id, sequence_number, min_time, max_time, serialized_predicate )
VALUES
    ( $1, $2, $3, $4, $5, $6 )
ON CONFLICT (table_id, shard_id, sequence_number)
DO UPDATE SET table_id = tombstone.table_id
RETURNING *;
        "#,
        )
        .bind(table_id) // $1
        .bind(shard_id) // $2
        .bind(sequence_number) // $3
        .bind(min_time) // $4
        .bind(max_time) // $5
        .bind(predicate) // $6
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        // If tombstone_unique is hit, a record with (table_id, shard_id,
        // sequence_number) already exists.
        //
        // Ensure the caller does not falsely believe they have created the
        // record with the provided values if the DB row contains different
        // values.
        assert_eq!(
            v.min_time, min_time,
            "attempted to overwrite min_time in tombstone record"
        );
        assert_eq!(
            v.max_time, max_time,
            "attempted to overwrite max_time in tombstone record"
        );
        assert_eq!(
            v.serialized_predicate, predicate,
            "attempted to overwrite predicate in tombstone record"
        );

        Ok(v)
    }

    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT
    tombstone.id as id,
    tombstone.table_id as table_id,
    tombstone.shard_id as shard_id,
    tombstone.sequence_number as sequence_number,
    tombstone.min_time as min_time,
    tombstone.max_time as max_time,
    tombstone.serialized_predicate as serialized_predicate
FROM table_name
INNER JOIN tombstone on tombstone.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(namespace_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table(&mut self, table_id: TableId) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE table_id = $1
ORDER BY id;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn get_by_id(&mut self, id: TombstoneId) -> Result<Option<Tombstone>> {
        let rec = sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE id = $1;
        "#,
        )
        .bind(id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let tombstone = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(tombstone))
    }

    async fn list_tombstones_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE shard_id = $1
  AND sequence_number > $2
ORDER BY id;
            "#,
        )
        .bind(shard_id) // $1
        .bind(sequence_number) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn remove(&mut self, tombstone_ids: &[TombstoneId]) -> Result<()> {
        let ids: Vec<_> = tombstone_ids.iter().map(|t| t.get()).collect();

        // Remove processed tombstones first
        sqlx::query(
            r#"
DELETE
FROM processed_tombstone
WHERE tombstone_id IN (SELECT value FROM json_each($1));
            "#,
        )
        .bind(Json(&ids[..])) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        // Remove tombstones
        sqlx::query(
            r#"
DELETE
FROM tombstone
WHERE id IN (SELECT value FROM json_each($1));
            "#,
        )
        .bind(Json(&ids[..])) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn list_tombstones_for_time_range(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE shard_id = $1
  AND table_id = $2
  AND sequence_number > $3
  AND ((min_time <= $4 AND max_time >= $4)
        OR (min_time > $4 AND min_time <= $5))
ORDER BY id;
            "#,
        )
        .bind(shard_id) // $1
        .bind(table_id) // $2
        .bind(sequence_number) // $3
        .bind(min_time) // $4
        .bind(max_time) // $5
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }
}

fn from_column_set(v: &ColumnSet) -> Json<Vec<i64>> {
    Json((*v).iter().map(ColumnId::get).collect())
}

fn to_column_set(v: &Json<Vec<i64>>) -> ColumnSet {
    ColumnSet::new(v.0.iter().map(|v| ColumnId::new(*v)))
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct ParquetFilePod {
    id: ParquetFileId,
    shard_id: ShardId,
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,
    object_store_id: Uuid,
    max_sequence_number: SequenceNumber,
    min_time: Timestamp,
    max_time: Timestamp,
    to_delete: Option<Timestamp>,
    file_size_bytes: i64,
    row_count: i64,
    compaction_level: CompactionLevel,
    created_at: Timestamp,
    column_set: Json<Vec<i64>>,
    max_l0_created_at: Timestamp,
}

impl From<ParquetFilePod> for ParquetFile {
    fn from(value: ParquetFilePod) -> Self {
        Self {
            id: value.id,
            shard_id: value.shard_id,
            namespace_id: value.namespace_id,
            table_id: value.table_id,
            partition_id: value.partition_id,
            object_store_id: value.object_store_id,
            max_sequence_number: value.max_sequence_number,
            min_time: value.min_time,
            max_time: value.max_time,
            to_delete: value.to_delete,
            file_size_bytes: value.file_size_bytes,
            row_count: value.row_count,
            compaction_level: value.compaction_level,
            created_at: value.created_at,
            column_set: to_column_set(&value.column_set),
            max_l0_created_at: value.max_l0_created_at,
        }
    }
}

#[async_trait]
impl ParquetFileRepo for SqliteTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        let ParquetFileParams {
            shard_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes,
            row_count,
            compaction_level,
            created_at,
            column_set,
            max_l0_created_at,
        } = parquet_file_params;

        let rec = sqlx::query_as::<_, ParquetFilePod>(
            r#"
INSERT INTO parquet_file (
    shard_id, table_id, partition_id, object_store_id,
    max_sequence_number, min_time, max_time, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING *;
        "#,
        )
        .bind(shard_id) // $1
        .bind(table_id) // $2
        .bind(partition_id) // $3
        .bind(object_store_id) // $4
        .bind(max_sequence_number) // $5
        .bind(min_time) // $6
        .bind(max_time) // $7
        .bind(file_size_bytes) // $8
        .bind(row_count) // $9
        .bind(compaction_level) // $10
        .bind(created_at) // $11
        .bind(namespace_id) // $12
        .bind(from_column_set(&column_set)) // $13
        .bind(max_l0_created_at) // $14
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                Error::FileExists { object_store_id }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec.into())
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let marked_at = Timestamp::from(self.time_provider.now());

        let _ = sqlx::query(r#"UPDATE parquet_file SET to_delete = $1 WHERE id = $2;"#)
            .bind(marked_at) // $1
            .bind(id) // $2
            .execute(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>> {
        let flagged_at = Timestamp::from(self.time_provider.now());
        // TODO - include check of table retention period once implemented
        let flagged = sqlx::query(
            r#"
                UPDATE parquet_file
                SET to_delete = $1
                FROM namespace
                WHERE namespace.retention_period_ns IS NOT NULL
                AND parquet_file.to_delete IS NULL
                AND parquet_file.max_time < $1 - namespace.retention_period_ns
                AND namespace.id = parquet_file.namespace_id
                RETURNING parquet_file.id;
            "#,
        )
        .bind(flagged_at) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let flagged = flagged.into_iter().map(|row| row.get("id")).collect();
        Ok(flagged)
    }

    async fn list_by_shard_greater_than(
        &mut self,
        shard_id: ShardId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE shard_id = $1
  AND max_sequence_number > $2
ORDER BY id;
            "#,
        )
        .bind(shard_id) // $1
        .bind(sequence_number) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT parquet_file.id, parquet_file.shard_id, parquet_file.namespace_id,
       parquet_file.table_id, parquet_file.partition_id, parquet_file.object_store_id,
       parquet_file.max_sequence_number, parquet_file.min_time,
       parquet_file.max_time, parquet_file.to_delete, parquet_file.file_size_bytes,
       parquet_file.row_count, parquet_file.compaction_level, parquet_file.created_at, parquet_file.column_set,
       parquet_file.max_l0_created_at
FROM parquet_file
INNER JOIN table_name on table_name.id = parquet_file.table_id
WHERE table_name.namespace_id = $1
  AND parquet_file.to_delete IS NULL;
             "#,
        )
            .bind(namespace_id) // $1
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE table_id = $1 AND to_delete IS NULL;
             "#,
        )
        .bind(table_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>> {
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
DELETE FROM parquet_file
WHERE to_delete < $1
RETURNING *;
             "#,
        )
        .bind(older_than) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>> {
        // see https://www.crunchydata.com/blog/simulating-update-or-delete-with-limit-in-sqlite-ctes-to-the-rescue
        let deleted = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT id
    FROM parquet_file
    WHERE to_delete < $1
    LIMIT $2
)
DELETE FROM parquet_file
WHERE id IN (SELECT id FROM parquet_file_ids)
RETURNING id;
             "#,
        )
        .bind(older_than) // $1
        .bind(MAX_PARQUET_FILES_DELETED_ONCE) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let deleted = deleted.into_iter().map(|row| row.get("id")).collect();
        Ok(deleted)
    }

    async fn level_0(&mut self, shard_id: ShardId) -> Result<Vec<ParquetFile>> {
        // this intentionally limits the returned files to 10,000 as it is used to make
        // a decision on the highest priority partitions. If compaction has never been
        // run this could end up returning millions of results and taking too long to run.
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE parquet_file.shard_id = $1
  AND parquet_file.compaction_level = $2
  AND parquet_file.to_delete IS NULL
  LIMIT 1000;
        "#,
        )
        .bind(shard_id) // $1
        .bind(CompactionLevel::Initial) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn level_1(
        &mut self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE parquet_file.shard_id = $1
  AND parquet_file.table_id = $2
  AND parquet_file.partition_id = $3
  AND parquet_file.compaction_level = $4
  AND parquet_file.to_delete IS NULL
  AND ((parquet_file.min_time <= $5 AND parquet_file.max_time >= $5)
      OR (parquet_file.min_time > $5 AND parquet_file.min_time <= $6));
        "#,
        )
        .bind(table_partition.shard_id) // $1
        .bind(table_partition.table_id) // $2
        .bind(table_partition.partition_id) // $3
        .bind(CompactionLevel::FileNonOverlapped) // $4
        .bind(min_time) // $5
        .bind(max_time) // $6
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn recent_highest_throughput_partitions(
        &mut self,
        shard_id: Option<ShardId>,
        time_in_the_past: Timestamp,
        min_num_files: usize,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let min_num_files = min_num_files as i32;
        let num_partitions = num_partitions as i32;

        match shard_id {
            Some(shard_id) => {
                sqlx::query_as::<_, PartitionParam>(
                    r#"
SELECT parquet_file.partition_id, parquet_file.table_id, parquet_file.shard_id,
       parquet_file.namespace_id, count(parquet_file.id)
FROM parquet_file
LEFT OUTER JOIN skipped_compactions ON parquet_file.partition_id = skipped_compactions.partition_id
WHERE compaction_level = $5
AND   to_delete is null
AND   shard_id = $1
AND   created_at > $2
AND   skipped_compactions.partition_id IS NULL
GROUP BY 1, 2, 3, 4
HAVING count(id) >= $3
ORDER BY 5 DESC
LIMIT $4;
                    "#,
                )
                .bind(shard_id) // $1
                .bind(time_in_the_past) //$2
                .bind(min_num_files) // $3
                .bind(num_partitions) // $4
                .bind(CompactionLevel::Initial) // $5
                .fetch_all(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })
            }
            None => {
                sqlx::query_as::<_, PartitionParam>(
                    r#"
SELECT parquet_file.partition_id, parquet_file.table_id, parquet_file.shard_id,
       parquet_file.namespace_id, count(parquet_file.id)
FROM parquet_file
LEFT OUTER JOIN skipped_compactions ON parquet_file.partition_id = skipped_compactions.partition_id
WHERE compaction_level = $4
AND   to_delete is null
AND   created_at > $1
AND   skipped_compactions.partition_id IS NULL
GROUP BY 1, 2, 3, 4
HAVING count(id) >= $2
ORDER BY 5 DESC
LIMIT $3;
                    "#,
                )
                .bind(time_in_the_past) //$1
                .bind(min_num_files) // $2
                .bind(num_partitions) // $3
                .bind(CompactionLevel::Initial) // $4
                .fetch_all(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })
            }
        }
    }

    async fn partitions_with_small_l1_file_count(
        &mut self,
        shard_id: Option<ShardId>,
        small_size_threshold_bytes: i64,
        min_small_file_count: usize,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        // This query returns partitions with at least `min_small_file_count` small L1 files,
        // where "small" means no bigger than `small_size_threshold_bytes`, limited to the top `num_partitions`.
        sqlx::query_as::<_, PartitionParam>(
            r#"
SELECT parquet_file.partition_id, parquet_file.shard_id, parquet_file.namespace_id,
       parquet_file.table_id,
       COUNT(1) AS l1_file_count
FROM   parquet_file
LEFT OUTER JOIN skipped_compactions ON parquet_file.partition_id = skipped_compactions.partition_id
WHERE  compaction_level = $5
AND    to_delete IS NULL
AND    shard_id = $1
AND    skipped_compactions.partition_id IS NULL
AND    file_size_bytes < $3
GROUP BY 1, 2, 3, 4
HAVING COUNT(1) >= $2
ORDER BY l1_file_count DESC
LIMIT $4;
            "#,
        )
        .bind(shard_id) // $1
        .bind(min_small_file_count as i32) // $2
        .bind(small_size_threshold_bytes) // $3
        .bind(num_partitions as i32) // $4
        .bind(CompactionLevel::FileNonOverlapped) // $5
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn most_cold_files_partitions(
        &mut self,
        shard_id: Option<ShardId>,
        time_in_the_past: Timestamp,
        num_partitions: usize,
    ) -> Result<Vec<PartitionParam>> {
        let num_partitions = num_partitions as i32;

        // This query returns partitions with most L0+L1 files and all L0 files (both deleted and
        // non deleted) are either created before the given time ($2) or not available (removed by
        // garbage collector)
        match shard_id {
            Some(shard_id) => {
                sqlx::query_as::<_, PartitionParam>(
                    r#"
SELECT parquet_file.partition_id, parquet_file.shard_id, parquet_file.namespace_id,
       parquet_file.table_id,
       count(case when to_delete is null then 1 end) total_count,
       max(case when compaction_level= $4 then parquet_file.created_at end)
FROM   parquet_file
LEFT OUTER JOIN skipped_compactions ON parquet_file.partition_id = skipped_compactions.partition_id
WHERE  (compaction_level = $4 OR compaction_level = $5)
AND    shard_id = $1
AND    skipped_compactions.partition_id IS NULL
GROUP BY 1, 2, 3, 4
HAVING count(case when to_delete is null then 1 end) > 0
       AND ( max(case when compaction_level= $4 then parquet_file.created_at end) < $2  OR
             max(case when compaction_level= $4 then parquet_file.created_at end) is null)
ORDER BY total_count DESC
LIMIT $3;
                    "#,
                )
                .bind(shard_id) // $1
                .bind(time_in_the_past) // $2
                .bind(num_partitions) // $3
                .bind(CompactionLevel::Initial) // $4
                .bind(CompactionLevel::FileNonOverlapped) // $5
                .fetch_all(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })
            }
            None => {
                sqlx::query_as::<_, PartitionParam>(
                    r#"
SELECT parquet_file.partition_id, parquet_file.shard_id, parquet_file.namespace_id,
       parquet_file.table_id,
       count(case when to_delete is null then 1 end) total_count,
       max(case when compaction_level= $4 then parquet_file.created_at end)
FROM   parquet_file
LEFT OUTER JOIN skipped_compactions ON parquet_file.partition_id = skipped_compactions.partition_id
WHERE  (compaction_level = $3 OR compaction_level = $4)
AND    skipped_compactions.partition_id IS NULL
GROUP BY 1, 2, 3, 4
HAVING count(case when to_delete is null then 1 end) > 0
       AND ( max(case when compaction_level= $3 then parquet_file.created_at end) < $1  OR
             max(case when compaction_level= $3 then parquet_file.created_at end) is null)
ORDER BY total_count DESC
LIMIT $2;
                    "#,
                )
                .bind(time_in_the_past) // $1
                .bind(num_partitions) // $2
                .bind(CompactionLevel::Initial) // $3
                .bind(CompactionLevel::FileNonOverlapped) // $4
                .fetch_all(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })
            }
        }
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE parquet_file.partition_id = $1
  AND parquet_file.to_delete IS NULL;
        "#,
        )
        .bind(partition_id) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn update_compaction_level(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
        compaction_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        // If I try to do `.bind(parquet_file_ids)` directly, I get a compile error from sqlx.
        // See https://github.com/launchbadge/sqlx/issues/1744
        let ids: Vec<_> = parquet_file_ids.iter().map(|p| p.get()).collect();
        let updated = sqlx::query(
            r#"
UPDATE parquet_file
SET compaction_level = $1
WHERE id IN (SELECT value FROM json_each($2))
RETURNING id;
        "#,
        )
        .bind(compaction_level) // $1
        .bind(Json(&ids[..])) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let updated = updated.into_iter().map(|row| row.get("id")).collect();
        Ok(updated)
    }

    async fn exist(&mut self, id: ParquetFileId) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(1) as count FROM parquet_file WHERE id = $1;"#,
        )
        .bind(id) // $1
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(1) as count FROM parquet_file;"#)
                .fetch_one(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn count_by_overlaps_with_level_0(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
        sequence_number: SequenceNumber,
    ) -> Result<i64> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"
SELECT count(1) as count
FROM parquet_file
WHERE table_id = $1
  AND shard_id = $2
  AND max_sequence_number < $3
  AND parquet_file.to_delete IS NULL
  AND compaction_level = $6
  AND ((parquet_file.min_time <= $4 AND parquet_file.max_time >= $4)
  OR (parquet_file.min_time > $4 AND parquet_file.min_time <= $5));
            "#,
        )
        .bind(table_id) // $1
        .bind(shard_id) // $2
        .bind(sequence_number) // $3
        .bind(min_time) // $4
        .bind(max_time) // $5
        .bind(CompactionLevel::Initial) // $6
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn count_by_overlaps_with_level_1(
        &mut self,
        table_id: TableId,
        shard_id: ShardId,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<i64> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"
SELECT count(1) as count
FROM parquet_file
WHERE table_id = $1
  AND shard_id = $2
  AND parquet_file.to_delete IS NULL
  AND compaction_level = $5
  AND ((parquet_file.min_time <= $3 AND parquet_file.max_time >= $3)
  OR (parquet_file.min_time > $3 AND parquet_file.min_time <= $4));
            "#,
        )
        .bind(table_id) // $1
        .bind(shard_id) // $2
        .bind(min_time) // $3
        .bind(max_time) // $4
        .bind(CompactionLevel::FileNonOverlapped) // $5
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        let rec = sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, shard_id, namespace_id, table_id, partition_id, object_store_id,
       max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
WHERE object_store_id = $1;
             "#,
        )
        .bind(object_store_id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let parquet_file = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(parquet_file.into()))
    }
}

#[async_trait]
impl ProcessedTombstoneRepo for SqliteTxn {
    async fn create(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<ProcessedTombstone> {
        sqlx::query_as::<_, ProcessedTombstone>(
            r#"
INSERT INTO processed_tombstone ( tombstone_id, parquet_file_id )
VALUES ( $1, $2 )
RETURNING *;
        "#,
        )
        .bind(tombstone_id) // $1
        .bind(parquet_file_id) // $2
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                Error::ProcessTombstoneExists {
                    tombstone_id: tombstone_id.get(),
                    parquet_file_id: parquet_file_id.get(),
                }
            } else if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn exist(
        &mut self,
        parquet_file_id: ParquetFileId,
        tombstone_id: TombstoneId,
    ) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"
SELECT count(1) as count
FROM processed_tombstone
WHERE parquet_file_id = $1
  AND tombstone_id = $2;
            "#,
        )
        .bind(parquet_file_id) // $1
        .bind(tombstone_id) // $2
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(1) as count FROM processed_tombstone;"#)
                .fetch_one(self.inner.get_mut())
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn count_by_tombstone_id(&mut self, tombstone_id: TombstoneId) -> Result<i64> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(1) as count FROM processed_tombstone WHERE tombstone_id = $1;"#,
        )
        .bind(tombstone_id) // $1
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }
}

/// The error code returned by SQLite for a unique constraint violation.
///
/// See <https://sqlite.org/rescode.html#constraint_unique>
const SQLITE_UNIQUE_VIOLATION: &str = "2067";

/// Error code returned by SQLite for a foreign key constraint violation.
/// See <https://sqlite.org/rescode.html#constraint_foreignkey>
const SQLITE_FK_VIOLATION: &str = "787";

fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == SQLITE_FK_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Returns true if `e` is a unique constraint violation error.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == SQLITE_UNIQUE_VIOLATION {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_or_get_default_records;
    use assert_matches::assert_matches;
    use metric::{Attributes, DurationHistogram, Metric};
    use std::{ops::DerefMut, sync::Arc};

    fn assert_metric_hit(metrics: &Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 0, "metric did not record any calls");
    }

    async fn setup_db() -> SqliteCatalog {
        let dsn =
            std::env::var("TEST_INFLUXDB_SQLITE_DSN").unwrap_or("sqlite::memory:".to_string());
        let options = SqliteConnectionOptions { dsn };
        let metrics = Arc::new(Registry::default());
        let cat = SqliteCatalog::connect(options, metrics)
            .await
            .expect("failed to connect to catalog");
        cat.setup().await.expect("failed to initialise database");
        cat
    }

    #[tokio::test]
    async fn test_catalog() {
        interface::test_helpers::test_catalog(|| async {
            let sqlite = setup_db().await;
            let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
            sqlite
        })
        .await;
    }

    #[tokio::test]
    async fn test_tombstone_create_or_get_idempotent() {
        let sqlite = setup_db().await;
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);

        let mut txn = sqlite.start_transaction().await.expect("txn start");
        let (kafka, query, shards) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = sqlite
            .repositories()
            .await
            .namespaces()
            .create("ns", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = sqlite
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let shard_id = *shards.keys().next().expect("no shard");
        let sequence_number = SequenceNumber::new(3);
        let min_timestamp = Timestamp::new(10);
        let max_timestamp = Timestamp::new(100);
        let predicate = "bananas";

        let a = sqlite
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                shard_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                predicate,
            )
            .await
            .expect("should create OK");

        // Call create_or_get for the same (table_id, shard_id,
        // sequence_number) triplet, setting the same metadata to ensure the
        // write is idempotent.
        let b = sqlite
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                shard_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                predicate,
            )
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);
    }

    #[tokio::test]
    #[should_panic = "attempted to overwrite predicate"]
    async fn test_tombstone_create_or_get_no_overwrite() {
        let sqlite = setup_db().await;
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);

        let mut txn = sqlite.start_transaction().await.expect("txn start");
        let (kafka, query, shards) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = sqlite
            .repositories()
            .await
            .namespaces()
            .create("ns2", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = sqlite
            .repositories()
            .await
            .tables()
            .create_or_get("table2", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let shard_id = *shards.keys().next().expect("no shard");
        let sequence_number = SequenceNumber::new(3);
        let min_timestamp = Timestamp::new(10);
        let max_timestamp = Timestamp::new(100);

        let a = sqlite
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                shard_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                "bananas",
            )
            .await
            .expect("should create OK");

        // Call create_or_get for the same (table_id, shard_id,
        // sequence_number) triplet with different metadata.
        //
        // The caller should not falsely believe it has persisted the incorrect
        // predicate.
        let b = sqlite
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                shard_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                "some other serialized predicate which is different",
            )
            .await
            .expect("should panic before result evaluated");

        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn test_partition_create_or_get_idempotent() {
        let sqlite = setup_db().await;

        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut txn = sqlite.start_transaction().await.expect("txn start");
        let (kafka, query, shards) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = sqlite
            .repositories()
            .await
            .namespaces()
            .create("ns4", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = sqlite
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";
        let shard_id = *shards.keys().next().expect("no shard");

        let a = sqlite
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), shard_id, table_id)
            .await
            .expect("should create OK");

        // Call create_or_get for the same (key, table_id, shard_id)
        // triplet, setting the same shard ID to ensure the write is
        // idempotent.
        let b = sqlite
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), shard_id, table_id)
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);
    }

    #[tokio::test]
    #[should_panic = "attempted to overwrite partition"]
    async fn test_partition_create_or_get_no_overwrite() {
        let sqlite = setup_db().await;

        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut txn = sqlite.start_transaction().await.expect("txn start");
        let (kafka, query, _) = create_or_get_default_records(2, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = sqlite
            .repositories()
            .await
            .namespaces()
            .create("ns3", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = sqlite
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";

        let shards = sqlite
            .repositories()
            .await
            .shards()
            .list()
            .await
            .expect("failed to list shards");
        assert!(
            shards.len() > 1,
            "expected more shards to be created, got {}",
            shards.len()
        );

        let a = sqlite
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), shards[0].id, table_id)
            .await
            .expect("should create OK");

        // Call create_or_get for the same (key, table_id) tuple, setting a
        // different shard ID
        let b = sqlite
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), shards[1].id, table_id)
            .await
            .expect("result should not be evaluated");

        assert_eq!(a, b);
    }

    macro_rules! test_column_create_or_get_many_unchecked {
        (
            $name:ident,
            calls = {$([$($col_name:literal => $col_type:expr),+ $(,)?]),+},
            want = $($want:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_column_create_or_get_many_unchecked_ $name>]() {
                    let sqlite = setup_db().await;
                    let metrics = Arc::clone(&sqlite.metrics);

                    let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
                    let mut txn = sqlite.start_transaction().await.expect("txn start");
                    let (kafka, query, _shards) = create_or_get_default_records(1, txn.deref_mut())
                        .await
                        .expect("db init failed");
                    txn.commit().await.expect("txn commit");

                    let namespace_id = sqlite
                        .repositories()
                        .await
                        .namespaces()
                        .create("ns4", None, kafka.id, query.id)
                        .await
                        .expect("namespace create failed")
                        .id;
                    let table_id = sqlite
                        .repositories()
                        .await
                        .tables()
                        .create_or_get("table", namespace_id)
                        .await
                        .expect("create table failed")
                        .id;

                    $(
                        let mut insert = HashMap::new();
                        $(
                            insert.insert($col_name, $col_type);
                        )+

                        let got = sqlite
                            .repositories()
                            .await
                            .columns()
                            .create_or_get_many_unchecked(table_id, insert.clone())
                            .await;

                        // The returned columns MUST always match the requested
                        // column values if successful.
                        if let Ok(got) = &got {
                            assert_eq!(insert.len(), got.len());

                            for got in got {
                                assert_eq!(table_id, got.table_id);
                                let requested_column_type = insert
                                    .get(got.name.as_str())
                                    .expect("Should have gotten back a column that was inserted");
                                assert_eq!(
                                    *requested_column_type,
                                    ColumnType::try_from(got.column_type)
                                        .expect("invalid column type")
                                );
                            }

                            assert_metric_hit(&metrics, "column_create_or_get_many_unchecked");
                        }
                    )+

                    assert_matches!(got, $($want)+);
                }
            }
        }
    }

    // Issue a few calls to create_or_get_many that contain distinct columns and
    // covers the full set of column types.
    test_column_create_or_get_many_unchecked!(
        insert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
            ],
            [
                "test8" => ColumnType::String,
                "test9" => ColumnType::Bool,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with overlapping columns - request should succeed (upsert
    // semantics).
    test_column_create_or_get_many_unchecked!(
        partial_upsert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ],
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
                "test8" => ColumnType::String,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with the same columns and types.
    test_column_create_or_get_many_unchecked!(
        full_upsert,
        calls = {
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ],
            [
                "test1" => ColumnType::I64,
                "test2" => ColumnType::U64,
                "test3" => ColumnType::F64,
                "test4" => ColumnType::Bool,
            ]
        },
        want = Ok(_)
    );

    // Issue two calls with overlapping columns with conflicting types and
    // observe a correctly populated ColumnTypeMismatch error.
    test_column_create_or_get_many_unchecked!(
        partial_type_conflict,
        calls = {
            [
                "test1" => ColumnType::String,
                "test2" => ColumnType::String,
                "test3" => ColumnType::String,
                "test4" => ColumnType::String,
            ],
            [
                "test1" => ColumnType::String,
                "test2" => ColumnType::Bool, // This one differs
                "test3" => ColumnType::String,
                // 4 is missing.
                "test5" => ColumnType::String,
                "test6" => ColumnType::Time,
                "test7" => ColumnType::Tag,
                "test8" => ColumnType::String,
            ]
        },
        want = Err(e) => {
            assert_matches!(e, Error::ColumnTypeMismatch { name, existing, new } => {
                assert_eq!(name, "test2");
                assert_eq!(existing, ColumnType::String);
                assert_eq!(new, ColumnType::Bool);
            })
        }
    );

    #[tokio::test]
    async fn test_billing_summary_on_parqet_file_creation() {
        let sqlite = setup_db().await;
        let pool = sqlite.pool.clone();

        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut txn = sqlite.start_transaction().await.expect("txn start");
        let (kafka, query, shards) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = sqlite
            .repositories()
            .await
            .namespaces()
            .create("ns4", None, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = sqlite
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";
        let shard_id = *shards.keys().next().expect("no shard");

        let partition_id = sqlite
            .repositories()
            .await
            .partitions()
            .create_or_get(key.into(), shard_id, table_id)
            .await
            .expect("should create OK")
            .id;

        // parquet file to create- all we care about here is the size, the rest is to satisfy DB
        // constraints
        let time_provider = Arc::new(SystemProvider::new());
        let time_now = Timestamp::from(time_provider.now());
        let mut p1 = ParquetFileParams {
            shard_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id: Uuid::new_v4(),
            max_sequence_number: SequenceNumber::new(100),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: CompactionLevel::Initial, // level of file of new writes
            created_at: time_now,
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: time_now,
        };
        let f1 = sqlite
            .repositories()
            .await
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");
        // insert the same again with a different size; we should then have 3x1337 as total file size
        p1.object_store_id = Uuid::new_v4();
        p1.file_size_bytes *= 2;
        let _f2 = sqlite
            .repositories()
            .await
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");

        // after adding two files we should have 3x1337 in the summary
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 3);

        // flag f1 for deletion and assert that the total file size is reduced accordingly.
        sqlite
            .repositories()
            .await
            .parquet_files()
            .flag_for_delete(f1.id)
            .await
            .expect("flag parquet file for deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        // we marked the first file of size 1337 for deletion leaving only the second that was 2x that
        assert_eq!(total_file_size_bytes, 1337 * 2);

        // actually deleting shouldn't change the total
        let now = Timestamp::from(time_provider.now());
        sqlite
            .repositories()
            .await
            .parquet_files()
            .delete_old(now)
            .await
            .expect("parquet file deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 2);
    }
}
