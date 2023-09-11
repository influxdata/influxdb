//! A SQLite backed implementation of the Catalog

use crate::{
    interface::{
        self, verify_sort_key_length, CasFailure, Catalog, ColumnRepo, ColumnTypeMismatchSnafu,
        Error, NamespaceRepo, ParquetFileRepo, PartitionRepo, RepoCollection, Result,
        SoftDeletedRows, TableRepo, MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION,
    },
    kafkaless_transition::{
        SHARED_QUERY_POOL, SHARED_QUERY_POOL_ID, SHARED_TOPIC_ID, SHARED_TOPIC_NAME,
        TRANSITION_SHARD_ID, TRANSITION_SHARD_INDEX,
    },
    metrics::MetricDecorator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES,
};
use async_trait::async_trait;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, TemplatePart,
    },
    Column, ColumnId, ColumnSet, ColumnType, CompactionLevel, Namespace, NamespaceId,
    NamespaceName, NamespaceServiceProtectionLimitsOverride, ParquetFile, ParquetFileId,
    ParquetFileParams, Partition, PartitionHashId, PartitionId, PartitionKey, SkippedCompaction,
    SortedColumnSet, Table, TableId, Timestamp, TransitionPartitionId,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use std::{collections::HashSet, fmt::Write};

use crate::interface::MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE;
use iox_time::{SystemProvider, TimeProvider};
use metric::Registry;
use observability_deps::tracing::debug;
use parking_lot::Mutex;
use snafu::prelude::*;
use sqlx::sqlite::SqliteRow;
use sqlx::types::Json;
use sqlx::{
    migrate::Migrator, sqlite::SqliteConnectOptions, types::Uuid, Executor, Pool, Row, Sqlite,
    SqlitePool,
};
use std::str::FromStr;
use std::sync::Arc;

static MIGRATOR: Migrator = sqlx::migrate!("sqlite/migrations");

/// SQLite connection options.
#[derive(Debug, Clone)]
pub struct SqliteConnectionOptions {
    /// local file path to .sqlite file
    pub file_path: String,
}

/// SQLite catalog.
#[derive(Debug)]
pub struct SqliteCatalog {
    metrics: Arc<Registry>,
    pool: Pool<Sqlite>,
    time_provider: Arc<dyn TimeProvider>,
    options: SqliteConnectionOptions,
}

/// transaction for [`SqliteCatalog`].
#[derive(Debug)]
pub struct SqliteTxn {
    inner: Mutex<SqliteTxnInner>,
    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
struct SqliteTxnInner {
    pool: Pool<Sqlite>,
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
        self.pool.fetch_many(query)
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
        self.pool.fetch_optional(query)
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
        self.pool.prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        self.pool.describe(sql)
    }
}

impl SqliteCatalog {
    /// Connect to the catalog store.
    pub async fn connect(options: SqliteConnectionOptions, metrics: Arc<Registry>) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(&options.file_path)
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
        write!(f, "Sqlite(dsn='{}')", self.options.file_path)
    }
}

#[async_trait]
impl Catalog for SqliteCatalog {
    async fn setup(&self) -> Result<()> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e.into() })?;

        // We need to manually insert the topic here so that we can create the transition shard
        // below.
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

        // We need to manually insert the query pool here so that we can create namespaces that
        // reference it.
        sqlx::query(
            r#"
INSERT INTO query_pool (name)
VALUES ($1)
ON CONFLICT (name)
DO NOTHING;
        "#,
        )
        .bind(SHARED_QUERY_POOL)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Setup { source: e })?;

        Ok(())
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            SqliteTxn {
                inner: Mutex::new(SqliteTxnInner {
                    pool: self.pool.clone(),
                }),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

#[async_trait]
impl RepoCollection for SqliteTxn {
    fn namespaces(&mut self) -> &mut dyn NamespaceRepo {
        self
    }

    fn tables(&mut self) -> &mut dyn TableRepo {
        self
    }

    fn columns(&mut self) -> &mut dyn ColumnRepo {
        self
    }

    fn partitions(&mut self) -> &mut dyn PartitionRepo {
        self
    }

    fn parquet_files(&mut self) -> &mut dyn ParquetFileRepo {
        self
    }
}

#[async_trait]
impl NamespaceRepo for SqliteTxn {
    async fn create(
        &mut self,
        name: &NamespaceName<'_>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
        retention_period_ns: Option<i64>,
        service_protection_limits: Option<NamespaceServiceProtectionLimitsOverride>,
    ) -> Result<Namespace> {
        let max_tables = service_protection_limits.and_then(|l| l.max_tables);
        let max_columns_per_table = service_protection_limits.and_then(|l| l.max_columns_per_table);

        let rec = sqlx::query_as::<_, Namespace>(
            r#"
INSERT INTO namespace ( name, topic_id, query_pool_id, retention_period_ns, max_tables, max_columns_per_table, partition_template )
VALUES ( $1, $2, $3, $4, $5, $6, $7 )
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
            "#,
        )
        .bind(name.as_str()) // $1
        .bind(SHARED_TOPIC_ID) // $2
        .bind(SHARED_QUERY_POOL_ID) // $3
        .bind(retention_period_ns) // $4
        .bind(max_tables.unwrap_or(DEFAULT_MAX_TABLES)) // $5
        .bind(max_columns_per_table.unwrap_or(DEFAULT_MAX_COLUMNS_PER_TABLE)) // $6
        .bind(partition_template); // $7

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

        Ok(rec)
    }

    async fn list(&mut self, deleted: SoftDeletedRows) -> Result<Vec<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            format!(
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template
FROM namespace
WHERE {v};
                "#,
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
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template
FROM namespace
WHERE id=$1 AND {v};
                "#,
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
                r#"
SELECT id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
       partition_template
FROM namespace
WHERE name=$1 AND {v};
                "#,
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
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
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
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
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
            r#"
UPDATE namespace
SET retention_period_ns = $1
WHERE name = $2
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
            "#,
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

/// [`TableRepo::create`] needs the ability to create some columns within the same transaction as
/// the table creation. Column creation might also happen through [`ColumnRepo::create_or_get`],
/// which doesn't need to be within an outer transaction. This function was extracted so that these
/// two functions can share code but pass in either the transaction or the regular database
/// connection as the query executor.
async fn insert_column_with_connection<'q, E>(
    executor: E,
    name: &str,
    table_id: TableId,
    column_type: ColumnType,
) -> Result<Column>
where
    E: Executor<'q, Database = Sqlite>,
{
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
        .fetch_one(executor)
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

#[async_trait]
impl TableRepo for SqliteTxn {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let mut tx = self
            .inner
            .get_mut()
            .pool
            .begin()
            .await
            .map_err(|e| Error::StartTransaction { source: e })?;

        // A simple insert statement becomes quite complicated in order to avoid checking the table
        // limits in a select and then conditionally inserting (which would be racey).
        //
        // from https://www.postgresql.org/docs/current/sql-insert.html
        //   "INSERT inserts new rows into a table. One can insert one or more rows specified by
        //   value expressions, or zero or more rows resulting from a query."
        // By using SELECT rather than VALUES it will insert zero rows if it finds a null in the
        // subquery, i.e. if count >= max_tables. fetch_one() will return a RowNotFound error if
        // nothing was inserted. Not pretty!
        let table = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
SELECT $1, id, $2 FROM (
    SELECT namespace.id AS id, max_tables, COUNT(table_name.id) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    WHERE namespace.id = $3
    GROUP BY namespace.max_tables, table_name.namespace_id, namespace.id
) AS get_count WHERE count < max_tables
RETURNING *;
        "#,
        )
        .bind(name) // $1
        .bind(partition_template) // $2
        .bind(namespace_id) // $3
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::TableCreateLimitError {
                table_name: name.to_string(),
                namespace_id,
            },
            _ => {
                if is_unique_violation(&e) {
                    Error::TableNameExists {
                        name: name.to_string(),
                        namespace_id,
                    }
                } else if is_fk_violation(&e) {
                    Error::ForeignKeyViolation { source: e }
                } else {
                    Error::SqlxError { source: e }
                }
            }
        })?;

        // Partitioning is only supported for tags, so create tag columns for all `TagValue`
        // partition template parts. It's important this happens within the table creation
        // transaction so that there isn't a possibility of a concurrent write creating these
        // columns with an unsupported type.
        for template_part in table.partition_template.parts() {
            if let TemplatePart::TagValue(tag_name) = template_part {
                insert_column_with_connection(&mut *tx, tag_name, table.id, ColumnType::Tag)
                    .await?;
            }
        }

        tx.commit()
            .await
            .map_err(|source| Error::FailedToCommit { source })?;

        Ok(table)
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
        insert_column_with_connection(self.inner.get_mut(), name, table_id, column_type).await
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
}

// We can't use [`Partition`], as uses Vec<String> which the Sqlite
// driver cannot serialise

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct PartitionPod {
    id: PartitionId,
    hash_id: Option<PartitionHashId>,
    table_id: TableId,
    partition_key: PartitionKey,
    sort_key: Json<Vec<String>>,
    sort_key_ids: Option<Json<Vec<i64>>>,
    new_file_at: Option<Timestamp>,
}

impl From<PartitionPod> for Partition {
    fn from(value: PartitionPod) -> Self {
        let sort_key_ids = value
            .sort_key_ids
            .map(|sort_key_ids| SortedColumnSet::from(sort_key_ids.0));

        Self::new_with_hash_id_from_sqlite_catalog_only(
            value.id,
            value.hash_id,
            value.table_id,
            value.partition_key,
            value.sort_key.0,
            sort_key_ids,
            value.new_file_at,
        )
    }
}

#[async_trait]
impl PartitionRepo for SqliteTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        // Note: since sort_key is now an array, we must explicitly insert '{}' which is an empty
        // array rather than NULL which sqlx will throw `UnexpectedNullError` while is is doing
        // `ColumnDecode`

        let hash_id = PartitionHashId::new(table_id, &key);

        let v = sqlx::query_as::<_, PartitionPod>(
            r#"
INSERT INTO partition
    (partition_key, shard_id, table_id, hash_id, sort_key, sort_key_ids)
VALUES
    ($1, $2, $3, $4, '[]', '[]')
ON CONFLICT (table_id, partition_key)
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
        )
        .bind(key) // $1
        .bind(TRANSITION_SHARD_ID) // $2
        .bind(table_id) // $3
        .bind(&hash_id) // $4
        .fetch_one(self.inner.get_mut())
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(v.into())
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE id = $1;
            "#,
        )
        .bind(partition_id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition.into()))
    }

    async fn get_by_id_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<Partition>> {
        // We use a JSON-based "IS IN" check.
        let ids: Vec<_> = partition_ids.iter().map(|p| p.get()).collect();

        sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE id IN (SELECT value FROM json_each($1));
            "#,
        )
        .bind(Json(&ids[..])) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map(|vals| vals.into_iter().map(Partition::from).collect())
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn get_by_hash_id(
        &mut self,
        partition_hash_id: &PartitionHashId,
    ) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE hash_id = $1;
            "#,
        )
        .bind(partition_hash_id) // $1
        .fetch_one(self.inner.get_mut())
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition.into()))
    }

    async fn get_by_hash_id_batch(
        &mut self,
        partition_hash_ids: &[&PartitionHashId],
    ) -> Result<Vec<Partition>> {
        // We use a JSON-based "IS IN" check.
        let ids: Vec<_> = partition_hash_ids
            .iter()
            .map(|id| {
                // convert partiion hash ID to uppercase hex string
                let bytes = id.as_bytes();
                let mut s = String::with_capacity(bytes.len() * 2);
                for b in bytes {
                    write!(&mut s, "{:02X}", b).expect("never fails");
                }
                s
            })
            .collect();

        sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE hex(hash_id) IN (SELECT value FROM json_each($1));
            "#,
        )
        .bind(Json(&ids[..])) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map(|vals| vals.into_iter().map(Partition::from).collect())
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
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
        partition_id: &TransitionPartitionId,
        old_sort_key: Option<Vec<String>>,
        new_sort_key: &[&str],
        new_sort_key_ids: &SortedColumnSet,
    ) -> Result<Partition, CasFailure<(Vec<String>, Option<SortedColumnSet>)>> {
        verify_sort_key_length(new_sort_key, new_sort_key_ids);

        let old_sort_key = old_sort_key.unwrap_or_default();
        let raw_new_sort_key_ids: Vec<_> = new_sort_key_ids.iter().map(|cid| cid.get()).collect();

        // This `match` will go away when all partitions have hash IDs in the database.
        let query = match partition_id {
            TransitionPartitionId::Deterministic(hash_id) => sqlx::query_as::<_, PartitionPod>(
                r#"
UPDATE partition
SET sort_key = $1, sort_key_ids = $4
WHERE hash_id = $2 AND sort_key = $3
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
            )
            .bind(Json(new_sort_key)) // $1
            .bind(hash_id) // $2
            .bind(Json(&old_sort_key)) // $3
            .bind(Json(&raw_new_sort_key_ids)), // $4
            TransitionPartitionId::Deprecated(id) => sqlx::query_as::<_, PartitionPod>(
                r#"
UPDATE partition
SET sort_key = $1, sort_key_ids = $4
WHERE id = $2 AND sort_key = $3
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
            )
            .bind(Json(new_sort_key)) // $1
            .bind(id) // $2
            .bind(Json(&old_sort_key)) // $3
            .bind(Json(&raw_new_sort_key_ids)), // $4
        };

        let res = query.fetch_one(self.inner.get_mut()).await;

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
                let partition = crate::partition_lookup(self, partition_id)
                    .await
                    .map_err(CasFailure::QueryError)?
                    .ok_or(CasFailure::QueryError(Error::PartitionNotFound {
                        id: partition_id.clone(),
                    }))?;
                return Err(CasFailure::ValueMismatch((
                    partition.sort_key,
                    partition.sort_key_ids,
                )));
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

    async fn get_in_skipped_compactions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let ids = partition_ids.iter().map(|p| p.get()).collect::<Vec<_>>();
        let rec = sqlx::query_as::<sqlx::sqlite::Sqlite, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id IN (SELECT value FROM json_each($1));"#,
        )
        .bind(Json(&ids[..]))
        .fetch_all(self.inner.get_mut())
        .await;

        let skipped_partition_records = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(skipped_partition_records)
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

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
ORDER BY id DESC
LIMIT $1;
        "#,
        )
        .bind(n as i64) // $1
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn partitions_new_file_between(
        &mut self,
        minimum_time: Timestamp,
        maximum_time: Option<Timestamp>,
    ) -> Result<Vec<PartitionId>> {
        let sql = format!(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            WHERE p.new_file_at > $1
            {}
            "#,
            maximum_time
                .map(|_| "AND p.new_file_at < $2")
                .unwrap_or_default()
        );

        sqlx::query_as(&sql)
            .bind(minimum_time) // $1
            .bind(maximum_time) // $2
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        Ok(sqlx::query_as::<_, PartitionPod>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE hash_id IS NULL
ORDER BY id DESC;
        "#,
        )
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
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
    namespace_id: NamespaceId,
    table_id: TableId,
    #[sqlx(flatten)]
    partition_id: TransitionPartitionId,
    object_store_id: Uuid,
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
            namespace_id: value.namespace_id,
            table_id: value.table_id,
            partition_id: value.partition_id,
            object_store_id: value.object_store_id,
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
        let executor = self.inner.get_mut();
        create_parquet_file(executor, parquet_file_params).await
    }

    async fn list_all(&mut self) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT parquet_file.id, parquet_file.namespace_id, parquet_file.table_id,
       parquet_file.partition_id, parquet_file.partition_hash_id, parquet_file.object_store_id,
       parquet_file.min_time, parquet_file.max_time, parquet_file.to_delete,
       parquet_file.file_size_bytes, parquet_file.row_count, parquet_file.compaction_level,
       parquet_file.created_at, parquet_file.column_set, parquet_file.max_l0_created_at
FROM parquet_file;
             "#,
        )
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?
        .into_iter()
        .map(Into::into)
        .collect())
    }

    async fn flag_for_delete_by_retention(&mut self) -> Result<Vec<ParquetFileId>> {
        let flagged_at = Timestamp::from(self.time_provider.now());
        // TODO - include check of table retention period once implemented
        let flagged = sqlx::query(
            r#"
WITH parquet_file_ids as (
    SELECT parquet_file.id
    FROM namespace, parquet_file
    WHERE namespace.retention_period_ns IS NOT NULL
    AND parquet_file.to_delete IS NULL
    AND parquet_file.max_time < $1 - namespace.retention_period_ns
    AND namespace.id = parquet_file.namespace_id
    LIMIT $2
)
UPDATE parquet_file
SET to_delete = $1
WHERE id IN (SELECT id FROM parquet_file_ids)
RETURNING id;
            "#,
        )
        .bind(flagged_at) // $1
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let flagged = flagged.into_iter().map(|row| row.get("id")).collect();
        Ok(flagged)
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT parquet_file.id, parquet_file.namespace_id, parquet_file.table_id,
       parquet_file.partition_id, parquet_file.partition_hash_id, parquet_file.object_store_id,
       parquet_file.min_time, parquet_file.max_time, parquet_file.to_delete,
       parquet_file.file_size_bytes, parquet_file.row_count, parquet_file.compaction_level,
       parquet_file.created_at, parquet_file.column_set, parquet_file.max_l0_created_at
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
        Ok(sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, namespace_id, table_id, partition_id, partition_hash_id, object_store_id,
       min_time, max_time, to_delete, file_size_bytes,
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
        .bind(MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE) // $2
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let deleted = deleted.into_iter().map(|row| row.get("id")).collect();
        Ok(deleted)
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: &TransitionPartitionId,
    ) -> Result<Vec<ParquetFile>> {
        // This `match` will go away when all partitions have hash IDs in the database.
        let query = match partition_id {
            TransitionPartitionId::Deterministic(hash_id) => sqlx::query_as::<_, ParquetFilePod>(
                r#"
SELECT parquet_file.id, namespace_id, parquet_file.table_id, partition_id, partition_hash_id,
       object_store_id, min_time, max_time, parquet_file.to_delete, file_size_bytes, row_count,
       compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
INNER JOIN partition
ON partition.id = parquet_file.partition_id OR partition.hash_id = parquet_file.partition_hash_id
WHERE partition.hash_id = $1
  AND parquet_file.to_delete IS NULL;
        "#,
            )
            .bind(hash_id), // $1
            TransitionPartitionId::Deprecated(id) => sqlx::query_as::<_, ParquetFilePod>(
                r#"
SELECT parquet_file.id, namespace_id, parquet_file.table_id, partition_id, partition_hash_id,
       object_store_id, min_time, max_time, parquet_file.to_delete, file_size_bytes, row_count,
       compaction_level, created_at, column_set, max_l0_created_at
FROM parquet_file
INNER JOIN partition
ON partition.id = parquet_file.partition_id OR partition.hash_id = parquet_file.partition_hash_id
WHERE partition.id = $1
  AND parquet_file.to_delete IS NULL;
        "#,
            )
            .bind(id), // $1
        };

        Ok(query
            .fetch_all(self.inner.get_mut())
            .await
            .map_err(|e| Error::SqlxError { source: e })?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        let rec = sqlx::query_as::<_, ParquetFilePod>(
            r#"
SELECT id, namespace_id, table_id, partition_id, partition_hash_id, object_store_id, min_time,
       max_time, to_delete, file_size_bytes, row_count, compaction_level, created_at, column_set,
       max_l0_created_at
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

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>> {
        let in_value = object_store_ids
            .into_iter()
            // use a sqlite blob literal
            .map(|id| format!("X'{}'", id.simple()))
            .collect::<Vec<String>>()
            .join(",");

        sqlx::query(&format!(
            "
SELECT object_store_id
FROM parquet_file
WHERE object_store_id IN ({v});",
            v = in_value
        ))
        .map(|slr: SqliteRow| slr.get::<Uuid, _>("object_store_id"))
        // limitation of sqlx: will not bind arrays
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        .fetch_all(self.inner.get_mut())
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn create_upgrade_delete(
        &mut self,
        delete: &[ParquetFileId],
        upgrade: &[ParquetFileId],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>> {
        let delete_set = delete.iter().copied().collect::<HashSet<_>>();
        let upgrade_set = upgrade.iter().copied().collect::<HashSet<_>>();

        assert!(
            delete_set.is_disjoint(&upgrade_set),
            "attempted to upgrade a file scheduled for delete"
        );
        let mut tx = self
            .inner
            .get_mut()
            .pool
            .begin()
            .await
            .map_err(|e| Error::StartTransaction { source: e })?;

        for id in delete {
            let marked_at = Timestamp::from(self.time_provider.now());
            flag_for_delete(&mut *tx, *id, marked_at).await?;
        }

        update_compaction_level(&mut *tx, upgrade, target_level).await?;

        let mut ids = Vec::with_capacity(create.len());
        for file in create {
            let res = create_parquet_file(&mut *tx, file.clone()).await?;
            ids.push(res.id);
        }
        tx.commit()
            .await
            .map_err(|e| Error::FailedToCommit { source: e })?;

        Ok(ids)
    }
}

// The following three functions are helpers to the create_upgrade_delete method.
// They are also used by the respective create/flag_for_delete/update_compaction_level methods.
async fn create_parquet_file<'q, E>(
    executor: E,
    parquet_file_params: ParquetFileParams,
) -> Result<ParquetFile>
where
    E: Executor<'q, Database = Sqlite>,
{
    let ParquetFileParams {
        namespace_id,
        table_id,
        partition_id,
        object_store_id,
        min_time,
        max_time,
        file_size_bytes,
        row_count,
        compaction_level,
        created_at,
        column_set,
        max_l0_created_at,
    } = parquet_file_params;

    let (partition_id, partition_hash_id) = match partition_id {
        TransitionPartitionId::Deterministic(hash_id) => (None, Some(hash_id)),
        TransitionPartitionId::Deprecated(id) => (Some(id), None),
    };
    let res = sqlx::query_as::<_, ParquetFilePod>(
        r#"
INSERT INTO parquet_file (
    shard_id, table_id, partition_id, partition_hash_id, object_store_id,
    min_time, max_time, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING
    id, table_id, partition_id, partition_hash_id, object_store_id, min_time, max_time, to_delete,
    file_size_bytes, row_count, compaction_level, created_at, namespace_id, column_set,
    max_l0_created_at;
        "#,
    )
    .bind(TRANSITION_SHARD_ID) // $1
    .bind(table_id) // $2
    .bind(partition_id) // $3
    .bind(partition_hash_id.as_ref()) // $4
    .bind(object_store_id) // $5
    .bind(min_time) // $6
    .bind(max_time) // $7
    .bind(file_size_bytes) // $8
    .bind(row_count) // $9
    .bind(compaction_level) // $10
    .bind(created_at) // $11
    .bind(namespace_id) // $12
    .bind(from_column_set(&column_set)) // $13
    .bind(max_l0_created_at) // $14
    .fetch_one(executor)
    .await;

    let rec = res.map_err(|e| {
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

async fn flag_for_delete<'q, E>(executor: E, id: ParquetFileId, marked_at: Timestamp) -> Result<()>
where
    E: Executor<'q, Database = Sqlite>,
{
    let query = sqlx::query(r#"UPDATE parquet_file SET to_delete = $1 WHERE id = $2;"#)
        .bind(marked_at) // $1
        .bind(id); // $2

    query
        .execute(executor)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

    Ok(())
}

async fn update_compaction_level<'q, E>(
    executor: E,
    parquet_file_ids: &[ParquetFileId],
    compaction_level: CompactionLevel,
) -> Result<Vec<ParquetFileId>>
where
    E: Executor<'q, Database = Sqlite>,
{
    // We use a JSON-based "IS IN" check.
    let ids: Vec<_> = parquet_file_ids.iter().map(|p| p.get()).collect();
    let query = sqlx::query(
        r#"
UPDATE parquet_file
SET compaction_level = $1
WHERE id IN (SELECT value FROM json_each($2))
RETURNING id;
        "#,
    )
    .bind(compaction_level) // $1
    .bind(Json(&ids[..])); // $2
    let updated = query
        .fetch_all(executor)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

    let updated = updated.into_iter().map(|row| row.get("id")).collect();
    Ok(updated)
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
    use crate::test_helpers::{
        arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table,
    };
    use assert_matches::assert_matches;
    use data_types::partition_template::TemplatePart;
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use metric::{Attributes, DurationHistogram, Metric};
    use std::sync::Arc;

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
        let options = SqliteConnectionOptions { file_path: dsn };
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
    async fn test_partition_create_or_get_idempotent() {
        let sqlite = setup_db().await;
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut repos = sqlite.repositories().await;

        let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
        let table_id = arbitrary_table(&mut *repos, "table", &namespace).await.id;

        let key = PartitionKey::from("bananas");

        let hash_id = PartitionHashId::new(table_id, &key);

        let a = repos
            .partitions()
            .create_or_get(key.clone(), table_id)
            .await
            .expect("should create OK");

        assert_eq!(a.hash_id().unwrap(), &hash_id);

        // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent.
        let b = repos
            .partitions()
            .create_or_get(key.clone(), table_id)
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);

        // Check that the hash_id is saved in the database and is returned when queried.
        let table_partitions = sqlite
            .repositories()
            .await
            .partitions()
            .list_by_table_id(table_id)
            .await
            .unwrap();
        assert_eq!(table_partitions.len(), 1);
        assert_eq!(table_partitions[0].hash_id().unwrap(), &hash_id);

        // Test: sort_key_ids from partition_create_or_get_idempotent
        assert!(table_partitions[0].sort_key_ids().unwrap().is_empty());
    }

    #[tokio::test]
    async fn existing_partitions_without_hash_id() {
        let sqlite: SqliteCatalog = setup_db().await;
        let pool = sqlite.pool.clone();
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut repos = sqlite.repositories().await;

        let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
        let table = arbitrary_table(&mut *repos, "table", &namespace).await;
        let table_id = table.id;
        let key = PartitionKey::from("francis-scott-key-key");

        // Create a partition record in the database that has `NULL` for its `hash_id`
        // value, which is what records existing before the migration adding that column will have.
        sqlx::query(
            r#"
INSERT INTO partition
    (partition_key, shard_id, table_id, sort_key, sort_key_ids)
VALUES
    ($1, $2, $3, '[]', '[]')
ON CONFLICT (table_id, partition_key)
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
        )
        .bind(&key) // $1
        .bind(TRANSITION_SHARD_ID) // $2
        .bind(table_id) // $3
        .fetch_one(&pool)
        .await
        .unwrap();

        // Check that the hash_id being null in the database doesn't break querying for partitions.
        let table_partitions = repos.partitions().list_by_table_id(table_id).await.unwrap();
        assert_eq!(table_partitions.len(), 1);
        let partition = &table_partitions[0];

        // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent
        // and that the hash_id still doesn't get set.
        let inserted_again = repos
            .partitions()
            .create_or_get(key, table_id)
            .await
            .expect("idempotent write should succeed");

        // Test: sort_key_ids from freshly insert with empty value
        assert!(inserted_again.sort_key_ids().unwrap().is_empty());

        assert_eq!(partition, &inserted_again);

        // Create a Parquet file record in this partition to ensure we don't break new data
        // ingestion for old-style partitions
        let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, partition);
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();
        assert_matches!(
            parquet_file.partition_id,
            TransitionPartitionId::Deprecated(_)
        );

        // Add a partition record WITH a hash ID
        repos
            .partitions()
            .create_or_get(PartitionKey::from("Something else"), table_id)
            .await
            .unwrap();

        // Ensure we can list only the old-style partitions
        let old_style_partitions = repos.partitions().list_old_style().await.unwrap();
        assert_eq!(old_style_partitions.len(), 1);
        assert_eq!(old_style_partitions[0].id, partition.id);
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
                    let mut repos = sqlite.repositories().await;

                    let namespace = arbitrary_namespace(&mut *repos, "ns4")
                        .await;
                    let table_id = arbitrary_table(&mut *repos, "table", &namespace)
                        .await
                        .id;

                    $(
                        let mut insert = HashMap::new();
                        $(
                            insert.insert($col_name, $col_type);
                        )+

                        let got = repos
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
        let mut repos = sqlite.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
        let table = arbitrary_table(&mut *repos, "table", &namespace).await;
        let key = "bananas";
        let partition = repos
            .partitions()
            .create_or_get(key.into(), table.id)
            .await
            .unwrap();

        // parquet file to create- all we care about here is the size
        let mut p1 = arbitrary_parquet_file_params(&namespace, &table, &partition);
        p1.file_size_bytes = 1337;
        let f1 = repos
            .parquet_files()
            .create(p1.clone())
            .await
            .expect("create parquet file should succeed");
        // insert the same again with a different size; we should then have 3x1337 as total file
        // size
        p1.object_store_id = Uuid::new_v4();
        p1.file_size_bytes *= 2;
        let _f2 = repos
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
        repos
            .parquet_files()
            .create_upgrade_delete(&[f1.id], &[], &[], CompactionLevel::Initial)
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
        let older_than = p1.created_at + 1;
        repos
            .parquet_files()
            .delete_old_ids_only(older_than)
            .await
            .expect("parquet file deletion should succeed");
        let total_file_size_bytes: i64 =
            sqlx::query_scalar("SELECT total_file_size_bytes FROM billing_summary;")
                .fetch_one(&pool)
                .await
                .expect("fetch total file size failed");
        assert_eq!(total_file_size_bytes, 1337 * 2);
    }

    #[tokio::test]
    async fn namespace_partition_template_null_is_the_default_in_the_database() {
        let sqlite = setup_db().await;
        let pool = sqlite.pool.clone();
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut repos = sqlite.repositories().await;

        let namespace_name = "apples";

        // Create a namespace record in the database that has `NULL` for its `partition_template`
        // value, which is what records existing before the migration adding that column will have.
        let insert_null_partition_template_namespace = sqlx::query(
            r#"
INSERT INTO namespace (
    name, topic_id, query_pool_id, retention_period_ns, max_tables, partition_template
)
VALUES ( $1, $2, $3, $4, $5, NULL )
RETURNING id, name, retention_period_ns, max_tables, max_columns_per_table, deleted_at,
          partition_template;
            "#,
        )
        .bind(namespace_name) // $1
        .bind(SHARED_TOPIC_ID) // $2
        .bind(SHARED_QUERY_POOL_ID) // $3
        .bind(None::<Option<i64>>) // $4
        .bind(DEFAULT_MAX_TABLES); // $5

        insert_null_partition_template_namespace
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_namespace = repos
            .namespaces()
            .get_by_name(namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap()
            .unwrap();
        // When fetching this namespace from the database, the `FromRow` impl should set its
        // `partition_template` to the default.
        assert_eq!(
            lookup_namespace.partition_template,
            NamespacePartitionTemplateOverride::default()
        );

        // When creating a namespace through the catalog functions without specifying a custom
        // partition template,
        let created_without_custom_template = repos
            .namespaces()
            .create(
                &"lemons".try_into().unwrap(),
                None, // no partition template
                None,
                None,
            )
            .await
            .unwrap();

        // it should have the default template in the application,
        assert_eq!(
            created_without_custom_template.partition_template,
            NamespacePartitionTemplateOverride::default()
        );

        // and store NULL in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM namespace WHERE id = $1;")
            .bind(created_without_custom_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(created_without_custom_template.name, name);
        let partition_template: Option<NamespacePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert!(partition_template.is_none());

        // When explicitly setting a template that happens to be equal to the application default,
        // assume it's important that it's being specially requested and store it rather than NULL.
        let namespace_custom_template_name = "kumquats";
        let custom_partition_template_equal_to_default =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat(
                        "%Y-%m-%d".to_owned(),
                    )),
                }],
            })
            .unwrap();
        let namespace_custom_template = repos
            .namespaces()
            .create(
                &namespace_custom_template_name.try_into().unwrap(),
                Some(custom_partition_template_equal_to_default.clone()),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            namespace_custom_template.partition_template,
            custom_partition_template_equal_to_default
        );
        let record = sqlx::query("SELECT name, partition_template FROM namespace WHERE id = $1;")
            .bind(namespace_custom_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(namespace_custom_template.name, name);
        let partition_template: Option<NamespacePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert_eq!(
            partition_template.unwrap(),
            custom_partition_template_equal_to_default
        );
    }

    #[tokio::test]
    async fn table_partition_template_null_is_the_default_in_the_database() {
        let sqlite = setup_db().await;
        let pool = sqlite.pool.clone();
        let sqlite: Arc<dyn Catalog> = Arc::new(sqlite);
        let mut repos = sqlite.repositories().await;

        let namespace_default_template_name = "oranges";
        let namespace_default_template = repos
            .namespaces()
            .create(
                &namespace_default_template_name.try_into().unwrap(),
                None, // no partition template
                None,
                None,
            )
            .await
            .unwrap();

        let namespace_custom_template_name = "limes";
        let namespace_custom_template = repos
            .namespaces()
            .create(
                &namespace_custom_template_name.try_into().unwrap(),
                Some(
                    NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                        parts: vec![proto::TemplatePart {
                            part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                        }],
                    })
                    .unwrap(),
                ),
                None,
                None,
            )
            .await
            .unwrap();

        // In a namespace that also has a NULL template, create a table record in the database that
        // has `NULL` for its `partition_template` value, which is what records existing before the
        // migration adding that column will have.
        let table_name = "null_template";
        let insert_null_partition_template_table = sqlx::query(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
VALUES ( $1, $2, NULL )
RETURNING *;
            "#,
        )
        .bind(table_name) // $1
        .bind(namespace_default_template.id); // $2

        insert_null_partition_template_table
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_table = repos
            .tables()
            .get_by_namespace_and_name(namespace_default_template.id, table_name)
            .await
            .unwrap()
            .unwrap();
        // When fetching this table from the database, the `FromRow` impl should set its
        // `partition_template` to the system default (because the namespace didn't have a template
        // either).
        assert_eq!(
            lookup_table.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // In a namespace that has a custom template, create a table record in the database that
        // has `NULL` for its `partition_template` value.
        //
        // THIS ACTUALLY SHOULD BE IMPOSSIBLE because:
        //
        // * Namespaces have to exist before tables
        // * `partition_tables` are immutable on both namespaces and tables
        // * When the migration adding the `partition_table` column is deployed, namespaces can
        //   begin to be created with `partition_templates`
        // * *Then* tables can be created with `partition_templates` or not
        // * When tables don't get a custom table partition template but their namespace has one,
        //   their database record will get the namespace partition template.
        //
        // In other words, table `partition_template` values in the database is allowed to possibly
        // be `NULL` IFF their namespace's `partition_template` is `NULL`.
        //
        // That said, this test creates this hopefully-impossible scenario to ensure that the
        // defined, expected behavior if a table record somehow exists in the database with a `NULL`
        // `partition_template` value is that it will have the application default partition
        // template *even if the namespace `partition_template` is not null*.
        let table_name = "null_template";
        let insert_null_partition_template_table = sqlx::query(
            r#"
INSERT INTO table_name ( name, namespace_id, partition_template )
VALUES ( $1, $2, NULL )
RETURNING *;
            "#,
        )
        .bind(table_name) // $1
        .bind(namespace_custom_template.id); // $2

        insert_null_partition_template_table
            .fetch_one(&pool)
            .await
            .unwrap();

        let lookup_table = repos
            .tables()
            .get_by_namespace_and_name(namespace_custom_template.id, table_name)
            .await
            .unwrap()
            .unwrap();
        // When fetching this table from the database, the `FromRow` impl should set its
        // `partition_template` to the system default *even though the namespace has a
        // template*, because this should be impossible as detailed above.
        assert_eq!(
            lookup_table.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // # Table template false, namespace template true
        //
        // When creating a table through the catalog functions *without* a custom table template in
        // a namespace *with* a custom partition template,
        let table_no_template_with_namespace_template = repos
            .tables()
            .create(
                "pomelo",
                TablePartitionTemplateOverride::try_new(
                    None, // no custom partition template
                    &namespace_custom_template.partition_template,
                )
                .unwrap(),
                namespace_custom_template.id,
            )
            .await
            .unwrap();

        // it should have the namespace's template
        assert_eq!(
            table_no_template_with_namespace_template.partition_template,
            TablePartitionTemplateOverride::try_new(
                None,
                &namespace_custom_template.partition_template
            )
            .unwrap()
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_no_template_with_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_no_template_with_namespace_template.name, name);
        let partition_template: Option<TablePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert_eq!(
            partition_template.unwrap(),
            TablePartitionTemplateOverride::try_new(
                None,
                &namespace_custom_template.partition_template
            )
            .unwrap()
        );

        // # Table template true, namespace template false
        //
        // When creating a table through the catalog functions *with* a custom table template in
        // a namespace *without* a custom partition template,
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("chemical".into())),
            }],
        };
        let table_with_template_no_namespace_template = repos
            .tables()
            .create(
                "tangerine",
                TablePartitionTemplateOverride::try_new(
                    Some(custom_table_template), // with custom partition template
                    &namespace_default_template.partition_template,
                )
                .unwrap(),
                namespace_default_template.id,
            )
            .await
            .unwrap();

        // it should have the custom table template
        let table_template_parts: Vec<_> = table_with_template_no_namespace_template
            .partition_template
            .parts()
            .collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "chemical"
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_with_template_no_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_with_template_no_namespace_template.name, name);
        let partition_template = record
            .try_get::<Option<TablePartitionTemplateOverride>, _>("partition_template")
            .unwrap()
            .unwrap();
        let table_template_parts: Vec<_> = partition_template.parts().collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "chemical"
        );

        // # Table template true, namespace template true
        //
        // When creating a table through the catalog functions *with* a custom table template in
        // a namespace *with* a custom partition template,
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("vegetable".into())),
            }],
        };
        let table_with_template_with_namespace_template = repos
            .tables()
            .create(
                "nectarine",
                TablePartitionTemplateOverride::try_new(
                    Some(custom_table_template), // with custom partition template
                    &namespace_custom_template.partition_template,
                )
                .unwrap(),
                namespace_custom_template.id,
            )
            .await
            .unwrap();

        // it should have the custom table template
        let table_template_parts: Vec<_> = table_with_template_with_namespace_template
            .partition_template
            .parts()
            .collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "vegetable"
        );

        // and store that value in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_with_template_with_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_with_template_with_namespace_template.name, name);
        let partition_template = record
            .try_get::<Option<TablePartitionTemplateOverride>, _>("partition_template")
            .unwrap()
            .unwrap();
        let table_template_parts: Vec<_> = partition_template.parts().collect();
        assert_eq!(table_template_parts.len(), 1);
        assert_matches!(
            table_template_parts[0],
            TemplatePart::TagValue(tag) if tag == "vegetable"
        );

        // # Table template false, namespace template false
        //
        // When creating a table through the catalog functions *without* a custom table template in
        // a namespace *without* a custom partition template,
        let table_no_template_no_namespace_template = repos
            .tables()
            .create(
                "grapefruit",
                TablePartitionTemplateOverride::try_new(
                    None, // no custom partition template
                    &namespace_default_template.partition_template,
                )
                .unwrap(),
                namespace_default_template.id,
            )
            .await
            .unwrap();

        // it should have the default template in the application,
        assert_eq!(
            table_no_template_no_namespace_template.partition_template,
            TablePartitionTemplateOverride::default()
        );

        // and store NULL in the database record.
        let record = sqlx::query("SELECT name, partition_template FROM table_name WHERE id = $1;")
            .bind(table_no_template_no_namespace_template.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let name: String = record.try_get("name").unwrap();
        assert_eq!(table_no_template_no_namespace_template.name, name);
        let partition_template: Option<TablePartitionTemplateOverride> =
            record.try_get("partition_template").unwrap();
        assert!(partition_template.is_none());
    }
}
