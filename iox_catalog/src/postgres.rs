//! A Postgres backed implementation of the Catalog

use crate::{
    interface::{
        sealed::TransactionFinalize, Catalog, ColumnRepo, ColumnUpsertRequest, Error,
        KafkaTopicRepo, NamespaceRepo, ParquetFileRepo, PartitionRepo, ProcessedTombstoneRepo,
        QueryPoolRepo, RepoCollection, Result, SequencerRepo, TablePersistInfo, TableRepo,
        TombstoneRepo, Transaction,
    },
    metrics::MetricDecorator,
};
use async_trait::async_trait;
use data_types2::{
    Column, ColumnType, KafkaPartition, KafkaTopic, KafkaTopicId, Namespace, NamespaceId,
    ParquetFile, ParquetFileId, ParquetFileParams, Partition, PartitionId, PartitionInfo,
    ProcessedTombstone, QueryPool, QueryPoolId, SequenceNumber, Sequencer, SequencerId, Table,
    TableId, TablePartition, Timestamp, Tombstone, TombstoneId,
};
use observability_deps::tracing::{info, warn};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, Acquire, Executor, Postgres, Row};
use sqlx_hotswap_pool::HotSwapPool;
use std::{collections::HashMap, sync::Arc, time::Duration};
use time::{SystemProvider, TimeProvider};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// the default schema name to use in Postgres
pub const SCHEMA_NAME: &str = "iox_catalog";

/// The file pointed to by a `dsn-file://` DSN is polled for change every `HOTSWAP_POLL_INTERVAL`.
const HOTSWAP_POLL_INTERVAL: std::time::Duration = Duration::from_secs(5);

static MIGRATOR: Migrator = sqlx::migrate!();

/// PostgreSQL catalog.
#[derive(Debug)]
pub struct PostgresCatalog {
    metrics: Arc<metric::Registry>,
    pool: HotSwapPool<Postgres>,
    schema_name: String,
    time_provider: Arc<dyn TimeProvider>,
}

// struct to get return value from "select count(*) ..." query
#[derive(sqlx::FromRow)]
struct Count {
    count: i64,
}

impl PostgresCatalog {
    /// Connect to the catalog store.
    pub async fn connect(
        app_name: &str,
        schema_name: &str,
        dsn: &str,
        max_conns: u32,
        metrics: Arc<metric::Registry>,
    ) -> Result<Self> {
        let schema_name = schema_name.to_string();
        let pool = new_pool(
            app_name,
            &schema_name,
            dsn,
            max_conns,
            HOTSWAP_POLL_INTERVAL,
        )
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Self {
            pool,
            metrics,
            schema_name,
            time_provider: Arc::new(SystemProvider::new()),
        })
    }
}

/// transaction for [`PostgresCatalog`].
#[derive(Debug)]
pub struct PostgresTxn {
    inner: PostgresTxnInner,
    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum PostgresTxnInner {
    Txn(Option<sqlx::Transaction<'static, Postgres>>),
    Oneshot(HotSwapPool<Postgres>),
}

impl<'c> Executor<'c> for &'c mut PostgresTxnInner {
    type Database = Postgres;

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
            PostgresTxnInner::Txn(txn) => {
                txn.as_mut().expect("Not yet finalized").fetch_many(query)
            }
            PostgresTxnInner::Oneshot(pool) => pool.fetch_many(query),
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
            PostgresTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .fetch_optional(query),
            PostgresTxnInner::Oneshot(pool) => pool.fetch_optional(query),
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
            PostgresTxnInner::Txn(txn) => txn
                .as_mut()
                .expect("Not yet finalized")
                .prepare_with(sql, parameters),
            PostgresTxnInner::Oneshot(pool) => pool.prepare_with(sql, parameters),
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
            PostgresTxnInner::Txn(txn) => txn.as_mut().expect("Not yet finalized").describe(sql),
            PostgresTxnInner::Oneshot(pool) => pool.describe(sql),
        }
    }
}

impl Drop for PostgresTxn {
    fn drop(&mut self) {
        if let PostgresTxnInner::Txn(Some(_)) = self.inner {
            warn!("Dropping PostgresTxn w/o finalizing (commit or abort)");

            // SQLx ensures that the inner transaction enqueues a rollback when it is dropped, so
            // we don't need to spawn a task here to call `rollback` manually.
        }
    }
}

#[async_trait]
impl TransactionFinalize for PostgresTxn {
    async fn commit_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            PostgresTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .commit()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            PostgresTxnInner::Oneshot(_) => {
                panic!("cannot commit oneshot");
            }
        }
    }

    async fn abort_inplace(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            PostgresTxnInner::Txn(txn) => txn
                .take()
                .expect("Not yet finalized")
                .rollback()
                .await
                .map_err(|e| Error::SqlxError { source: e }),
            PostgresTxnInner::Oneshot(_) => {
                panic!("cannot abort oneshot");
            }
        }
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn setup(&self) -> Result<(), Error> {
        // We need to create the schema if we're going to set it as the first item of the
        // search_path otherwise when we run the sqlx migration scripts for the first time, sqlx
        // will create the `_sqlx_migrations` table in the public namespace (the only namespace
        // that exists), but the second time it will create it in the `<schema_name>` namespace and
        // re-run all the migrations without skipping the ones already applied (see #3893).
        //
        // This makes the migrations/20210217134322_create_schema.sql step unnecessary; we need to
        // keep that file because migration files are immutable.
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {};", &self.schema_name);
        self.pool
            .execute(sqlx::query(&create_schema_query))
            .await
            .map_err(|e| Error::Setup { source: e })?;

        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Setup { source: e.into() })?;

        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Box::new(MetricDecorator::new(
            PostgresTxn {
                inner: PostgresTxnInner::Txn(Some(transaction)),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        )))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(MetricDecorator::new(
            PostgresTxn {
                inner: PostgresTxnInner::Oneshot(self.pool.clone()),
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        ))
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

/// Creates a new [`sqlx::Pool`] from a database config and an explicit DSN.
///
/// This function doesn't support the IDPE specific `dsn-file://` uri scheme.
async fn new_raw_pool(
    app_name: &str,
    schema_name: &str,
    dsn: &str,
    max_conns: u32,
) -> Result<sqlx::Pool<Postgres>, sqlx::Error> {
    let app_name = app_name.to_owned();
    let app_name2 = app_name.clone(); // just to log below
    let schema_name = schema_name.to_owned();
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(max_conns)
        .connect_timeout(CONNECT_TIMEOUT)
        .idle_timeout(IDLE_TIMEOUT)
        .test_before_acquire(true)
        .after_connect(move |c| {
            let app_name = app_name.to_owned();
            let schema_name = schema_name.to_owned();
            Box::pin(async move {
                // Tag the connection with the provided application name, while allowing it to
                // be overriden from the connection string (aka DSN).
                // If current_application_name is empty here it means the application name wasn't
                // set as part of the DSN, and we can set it explictly.
                // Recall that this block is running on connection, not when creating the pool!
                let current_application_name: String =
                    sqlx::query_scalar("SELECT current_setting('application_name');")
                        .fetch_one(&mut *c)
                        .await?;
                if current_application_name.is_empty() {
                    sqlx::query("SELECT set_config('application_name', $1, false);")
                        .bind(&*app_name)
                        .execute(&mut *c)
                        .await?;
                }
                let search_path_query = format!("SET search_path TO {},public;", schema_name);
                c.execute(sqlx::query(&search_path_query)).await?;
                Ok(())
            })
        })
        .connect(dsn)
        .await?;

    // Log a connection was successfully established and include the application
    // name for cross-correlation between Conductor logs & database connections.
    info!(application_name=%app_name2, "connected to config store");

    Ok(pool)
}

/// Creates a new HotSwapPool
///
/// This function understands the IDPE specific `dsn-file://` dsn uri scheme
/// and hot swaps the pool with a new sqlx::Pool when the file changes.
/// This is useful because the credentials can be rotated by infrastructure
/// agents while the service is running.
///
/// The file is polled for changes every `polling_interval`.
///
/// The pool is replaced only once the new pool is successfully created.
/// The [`new_raw_pool`] function will return a new pool only if the connection
/// is successfull (see [`sqlx::pool::PoolOptions::test_before_acquire`]).
async fn new_pool(
    app_name: &str,
    schema_name: &str,
    dsn: &str,
    max_conns: u32,
    polling_interval: Duration,
) -> Result<HotSwapPool<Postgres>, sqlx::Error> {
    let app_name: Arc<str> = Arc::from(app_name);
    let schema_name: Arc<str> = Arc::from(schema_name);
    let parsed_dsn = match get_dsn_file_path(dsn) {
        Some(filename) => std::fs::read_to_string(&filename)?,
        None => dsn.to_owned(),
    };
    let pool =
        HotSwapPool::new(new_raw_pool(&app_name, &schema_name, &parsed_dsn, max_conns).await?);

    if let Some(dsn_file) = get_dsn_file_path(dsn) {
        let pool = pool.clone();

        // TODO(mkm): return a guard that stops this background worker.
        // We create only one pool per process, but it would be cleaner to be
        // able to properly destroy the pool. If we don't kill this worker we
        // effectively keep the pool alive (since it holds a reference to the
        // Pool) and we also potentially pollute the logs with spurious warnings
        // if the dsn file disappears (this may be annoying if they show up in the test
        // logs).
        tokio::spawn(async move {
            let mut current_dsn = parsed_dsn.clone();
            loop {
                tokio::time::sleep(polling_interval).await;

                async fn try_update(
                    app_name: &str,
                    schema_name: &str,
                    current_dsn: &str,
                    dsn_file: &str,
                    max_conns: u32,
                    pool: &HotSwapPool<Postgres>,
                ) -> Result<Option<String>, sqlx::Error> {
                    let new_dsn = std::fs::read_to_string(&dsn_file)?;
                    if new_dsn == current_dsn {
                        Ok(None)
                    } else {
                        let new_pool =
                            new_raw_pool(app_name, schema_name, &new_dsn, max_conns).await?;
                        pool.replace(new_pool);
                        Ok(Some(new_dsn))
                    }
                }

                match try_update(
                    &app_name,
                    &schema_name,
                    &current_dsn,
                    &dsn_file,
                    max_conns,
                    &pool,
                )
                .await
                {
                    Ok(None) => {}
                    Ok(Some(new_dsn)) => {
                        info!("replaced hotswap pool");
                        current_dsn = new_dsn;
                    }
                    Err(e) => {
                        warn!(
                            error=%e,
                            filename=%dsn_file,
                            "not replacing hotswap pool because of an error \
                            connecting to the new DSN"
                        );
                    }
                }
            }
        });
    }

    Ok(pool)
}

// Parses a `dsn-file://` scheme, according to the rules of the IDPE kit/sql package.
//
// If the dsn matches the `dsn-file://` prefix, the prefix is removed and the rest is interpreted
// as a file name, in which case this function will return `Some(filename)`.
// Otherwise it will return None. No URI decoding is performed on the filename.
fn get_dsn_file_path(dsn: &str) -> Option<String> {
    const DSN_SCHEME: &str = "dsn-file://";
    dsn.starts_with(DSN_SCHEME)
        .then(|| dsn[DSN_SCHEME.len()..].to_owned())
}

#[async_trait]
impl RepoCollection for PostgresTxn {
    fn kafka_topics(&mut self) -> &mut dyn KafkaTopicRepo {
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

    fn sequencers(&mut self) -> &mut dyn SequencerRepo {
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
impl KafkaTopicRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<KafkaTopic> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
INSERT INTO kafka_topic ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT kafka_topic_name_unique
DO UPDATE SET name = kafka_topic.name
RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<KafkaTopic>> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
SELECT *
FROM kafka_topic
WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let kafka_topic = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(kafka_topic))
    }
}

#[async_trait]
impl QueryPoolRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT query_pool_name_unique
DO UPDATE SET name = query_pool.name
RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl NamespaceRepo for PostgresTxn {
    async fn create(
        &mut self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: KafkaTopicId,
        query_pool_id: QueryPoolId,
    ) -> Result<Namespace> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
INSERT INTO namespace ( name, retention_duration, kafka_topic_id, query_pool_id )
VALUES ( $1, $2, $3, $4 )
RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&retention_duration) // $2
        .bind(kafka_topic_id) // $3
        .bind(query_pool_id) // $4
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
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

    async fn list(&mut self) -> Result<Vec<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT *
FROM namespace;
            "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_by_id(&mut self, id: NamespaceId) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT *
FROM namespace
WHERE id = $1;
        "#,
        )
        .bind(&id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT *
FROM namespace
WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(namespace))
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
        .bind(&new_max)
        .bind(&name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFound {
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
        .bind(&new_max)
        .bind(&name)
        .fetch_one(&mut self.inner)
        .await;

        let namespace = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::NamespaceNotFound {
                name: name.to_string(),
            },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(namespace)
    }
}

#[async_trait]
impl TableRepo for PostgresTxn {
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
    SELECT namespace.id AS id, max_tables, COUNT(table_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    WHERE namespace.id = $2
    GROUP BY namespace.max_tables, table_name.namespace_id, namespace.id
) AS get_count WHERE count < max_tables
ON CONFLICT ON CONSTRAINT table_name_unique
DO UPDATE SET name = table_name.name
RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&namespace_id) // $2
        .fetch_one(&mut self.inner)
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
        .bind(&table_id) // $1
        .fetch_one(&mut self.inner)
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
        .bind(&namespace_id) // $1
        .bind(&name) // $2
        .fetch_one(&mut self.inner)
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
        .bind(&namespace_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn get_table_persist_info(
        &mut self,
        sequencer_id: SequencerId,
        namespace_id: NamespaceId,
        table_name: &str,
    ) -> Result<Option<TablePersistInfo>> {
        let rec = sqlx::query_as::<_, TablePersistInfo>(
            r#"
WITH tid as (SELECT id FROM table_name WHERE name = $2 AND namespace_id = $3)
SELECT $1 as sequencer_id, id as table_id,
       tombstone.sequence_number as tombstone_max_sequence_number
FROM tid
LEFT JOIN (
  SELECT tombstone.table_id, sequence_number
  FROM tombstone
  WHERE sequencer_id = $1 AND tombstone.table_id = (SELECT id FROM tid)
  ORDER BY sequence_number DESC
  LIMIT 1
) tombstone ON tombstone.table_id = tid.id
            "#,
        )
        .bind(&sequencer_id) // $1
        .bind(&table_name) // $2
        .bind(&namespace_id) // $3
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let info = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(info))
    }
}

#[async_trait]
impl ColumnRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        name: &str,
        table_id: TableId,
        column_type: ColumnType,
    ) -> Result<Column> {
        let ct = column_type as i16;
        let rec = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT $1, table_id, $3 FROM (
    SELECT max_columns_per_table, namespace.id, table_name.id as table_id, COUNT(column_name.*) AS count
    FROM namespace LEFT JOIN table_name ON namespace.id = table_name.namespace_id
                   LEFT JOIN column_name ON table_name.id = column_name.table_id
    WHERE table_name.id = $2
    GROUP BY namespace.max_columns_per_table, namespace.id, table_name.id
) AS get_count WHERE count < max_columns_per_table
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&table_id) // $2
        .bind(&ct) // $3
        .fetch_one(&mut self.inner)
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

        if rec.column_type != ct {
            return Err(Error::ColumnTypeMismatch {
                name: name.to_string(),
                existing: rec.name,
                new: column_type.to_string(),
            });
        }

        Ok(rec)
    }

    // Will partially fail if namespace limits on the number of columns per table are hit- all
    // inserts for that table will fail but inserts for other tables will succeed. A partial
    // failure error will be returned in this case.
    async fn create_or_get_many(
        &mut self,
        columns: &[ColumnUpsertRequest<'_>],
    ) -> Result<Vec<Column>> {
        let mut v_name = Vec::new();
        let mut v_table_id = Vec::new();
        let mut v_column_type = Vec::new();
        for c in columns {
            v_name.push(c.name.to_string());
            v_table_id.push(c.table_id.get());
            v_column_type.push(c.column_type as i16);
        }

        let out = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT column_name, input.table_id, column_type
FROM UNNEST($1, $2, $3) AS input(column_name, table_id, column_type)
INNER JOIN
(
    -- this subquery counts up the number of columns per table in the input and adds that as
    -- 'input_per_table_count' that we can use later in the WHERE clause
    SELECT table_id, COUNT(table_id) AS input_per_table_count
    FROM UNNEST($1, $2, $3) AS input2(column_name, table_id, column_type)
    GROUP BY table_id
) AS get_input_count(table_id, input_per_table_count)
ON input.table_id = get_input_count.table_id
INNER JOIN
(
    -- this subquery gets the limit, and the current count of columns per table in the database
    -- so we can later check if the inserts would go over the limit by comparing against
    -- input_per_table_count in the WHERE clause
    SELECT max_columns_per_table, namespace.id, table_name.id AS table_id, COUNT(column_name.*) AS count_existing
    FROM namespace
    LEFT JOIN table_name ON namespace.id = table_name.namespace_id
    LEFT JOIN column_name ON table_name.id = column_name.table_id
    GROUP BY namespace.max_columns_per_table, namespace.id, table_name.id
) AS get_existing_count(max_columns_per_table, namespace_id, table_id, count_existing)
ON input.table_id = get_existing_count.table_id
-- this is where the limits are actually checked
WHERE count_existing + input_per_table_count <= max_columns_per_table
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
            "#,
        )
        .bind(&v_name)
        .bind(&v_table_id)
        .bind(&v_column_type)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        if columns.len() != out.len() {
            return Err(Error::PartialColumnCreateError {
                input_column_count: columns.len(),
                inserted_column_count: out.len(),
            });
        }

        let want_types: HashMap<String, i16> = columns
            .iter()
            .map(|req| (req.name.to_string(), req.column_type as i16))
            .collect();
        out.into_iter()
            .map(|existing| {
                let wanted_type = *want_types
                    .get(&existing.name)
                    .expect("Got back a record from multi column insert that we didn't insert!");
                if existing.column_type as i16 != wanted_type {
                    return Err(Error::ColumnTypeMismatch {
                        name: existing.name,
                        existing: ColumnType::try_from(existing.column_type)
                            .unwrap()
                            .to_string(),
                        new: ColumnType::try_from(wanted_type).unwrap().to_string(),
                    });
                }
                Ok(existing)
            })
            .collect()
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl SequencerRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        topic: &KafkaTopic,
        partition: KafkaPartition,
    ) -> Result<Sequencer> {
        sqlx::query_as::<_, Sequencer>(
            r#"
INSERT INTO sequencer
    ( kafka_topic_id, kafka_partition, min_unpersisted_sequence_number )
VALUES
    ( $1, $2, 0 )
ON CONFLICT ON CONSTRAINT sequencer_unique
DO UPDATE SET kafka_topic_id = sequencer.kafka_topic_id
RETURNING *;;
        "#,
        )
        .bind(&topic.id) // $1
        .bind(&partition) // $2
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn get_by_topic_id_and_partition(
        &mut self,
        topic_id: KafkaTopicId,
        partition: KafkaPartition,
    ) -> Result<Option<Sequencer>> {
        let rec = sqlx::query_as::<_, Sequencer>(
            r#"
SELECT *
FROM sequencer
WHERE kafka_topic_id = $1
  AND kafka_partition = $2;
        "#,
        )
        .bind(topic_id) // $1
        .bind(partition) // $2
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let sequencer = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(sequencer))
    }

    async fn list(&mut self) -> Result<Vec<Sequencer>> {
        sqlx::query_as::<_, Sequencer>(r#"SELECT * FROM sequencer;"#)
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_kafka_topic(&mut self, topic: &KafkaTopic) -> Result<Vec<Sequencer>> {
        sqlx::query_as::<_, Sequencer>(r#"SELECT * FROM sequencer WHERE kafka_topic_id = $1;"#)
            .bind(&topic.id) // $1
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn update_min_unpersisted_sequence_number(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<()> {
        let _ = sqlx::query(
            r#"UPDATE sequencer SET min_unpersisted_sequence_number = $1 WHERE id = $2;"#,
        )
        .bind(&sequence_number.get()) // $1
        .bind(&sequencer_id) // $2
        .execute(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }
}

#[async_trait]
impl PartitionRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        key: &str,
        sequencer_id: SequencerId,
        table_id: TableId,
    ) -> Result<Partition> {
        let v = sqlx::query_as::<_, Partition>(
            r#"
INSERT INTO partition
    ( partition_key, sequencer_id, table_id )
VALUES
    ( $1, $2, $3 )
ON CONFLICT ON CONSTRAINT partition_key_unique
DO UPDATE SET partition_key = partition.partition_key
RETURNING *;
        "#,
        )
        .bind(key) // $1
        .bind(&sequencer_id) // $2
        .bind(&table_id) // $3
        .fetch_one(&mut self.inner)
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
        // key in the DB is mapped to the same sequencer_id the caller
        // requested.
        assert_eq!(
            v.sequencer_id, sequencer_id,
            "attempted to overwrite partition with different sequencer ID"
        );

        Ok(v)
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, Partition>(r#"SELECT * FROM partition WHERE id = $1;"#)
            .bind(&partition_id) // $1
            .fetch_one(&mut self.inner)
            .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition))
    }

    async fn list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(r#"SELECT * FROM partition WHERE sequencer_id = $1;"#)
            .bind(&sequencer_id) // $1
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_namespace(&mut self, namespace_id: NamespaceId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(
            r#"
SELECT partition.*
FROM table_name
INNER JOIN partition on partition.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn partition_info_by_id(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionInfo>> {
        let info = sqlx::query(
            r#"
SELECT namespace.name as namespace_name, table_name.name as table_name, partition.*
FROM partition
INNER JOIN table_name on table_name.id = partition.table_id
INNER JOIN namespace on namespace.id = table_name.namespace_id
WHERE partition.id = $1;
        "#,
        )
        .bind(&partition_id) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let namespace_name = info.get("namespace_name");
        let table_name = info.get("table_name");
        let partition = Partition {
            id: info.get("id"),
            sequencer_id: info.get("sequencer_id"),
            table_id: info.get("table_id"),
            partition_key: info.get("partition_key"),
            sort_key: info.get("sort_key"),
        };

        Ok(Some(PartitionInfo {
            namespace_name,
            table_name,
            partition,
        }))
    }

    async fn update_sort_key(
        &mut self,
        partition_id: PartitionId,
        sort_key: &str,
    ) -> Result<Partition> {
        let rec = sqlx::query_as::<_, Partition>(
            r#"
UPDATE partition
SET sort_key = $1
WHERE id = $2
RETURNING *;
        "#,
        )
        .bind(&sort_key)
        .bind(&partition_id)
        .fetch_one(&mut self.inner)
        .await;

        let partition = rec.map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::PartitionNotFound { id: partition_id },
            _ => Error::SqlxError { source: e },
        })?;

        Ok(partition)
    }
}

#[async_trait]
impl TombstoneRepo for PostgresTxn {
    async fn create_or_get(
        &mut self,
        table_id: TableId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
        predicate: &str,
    ) -> Result<Tombstone> {
        let v = sqlx::query_as::<_, Tombstone>(
            r#"
INSERT INTO tombstone
    ( table_id, sequencer_id, sequence_number, min_time, max_time, serialized_predicate )
VALUES
    ( $1, $2, $3, $4, $5, $6 )
ON CONFLICT ON CONSTRAINT tombstone_unique
DO UPDATE SET table_id = tombstone.table_id
RETURNING *;
        "#,
        )
        .bind(&table_id) // $1
        .bind(&sequencer_id) // $2
        .bind(&sequence_number) // $3
        .bind(&min_time) // $4
        .bind(&max_time) // $5
        .bind(predicate) // $6
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        // If tombstone_unique is hit, a record with (table_id, sequencer_id,
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
    tombstone.sequencer_id as sequencer_id,
    tombstone.sequence_number as sequence_number,
    tombstone.min_time as min_time,
    tombstone.max_time as max_time,
    tombstone.serialized_predicate as serialized_predicate
FROM table_name
INNER JOIN tombstone on tombstone.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id) // $1
        .fetch_all(&mut self.inner)
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
        .bind(&table_id) // $1
        .fetch_all(&mut self.inner)
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
        .bind(&id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let tombstone = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(tombstone))
    }

    async fn list_tombstones_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE sequencer_id = $1
  AND sequence_number > $2
ORDER BY id;
            "#,
        )
        .bind(&sequencer_id) // $1
        .bind(&sequence_number) // $2
        .fetch_all(&mut self.inner)
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
WHERE tombstone_id = ANY($1);
            "#,
        )
        .bind(&ids[..]) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        // Remove tombstones
        sqlx::query(
            r#"
DELETE
FROM tombstone
WHERE id = ANY($1);
            "#,
        )
        .bind(&ids[..]) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn list_tombstones_for_time_range(
        &mut self,
        sequencer_id: SequencerId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(
            r#"
SELECT *
FROM tombstone
WHERE sequencer_id = $1
  AND table_id = $2
  AND sequence_number > $3
  AND ((min_time <= $4 AND max_time >= $4)
        OR (min_time > $4 AND min_time <= $5))
ORDER BY id;
            "#,
        )
        .bind(&sequencer_id) // $1
        .bind(&table_id) // $2
        .bind(&sequence_number) // $3
        .bind(&min_time) // $4
        .bind(&max_time) // $5
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        let ParquetFileParams {
            sequencer_id,
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
            min_sequence_number,
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes,
            parquet_metadata,
            row_count,
            compaction_level,
            created_at,
        } = parquet_file_params;

        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
INSERT INTO parquet_file (
    sequencer_id, table_id, partition_id, object_store_id, min_sequence_number,
    max_sequence_number, min_time, max_time, file_size_bytes, parquet_metadata,
    row_count, compaction_level, created_at, namespace_id )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING *;
        "#,
        )
        .bind(sequencer_id) // $1
        .bind(table_id) // $2
        .bind(partition_id) // $3
        .bind(object_store_id) // $4
        .bind(min_sequence_number) // $5
        .bind(max_sequence_number) // $6
        .bind(min_time) // $7
        .bind(max_time) // $8
        .bind(file_size_bytes) // $9
        .bind(parquet_metadata) // $10
        .bind(row_count) // $11
        .bind(compaction_level) // $12
        .bind(created_at) // $13
        .bind(namespace_id) // $14
        .fetch_one(&mut self.inner)
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

        Ok(rec)
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let marked_at = Timestamp::new(self.time_provider.now().timestamp_nanos());

        let _ = sqlx::query(r#"UPDATE parquet_file SET to_delete = $1 WHERE id = $2;"#)
            .bind(&marked_at) // $1
            .bind(&id) // $2
            .execute(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(())
    }

    async fn list_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, sequencer_id, namespace_id, table_id, partition_id, object_store_id,
       min_sequence_number, max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at
FROM parquet_file
WHERE sequencer_id = $1
  AND max_sequence_number > $2
ORDER BY id;
            "#,
        )
        .bind(&sequencer_id) // $1
        .bind(&sequence_number) // $2
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT parquet_file.id, parquet_file.sequencer_id, parquet_file.namespace_id,
       parquet_file.table_id, parquet_file.partition_id, parquet_file.object_store_id,
       parquet_file.min_sequence_number, parquet_file.max_sequence_number, parquet_file.min_time,
       parquet_file.max_time, parquet_file.to_delete, parquet_file.file_size_bytes,
       parquet_file.row_count, parquet_file.compaction_level, parquet_file.created_at
FROM parquet_file
INNER JOIN table_name on table_name.id = parquet_file.table_id
WHERE table_name.namespace_id = $1
  AND parquet_file.to_delete IS NULL;
             "#,
        )
        .bind(&namespace_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, sequencer_id, namespace_id, table_id, partition_id, object_store_id,
       min_sequence_number, max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at
FROM parquet_file
WHERE table_id = $1 AND to_delete IS NULL;
             "#,
        )
        .bind(&table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn delete_old(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
DELETE FROM parquet_file
WHERE to_delete < $1
RETURNING *;
             "#,
        )
        .bind(&older_than) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn level_0(&mut self, sequencer_id: SequencerId) -> Result<Vec<ParquetFile>> {
        // this intentionally limits the returned files to 10,000 as it is used to make
        // a decision on the highest priority partitions. If compaction has never been
        // run this could end up returning millions of results and taking too long to run.
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, sequencer_id, namespace_id, table_id, partition_id, object_store_id,
       min_sequence_number, max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at
FROM parquet_file
WHERE parquet_file.sequencer_id = $1
  AND parquet_file.compaction_level = 0
  AND parquet_file.to_delete IS NULL
  LIMIT 1000;
        "#,
        )
        .bind(&sequencer_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn level_1(
        &mut self,
        table_partition: TablePartition,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, sequencer_id, namespace_id, table_id, partition_id, object_store_id,
       min_sequence_number, max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at
FROM parquet_file
WHERE parquet_file.sequencer_id = $1
  AND parquet_file.table_id = $2
  AND parquet_file.partition_id = $3
  AND parquet_file.compaction_level = 1
  AND parquet_file.to_delete IS NULL
  AND ((parquet_file.min_time <= $4 AND parquet_file.max_time >= $4)
      OR (parquet_file.min_time > $4 AND parquet_file.min_time <= $5));
        "#,
        )
        .bind(&table_partition.sequencer_id) // $1
        .bind(&table_partition.table_id) // $2
        .bind(&table_partition.partition_id) // $3
        .bind(min_time) // $4
        .bind(max_time) // $5
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_partition_not_to_delete(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>> {
        // Deliberately doesn't use `SELECT *` to avoid the performance hit of fetching the large
        // `parquet_metadata` column!!
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, sequencer_id, namespace_id, table_id, partition_id, object_store_id,
       min_sequence_number, max_sequence_number, min_time, max_time, to_delete, file_size_bytes,
       row_count, compaction_level, created_at
FROM parquet_file
WHERE parquet_file.partition_id = $1
  AND parquet_file.to_delete IS NULL;
        "#,
        )
        .bind(&partition_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn update_to_level_1(
        &mut self,
        parquet_file_ids: &[ParquetFileId],
    ) -> Result<Vec<ParquetFileId>> {
        // If I try to do `.bind(parquet_file_ids)` directly, I get a compile error from sqlx.
        // See https://github.com/launchbadge/sqlx/issues/1744
        let ids: Vec<_> = parquet_file_ids.iter().map(|p| p.get()).collect();
        let updated = sqlx::query(
            r#"
UPDATE parquet_file
SET compaction_level = 1
WHERE id = ANY($1)
RETURNING id;
        "#,
        )
        .bind(&ids[..])
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let updated = updated.into_iter().map(|row| row.get("id")).collect();
        Ok(updated)
    }

    async fn exist(&mut self, id: ParquetFileId) -> Result<bool> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(*) as count FROM parquet_file WHERE id = $1;"#,
        )
        .bind(&id) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn parquet_metadata(&mut self, id: ParquetFileId) -> Result<Vec<u8>> {
        let read_result =
            sqlx::query(r#"SELECT parquet_metadata FROM parquet_file WHERE id = $1;"#)
                .bind(&id) // $1
                .fetch_one(&mut self.inner)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.get("parquet_metadata"))
    }

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(*) as count FROM parquet_file;"#)
                .fetch_one(&mut self.inner)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn count_by_overlaps(
        &mut self,
        table_id: TableId,
        sequencer_id: SequencerId,
        min_time: Timestamp,
        max_time: Timestamp,
        sequence_number: SequenceNumber,
    ) -> Result<i64> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"
SELECT count(*) as count
FROM parquet_file
WHERE table_id = $1
  AND sequencer_id = $2
  AND min_sequence_number < $3
  AND parquet_file.to_delete IS NULL
  AND ((parquet_file.min_time <= $4 AND parquet_file.max_time >= $4)
  OR (parquet_file.min_time > $4 AND parquet_file.min_time <= $5));
            "#,
        )
        .bind(&table_id) // $1
        .bind(&sequencer_id) // $2
        .bind(sequence_number) // $3
        .bind(min_time) // $4
        .bind(max_time) // $5
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }
}

#[async_trait]
impl ProcessedTombstoneRepo for PostgresTxn {
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
        .fetch_one(&mut self.inner)
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
SELECT count(*) as count
FROM processed_tombstone
WHERE parquet_file_id = $1
  AND tombstone_id = $2;
            "#,
        )
        .bind(&parquet_file_id) // $1
        .bind(&tombstone_id) // $2
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count > 0)
    }

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(*) as count FROM processed_tombstone;"#)
                .fetch_one(&mut self.inner)
                .await
                .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }

    async fn count_by_tombstone_id(&mut self, tombstone_id: TombstoneId) -> Result<i64> {
        let read_result = sqlx::query_as::<_, Count>(
            r#"SELECT count(*) as count FROM processed_tombstone WHERE tombstone_id = $1;"#,
        )
        .bind(&tombstone_id) // $1
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(read_result.count)
    }
}

/// The error code returned by Postgres for a unique constraint violation.
///
/// See <https://www.postgresql.org/docs/9.2/errcodes-appendix.html>
const PG_UNIQUE_VIOLATION: &str = "23505";

/// Returns true if `e` is a unique constraint violation error.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_UNIQUE_VIOLATION {
                return true;
            }
        }
    }

    false
}

/// Error code returned by Postgres for a foreign key constraint violation.
const PG_FK_VIOLATION: &str = "23503";

fn is_fk_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(inner) = e {
        if let Some(code) = inner.code() {
            if code == PG_FK_VIOLATION {
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
    use metric::{Attributes, Metric, U64Histogram};
    use rand::Rng;
    use sqlx::migrate::MigrateDatabase;
    use std::{env, io::Write, ops::DerefMut, sync::Arc, time::Instant};
    use tempfile::NamedTempFile;

    // Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        ($panic_msg:expr) => {{
            dotenv::dotenv().ok();

            let required_vars = ["TEST_INFLUXDB_IOX_CATALOG_DSN"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping Postgres integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );

                let panic_msg: &'static str = $panic_msg;
                if !panic_msg.is_empty() {
                    panic!("{}", panic_msg);
                }

                return;
            }
        }};
        () => {
            maybe_skip_integration!("")
        };
    }

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>("catalog_op_duration_ms")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
        assert!(hit_count > 0, "metric did not record any calls");
    }

    async fn create_db(dsn: &str) {
        // Create the catalog database if it doesn't exist
        if !Postgres::database_exists(dsn).await.unwrap() {
            // Ignore failure if another test has already created the database
            let _ = Postgres::create_database(dsn).await;
        }
    }

    async fn setup_db() -> PostgresCatalog {
        // create a random schema for this particular pool
        let schema_name = {
            // use scope to make it clear to clippy / rust that `rng` is
            // not carried past await points
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(rand::distributions::Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
        };

        let metrics = Arc::new(metric::Registry::default());
        let dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();

        create_db(&dsn).await;

        let pg = PostgresCatalog::connect("test", &schema_name, &dsn, 3, metrics)
            .await
            .expect("failed to connect catalog");

        // Create the test schema
        pg.pool
            .execute(format!("CREATE SCHEMA {};", schema_name).as_str())
            .await
            .expect("failed to create test schema");

        // Ensure the test user has permission to interact with the test schema.
        pg.pool
            .execute(
                format!(
                    "GRANT USAGE ON SCHEMA {} TO public; GRANT CREATE ON SCHEMA {} TO public;",
                    schema_name, schema_name
                )
                .as_str(),
            )
            .await
            .expect("failed to grant privileges to schema");

        // Run the migrations against this random schema.
        pg.setup().await.expect("failed to initialise database");
        pg
    }

    #[tokio::test]
    async fn test_catalog() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        crate::interface::test_helpers::test_catalog(postgres).await;
    }

    #[tokio::test]
    async fn test_tombstone_create_or_get_idempotent() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query, sequencers) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let sequencer_id = *sequencers.keys().next().expect("no sequencer");
        let sequence_number = SequenceNumber::new(3);
        let min_timestamp = Timestamp::new(10);
        let max_timestamp = Timestamp::new(100);
        let predicate = "bananas";

        let a = postgres
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                sequencer_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                predicate,
            )
            .await
            .expect("should create OK");

        // Call create_or_get for the same (table_id, sequencer_id,
        // sequence_number) triplet, setting the same metadata to ensure the
        // write is idempotent.
        let b = postgres
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                sequencer_id,
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
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!("attempted to overwrite predicate");

        let postgres = setup_db().await;
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query, sequencers) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns2", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table2", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let sequencer_id = *sequencers.keys().next().expect("no sequencer");
        let sequence_number = SequenceNumber::new(3);
        let min_timestamp = Timestamp::new(10);
        let max_timestamp = Timestamp::new(100);

        let a = postgres
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                sequencer_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                "bananas",
            )
            .await
            .expect("should create OK");

        // Call create_or_get for the same (table_id, sequencer_id,
        // sequence_number) triplet with different metadata.
        //
        // The caller should not falsely believe it has persisted the incorrect
        // predicate.
        let b = postgres
            .repositories()
            .await
            .tombstones()
            .create_or_get(
                table_id,
                sequencer_id,
                sequence_number,
                min_timestamp,
                max_timestamp,
                "some other serialised predicate which is different",
            )
            .await
            .expect("should panic before result evaluated");

        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn test_partition_create_or_get_idempotent() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let postgres = setup_db().await;

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query, sequencers) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns4", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";
        let sequencer_id = *sequencers.keys().next().expect("no sequencer");

        let a = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key, sequencer_id, table_id)
            .await
            .expect("should create OK");

        // Call create_or_get for the same (key, table_id, sequencer_id)
        // triplet, setting the same sequencer ID to ensure the write is
        // idempotent.
        let b = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key, sequencer_id, table_id)
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);
    }

    #[tokio::test]
    #[should_panic = "attempted to overwrite partition"]
    async fn test_partition_create_or_get_no_overwrite() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!("attempted to overwrite partition");

        let postgres = setup_db().await;

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut txn = postgres.start_transaction().await.expect("txn start");
        let (kafka, query, _) = create_or_get_default_records(2, txn.deref_mut())
            .await
            .expect("db init failed");
        txn.commit().await.expect("txn commit");

        let namespace_id = postgres
            .repositories()
            .await
            .namespaces()
            .create("ns3", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
            .await
            .expect("namespace create failed")
            .id;
        let table_id = postgres
            .repositories()
            .await
            .tables()
            .create_or_get("table", namespace_id)
            .await
            .expect("create table failed")
            .id;

        let key = "bananas";

        let sequencers = postgres
            .repositories()
            .await
            .sequencers()
            .list()
            .await
            .expect("failed to list sequencers");
        assert!(
            sequencers.len() > 1,
            "expected more sequencers to be created, got {}",
            sequencers.len()
        );

        let a = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key, sequencers[0].id, table_id)
            .await
            .expect("should create OK");

        // Call create_or_get for the same (key, table_id) tuple, setting a
        // different sequencer ID
        let b = postgres
            .repositories()
            .await
            .partitions()
            .create_or_get(key, sequencers[1].id, table_id)
            .await
            .expect("result should not be evaluated");

        assert_eq!(a, b);
    }

    #[test]
    fn test_parse_dsn_file() {
        assert_eq!(
            get_dsn_file_path("dsn-file:///tmp/my foo.txt"),
            Some("/tmp/my foo.txt".to_owned()),
        );
        assert_eq!(get_dsn_file_path("dsn-file:blah"), None,);
        assert_eq!(get_dsn_file_path("postgres://user:pw@host/db"), None,);
    }

    #[tokio::test]
    async fn test_reload() {
        maybe_skip_integration!();

        const POLLING_INTERVAL: Duration = Duration::from_millis(10);

        // fetch dsn from envvar
        let test_dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();
        create_db(&test_dsn).await;
        eprintln!("TEST_DSN={}", test_dsn);

        // create a temp file to store the initial dsn
        let mut dsn_file = NamedTempFile::new().expect("create temp file");
        dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");

        const TEST_APPLICATION_NAME: &str = "test_application_name";
        let dsn_good = format!("dsn-file://{}", dsn_file.path().display());
        eprintln!("dsn_good={}", dsn_good);

        // create a hot swap pool with test application name and dsn file pointing to tmp file.
        // we will later update this file and the pool should be replaced.
        let pool = new_pool(
            TEST_APPLICATION_NAME,
            "test",
            dsn_good.as_str(),
            3,
            POLLING_INTERVAL,
        )
        .await
        .expect("connect");
        eprintln!("got a pool");

        // ensure the application name is set as expected
        let application_name: String =
            sqlx::query_scalar("SELECT current_setting('application_name') as application_name;")
                .fetch_one(&pool)
                .await
                .expect("read application_name");
        assert_eq!(application_name, TEST_APPLICATION_NAME);

        // create a new temp file object with updated dsn and overwrite the previous tmp file
        const TEST_APPLICATION_NAME_NEW: &str = "changed_application_name";
        let mut new_dsn_file = NamedTempFile::new().expect("create temp file");
        new_dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");
        new_dsn_file
            .write_all(format!("?application_name={}", TEST_APPLICATION_NAME_NEW).as_bytes())
            .expect("write temp file");
        new_dsn_file
            .persist(dsn_file.path())
            .expect("overwrite new dsn file");

        // wait until the hotswap machinery has reloaded the updated DSN file and
        // successfully performed a new connection with the new DSN.
        let mut application_name = "".to_string();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5)
            && application_name != TEST_APPLICATION_NAME_NEW
        {
            tokio::time::sleep(POLLING_INTERVAL).await;

            application_name = sqlx::query_scalar(
                "SELECT current_setting('application_name') as application_name;",
            )
            .fetch_one(&pool)
            .await
            .expect("read application_name");
        }
        assert_eq!(application_name, TEST_APPLICATION_NAME_NEW);
    }

    macro_rules! test_column_create_or_get_many {
        (
            $name:ident,
            calls = {$([$($col_name:literal => $col_type:expr),+ $(,)?]),+},
            want = $($want:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_column_create_or_get_many_ $name>]() {
                    // If running an integration test on your laptop, this requires that you have
                    // Postgres running and that you've done the sqlx migrations. See the README in
                    // this crate for info to set it up.
                    maybe_skip_integration!();

                    let postgres = setup_db().await;
                    let metrics = Arc::clone(&postgres.metrics);

                    let postgres: Arc<dyn Catalog> = Arc::new(postgres);
                    let mut txn = postgres.start_transaction().await.expect("txn start");
                    let (kafka, query, _sequencers) = create_or_get_default_records(1, txn.deref_mut())
                        .await
                        .expect("db init failed");
                    txn.commit().await.expect("txn commit");

                    let namespace_id = postgres
                        .repositories()
                        .await
                        .namespaces()
                        .create("ns4", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
                        .await
                        .expect("namespace create failed")
                        .id;
                    let table_id = postgres
                        .repositories()
                        .await
                        .tables()
                        .create_or_get("table", namespace_id)
                        .await
                        .expect("create table failed")
                        .id;

                    $(
                        let insert = [
                            $(
                                ColumnUpsertRequest {
                                    name: $col_name,
                                    table_id,
                                    column_type: $col_type,
                                },
                            )+
                        ];
                        let got = postgres
                            .repositories()
                            .await
                            .columns()
                            .create_or_get_many(&insert)
                            .await;

                        // The returned columns MUST always match the requested
                        // column values if successful.
                        if let Ok(got) = &got {
                            assert_eq!(insert.len(), got.len());
                            let got_map: HashMap<String, Column> = got
                                .into_iter()
                                .map(|col| (col.name.to_string(), col.clone()))
                                .collect();
                            insert.iter().for_each(|req| {
                                let got_col = got_map.get(req.name).expect("should get back what was inserted");
                                assert_eq!(req.table_id, got_col.table_id);
                                assert_eq!(
                                    req.column_type,
                                    ColumnType::try_from(got_col.column_type).expect("invalid column type")
                                );
                            });
                            assert_metric_hit(&metrics, "column_create_or_get_many");
                        }
                    )+

                    assert_matches!(got, $($want)+);
                }
            }
        }
    }

    // Issue a few calls to create_or_get_many that contain distinct columns and
    // covers the full set of column types.
    test_column_create_or_get_many!(
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
    test_column_create_or_get_many!(
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
    test_column_create_or_get_many!(
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
    test_column_create_or_get_many!(
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
                assert_eq!(existing, "string");
                assert_eq!(new, "bool");
            })
        }
    );

    // Issue one call containing a column specified twice, with differing types
    // and observe an error different from the above test case.
    test_column_create_or_get_many!(
        intra_request_type_conflict,
        calls = {
            [
                "test1" => ColumnType::String,
                "test1" => ColumnType::Bool,
            ]
        },
        want = Err(Error::SqlxError{ .. })
    );

    macro_rules! test_column_create_or_get_many_two_tables_limits {
        (
            $name:ident,
            limit = $col_limit:literal,
            preseed = $col_preseed_count:literal,
            preseed_table = $preseed_table_id:literal,
            calls = {$([$(($col_name:literal, $table_id:literal) => $col_type:expr),+ $(,)?]),+},
            want = $($want:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_column_create_or_get_many_column_limits_$name>]() {
                    // If running an integration test on your laptop, this requires that you have
                    // Postgres running and that you've done the sqlx migrations. See the README in
                    // this crate for info to set it up.
                    maybe_skip_integration!();

                    let postgres = setup_db().await;

                    let postgres: Arc<dyn Catalog> = Arc::new(postgres);
                    let mut txn = postgres.start_transaction().await.expect("txn start");
                    let (kafka, query, _sequencers) = create_or_get_default_records(1, txn.deref_mut())
                        .await
                        .expect("db init failed");
                    txn.commit().await.expect("txn commit");

                    let namespace_id = postgres
                        .repositories()
                        .await
                        .namespaces()
                        .create("ns4", crate::INFINITE_RETENTION_POLICY, kafka.id, query.id)
                        .await
                        .expect("namespace create failed")
                        .id;
                    postgres
                        .repositories()
                        .await
                        .tables()
                        .create_or_get("table1", namespace_id)
                        .await
                        .expect("create table failed");
                    postgres
                        .repositories()
                        .await
                        .tables()
                        .create_or_get("table2", namespace_id)
                        .await
                        .expect("create table failed");

                    // create the desired number of columns for the table to preseed the test so we
                    // can more easily test the limits
                    let mut column_names: Vec<String> = Vec::with_capacity($col_preseed_count);
                    for n in 1..=$col_preseed_count {
                        column_names.push(format!("a{}", n));
                    }
                    let preseed_inserts: Vec<ColumnUpsertRequest> = column_names
                        .iter()
                        .map(|name| ColumnUpsertRequest {
                            name: name.as_str(),
                            table_id: TableId::new($preseed_table_id),
                            column_type: ColumnType::Tag,
                        })
                        .collect();
                    if $col_preseed_count > 0 {
                        postgres
                            .repositories()
                            .await
                            .columns()
                            .create_or_get_many(&preseed_inserts)
                            .await
                            .expect("couldn't insert preseed columns");
                    }

                    // deliberately setting the limits after pre-seeding with data in case we want
                    // to write tests for the case where the limits are set below the amount of
                    // columns we already have in the DB (which can happen- setting limits doesn't
                    // truncate the data, or fail if data already exceeds it)
                    postgres
                        .repositories()
                        .await
                        .namespaces()
                        .update_column_limit("ns4", $col_limit)
                        .await
                        .expect("namespace limit update failed");

                    $(
                        let insert = [
                            $(
                                ColumnUpsertRequest {
                                    name: $col_name,
                                    table_id: TableId::new($table_id),
                                    column_type: $col_type,
                                },
                            )+
                        ];
                        let got = postgres
                            .repositories()
                            .await
                            .columns()
                            .create_or_get_many(&insert)
                            .await;
                    )+

                    assert_matches!(got, $($want)+);
                }
            }
        }
    }

    test_column_create_or_get_many_two_tables_limits!(
        single_table_ok_exact_limit,
        limit = 5,
        preseed = 1,
        preseed_table = 1,
        calls = {
            [
                ("test1", 1) => ColumnType::Tag,
                ("test2", 1) => ColumnType::Tag,
                ("test3", 1) => ColumnType::Tag,
                ("test4", 1) => ColumnType::Tag,
            ]
        },
        want = Ok(_)
    );

    test_column_create_or_get_many_two_tables_limits!(
        single_table_err_one_over,
        limit = 5,
        preseed = 1,
        preseed_table = 1,
        calls = {
            [
                ("test1", 1) => ColumnType::Tag,
                ("test2", 1) => ColumnType::Tag,
                ("test3", 1) => ColumnType::Tag,
                ("test4", 1) => ColumnType::Tag,
                ("test5", 1) => ColumnType::Tag,
            ]
        },
        want = Err(Error::PartialColumnCreateError {
            input_column_count: 5,
            inserted_column_count: 0
        })
    );

    test_column_create_or_get_many_two_tables_limits!(
        single_table_ok_one_under,
        limit = 5,
        preseed = 2,
        preseed_table = 1,
        calls = {
            [
                ("test1", 1) => ColumnType::Tag,
                ("test2", 1) => ColumnType::Tag,
            ]
        },
        want = Ok(_)
    );

    test_column_create_or_get_many_two_tables_limits!(
        multi_table_ok,
        limit = 5,
        preseed = 1,
        preseed_table = 1,
        calls = {
            [
                ("test1", 1) => ColumnType::Tag,
                ("test2", 1) => ColumnType::Tag,
                ("test3", 1) => ColumnType::Tag,
                ("test4", 1) => ColumnType::Tag,
                ("test1", 2) => ColumnType::Tag,
                ("test2", 2) => ColumnType::Tag,
                ("test3", 2) => ColumnType::Tag,
                ("test4", 2) => ColumnType::Tag,
                ("test5", 2) => ColumnType::Tag,
            ]
        },
        want = Ok(_)
    );

    test_column_create_or_get_many_two_tables_limits!(
        multi_table_partial,
        limit = 5,
        preseed = 3,
        preseed_table = 1,
        calls = {
            [
                ("test1", 1) => ColumnType::Tag,
                ("test2", 1) => ColumnType::Tag,
                ("test3", 1) => ColumnType::Tag,
                ("test4", 1) => ColumnType::Tag,
                ("test5", 1) => ColumnType::Tag,
                ("test1", 2) => ColumnType::Tag,
                ("test2", 2) => ColumnType::Tag,
                ("test3", 2) => ColumnType::Tag,
                ("test4", 2) => ColumnType::Tag,
                ("test5", 2) => ColumnType::Tag,
            ]
        },
        want = Err(Error::PartialColumnCreateError {
            input_column_count: 10,
            inserted_column_count: 5
        })
    );

    test_column_create_or_get_many_two_tables_limits!(
        invalid_table_id,
        limit = 5,
        preseed = 1,
        preseed_table = 1,
        calls = {
            [
                ("test1", 3) => ColumnType::Tag,
            ]
        },
        want = Err(Error::PartialColumnCreateError {
            input_column_count: 1,
            inserted_column_count: 0
        })
    );
}
