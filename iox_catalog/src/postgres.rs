//! A Postgres backed implementation of the Catalog

use crate::interface::{verify_sort_key_length, MAX_PARQUET_FILES_SELECTED_ONCE_FOR_DELETE};
use crate::{
    interface::{
        self, CasFailure, Catalog, ColumnRepo, ColumnTypeMismatchSnafu, Error, NamespaceRepo,
        ParquetFileRepo, PartitionRepo, RepoCollection, Result, SoftDeletedRows, TableRepo,
        MAX_PARQUET_FILES_SELECTED_ONCE_FOR_RETENTION,
    },
    kafkaless_transition::{
        SHARED_QUERY_POOL, SHARED_QUERY_POOL_ID, SHARED_TOPIC_ID, SHARED_TOPIC_NAME,
        TRANSITION_SHARD_ID, TRANSITION_SHARD_INDEX,
    },
    metrics::MetricDecorator,
    migrate::IOxMigrator,
    DEFAULT_MAX_COLUMNS_PER_TABLE, DEFAULT_MAX_TABLES,
};
use async_trait::async_trait;
use data_types::SortedColumnSet;
use data_types::{
    partition_template::{
        NamespacePartitionTemplateOverride, TablePartitionTemplateOverride, TemplatePart,
    },
    Column, ColumnType, CompactionLevel, Namespace, NamespaceId, NamespaceName,
    NamespaceServiceProtectionLimitsOverride, ParquetFile, ParquetFileId, ParquetFileParams,
    Partition, PartitionHashId, PartitionId, PartitionKey, SkippedCompaction, Table, TableId,
    Timestamp, TransitionPartitionId,
};
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, Instrument, MetricKind};
use observability_deps::tracing::{debug, info, warn};
use once_cell::sync::Lazy;
use parking_lot::{RwLock, RwLockWriteGuard};
use snafu::prelude::*;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Uuid,
    Acquire, ConnectOptions, Executor, Postgres, Row,
};
use sqlx_hotswap_pool::HotSwapPool;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc, time::Duration};

static MIGRATOR: Lazy<IOxMigrator> =
    Lazy::new(|| IOxMigrator::try_from(&sqlx::migrate!()).expect("valid migration"));

/// Postgres connection options.
#[derive(Debug, Clone)]
pub struct PostgresConnectionOptions {
    /// Application name.
    ///
    /// This will be reported to postgres.
    pub app_name: String,

    /// Schema name.
    pub schema_name: String,

    /// DSN.
    pub dsn: String,

    /// Maximum number of concurrent connections.
    pub max_conns: u32,

    /// Set the amount of time to attempt connecting to the database.
    pub connect_timeout: Duration,

    /// Set a maximum idle duration for individual connections.
    pub idle_timeout: Duration,

    /// If the DSN points to a file (i.e. starts with `dsn-file://`), this sets the interval how often the the file
    /// should be polled for updates.
    ///
    /// If an update is encountered, the underlying connection pool will be hot-swapped.
    pub hotswap_poll_interval: Duration,
}

impl PostgresConnectionOptions {
    /// Default value for [`schema_name`](Self::schema_name).
    pub const DEFAULT_SCHEMA_NAME: &'static str = "iox_catalog";

    /// Default value for [`max_conns`](Self::max_conns).
    pub const DEFAULT_MAX_CONNS: u32 = 10;

    /// Default value for [`connect_timeout`](Self::connect_timeout).
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

    /// Default value for [`idle_timeout`](Self::idle_timeout).
    pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Default value for [`hotswap_poll_interval`](Self::hotswap_poll_interval).
    pub const DEFAULT_HOTSWAP_POLL_INTERVAL: Duration = Duration::from_secs(5);
}

impl Default for PostgresConnectionOptions {
    fn default() -> Self {
        Self {
            app_name: String::from("iox"),
            schema_name: String::from(Self::DEFAULT_SCHEMA_NAME),
            dsn: String::new(),
            max_conns: Self::DEFAULT_MAX_CONNS,
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: Self::DEFAULT_IDLE_TIMEOUT,
            hotswap_poll_interval: Self::DEFAULT_HOTSWAP_POLL_INTERVAL,
        }
    }
}

/// PostgreSQL catalog.
#[derive(Debug)]
pub struct PostgresCatalog {
    metrics: Arc<metric::Registry>,
    pool: HotSwapPool<Postgres>,
    time_provider: Arc<dyn TimeProvider>,
    // Connection options for display
    options: PostgresConnectionOptions,
}

impl PostgresCatalog {
    /// Connect to the catalog store.
    pub async fn connect(
        options: PostgresConnectionOptions,
        metrics: Arc<metric::Registry>,
    ) -> Result<Self> {
        let pool = new_pool(&options, Arc::clone(&metrics))
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Self {
            pool,
            metrics,
            time_provider: Arc::new(SystemProvider::new()),
            options,
        })
    }

    fn schema_name(&self) -> &str {
        &self.options.schema_name
    }

    #[cfg(test)]
    pub(crate) fn into_pool(self) -> HotSwapPool<Postgres> {
        self.pool
    }
}

impl Display for PostgresCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            // Do not include dsn in log as it may have credentials
            // that should not end up in the log
            "Postgres(dsn=OMITTED, schema_name='{}')",
            self.schema_name()
        )
    }
}

/// transaction for [`PostgresCatalog`].
#[derive(Debug)]
pub struct PostgresTxn {
    inner: PostgresTxnInner,
    time_provider: Arc<dyn TimeProvider>,
}

#[derive(Debug)]
struct PostgresTxnInner {
    pool: HotSwapPool<Postgres>,
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
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {};", self.schema_name());
        self.pool
            .execute(sqlx::query(&create_schema_query))
            .await
            .map_err(|e| Error::Setup { source: e })?;

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
ON CONFLICT ON CONSTRAINT topic_name_unique
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
OVERRIDING SYSTEM VALUE
VALUES ($1, $2, $3, 0)
ON CONFLICT ON CONSTRAINT shard_unique
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
ON CONFLICT ON CONSTRAINT query_pool_name_unique
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
            PostgresTxn {
                inner: PostgresTxnInner {
                    pool: self.pool.clone(),
                },
                time_provider: Arc::clone(&self.time_provider),
            },
            Arc::clone(&self.metrics),
        ))
    }

    #[cfg(test)]
    fn metrics(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }
}

/// Adapter to connect sqlx pools with our metrics system.
#[derive(Debug, Clone, Default)]
struct PoolMetrics {
    /// Actual shared state.
    state: Arc<PoolMetricsInner>,
}

/// Inner state of [`PoolMetrics`] that is wrapped into an [`Arc`].
#[derive(Debug, Default)]
struct PoolMetricsInner {
    /// Next pool ID.
    pool_id_gen: AtomicU64,

    /// Set of known pools and their ID labels.
    ///
    /// Note: The pool is internally ref-counted via an [`Arc`]. Holding a reference does NOT prevent it from being closed.
    pools: RwLock<Vec<(Arc<str>, sqlx::Pool<Postgres>)>>,
}

impl PoolMetrics {
    /// Create new pool metrics.
    fn new(metrics: Arc<metric::Registry>) -> Self {
        metrics.register_instrument("iox_catalog_postgres", Self::default)
    }

    /// Register a new pool.
    fn register_pool(&self, pool: sqlx::Pool<Postgres>) {
        let id = self
            .state
            .pool_id_gen
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .into();
        let mut pools = self.state.pools.write();
        pools.push((id, pool));
    }

    /// Remove closed pools from given list.
    fn clean_pools(pools: &mut Vec<(Arc<str>, sqlx::Pool<Postgres>)>) {
        pools.retain(|(_id, p)| !p.is_closed());
    }
}

impl Instrument for PoolMetrics {
    fn report(&self, reporter: &mut dyn metric::Reporter) {
        let mut pools = self.state.pools.write();
        Self::clean_pools(&mut pools);
        let pools = RwLockWriteGuard::downgrade(pools);

        reporter.start_metric(
            "sqlx_postgres_pools",
            "Number of pools that sqlx uses",
            MetricKind::U64Gauge,
        );
        reporter.report_observation(
            &Attributes::from([]),
            metric::Observation::U64Gauge(pools.len() as u64),
        );
        reporter.finish_metric();

        reporter.start_metric(
            "sqlx_postgres_connections",
            "Number of connections within the postgres connection pool that sqlx uses",
            MetricKind::U64Gauge,
        );
        for (id, p) in pools.iter() {
            let active = p.size() as u64;
            let idle = p.num_idle() as u64;

            // We get both values independently (from underlying atomic counters) so they might be out of sync (with a
            // low likelyhood). Calculating this value and emitting it is useful though since it allows easier use in
            // dashboards since you can `max_over_time` w/o any recording rules.
            let used = active.saturating_sub(idle);

            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("active")),
                ]),
                metric::Observation::U64Gauge(active),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("idle")),
                ]),
                metric::Observation::U64Gauge(idle),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("used")),
                ]),
                metric::Observation::U64Gauge(used),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("max")),
                ]),
                metric::Observation::U64Gauge(p.options().get_max_connections() as u64),
            );
            reporter.report_observation(
                &Attributes::from([
                    ("pool_id", Cow::Owned(id.as_ref().to_owned())),
                    ("state", Cow::Borrowed("min")),
                ]),
                metric::Observation::U64Gauge(p.options().get_min_connections() as u64),
            );
        }

        reporter.finish_metric();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Creates a new [`sqlx::Pool`] from a database config and an explicit DSN.
///
/// This function doesn't support the IDPE specific `dsn-file://` uri scheme.
async fn new_raw_pool(
    options: &PostgresConnectionOptions,
    parsed_dsn: &str,
    metrics: PoolMetrics,
) -> Result<sqlx::Pool<Postgres>, sqlx::Error> {
    // sqlx exposes some options as pool options, while other options are available as connection options.
    let connect_options = PgConnectOptions::from_str(parsed_dsn)?
        // the default is INFO, which is frankly surprising.
        .log_statements(log::LevelFilter::Trace);

    let app_name = options.app_name.clone();
    let app_name2 = options.app_name.clone(); // just to log below
    let schema_name = options.schema_name.clone();
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(options.max_conns)
        .acquire_timeout(options.connect_timeout)
        .idle_timeout(options.idle_timeout)
        .test_before_acquire(true)
        .after_connect(move |c, _meta| {
            let app_name = app_name.to_owned();
            let schema_name = schema_name.to_owned();
            Box::pin(async move {
                // Tag the connection with the provided application name, while allowing it to
                // be override from the connection string (aka DSN).
                // If current_application_name is empty here it means the application name wasn't
                // set as part of the DSN, and we can set it explicitly.
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
                let search_path_query = format!("SET search_path TO {schema_name},public;");
                c.execute(sqlx::query(&search_path_query)).await?;

                // Ensure explicit timezone selection, instead of deferring to
                // the server value.
                c.execute("SET timezone = 'UTC';").await?;
                Ok(())
            })
        })
        .connect_with(connect_options)
        .await?;

    // Log a connection was successfully established and include the application
    // name for cross-correlation between Conductor logs & database connections.
    info!(application_name=%app_name2, "connected to config store");

    metrics.register_pool(pool.clone());
    Ok(pool)
}

/// Parse a postgres catalog dsn, handling the special `dsn-file://`
/// syntax (see [`new_pool`] for more details).
///
/// Returns an error if the dsn-file could not be read correctly.
pub fn parse_dsn(dsn: &str) -> Result<String, sqlx::Error> {
    let dsn = match get_dsn_file_path(dsn) {
        Some(filename) => std::fs::read_to_string(filename)?,
        None => dsn.to_string(),
    };
    Ok(dsn)
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
    options: &PostgresConnectionOptions,
    metrics: Arc<metric::Registry>,
) -> Result<HotSwapPool<Postgres>, sqlx::Error> {
    let parsed_dsn = parse_dsn(&options.dsn)?;
    let metrics = PoolMetrics::new(metrics);
    let pool = HotSwapPool::new(new_raw_pool(options, &parsed_dsn, metrics.clone()).await?);
    let polling_interval = options.hotswap_poll_interval;

    if let Some(dsn_file) = get_dsn_file_path(&options.dsn) {
        let pool = pool.clone();
        let options = options.clone();

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
                    options: &PostgresConnectionOptions,
                    current_dsn: &str,
                    dsn_file: &str,
                    pool: &HotSwapPool<Postgres>,
                    metrics: PoolMetrics,
                ) -> Result<Option<String>, sqlx::Error> {
                    let new_dsn = std::fs::read_to_string(dsn_file)?;
                    if new_dsn == current_dsn {
                        Ok(None)
                    } else {
                        let new_pool = new_raw_pool(options, &new_dsn, metrics).await?;
                        let old_pool = pool.replace(new_pool);
                        info!("replaced hotswap pool");
                        info!(?old_pool, "closing old DB connection pool");
                        // The pool is not closed on drop. We need to call `close`.
                        // It will close all idle connections, and wait until acquired connections
                        // are returned to the pool or closed.
                        old_pool.close().await;
                        info!(?old_pool, "closed old DB connection pool");
                        Ok(Some(new_dsn))
                    }
                }

                match try_update(&options, &current_dsn, &dsn_file, &pool, metrics.clone()).await {
                    Ok(None) => {}
                    Ok(Some(new_dsn)) => {
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

async fn insert_column_with_connection<'q, E>(
    executor: E,
    name: &str,
    table_id: TableId,
    column_type: ColumnType,
) -> Result<Column>
where
    E: Executor<'q, Database = Postgres>,
{
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
impl NamespaceRepo for PostgresTxn {
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
INSERT INTO namespace (
    name, topic_id, query_pool_id, retention_period_ns, max_tables, max_columns_per_table, partition_template
)
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

        let rec = rec.fetch_one(&mut self.inner).await.map_err(|e| {
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
        .fetch_all(&mut self.inner)
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
        .fetch_one(&mut self.inner)
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
        .fetch_one(&mut self.inner)
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
            .execute(&mut self.inner)
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
        .fetch_one(&mut self.inner)
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
        .fetch_one(&mut self.inner)
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
        .fetch_one(&mut self.inner)
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
impl TableRepo for PostgresTxn {
    async fn create(
        &mut self,
        name: &str,
        partition_template: TablePartitionTemplateOverride,
        namespace_id: NamespaceId,
    ) -> Result<Table> {
        let mut tx = self
            .inner
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
    SELECT namespace.id AS id, max_tables, COUNT(table_name.*) AS count
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
        .bind(namespace_id) // $1
        .bind(name) // $2
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
        .bind(namespace_id)
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>("SELECT * FROM table_name;")
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
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
        insert_column_with_connection(&mut self.inner, name, table_id, column_type).await
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
        .fetch_all(&mut self.inner)
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
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }

    async fn list(&mut self) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>("SELECT * FROM column_name;")
            .fetch_all(&mut self.inner)
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
        let (v_name, v_column_type): (Vec<&str>, Vec<i16>) = columns
            .iter()
            .map(|(&name, &column_type)| (name, column_type as i16))
            .unzip();

        // The `ORDER BY` in this statement is important to avoid deadlocks during concurrent
        // writes to the same IOx table that each add many new columns. See:
        //
        // - <https://rcoh.svbtle.com/postgres-unique-constraints-can-cause-deadlock>
        // - <https://dba.stackexchange.com/a/195220/27897>
        // - <https://github.com/influxdata/idpe/issues/16298>
        let out = sqlx::query_as::<_, Column>(
            r#"
INSERT INTO column_name ( name, table_id, column_type )
SELECT name, $1, column_type
FROM UNNEST($2, $3) as a(name, column_type)
ORDER BY name
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name
RETURNING *;
            "#,
        )
        .bind(table_id) // $1
        .bind(&v_name) // $2
        .bind(&v_column_type) // $3
        .fetch_all(&mut self.inner)
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

#[async_trait]
impl PartitionRepo for PostgresTxn {
    async fn create_or_get(&mut self, key: PartitionKey, table_id: TableId) -> Result<Partition> {
        let hash_id = PartitionHashId::new(table_id, &key);

        let v = sqlx::query_as::<_, Partition>(
            r#"
INSERT INTO partition
    (partition_key, shard_id, table_id, hash_id, sort_key, sort_key_ids)
VALUES
    ( $1, $2, $3, $4, '{}', '{}')
ON CONFLICT ON CONSTRAINT partition_key_unique
DO UPDATE SET partition_key = partition.partition_key
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
        )
        .bind(key) // $1
        .bind(TRANSITION_SHARD_ID) // $2
        .bind(table_id) // $3
        .bind(&hash_id) // $4
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(v)
    }

    async fn get_by_id(&mut self, partition_id: PartitionId) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE id = $1;
        "#,
        )
        .bind(partition_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition))
    }

    async fn get_by_id_batch(&mut self, partition_ids: Vec<PartitionId>) -> Result<Vec<Partition>> {
        let ids: Vec<_> = partition_ids.iter().map(|p| p.get()).collect();

        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE id = ANY($1);
        "#,
        )
        .bind(&ids[..]) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn get_by_hash_id(
        &mut self,
        partition_hash_id: &PartitionHashId,
    ) -> Result<Option<Partition>> {
        let rec = sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE hash_id = $1;
        "#,
        )
        .bind(partition_hash_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let partition = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(partition))
    }

    async fn get_by_hash_id_batch(
        &mut self,
        partition_ids: &[&PartitionHashId],
    ) -> Result<Vec<Partition>> {
        let ids: Vec<_> = partition_ids.iter().map(|p| p.as_bytes()).collect();

        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE hash_id = ANY($1);
        "#,
        )
        .bind(&ids[..]) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table_id(&mut self, table_id: TableId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at
FROM partition
WHERE table_id = $1;
            "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_ids(&mut self) -> Result<Vec<PartitionId>> {
        sqlx::query_as(
            r#"
            SELECT p.id as partition_id
            FROM partition p
            "#,
        )
        .fetch_all(&mut self.inner)
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
    ) -> Result<Partition, CasFailure<Vec<String>>> {
        verify_sort_key_length(new_sort_key, new_sort_key_ids);

        let old_sort_key = old_sort_key.unwrap_or_default();
        // This `match` will go away when all partitions have hash IDs in the database.
        let query = match partition_id {
            TransitionPartitionId::Deterministic(hash_id) => sqlx::query_as::<_, Partition>(
                r#"
UPDATE partition
SET sort_key = $1, sort_key_ids = $4
WHERE hash_id = $2 AND sort_key = $3
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
            )
            .bind(new_sort_key) // $1
            .bind(hash_id) // $2
            .bind(&old_sort_key) // $3
            .bind(new_sort_key_ids), // $4
            TransitionPartitionId::Deprecated(id) => sqlx::query_as::<_, Partition>(
                r#"
UPDATE partition
SET sort_key = $1, sort_key_ids = $4
WHERE id = $2 AND sort_key = $3
RETURNING id, hash_id, table_id, partition_key, sort_key, sort_key_ids, new_file_at;
        "#,
            )
            .bind(new_sort_key) // $1
            .bind(id) // $2
            .bind(&old_sort_key) // $3
            .bind(new_sort_key_ids), // $4
        };

        let res = query.fetch_one(&mut self.inner).await;

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
                    crate::partition_lookup(self, partition_id)
                        .await
                        .map_err(CasFailure::QueryError)?
                        .ok_or(CasFailure::QueryError(Error::PartitionNotFound {
                            id: partition_id.clone(),
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
            ?new_sort_key_ids,
            "partition sort key cas successful"
        );

        Ok(partition)
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
    ( $1, $2, $3, $4, $5, $6, $7, extract(epoch from NOW()) )
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
        .execute(&mut self.inner)
        .await
        .context(interface::CouldNotRecordSkippedCompactionSnafu { partition_id })?;
        Ok(())
    }

    async fn get_in_skipped_compactions(
        &mut self,
        partition_ids: &[PartitionId],
    ) -> Result<Vec<SkippedCompaction>> {
        let rec = sqlx::query_as::<_, SkippedCompaction>(
            r#"SELECT * FROM skipped_compactions WHERE partition_id = ANY($1);"#,
        )
        .bind(partition_ids) // $1
        .fetch_all(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(Vec::new());
        }

        let skipped_partition_records = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(skipped_partition_records)
    }

    async fn list_skipped_compactions(&mut self) -> Result<Vec<SkippedCompaction>> {
        sqlx::query_as::<_, SkippedCompaction>(
            r#"
SELECT * FROM skipped_compactions
        "#,
        )
        .fetch_all(&mut self.inner)
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
        .fetch_optional(&mut self.inner)
        .await
        .context(interface::CouldNotDeleteSkippedCompactionsSnafu)
    }

    async fn most_recent_n(&mut self, n: usize) -> Result<Vec<Partition>> {
        sqlx::query_as(
    // TODO: Carol has confirmed the persisted_sequence_number is not needed anywhere so let us remove it
    // but in a seperate PR to ensure we don't break anything
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, persisted_sequence_number, new_file_at
FROM partition
ORDER BY id DESC
LIMIT $1;"#,
        )
        .bind(n as i64) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
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
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_old_style(&mut self) -> Result<Vec<Partition>> {
        // Correctness: the main caller of this function, the partition bloom
        // filter, relies on all partitions being made available to it.
        //
        // This function MUST return the full set of old partitions to the
        // caller - do NOT apply a LIMIT to this query.
        //
        // The load this query saves vastly outsizes the load this query causes.
        sqlx::query_as(
            r#"
SELECT id, hash_id, table_id, partition_key, sort_key, sort_key_ids, persisted_sequence_number,
       new_file_at
FROM partition
WHERE hash_id IS NULL
ORDER BY id DESC;"#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresTxn {
    async fn create(&mut self, parquet_file_params: ParquetFileParams) -> Result<ParquetFile> {
        let executor = &mut self.inner;
        let id = create_parquet_file(executor, &parquet_file_params).await?;
        Ok(ParquetFile::from_params(parquet_file_params, id))
    }

    async fn list_all(&mut self) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT parquet_file.id, parquet_file.namespace_id, parquet_file.table_id,
       parquet_file.partition_id, parquet_file.partition_hash_id, parquet_file.object_store_id,
       parquet_file.min_time, parquet_file.max_time, parquet_file.to_delete,
       parquet_file.file_size_bytes, parquet_file.row_count, parquet_file.compaction_level,
       parquet_file.created_at, parquet_file.column_set, parquet_file.max_l0_created_at
FROM parquet_file;
             "#,
        )
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
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
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        let flagged = flagged.into_iter().map(|row| row.get("id")).collect();
        Ok(flagged)
    }

    async fn list_by_namespace_not_to_delete(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
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
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn list_by_table_not_to_delete(&mut self, table_id: TableId) -> Result<Vec<ParquetFile>> {
        sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, partition_hash_id, object_store_id,
       min_time, max_time, to_delete, file_size_bytes, row_count, compaction_level, created_at,
       column_set, max_l0_created_at
FROM parquet_file
WHERE table_id = $1 AND to_delete IS NULL;
             "#,
        )
        .bind(table_id) // $1
        .fetch_all(&mut self.inner)
        .await
        .map_err(|e| Error::SqlxError { source: e })
    }

    async fn delete_old_ids_only(&mut self, older_than: Timestamp) -> Result<Vec<ParquetFileId>> {
        // see https://www.crunchydata.com/blog/simulating-update-or-delete-with-limit-in-postgres-ctes-to-the-rescue
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
        .fetch_all(&mut self.inner)
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
            TransitionPartitionId::Deterministic(hash_id) => sqlx::query_as::<_, ParquetFile>(
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
            TransitionPartitionId::Deprecated(id) => sqlx::query_as::<_, ParquetFile>(
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

        query
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }

    async fn get_by_object_store_id(
        &mut self,
        object_store_id: Uuid,
    ) -> Result<Option<ParquetFile>> {
        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
SELECT id, namespace_id, table_id, partition_id, partition_hash_id, object_store_id, min_time,
       max_time, to_delete, file_size_bytes, row_count, compaction_level, created_at, column_set,
       max_l0_created_at
FROM parquet_file
WHERE object_store_id = $1;
             "#,
        )
        .bind(object_store_id) // $1
        .fetch_one(&mut self.inner)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let parquet_file = rec.map_err(|e| Error::SqlxError { source: e })?;

        Ok(Some(parquet_file))
    }

    async fn exists_by_object_store_id_batch(
        &mut self,
        object_store_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>> {
        sqlx::query(
            // sqlx's readme suggests using PG's ANY operator instead of IN; see link below.
            // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
            r#"
SELECT object_store_id
FROM parquet_file
WHERE object_store_id = ANY($1);
             "#,
        )
        .bind(object_store_ids) // $1
        .map(|pgr| pgr.get::<Uuid, _>("object_store_id"))
        .fetch_all(&mut self.inner)
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
        let delete_set: HashSet<_> = delete.iter().map(|d| d.get()).collect();
        let upgrade_set: HashSet<_> = upgrade.iter().map(|u| u.get()).collect();

        assert!(
            delete_set.is_disjoint(&upgrade_set),
            "attempted to upgrade a file scheduled for delete"
        );

        let mut tx = self
            .inner
            .pool
            .begin()
            .await
            .map_err(|e| Error::StartTransaction { source: e })?;

        let marked_at = Timestamp::from(self.time_provider.now());
        flag_for_delete(&mut *tx, delete, marked_at).await?;

        update_compaction_level(&mut *tx, upgrade, target_level).await?;

        let mut ids = Vec::with_capacity(create.len());
        for file in create {
            let id = create_parquet_file(&mut *tx, file).await?;
            ids.push(id);
        }

        tx.commit()
            .await
            .map_err(|source| Error::FailedToCommit { source })?;
        Ok(ids)
    }
}

// The following three functions are helpers to the create_upgrade_delete method.
// They are also used by the respective create/flag_for_delete/update_compaction_level methods.
async fn create_parquet_file<'q, E>(
    executor: E,
    parquet_file_params: &ParquetFileParams,
) -> Result<ParquetFileId>
where
    E: Executor<'q, Database = Postgres>,
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

    let partition_hash_id_ref = &partition_hash_id.as_ref();
    let query = sqlx::query_scalar::<_, ParquetFileId>(
        r#"
INSERT INTO parquet_file (
    shard_id, table_id, partition_id, partition_hash_id, object_store_id,
    min_time, max_time, file_size_bytes,
    row_count, compaction_level, created_at, namespace_id, column_set, max_l0_created_at )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 )
RETURNING id;
        "#,
    )
    .bind(TRANSITION_SHARD_ID) // $1
    .bind(table_id) // $2
    .bind(partition_id) // $3
    .bind(partition_hash_id_ref) // $4
    .bind(object_store_id) // $5
    .bind(min_time) // $6
    .bind(max_time) // $7
    .bind(file_size_bytes) // $8
    .bind(row_count) // $9
    .bind(compaction_level) // $10
    .bind(created_at) // $11
    .bind(namespace_id) // $12
    .bind(column_set) // $13
    .bind(max_l0_created_at); // $14

    let parquet_file_id = query.fetch_one(executor).await.map_err(|e| {
        if is_unique_violation(&e) {
            Error::FileExists {
                object_store_id: *object_store_id,
            }
        } else if is_fk_violation(&e) {
            Error::ForeignKeyViolation { source: e }
        } else {
            Error::SqlxError { source: e }
        }
    })?;

    Ok(parquet_file_id)
}

async fn flag_for_delete<'q, E>(
    executor: E,
    ids: &[ParquetFileId],
    marked_at: Timestamp,
) -> Result<()>
where
    E: Executor<'q, Database = Postgres>,
{
    let query = sqlx::query(r#"UPDATE parquet_file SET to_delete = $1 WHERE id = ANY($2);"#)
        .bind(marked_at) // $1
        .bind(ids); // $2
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
) -> Result<()>
where
    E: Executor<'q, Database = Postgres>,
{
    let query = sqlx::query(
        r#"
UPDATE parquet_file
SET compaction_level = $1
WHERE id = ANY($2);
        "#,
    )
    .bind(compaction_level) // $1
    .bind(parquet_file_ids); // $2
    query
        .execute(executor)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

    Ok(())
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

/// Test helpers postgres testing.
#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use rand::Rng;
    use sqlx::migrate::MigrateDatabase;

    pub const TEST_DSN_ENV: &str = "TEST_INFLUXDB_IOX_CATALOG_DSN";

    /// Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
    /// variables are not set.
    macro_rules! maybe_skip_integration {
        ($panic_msg:expr) => {{
            dotenvy::dotenv().ok();

            let required_vars = [crate::postgres::test_utils::TEST_DSN_ENV];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match std::env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

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

    pub(crate) use maybe_skip_integration;

    pub async fn create_db(dsn: &str) {
        // Create the catalog database if it doesn't exist
        if !Postgres::database_exists(dsn).await.unwrap() {
            // Ignore failure if another test has already created the database
            let _ = Postgres::create_database(dsn).await;
        }
    }

    pub async fn setup_db_no_migration() -> PostgresCatalog {
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

        let options = PostgresConnectionOptions {
            app_name: String::from("test"),
            schema_name: schema_name.clone(),
            dsn,
            max_conns: 3,
            ..Default::default()
        };
        let pg = PostgresCatalog::connect(options, metrics)
            .await
            .expect("failed to connect catalog");

        // Create the test schema
        pg.pool
            .execute(format!("CREATE SCHEMA {schema_name};").as_str())
            .await
            .expect("failed to create test schema");

        // Ensure the test user has permission to interact with the test schema.
        pg.pool
            .execute(
                format!(
                    "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                )
                .as_str(),
            )
            .await
            .expect("failed to grant privileges to schema");

        pg
    }

    pub async fn setup_db() -> PostgresCatalog {
        let pg = setup_db_no_migration().await;
        // Run the migrations against this random schema.
        pg.setup().await.expect("failed to initialise database");
        pg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        postgres::test_utils::{
            create_db, maybe_skip_integration, setup_db, setup_db_no_migration,
        },
        test_helpers::{arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table},
    };
    use assert_matches::assert_matches;
    use data_types::partition_template::TemplatePart;
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use metric::{Attributes, DurationHistogram, Metric, Observation, RawReporter};
    use std::{io::Write, ops::Deref, sync::Arc, time::Instant};
    use tempfile::NamedTempFile;
    use test_helpers::maybe_start_logging;

    fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert!(hit_count > 0, "metric did not record any calls");
    }

    /// Small no-op test just to print out the migrations.
    ///
    /// This is helpful to look up migration checksums and debug parsing of the migration files.
    #[test]
    fn print_migrations() {
        println!("{:#?}", MIGRATOR.deref());
    }

    #[tokio::test]
    async fn test_migration() {
        maybe_skip_integration!();
        maybe_start_logging();

        let postgres = setup_db_no_migration().await;

        // 1st setup
        postgres.setup().await.unwrap();

        // 2nd setup
        postgres.setup().await.unwrap();
    }

    #[tokio::test]
    async fn test_migration_generic() {
        use crate::migrate::test_utils::test_migration;

        maybe_skip_integration!();
        maybe_start_logging();

        test_migration(&MIGRATOR, || async {
            setup_db_no_migration().await.into_pool()
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_catalog() {
        maybe_skip_integration!();

        let postgres = setup_db().await;

        // Validate the connection time zone is the expected UTC value.
        let tz: String = sqlx::query_scalar("SHOW TIME ZONE;")
            .fetch_one(&postgres.pool)
            .await
            .expect("read time zone");
        assert_eq!(tz, "UTC");

        let pool = postgres.pool.clone();
        let schema_name = postgres.schema_name().to_string();

        let postgres: Arc<dyn Catalog> = Arc::new(postgres);

        crate::interface::test_helpers::test_catalog(|| async {
            // Clean the schema.
            pool
                .execute(format!("DROP SCHEMA {schema_name} CASCADE").as_str())
                .await
                .expect("failed to clean schema between tests");

            // Recreate the test schema
            pool
                .execute(format!("CREATE SCHEMA {schema_name};").as_str())
                .await
                .expect("failed to create test schema");

            // Ensure the test user has permission to interact with the test schema.
            pool
                .execute(
                    format!(
                        "GRANT USAGE ON SCHEMA {schema_name} TO public; GRANT CREATE ON SCHEMA {schema_name} TO public;"
                    )
                    .as_str(),
                )
                .await
                .expect("failed to grant privileges to schema");

            // Run the migrations against this random schema.
            postgres.setup().await.expect("failed to initialise database");

            Arc::clone(&postgres)
        })
        .await;
    }

    #[tokio::test]
    async fn test_partition_create_or_get_idempotent() {
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories().await;

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
        // Test: sort_key_ids from partition_create_or_get_idempotent
        assert!(a.sort_key_ids().unwrap().is_empty());

        // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent.
        let b = repos
            .partitions()
            .create_or_get(key.clone(), table_id)
            .await
            .expect("idempotent write should succeed");

        assert_eq!(a, b);

        // Check that the hash_id is saved in the database and is returned when queried.
        let table_partitions = postgres
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
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories().await;

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
    ( $1, $2, $3, '{}', '{}')
ON CONFLICT ON CONSTRAINT partition_key_unique
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
        assert!(partition.hash_id().is_none());

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
        eprintln!("TEST_DSN={test_dsn}");

        // create a temp file to store the initial dsn
        let mut dsn_file = NamedTempFile::new().expect("create temp file");
        dsn_file
            .write_all(test_dsn.as_bytes())
            .expect("write temp file");

        const TEST_APPLICATION_NAME: &str = "test_application_name";
        let dsn_good = format!("dsn-file://{}", dsn_file.path().display());
        eprintln!("dsn_good={dsn_good}");

        // create a hot swap pool with test application name and dsn file pointing to tmp file.
        // we will later update this file and the pool should be replaced.
        let options = PostgresConnectionOptions {
            app_name: TEST_APPLICATION_NAME.to_owned(),
            schema_name: String::from("test"),
            dsn: dsn_good,
            max_conns: 3,
            hotswap_poll_interval: POLLING_INTERVAL,
            ..Default::default()
        };
        let metrics = Arc::new(metric::Registry::new());
        let pool = new_pool(&options, metrics).await.expect("connect");
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
            .write_all(format!("?application_name={TEST_APPLICATION_NAME_NEW}").as_bytes())
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

    macro_rules! test_column_create_or_get_many_unchecked {
        (
            $name:ident,
            calls = {$([$($col_name:literal => $col_type:expr),+ $(,)?]),+},
            want = $($want:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_column_create_or_get_many_unchecked_ $name>]() {
                    maybe_skip_integration!();

                    let postgres = setup_db().await;
                    let metrics = Arc::clone(&postgres.metrics);
                    let postgres: Arc<dyn Catalog> = Arc::new(postgres);
                    let mut repos = postgres.repositories().await;

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
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories().await;
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
        let f1 = repos.parquet_files().create(p1.clone()).await.unwrap();
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
        // we marked the first file of size 1337 for deletion leaving only the second that was 2x
        // that
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
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories().await;

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
        maybe_skip_integration!();

        let postgres = setup_db().await;
        let pool = postgres.pool.clone();
        let postgres: Arc<dyn Catalog> = Arc::new(postgres);
        let mut repos = postgres.repositories().await;

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

    #[tokio::test]
    async fn test_metrics() {
        maybe_skip_integration!();

        let postgres = setup_db_no_migration().await;

        let mut reporter = RawReporter::default();
        postgres.metrics.report(&mut reporter);
        assert_eq!(
            reporter
                .metric("sqlx_postgres_connections")
                .unwrap()
                .observation(&[("pool_id", "0"), ("state", "min")])
                .unwrap(),
            &Observation::U64Gauge(1),
        );
        assert_eq!(
            reporter
                .metric("sqlx_postgres_connections")
                .unwrap()
                .observation(&[("pool_id", "0"), ("state", "max")])
                .unwrap(),
            &Observation::U64Gauge(3),
        );
    }
}
