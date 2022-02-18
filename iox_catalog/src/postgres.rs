//! A Postgres backed implementation of the Catalog

use crate::interface::{
    sealed::TransactionFinalize, Catalog, Column, ColumnRepo, ColumnType, Error, KafkaPartition,
    KafkaTopic, KafkaTopicId, KafkaTopicRepo, Namespace, NamespaceId, NamespaceRepo, ParquetFile,
    ParquetFileId, ParquetFileRepo, Partition, PartitionId, PartitionInfo, PartitionRepo,
    ProcessedTombstone, ProcessedTombstoneRepo, QueryPool, QueryPoolId, QueryPoolRepo,
    RepoCollection, Result, SequenceNumber, Sequencer, SequencerId, SequencerRepo, Table, TableId,
    TableRepo, Timestamp, Tombstone, TombstoneId, TombstoneRepo, Transaction,
};
use async_trait::async_trait;
use observability_deps::tracing::{info, warn};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, Acquire, Executor, Postgres, Row};
use sqlx_hotswap_pool::HotSwapPool;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

const MAX_CONNECTIONS: u32 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(500);
/// the default schema name to use in Postgres
pub const SCHEMA_NAME: &str = "iox_catalog";

/// The file pointed to by a `dsn-file://` DSN is polled for change every `HOTSWAP_POLL_INTERVAL`.
const HOTSWAP_POLL_INTERVAL: std::time::Duration = Duration::from_secs(5);

static MIGRATOR: Migrator = sqlx::migrate!();

/// PostgreSQL catalog.
#[derive(Debug)]
pub struct PostgresCatalog {
    pool: HotSwapPool<Postgres>,
}

// struct to get return value from "select count(*) ..." wuery"
#[derive(sqlx::FromRow)]
struct Count {
    count: i64,
}

impl PostgresCatalog {
    /// Connect to the catalog store.
    pub async fn connect(app_name: &str, schema_name: &str, dsn: &str) -> Result<Self> {
        let pool = new_pool(app_name, schema_name, dsn, HOTSWAP_POLL_INTERVAL)
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Self { pool })
    }
}

/// transaction for [`PostgresCatalog`].
#[derive(Debug)]
pub struct PostgresTxn {
    inner: PostgresTxnInner,
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

            // SQLx ensures that the inner transaction enqueues a rollback when it is dropped, so we don't need to spawn
            // a task here to call `rollback` manually.
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
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e.into() })?;

        Ok(())
    }

    async fn start_transaction(&self) -> Result<Box<dyn Transaction>, Error> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::SqlxError { source: e })?;

        Ok(Box::new(PostgresTxn {
            inner: PostgresTxnInner::Txn(Some(transaction)),
        }))
    }

    async fn repositories(&self) -> Box<dyn RepoCollection> {
        Box::new(PostgresTxn {
            inner: PostgresTxnInner::Oneshot(self.pool.clone()),
        })
    }
}

/// Creates a new [`sqlx::Pool`] from a database config and an explicit DSN.
///
/// This function doesn't support the IDPE specific `dsn-file://` uri scheme.
async fn new_raw_pool(
    app_name: &str,
    schema_name: &str,
    dsn: &str,
) -> Result<sqlx::Pool<Postgres>, sqlx::Error> {
    let app_name = app_name.to_owned();
    let app_name2 = app_name.clone(); // just to log below
    let schema_name = schema_name.to_owned();
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(MAX_CONNECTIONS)
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
                    sqlx::query_scalar("SELECT current_setting('application_name')")
                        .fetch_one(&mut *c)
                        .await?;
                if current_application_name.is_empty() {
                    sqlx::query("SELECT set_config('application_name', $1, false)")
                        .bind(&*app_name)
                        .execute(&mut *c)
                        .await?;
                }
                let search_path_query = format!("SET search_path TO {}", schema_name);
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
    polling_interval: Duration,
) -> Result<HotSwapPool<Postgres>, sqlx::Error> {
    let app_name: Arc<str> = Arc::from(app_name);
    let schema_name: Arc<str> = Arc::from(schema_name);
    let parsed_dsn = match get_dsn_file_path(dsn) {
        Some(filename) => std::fs::read_to_string(&filename)?,
        None => dsn.to_owned(),
    };
    let pool = HotSwapPool::new(new_raw_pool(&app_name, &schema_name, &parsed_dsn).await?);

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
                    pool: &HotSwapPool<Postgres>,
                ) -> Result<Option<String>, sqlx::Error> {
                    let new_dsn = std::fs::read_to_string(&dsn_file)?;
                    if new_dsn == current_dsn {
                        Ok(None)
                    } else {
                        let new_pool = new_raw_pool(app_name, schema_name, &new_dsn).await?;
                        pool.replace(new_pool);
                        Ok(Some(new_dsn))
                    }
                }

                match try_update(&app_name, &schema_name, &current_dsn, &dsn_file, &pool).await {
                    Ok(None) => {}
                    Ok(Some(new_dsn)) => {
                        info!("replaced hotswap pool");
                        current_dsn = new_dsn;
                    }
                    Err(e) => {
                        warn!(error=%e, filename=%dsn_file, "not replacing hotswap pool because of an error connecting to the new DSN");
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
DO UPDATE SET name = kafka_topic.name RETURNING *;
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
SELECT * FROM kafka_topic WHERE name = $1;
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
DO UPDATE SET name = query_pool.name RETURNING *;
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
RETURNING *
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

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Namespace>> {
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT * FROM namespace WHERE name = $1;
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
}

#[async_trait]
impl TableRepo for PostgresTxn {
    async fn create_or_get(&mut self, name: &str, namespace_id: NamespaceId) -> Result<Table> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
INSERT INTO table_name ( name, namespace_id )
VALUES ( $1, $2 )
ON CONFLICT ON CONSTRAINT table_name_unique
DO UPDATE SET name = table_name.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&namespace_id) // $2
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        Ok(rec)
    }

    async fn list_by_namespace_id(&mut self, namespace_id: NamespaceId) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT * FROM table_name
WHERE namespace_id = $1;
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
VALUES ( $1, $2, $3 )
ON CONFLICT ON CONSTRAINT column_name_unique
DO UPDATE SET name = column_name.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&table_id) // $2
        .bind(&ct) // $3
        .fetch_one(&mut self.inner)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        if rec.column_type != ct {
            return Err(Error::ColumnTypeMismatch {
                name: name.to_string(),
                existing: rec.name,
                new: column_type.to_string(),
            });
        }

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
        DO UPDATE SET kafka_topic_id = sequencer.kafka_topic_id RETURNING *;
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
SELECT * FROM sequencer WHERE kafka_topic_id = $1 AND kafka_partition = $2;
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
        DO UPDATE SET partition_key = partition.partition_key RETURNING *;
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

    async fn list_by_sequencer(&mut self, sequencer_id: SequencerId) -> Result<Vec<Partition>> {
        sqlx::query_as::<_, Partition>(r#"SELECT * FROM partition WHERE sequencer_id = $1;"#)
            .bind(&sequencer_id) // $1
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
        SELECT namespace.name as namespace_name, table_name.name as table_name, partition.id,
               partition.sequencer_id, partition.table_id, partition.partition_key
        FROM partition
        INNER JOIN table_name on table_name.id = partition.table_id
        INNER JOIN namespace on namespace.id = table_name.namespace_id
        WHERE partition.id = $1;"#,
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
        };

        Ok(Some(PartitionInfo {
            namespace_name,
            table_name,
            partition,
        }))
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
        DO UPDATE SET table_id = tombstone.table_id RETURNING *;
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

    async fn list_tombstones_by_sequencer_greater_than(
        &mut self,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    ) -> Result<Vec<Tombstone>> {
        sqlx::query_as::<_, Tombstone>(r#"SELECT * FROM tombstone WHERE sequencer_id = $1 AND sequence_number > $2 ORDER BY id;"#)
            .bind(&sequencer_id) // $1
            .bind(&sequence_number) // $2
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
    }
}

#[async_trait]
impl ParquetFileRepo for PostgresTxn {
    async fn create(
        &mut self,
        sequencer_id: SequencerId,
        table_id: TableId,
        partition_id: PartitionId,
        object_store_id: Uuid,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
        min_time: Timestamp,
        max_time: Timestamp,
    ) -> Result<ParquetFile> {
        let rec = sqlx::query_as::<_, ParquetFile>(
            r#"
INSERT INTO parquet_file ( sequencer_id, table_id, partition_id, object_store_id, min_sequence_number, max_sequence_number, min_time, max_time, to_delete )
VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, false )
RETURNING *
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
            .fetch_one(&mut self.inner)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    Error::FileExists {
                        object_store_id,
                    }
                } else if is_fk_violation(&e) {
                    Error::ForeignKeyViolation { source: e }
                } else {
                    Error::SqlxError { source: e }
                }
            })?;

        Ok(rec)
    }

    async fn flag_for_delete(&mut self, id: ParquetFileId) -> Result<()> {
        let _ = sqlx::query(r#"UPDATE parquet_file SET to_delete = true WHERE id = $1;"#)
            .bind(&id) // $1
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
        sqlx::query_as::<_, ParquetFile>(r#"SELECT * FROM parquet_file WHERE sequencer_id = $1 AND max_sequence_number > $2 ORDER BY id;"#)
            .bind(&sequencer_id) // $1
            .bind(&sequence_number) // $2
            .fetch_all(&mut self.inner)
            .await
            .map_err(|e| Error::SqlxError { source: e })
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

    async fn count(&mut self) -> Result<i64> {
        let read_result =
            sqlx::query_as::<_, Count>(r#"SELECT count(*) as count  FROM parquet_file;"#)
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
            RETURNING *
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
            r#"SELECT count(*) as count FROM processed_tombstone WHERE parquet_file_id = $1 AND tombstone_id = $2;"#)
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
    use crate::create_or_get_default_records;

    use super::*;

    use rand::Rng;
    use std::{env, ops::DerefMut, sync::Arc};
    use std::{io::Write, time::Instant};

    use tempfile::NamedTempFile;

    // Helper macro to skip tests if TEST_INTEGRATION and the AWS environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["DATABASE_URL"];
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
                return;
            }
        }};
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

        let dsn = std::env::var("DATABASE_URL").unwrap();
        let pg = PostgresCatalog::connect("test", &schema_name, &dsn)
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
        maybe_skip_integration!();

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
        let test_dsn = std::env::var("DATABASE_URL").unwrap();
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
}
