//! Better migrations.
//!
//! # Why
//! SQLx migrations don't work for use, see:
//!
//! - <https://github.com/launchbadge/sqlx/issues/2085>
//! - <https://github.com/influxdata/influxdb_iox/issues/5031>
//!
//! # Usage
//! Just place your migration in the `migrations` folder. They basically work like normal SQLx migrations but there are
//! a few extra, magic comments you can put in your code to modify the behavior.
//!
//! ## Steps
//! The entire SQL text will be executed as a single statement. However you can split it into multiple steps by using a marker:
//!
//! ```sql
//! CREATE TABLE t1 (x INT);
//!
//! -- IOX_STEP_BOUNDARY
//!
//! CREATE TABLE t2 (x INT);
//! ```
//!
//! ## Transactions & Idemptoency
//! Each step will be executed within a transaction. However you can opt-out of this:
//!
//! ```sql
//! -- this step is wrapped in a transaction
//! CREATE TABLE t1 (x INT);
//!
//! -- IOX_STEP_BOUNDARY
//!
//! -- this step isn't
//! -- IOX_NO_TRANSACTION
//! CREATE TABLE t2 (x INT);
//! ```
//!
//! If all steps steps can run in a transaction, the entire migration including its bookkeeping will be executed in a
//! transaction. In this case the transaction is automatically idempotent.
//!
//! Migrations that opt out of the transaction handling MUST ensure that they are idempotent. This also includes that
//! they end up in the desired target state even if they were interrupted midway in a previous run.
//!
//! ## Updating / Fixing Migrations
//! **⚠️ In general a migration MUST NOT be updated / changed after it was committed to `main`. ⚠️**
//!
//! However there is one exception to this rule: if the new version has the same outcome when applied successfully. This
//! can be due to:
//!
//! - **Optimization:** The migration script turns out to be too slow in production workloads but you find a better
//!   version that does the same but runs faster.
//! - **Failure:** The script worked fine during testing but in prod it always fails, e.g. because it is missing NULL
//!   handling. It is important to remember that the fix MUST NOT change the outcome of the success runs.
//! - **Idemptoency:** The script works only w/o transactions (see section above) and cannot be re-applied when be
//!   interrupted midway. One common case is `CREATE INDEX CONCURRENTLY ...` where you MUST drop the index beforehand
//!   via `DROP INDEX IF EXISTS ...` because a previous interrupted migration might have left it in an invalid state.
//!   See ["Building Indexes Concurrently"].
//!
//! If you are very sure that you found a fix for your migration that does the same, you still MUST NOT just change it.
//! The reason is that we keep a checksum of the migration stored in the database and changing the script will change
//! the checksum will lead to a [failure](MigrateError::VersionMismatch) when running the migrations. You can work
//! around that by obtaining the old checksum (in hex) and add it to the new version as: `-- IOX_OTHER_CHECKSUM:
//! 42feedbull`. This pragma can be repeated multiple times.
//!
//! ### Example
//! The old script looks like this:
//!
//! ```sql
//! -- IOX_NO_TRANSACTION
//! SET statement_timeout TO '60min';
//!
//! -- IOX_STEP_BOUNDARY
//!
//! -- IOX_NO_TRANSACTION
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON t (x);
//! ```
//!
//! You can fix the idemptoency by updating it to:
//!
//! ```sql
//! -- IOX_OTHER_CHECKSUM: 067431eaa74f26ee86200aaed4992a5fe22354322102f1ed795e424ec529469079569072d856e96ee9fdb6cc848b6137
//! -- IOX_NO_TRANSACTION
//! SET statement_timeout TO '60min';
//!
//! -- IOX_STEP_BOUNDARY
//! DROP INDEX CONCURRENTLY IF EXISTS i;
//!
//! -- IOX_NO_TRANSACTION
//!
//! -- IOX_STEP_BOUNDARY
//!
//! -- IOX_NO_TRANSACTION
//! CREATE INDEX CONCURRENTLY IF NOT EXISTS i ON t (x);
//! ```
//!
//! ## Non-SQL steps
//! At the moment, we only support SQL-based migrationsteps but other step types can easily be added.
//!
//!
//! ["Building Indexes Concurrently"]: https://www.postgresql.org/docs/15/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::Deref,
    str::FromStr,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use observability_deps::tracing::{debug, info, warn};
use siphasher::sip::SipHasher13;
use sqlx::{
    migrate::{Migrate, MigrateError, Migration, MigrationType, Migrator},
    query, query_as, query_scalar, Acquire, Connection, Executor, PgConnection, Postgres,
    Transaction,
};

/// A single [`IOxMigration`] step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IOxMigrationStep {
    /// Execute a SQL statement.
    ///
    /// A SQL statement MAY contain multiple sub-statements, e.g.:
    ///
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS table1 (
    ///     id BIGINT GENERATED ALWAYS AS IDENTITY,
    ///     PRIMARY KEY (id),
    /// );
    ///
    /// CREATE TABLE IF NOT EXISTS table2 (
    ///     id BIGINT GENERATED ALWAYS AS IDENTITY,
    ///     PRIMARY KEY (id),
    /// );
    /// ```
    SqlStatement {
        /// The SQL text.
        ///
        /// If [`in_transaction`](Self::SqlStatement::in_transaction) is set, this MUST NOT contain any transaction modifiers like `COMMIT`/`ROLLBACK`/`BEGIN`!
        sql: Cow<'static, str>,

        /// Should the execution of the SQL text be wrapped into a transaction?
        ///
        /// Whenever possible, you likely want to set this to `true`. However some database changes like `CREATE INDEX
        /// CONCURRENTLY` under PostgreSQL cannot be executed within a transaction.
        in_transaction: bool,
    },
}

impl IOxMigrationStep {
    /// Apply migration step.
    async fn apply<C>(&self, conn: &mut C) -> Result<(), MigrateError>
    where
        C: IOxMigrate,
    {
        match self {
            Self::SqlStatement { sql, .. } => {
                conn.exec(sql).await?;
            }
        }

        Ok(())
    }

    /// Will this step set up a transaction if there is non yet?
    fn in_transaction(&self) -> bool {
        match self {
            Self::SqlStatement { in_transaction, .. } => *in_transaction,
        }
    }
}

/// Migration checksum.
#[derive(Clone, PartialEq, Eq)]
pub struct Checksum(Box<[u8]>);

impl Checksum {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in self.0.iter() {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<const N: usize> From<[u8; N]> for Checksum {
    fn from(value: [u8; N]) -> Self {
        Self(value.into())
    }
}

impl From<&[u8]> for Checksum {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

impl FromStr for Checksum {
    type Err = MigrateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..(i + 2).min(s.len())], 16))
            .collect::<Result<Box<[u8]>, _>>()
            .map_err(|e| {
                MigrateError::Source(format!("cannot parse checksum '{s}': {e}").into())
            })?;

        Ok(Self(inner))
    }
}

/// Database migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IOxMigration {
    /// Version.
    ///
    /// This is used to order migrations.
    pub version: i64,

    /// Humand-readable description.
    pub description: Cow<'static, str>,

    /// Steps that compose this migration.
    ///
    /// In most cases you want a single [SQL step](IOxMigrationStep::SqlStatement) which is executed
    /// [in a transaction](IOxMigrationStep::SqlStatement::in_transaction).
    pub steps: Box<[IOxMigrationStep]>,

    /// Checksum of the given steps.
    pub checksum: Checksum,

    /// Checksums of other versions of this migration that are known to be compatible.
    ///
    /// **Using this should be a rare exception!**
    ///
    /// This can be used to convert an non-idempotent migration into an idempotent one.
    pub other_compatible_checksums: Box<[Checksum]>,
}

impl IOxMigration {
    /// Apply migration and return elapsed wall-clock time (measured locally).
    async fn apply<C>(&self, conn: &mut C) -> Result<Duration, MigrateError>
    where
        C: IOxMigrate,
    {
        let single_transaction = self.single_transaction();
        info!(
            version = self.version,
            description = self.description.as_ref(),
            steps = self.steps.len(),
            single_transaction,
            "applying migration"
        );

        let elapsed = if single_transaction {
            let mut txn = conn.begin_txn().await?;
            let elapsed = {
                let conn = txn.acquire_conn().await?;
                self.apply_inner(conn, true).await?
            };
            txn.commit_txn().await?;
            elapsed
        } else {
            self.apply_inner(conn, false).await?
        };

        info!(
            version = self.version,
            description = self.description.as_ref(),
            steps = self.steps.len(),
            elapsed_secs = elapsed.as_secs_f64(),
            "migration applied"
        );

        Ok(elapsed)
    }

    /// Run actual application.
    ///
    /// This may or may NOT be guarded by an transaction block.
    async fn apply_inner<C>(&self, conn: &mut C, single_txn: bool) -> Result<Duration, MigrateError>
    where
        C: IOxMigrate,
    {
        let start = Instant::now();
        conn.start_migration(self).await?;

        for (i, step) in self.steps.iter().enumerate() {
            info!(
                version = self.version,
                steps = self.steps.len(),
                step = i + 1,
                single_txn,
                in_transaction = step.in_transaction(),
                "applying migration step"
            );

            if step.in_transaction() && !single_txn {
                let mut txn = conn.begin_txn().await?;
                {
                    let conn = txn.acquire_conn().await?;
                    step.apply(conn).await?;
                }
                txn.commit_txn().await?;
            } else {
                step.apply(conn).await?;
            }

            info!(
                version = self.version,
                steps = self.steps.len(),
                step = i + 1,
                "applied migration step"
            );
        }

        let elapsed = start.elapsed();
        conn.run_sanity_checks().await?;
        conn.finish_migration(self, elapsed).await?;

        Ok(elapsed)
    }

    /// This migration can be run in a single transaction and will never by dirty.
    pub fn single_transaction(&self) -> bool {
        self.steps.iter().all(|s| s.in_transaction())
    }
}

impl TryFrom<&Migration> for IOxMigration {
    type Error = MigrateError;

    fn try_from(migration: &Migration) -> Result<Self, Self::Error> {
        if migration.migration_type != MigrationType::Simple {
            return Err(MigrateError::Source(
                format!(
                    "migration type has to be simple but is {:?}",
                    migration.migration_type
                )
                .into(),
            ));
        }

        let other_compatible_checksums = migration
            .sql
            .lines()
            .filter_map(|s| {
                s.strip_prefix("-- IOX_OTHER_CHECKSUM:")
                    .map(|s| s.trim().parse())
            })
            .collect::<Result<_, _>>()?;

        let steps = migration
            .sql
            .split("-- IOX_STEP_BOUNDARY")
            .map(|sql| {
                let sql = sql.trim().to_owned();
                let in_transaction = !sql.contains("IOX_NO_TRANSACTION");
                IOxMigrationStep::SqlStatement {
                    sql: sql.into(),
                    in_transaction,
                }
            })
            .collect();

        Ok(Self {
            version: migration.version,
            description: migration.description.clone(),
            steps,
            // Keep original (unprocessed) checksum for backwards compatibility.
            checksum: migration.checksum.deref().into(),
            other_compatible_checksums,
        })
    }
}

/// Migration manager.
#[derive(Debug, PartialEq, Eq)]
pub struct IOxMigrator {
    /// List of migrations.
    migrations: Vec<IOxMigration>,
}

impl IOxMigrator {
    /// Create new migrator.
    ///
    /// # Error
    /// Fails if migrations are not sorted or if there are duplication [versions](IOxMigration::version).
    pub fn try_new(
        migrations: impl IntoIterator<Item = IOxMigration>,
    ) -> Result<Self, MigrateError> {
        let migrations = migrations.into_iter().collect::<Vec<_>>();

        if let Some(m) = migrations.windows(2).find(|m| m[0].version > m[1].version) {
            return Err(MigrateError::Source(
                format!(
                    "migrations are not sorted: version {} is before {} but should not",
                    m[0].version, m[1].version,
                )
                .into(),
            ));
        }
        if let Some(m) = migrations.windows(2).find(|m| m[0].version == m[1].version) {
            return Err(MigrateError::Source(
                format!(
                    "migrations are not not unique: version {} found twice",
                    m[0].version,
                )
                .into(),
            ));
        }

        Ok(Self { migrations })
    }

    /// Run migrator on connection/pool.
    ///
    /// Returns set of executed [migrations](IOxMigration).
    ///
    /// This may fail and some migrations may be applied. Also it is possible that a migration itself fails half-way in
    /// which case it is marked as dirty. Subsequent migrations will fail until the issue is resolved.
    pub async fn run<'a, A>(&self, migrator: A) -> Result<HashSet<i64>, MigrateError>
    where
        A: Acquire<'a> + Send,
        <A::Connection as Deref>::Target: IOxMigrate,
    {
        let mut conn = migrator.acquire().await?;
        self.run_direct(&mut *conn).await
    }

    /// Run migrator on open connection.
    ///
    /// See docs for [run](Self::run).
    async fn run_direct<C>(&self, conn: &mut C) -> Result<HashSet<i64>, MigrateError>
    where
        C: IOxMigrate,
    {
        let lock_id = conn.generate_lock_id().await?;
        <C as IOxMigrate>::lock(conn, lock_id).await?;

        let run_res = self.run_inner(conn).await;

        // always try to unlock, even when we failed.
        // While PG is timing out the lock, unlocking manually will give others the chance to re-lock faster. This is
        // mostly relevant for tests where we re-use connections.
        let unlock_res = <C as IOxMigrate>::unlock(conn, lock_id).await;

        // return first error but also first OK (there doesn't seem to be an stdlib method for this)
        match (run_res, unlock_res) {
            (Err(e), _) => Err(e),
            (Ok(_), Err(e)) => Err(e),
            (Ok(res), Ok(())) => Ok(res),
        }
    }

    /// Run migragtor.
    ///
    /// This expects that locking was already done.
    async fn run_inner<C>(&self, conn: &mut C) -> Result<HashSet<i64>, MigrateError>
    where
        C: IOxMigrate,
    {
        // creates [_migrations] table only if needed
        // eventually this will likely migrate previous versions of the table
        conn.ensure_migrations_table().await?;

        let applied_migrations = <C as IOxMigrate>::list_applied_migrations(conn).await?;
        validate_applied_migrations(&applied_migrations, self)?;

        let applied_and_not_dirty: HashSet<_> = applied_migrations
            .into_iter()
            .filter(|m| !m.dirty)
            .map(|m| m.version)
            .collect();

        let mut new_migrations = HashSet::new();
        for migration in &self.migrations {
            if applied_and_not_dirty.contains(&migration.version) {
                continue;
            }
            migration.apply(conn).await?;
            new_migrations.insert(migration.version);
        }

        Ok(new_migrations)
    }
}

impl TryFrom<&Migrator> for IOxMigrator {
    type Error = MigrateError;

    fn try_from(migrator: &Migrator) -> Result<Self, Self::Error> {
        if migrator.ignore_missing {
            return Err(MigrateError::Source(
                "`Migrator::ignore_missing` MUST NOT be set"
                    .to_owned()
                    .into(),
            ));
        }
        if !migrator.locking {
            return Err(MigrateError::Source(
                "`Migrator::locking` MUST be set".to_owned().into(),
            ));
        }

        let migrations = migrator
            .migrations
            .iter()
            .map(|migration| migration.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Self::try_new(migrations)
    }
}

/// Validate an already-applied migration.
///
/// Checks that:
///
/// - applied migration is known
/// - checksum of applied migration and known migration match
/// - new migrations are newer than both the successfully applied and the dirty version
/// - there is at most one dirty migration (bug check)
/// - the dirty migration is the last applied one (bug check)
fn validate_applied_migrations(
    applied_migrations: &[IOxAppliedMigration],
    migrator: &IOxMigrator,
) -> Result<(), MigrateError> {
    let migrations: HashMap<_, _> = migrator.migrations.iter().map(|m| (m.version, m)).collect();

    for applied_migration in applied_migrations {
        match migrations.get(&applied_migration.version) {
            None => {
                return Err(MigrateError::VersionMissing(applied_migration.version));
            }
            Some(migration) => {
                if !std::iter::once(&migration.checksum)
                    .chain(migration.other_compatible_checksums.iter())
                    .any(|cs| cs.as_bytes() == applied_migration.checksum.deref())
                {
                    return Err(MigrateError::VersionMismatch(migration.version));
                }
                if applied_migration.dirty {
                    warn!(
                        version = migration.version,
                        "found dirty migration, trying to recover"
                    );
                }
            }
        }
    }

    let dirty_versions = applied_migrations
        .iter()
        .filter(|m| m.dirty)
        .map(|m| m.version)
        .collect::<Vec<_>>();
    if dirty_versions.len() > 1 {
        return Err(MigrateError::Source(format!(
            "there are multiple dirty versions, this should not happen and is considered a bug: {:?}",
            dirty_versions,
        ).into()));
    }
    let dirty_version = dirty_versions.into_iter().next();

    let applied_last = applied_migrations
        .iter()
        .filter(|m| Some(m.version) != dirty_version)
        .map(|m| m.version)
        .max();
    if let (Some(applied_last), Some(dirty_version)) = (applied_last, dirty_version) {
        // algorithm error in this method, use an assertion
        assert_ne!(applied_last, dirty_version);

        if applied_last > dirty_version {
            // database state error, so use a proper error
            return Err(MigrateError::Source(format!(
                "dirty version ({dirty_version}) is not the last applied version ({applied_last}), this is a bug",
            ).into()));
        }
    }

    let applied_set = applied_migrations
        .iter()
        .map(|m| m.version)
        .collect::<HashSet<_>>();
    let new_first = migrator
        .migrations
        .iter()
        .filter(|m| !applied_set.contains(&m.version))
        .map(|m| m.version)
        .min();
    if let (Some(dirty_version), Some(new_first)) = (dirty_version, new_first) {
        // algorithm error in this method, use an assertion
        assert_ne!(dirty_version, new_first);

        if dirty_version > new_first {
            // database state error, so use a proper error
            return Err(MigrateError::Source(format!(
                "new migration ({new_first}) goes before dirty version ({dirty_version}), this should not have been merged!",
            ).into()));
        }
    }
    if let (Some(applied_last), Some(new_first)) = (applied_last, new_first) {
        // algorithm error in this method, use an assertion
        assert_ne!(applied_last, new_first);

        if applied_last > new_first {
            // database state error, so use a proper error
            return Err(MigrateError::Source(format!(
                "new migration ({new_first}) goes before last applied migration ({applied_last}), this should not have been merged!",
            ).into()));
        }
    }

    Ok(())
}

/// Information about an migration found in the database.
#[derive(Debug)]
pub struct IOxAppliedMigration {
    /// Version of the migration.
    pub version: i64,

    /// Checksum.
    pub checksum: Cow<'static, [u8]>,

    /// Dirty flag.
    ///
    /// If this is set, then the migration was interrupted midway.
    pub dirty: bool,
}

/// Transaction type linked to [`IOxMigrate`].
///
/// This is a separate type because we need to own the transaction object at some point before handing out mutable
/// borrows to the actual connection again.
#[async_trait]
pub trait IOxMigrateTxn: Send {
    /// The migration interface.
    type M: IOxMigrate;

    /// Acquire connection.
    async fn acquire_conn(&mut self) -> Result<&mut Self::M, MigrateError>;

    /// Commit transaction.
    async fn commit_txn(self) -> Result<(), MigrateError>;
}

/// Interface of a specific database implementation (like Postgres) and the IOx migration system.
///
/// This mostly delegates to the SQLx [`Migrate`] interface but also has some extra methods.
#[async_trait]
pub trait IOxMigrate: Connection + Migrate + Send {
    /// Transaction type.
    type Txn<'a>: IOxMigrateTxn
    where
        Self: 'a;

    /// Start new transaction.
    async fn begin_txn<'a>(&'a mut self) -> Result<Self::Txn<'a>, MigrateError>;

    /// Generate a lock ID that is used for [`lock`](Self::lock) and [`unlock`](Self::unlock).
    async fn generate_lock_id(&mut self) -> Result<i64, MigrateError>;

    /// Lock database for migrations.
    async fn lock(&mut self, lock_id: i64) -> Result<(), MigrateError>;

    /// Unlock database after migration.
    async fn unlock(&mut self, lock_id: i64) -> Result<(), MigrateError>;

    /// Get list of applied migrations.
    async fn list_applied_migrations(&mut self) -> Result<Vec<IOxAppliedMigration>, MigrateError>;

    /// Start a migration and mark it as "not finished".
    async fn start_migration(&mut self, migration: &IOxMigration) -> Result<(), MigrateError>;

    /// Finish a migration and register the elapsed time.
    async fn finish_migration(
        &mut self,
        migration: &IOxMigration,
        elapsed: Duration,
    ) -> Result<(), MigrateError>;

    /// Execute a SQL statement (that may contain multiple sub-statements)
    async fn exec(&mut self, sql: &str) -> Result<(), MigrateError>;

    /// Run DB-specific sanity checks on the schema.
    ///
    /// This mostly includes checks for "validity" markers (e.g. for indices).
    async fn run_sanity_checks(&mut self) -> Result<(), MigrateError>;
}

#[async_trait]
impl<'a> IOxMigrateTxn for Transaction<'a, Postgres> {
    type M = PgConnection;

    async fn acquire_conn(&mut self) -> Result<&mut Self::M, MigrateError> {
        let conn = self.acquire().await?;
        Ok(conn)
    }

    async fn commit_txn(self) -> Result<(), MigrateError> {
        self.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl IOxMigrate for PgConnection {
    type Txn<'a> = Transaction<'a, Postgres>;

    async fn begin_txn<'a>(&'a mut self) -> Result<Self::Txn<'a>, MigrateError> {
        let txn = <Self as Connection>::begin(self).await?;
        Ok(txn)
    }

    async fn generate_lock_id(&mut self) -> Result<i64, MigrateError> {
        let db: String = query_scalar("SELECT current_database()")
            .fetch_one(self)
            .await?;

        // A randomly generated static siphash key to ensure all migrations use the same locks.
        //
        // Generated with: xxd -i -l 16 /dev/urandom
        let key = [
            0xb8, 0x52, 0x81, 0x3c, 0x12, 0x83, 0x6f, 0xd9, 0x00, 0x4f, 0xe7, 0xe3, 0x61, 0xbd,
            0x03, 0xaf,
        ];

        let mut hasher = SipHasher13::new_with_key(&key);
        db.hash(&mut hasher);

        Ok(i64::from_ne_bytes(hasher.finish().to_ne_bytes()))
    }

    async fn lock(&mut self, lock_id: i64) -> Result<(), MigrateError> {
        loop {
            let is_locked: bool = query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_id)
                .fetch_one(&mut *self)
                .await?;

            if is_locked {
                return Ok(());
            }

            let t_wait = Duration::from_millis(20);
            debug!(
                lock_id,
                t_wait_millis = t_wait.as_millis(),
                "lock held, waiting"
            );
            tokio::time::sleep(t_wait).await;
        }
    }

    async fn unlock(&mut self, lock_id: i64) -> Result<(), MigrateError> {
        let was_locked: bool = query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .fetch_one(self)
            .await?;

        if !was_locked {
            return Err(MigrateError::Source(
                format!("did not own lock: {lock_id}").into(),
            ));
        }

        Ok(())
    }

    async fn list_applied_migrations(&mut self) -> Result<Vec<IOxAppliedMigration>, MigrateError> {
        let rows: Vec<(i64, Vec<u8>, bool)> = query_as(
            "SELECT version, checksum, NOT success FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(self)
        .await?;

        let migrations = rows
            .into_iter()
            .map(|(version, checksum, dirty)| IOxAppliedMigration {
                version,
                checksum: checksum.into(),
                dirty,
            })
            .collect();

        Ok(migrations)
    }

    async fn start_migration(&mut self, migration: &IOxMigration) -> Result<(), MigrateError> {
        let _ = query(
            r#"
INSERT INTO _sqlx_migrations ( version, description, success, checksum, execution_time )
VALUES ( $1, $2, FALSE, $3, -1 )
ON CONFLICT (version)
DO NOTHING
            "#,
        )
        .bind(migration.version)
        .bind(&*migration.description)
        .bind(migration.checksum.as_bytes())
        .execute(self)
        .await?;

        Ok(())
    }

    async fn finish_migration(
        &mut self,
        migration: &IOxMigration,
        elapsed: Duration,
    ) -> Result<(), MigrateError> {
        let _ = query(
            r#"
UPDATE _sqlx_migrations
SET success = TRUE, execution_time = $1
WHERE version = $2
            "#,
        )
        .bind(elapsed.as_nanos() as i64)
        .bind(migration.version)
        .execute(self)
        .await?;

        Ok(())
    }

    async fn exec(&mut self, sql: &str) -> Result<(), MigrateError> {
        let _ = self.execute(sql).await?;
        Ok(())
    }

    async fn run_sanity_checks(&mut self) -> Result<(), MigrateError> {
        let dirty_indices: Vec<String> = query_scalar(
            r#"
SELECT pg_class.relname
FROM pg_index
JOIN pg_class     ON pg_index.indexrelid   = pg_class.oid
JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
WHERE pg_namespace.nspname = current_schema() AND NOT pg_index.indisvalid
ORDER BY pg_class.relname
            "#,
        )
        .fetch_all(self)
        .await?;

        if !dirty_indices.is_empty() {
            return Err(MigrateError::Source(
                format!("Found invalid indexes: {}", dirty_indices.join(", ")).into(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod generic {
        use super::*;

        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_checksum_string_roundtrip(s: Vec<u8>) {
                let checksum_1 = Checksum::from(s.as_slice());
                let string_1 = checksum_1.to_string();
                let checksum_2 = Checksum::from_str(&string_1).unwrap();
                let string_2 = checksum_2.to_string();
                assert_eq!(checksum_1, checksum_2);
                assert_eq!(string_1, string_2);
            }
        }

        #[test]
        fn test_parse_valid_checksum() {
            let actual = Checksum::from_str("b88c635e27f8b9ba8547b24efcb081429a8f3e85b70f35916e1900dffc4e6a77eed8a02acc7c72526dd7d50166b63fbd").unwrap();
            let expected = Checksum::from([
                184, 140, 99, 94, 39, 248, 185, 186, 133, 71, 178, 78, 252, 176, 129, 66, 154, 143,
                62, 133, 183, 15, 53, 145, 110, 25, 0, 223, 252, 78, 106, 119, 238, 216, 160, 42,
                204, 124, 114, 82, 109, 215, 213, 1, 102, 182, 63, 189,
            ]);

            assert_eq!(actual, expected);
        }

        #[test]
        fn test_parse_invalid_checksum() {
            let err = Checksum::from_str("foo").unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: cannot parse checksum 'foo': invalid digit found in string",
            );
        }

        #[test]
        fn test_migrator_new_error_not_sorted() {
            let err = IOxMigrator::try_new([
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
                IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
            ])
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: migrations are not sorted: version 2 is before 1 but should not",
            );
        }

        #[test]
        fn test_migrator_new_error_not_unique() {
            let err = IOxMigrator::try_new([
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
            ])
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: migrations are not not unique: version 2 found twice",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_error_no_locking() {
            let err = IOxMigrator::try_from(&Migrator {
                migrations: vec![].into(),
                ignore_missing: false,
                locking: false,
            })
            .unwrap_err();
            assert_eq!(
                err.to_string(),
                "while resolving migrations: `Migrator::locking` MUST be set",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_error_ignore_missing() {
            let err = IOxMigrator::try_from(&Migrator {
                migrations: vec![].into(),
                ignore_missing: true,
                locking: true,
            })
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: `Migrator::ignore_missing` MUST NOT be set",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_error_invalid_migration_type_rev_up() {
            let err = IOxMigrator::try_from(&Migrator {
                migrations: vec![Migration {
                    version: 1,
                    description: "".into(),
                    migration_type: MigrationType::ReversibleUp,
                    sql: "".into(),
                    checksum: vec![].into(),
                }]
                .into(),
                ignore_missing: false,
                locking: true,
            })
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: migration type has to be simple but is ReversibleUp",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_error_invalid_migration_type_rev_down() {
            let err = IOxMigrator::try_from(&Migrator {
                migrations: vec![Migration {
                    version: 1,
                    description: "".into(),
                    migration_type: MigrationType::ReversibleDown,
                    sql: "".into(),
                    checksum: vec![].into(),
                }]
                .into(),
                ignore_missing: false,
                locking: true,
            })
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: migration type has to be simple but is ReversibleDown",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_error_invalid_other_compatible_checksum() {
            let err = IOxMigrator::try_from(&Migrator {
                migrations: vec![Migration {
                    version: 1,
                    description: "".into(),
                    migration_type: MigrationType::Simple,
                    sql: "-- IOX_OTHER_CHECKSUM: foo".into(),
                    checksum: vec![].into(),
                }]
                .into(),
                ignore_missing: false,
                locking: true,
            })
            .unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: cannot parse checksum 'foo': invalid digit found in string",
            );
        }

        #[test]
        fn test_convert_migrator_from_sqlx_ok() {
            let actual = IOxMigrator::try_from(&Migrator {
                migrations: vec![
                    Migration {
                        version: 1,
                        description: "some descr".into(),
                        migration_type: MigrationType::Simple,
                        sql: "SELECT 1;".into(),
                        checksum: vec![1, 2, 3].into(),
                    },
                    Migration {
                        version: 10,
                        description: "more descr".into(),
                        migration_type: MigrationType::Simple,
                        sql: "SELECT 2;\n-- IOX_STEP_BOUNDARY\n-- IOX_NO_TRANSACTION\nSELECT 3;"
                            .into(),
                        checksum: vec![4, 5, 6].into(),
                    },
                    Migration {
                        version: 11,
                        description: "xxx".into(),
                        migration_type: MigrationType::Simple,
                        sql: "-- IOX_OTHER_CHECKSUM:1ff\n-- IOX_OTHER_CHECKSUM:   2ff   \nSELECT4;"
                            .into(),
                        checksum: vec![7, 8, 9].into(),
                    },
                ]
                .into(),
                ignore_missing: false,
                locking: true,
            })
            .unwrap();

            let expected = IOxMigrator {
                migrations: vec![
                    IOxMigration {
                        version: 1,
                        description: "some descr".into(),
                        steps: [IOxMigrationStep::SqlStatement {
                            sql: "SELECT 1;".into(),
                            in_transaction: true,
                        }]
                        .into(),
                        checksum: [1, 2, 3].into(),
                        other_compatible_checksums: [].into(),
                    },
                    IOxMigration {
                        version: 10,
                        description: "more descr".into(),
                        steps: [
                            IOxMigrationStep::SqlStatement {
                                sql: "SELECT 2;".into(),
                                in_transaction: true,
                            },
                            IOxMigrationStep::SqlStatement {
                                sql: "-- IOX_NO_TRANSACTION\nSELECT 3;".into(),
                                in_transaction: false,
                            },
                        ]
                        .into(),
                        checksum: [4, 5, 6].into(),
                        other_compatible_checksums: [].into(),
                    },
                    IOxMigration {
                        version: 11,
                        description: "xxx".into(),
                        steps: [IOxMigrationStep::SqlStatement {
                            sql: "-- IOX_OTHER_CHECKSUM:1ff\n-- IOX_OTHER_CHECKSUM:   2ff   \nSELECT4;".into(),
                            in_transaction: true,
                        }]
                        .into(),
                        checksum: [7, 8, 9].into(),
                        other_compatible_checksums: [
                            Checksum::from_str("1ff").unwrap(),
                            Checksum::from_str("2ff").unwrap(),
                        ].into(),
                    },
                ],
            };

            assert_eq!(actual, expected);
        }
    }

    mod postgres {
        use std::sync::Arc;

        use futures::{stream::FuturesUnordered, StreamExt};
        use sqlx::{pool::PoolConnection, Postgres};
        use sqlx_hotswap_pool::HotSwapPool;
        use test_helpers::maybe_start_logging;

        use crate::postgres::test_utils::{maybe_skip_integration, setup_db_no_migration};

        use super::*;

        #[tokio::test]
        async fn test_lock_id_deterministic() {
            maybe_skip_integration!();

            let mut conn = setup().await;
            let conn = &mut *conn;

            let first = conn.generate_lock_id().await.unwrap();
            let second = conn.generate_lock_id().await.unwrap();
            assert_eq!(first, second);
        }

        #[tokio::test]
        async fn test_lock_unlock_twice() {
            maybe_skip_integration!();

            let mut conn = setup().await;
            let conn = &mut *conn;

            let lock_id = conn.generate_lock_id().await.unwrap();

            <PgConnection as IOxMigrate>::lock(conn, lock_id)
                .await
                .unwrap();
            <PgConnection as IOxMigrate>::unlock(conn, lock_id)
                .await
                .unwrap();

            <PgConnection as IOxMigrate>::lock(conn, lock_id)
                .await
                .unwrap();
            <PgConnection as IOxMigrate>::unlock(conn, lock_id)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_lock_prevents_2nd_lock() {
            maybe_skip_integration!();

            let pool = setup_pool().await;

            let mut conn1 = pool.acquire().await.unwrap();
            let conn1 = &mut *conn1;

            let mut conn2 = pool.acquire().await.unwrap();
            let conn2 = &mut *conn2;

            let lock_id = conn1.generate_lock_id().await.unwrap();

            <PgConnection as IOxMigrate>::lock(conn1, lock_id)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                <PgConnection as IOxMigrate>::lock(conn2, lock_id)
                    .await
                    .unwrap();
            })
            .await
            .unwrap_err();
            <PgConnection as IOxMigrate>::unlock(conn1, lock_id)
                .await
                .unwrap();

            <PgConnection as IOxMigrate>::lock(conn2, lock_id)
                .await
                .unwrap();
            <PgConnection as IOxMigrate>::unlock(conn2, lock_id)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_locks_are_scoped() {
            maybe_skip_integration!();

            let pool = setup_pool().await;

            let mut conn1 = pool.acquire().await.unwrap();
            let conn1 = &mut *conn1;

            let mut conn2 = pool.acquire().await.unwrap();
            let conn2 = &mut *conn2;

            let lock_id1 = conn1.generate_lock_id().await.unwrap();
            let lock_id2 = !lock_id1;

            <PgConnection as IOxMigrate>::lock(conn1, lock_id1)
                .await
                .unwrap();
            <PgConnection as IOxMigrate>::lock(conn1, lock_id2)
                .await
                .unwrap();
            <PgConnection as IOxMigrate>::unlock(conn1, lock_id1)
                .await
                .unwrap();

            // id2 is still lock (i.e. unlock is also scoped)
            tokio::time::timeout(Duration::from_secs(1), async {
                <PgConnection as IOxMigrate>::lock(conn2, lock_id2)
                    .await
                    .unwrap();
            })
            .await
            .unwrap_err();

            <PgConnection as IOxMigrate>::unlock(conn1, lock_id2)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_unlock_without_lock_fails() {
            maybe_skip_integration!();

            let mut conn = setup().await;
            let conn = &mut *conn;

            let lock_id = conn.generate_lock_id().await.unwrap();

            let err = <PgConnection as IOxMigrate>::unlock(conn, lock_id)
                .await
                .unwrap_err();

            assert_starts_with(
                &err.to_string(),
                "while resolving migrations: did not own lock:",
            );
        }

        #[tokio::test]
        async fn test_step_sql_statement_no_transaction() {
            maybe_skip_integration!();

            for in_transaction in [false, true] {
                println!("in_transaction: {in_transaction}");

                let mut conn = setup().await;
                let conn = &mut *conn;

                conn.execute("CREATE TABLE t (x INT);").await.unwrap();

                let migrator = IOxMigrator::try_new([IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: [IOxMigrationStep::SqlStatement {
                        sql: "CREATE INDEX CONCURRENTLY i ON t (x);".into(),
                        in_transaction,
                    }]
                    .into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                }])
                .unwrap();
                let res = migrator.run_direct(conn).await;

                match in_transaction {
                    false => {
                        assert_eq!(res.unwrap(), HashSet::from([1]),);
                    }
                    true => {
                        // `CREATE INDEX CONCURRENTLY` is NOT possible w/ a transaction. Verify that.
                        assert_eq!(
                            res.unwrap_err().to_string(),
                            "while executing migrations: error returned from database: CREATE INDEX CONCURRENTLY cannot run inside a transaction block",
                        );
                    }
                }
            }
        }

        #[tokio::test]
        async fn test_migrator_happy_path() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([
                IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: [
                        IOxMigrationStep::SqlStatement {
                            sql: "CREATE TABLE t (x INT);".into(),
                            in_transaction: false,
                        },
                        IOxMigrationStep::SqlStatement {
                            sql: "INSERT INTO t (x) VALUES (1); INSERT INTO t (x) VALUES (10);"
                                .into(),
                            in_transaction: true,
                        },
                    ]
                    .into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [IOxMigrationStep::SqlStatement {
                        sql: "INSERT INTO t (x) VALUES (100);".into(),
                        in_transaction: true,
                    }]
                    .into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                },
            ])
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1, 2]));

            let r: i32 = query_scalar("SELECT SUM(x)::INT AS r FROM t;")
                .fetch_one(conn)
                .await
                .unwrap();

            assert_eq!(r, 111);
        }

        #[tokio::test]
        async fn test_migrator_only_apply_new_migrations() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    // NOT idempotent!
                    sql: "CREATE TABLE t (x INT);".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1]));

            let migrator = IOxMigrator::try_new(
                migrator.migrations.iter().cloned().chain([IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [IOxMigrationStep::SqlStatement {
                        // NOT idempotent!
                        sql: "CREATE TABLE s (x INT);".into(),
                        in_transaction: false,
                    }]
                    .into(),
                    checksum: [].into(),
                    other_compatible_checksums: [].into(),
                }]),
            )
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([2]));

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([]));
        }

        #[tokio::test]
        async fn test_migrator_fail_clean_migration_missing() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 2,
                description: "".into(),
                steps: [].into(),
                checksum: [].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but is missing in the resolved migrations"
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_dirty_migration_missing() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    sql: "foo".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap_err();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 2,
                description: "".into(),
                steps: [].into(),
                checksum: [].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but is missing in the resolved migrations"
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_clean_checksum_mismatch() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [4, 5, 6].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but has been modified"
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_dirty_checksum_mismatch() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    sql: "foo".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap_err();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    sql: "foo".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [4, 5, 6].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but has been modified"
            );
        }

        #[tokio::test]
        async fn test_migrator_other_compatible_checksum() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [4, 5, 6].into(),
                other_compatible_checksums: [[1, 2, 3].into()].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap();
        }

        /// Migrations may have the same checksum.
        ///
        /// This is helpful if you want to revert a change later, e.g.:
        ///
        /// 1. add a index
        /// 2. remove the index
        /// 3. decide that you actually need the index again
        #[tokio::test]
        async fn test_migrator_migrations_can_have_same_checksum() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::try_new([
                IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [1, 2, 3].into(),
                    other_compatible_checksums: [].into(),
                },
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: [].into(),
                    checksum: [1, 2, 3].into(),
                    other_compatible_checksums: [].into(),
                },
            ])
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1, 2]));
        }

        #[tokio::test]
        async fn test_migrator_recover_dirty_same() {
            test_migrator_recover_dirty_inner(RecoverFromDirtyMode::Same).await;
        }

        #[tokio::test]
        async fn test_migrator_recover_dirty_fix_non_transactional() {
            test_migrator_recover_dirty_inner(RecoverFromDirtyMode::FixNonTransactional).await;
        }

        #[tokio::test]
        async fn test_migrator_recover_dirty_fix_transactional() {
            test_migrator_recover_dirty_inner(RecoverFromDirtyMode::FixTransactional).await;
        }

        /// Modes for [`test_migrator_recover_dirty_inner`]
        #[derive(Debug)]
        enum RecoverFromDirtyMode {
            /// Recover from a fluke.
            ///
            /// The checksum of the migration stays the same and it is non-transactional (otherwise we wouldn't have
            /// ended up in a dirty state to begin with).
            Same,

            /// Recover using a fixed version, the fix is still non-transactional.
            FixNonTransactional,

            /// Recover using a fixed version, the fix is transactional (in contrast to the original version).
            FixTransactional,
        }

        impl RecoverFromDirtyMode {
            fn same_checksum(&self) -> bool {
                match self {
                    Self::Same => true,
                    Self::FixNonTransactional => false,
                    Self::FixTransactional => false,
                }
            }

            fn fix_is_transactional(&self) -> bool {
                match self {
                    Self::Same => false,
                    Self::FixNonTransactional => false,
                    Self::FixTransactional => true,
                }
            }
        }

        async fn test_migrator_recover_dirty_inner(mode: RecoverFromDirtyMode) {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            conn.execute("CREATE TABLE t (x INT);").await.unwrap();
            let test_query = "SELECT COALESCE(SUM(x), 0)::INT AS r FROM t;";

            let steps_ok = vec![
                IOxMigrationStep::SqlStatement {
                    sql: "INSERT INTO t VALUES (1);".into(),
                    // set to NO transaction, otherwise the migrator will happily wrap the migration bookkeeping and the
                    // migration script itself into a single transaction to avoid the "dirty" state
                    in_transaction: mode.fix_is_transactional(),
                },
                IOxMigrationStep::SqlStatement {
                    sql: "INSERT INTO t VALUES (2);".into(),
                    in_transaction: mode.fix_is_transactional(),
                },
            ];

            let mut steps_broken = steps_ok.clone();
            steps_broken[0] = IOxMigrationStep::SqlStatement {
                sql: "foo".into(),
                // set to NO transaction, otherwise the migrator will happily wrap the migration bookkeeping and the
                // migration script itself into a single transaction to avoid the "dirty" state
                in_transaction: false,
            };

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: steps_broken.into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            migrator.run_direct(conn).await.unwrap_err();

            let r: i32 = query_scalar(test_query)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(r, 0);

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: steps_ok.into(),
                checksum: if mode.same_checksum() {
                    [1, 2, 3].into()
                } else {
                    [4, 5, 6].into()
                },
                other_compatible_checksums: if mode.same_checksum() {
                    [].into()
                } else {
                    [[1, 2, 3].into()].into()
                },
            }])
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1]));

            let r: i32 = query_scalar(test_query)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(r, 3);
        }

        #[tokio::test]
        async fn test_migrator_uses_single_transaction_when_possible() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            conn.execute("CREATE TABLE t (x INT);").await.unwrap();

            let steps_ok = vec![
                IOxMigrationStep::SqlStatement {
                    sql: "INSERT INTO t VALUES (1);".into(),
                    in_transaction: true,
                },
                IOxMigrationStep::SqlStatement {
                    sql: "INSERT INTO t VALUES (2);".into(),
                    in_transaction: true,
                },
                IOxMigrationStep::SqlStatement {
                    sql: "INSERT INTO t VALUES (3);".into(),
                    in_transaction: true,
                },
            ];

            // break in-between step that is sandwiched by two valid ones
            let mut steps_broken = steps_ok.clone();
            steps_broken[1] = IOxMigrationStep::SqlStatement {
                sql: "foo".into(),
                in_transaction: true,
            };

            let test_query = "SELECT COALESCE(SUM(x), 0)::INT AS r FROM t;";

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: steps_broken.into(),
                // use a placeholder checksum (normally this would be calculated based on the steps)
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();
            migrator.run_direct(conn).await.unwrap_err();

            // all or nothing: nothing
            let r: i32 = query_scalar(test_query)
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(r, 0);

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: steps_ok.into(),
                // same checksum, but now w/ valid steps (to simulate a once failed SQL statement)
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1]),);

            // all or nothing: all
            let r: i32 = query_scalar(test_query).fetch_one(conn).await.unwrap();
            assert_eq!(r, 6);
        }

        /// Tests that `CREATE INDEX CONCURRENTLY` doesn't deadlock.
        ///
        /// Originally we used SQLx to acquire the locks which uses `pg_advisory_lock`. However this seems to acquire a
        /// global "shared lock". Other migration frameworks faced the same issue and use `pg_try_advisory_lock`
        /// instead. Also see:
        ///
        /// - <https://github.com/flyway/flyway/issues/1654>
        /// - <https://github.com/flyway/flyway/commit/4a185ebcddfb7dac875b7afa5fa270aca621ce1d>
        #[tokio::test]
        async fn test_locking() {
            const N_TABLES_AND_INDICES: usize = 10;
            const N_CONCURRENT_MIGRATIONS: usize = 100;

            maybe_skip_integration!();
            maybe_start_logging();
            let pool = setup_pool().await;

            let migrator = Arc::new(
                IOxMigrator::try_new((0..N_TABLES_AND_INDICES).map(|i| {
                    IOxMigration {
                        version: i as i64,
                        description: "".into(),
                        steps: [
                            IOxMigrationStep::SqlStatement {
                                sql: format!("CREATE TABLE t{i} (x INT);").into(),
                                in_transaction: false,
                            },
                            IOxMigrationStep::SqlStatement {
                                sql: format!("CREATE INDEX CONCURRENTLY i{i} ON t{i} (x);").into(),
                                in_transaction: false,
                            },
                        ]
                        .into(),
                        checksum: [].into(),
                        other_compatible_checksums: [].into(),
                    }
                }))
                .unwrap(),
            );

            let mut futures: FuturesUnordered<_> = (0..N_CONCURRENT_MIGRATIONS)
                .map(move |_| {
                    let migrator = Arc::clone(&migrator);
                    let pool = pool.clone();
                    async move {
                        // pool might timeout, so add another retry loop around it
                        let mut conn = loop {
                            let pool = pool.clone();
                            if let Ok(conn) = pool.acquire().await {
                                break conn;
                            }
                        };
                        let conn = &mut *conn;
                        migrator.run_direct(conn).await.unwrap();
                    }
                })
                .collect();
            while futures.next().await.is_some() {}
        }

        /// This tests that:
        ///
        /// - indexes are sanity-checked
        /// - sanity checks are applied after each new/dirty migration and we keep the migration dirty until the checks pass
        /// - we can manually recover the database and make the non-idempotent migration pass
        #[tokio::test]
        async fn test_sanity_checks_index_1() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            conn.execute("CREATE TABLE t (x INT, y INT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1, 2);").await.unwrap();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    sql: "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS i ON t (x);".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            // fails because is not unique
            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(err.to_string(), "while executing migrations: error returned from database: could not create unique index \"i\"");

            // re-applying fails due to sanity checks
            // NOTE: Even though the actual migration script passes, the sanity checks DO NOT and hence the migration is
            //       still considered dirty. It will be re-applied after the manual intervention below.
            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "while resolving migrations: Found invalid indexes: i"
            );

            // fix data and wipe index
            conn.execute("DELETE FROM t WHERE y = 2;").await.unwrap();
            conn.execute("DROP INDEX i;").await.unwrap();

            // applying works
            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(HashSet::from([1]), applied);
        }

        /// This tests that:
        ///
        /// - indexes are sanity-checked
        /// - we can fix a data error and a proper, idempotent migration will eventually pass
        #[tokio::test]
        async fn test_sanity_checks_index_2() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            conn.execute("CREATE TABLE t (x INT, y INT);")
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1, 1);").await.unwrap();
            conn.execute("INSERT INTO t VALUES (1, 2);").await.unwrap();

            let migrator = IOxMigrator::try_new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: [
                    IOxMigrationStep::SqlStatement {
                        sql: "DROP INDEX IF EXISTS i;".into(),
                        in_transaction: false,
                    },
                    IOxMigrationStep::SqlStatement {
                        sql: "CREATE UNIQUE INDEX CONCURRENTLY i ON t (x);".into(),
                        in_transaction: false,
                    },
                ]
                .into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            }])
            .unwrap();

            // fails because is not unique
            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(err.to_string(), "while executing migrations: error returned from database: could not create unique index \"i\"");

            // re-applying fails with same error (index is wiped but fails w/ same error)
            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(err.to_string(), "while executing migrations: error returned from database: could not create unique index \"i\"");

            // fix data issue
            conn.execute("UPDATE t SET x = 2 WHERE y = 2")
                .await
                .unwrap();

            // now it works
            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(HashSet::from([1]), applied);
        }

        #[tokio::test]
        async fn test_migrator_fail_new_migration_before_applied() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migration_1 = IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            };
            let migration_2 = IOxMigration {
                version: 2,
                description: "".into(),
                steps: [].into(),
                checksum: [4, 5, 6].into(),
                other_compatible_checksums: [].into(),
            };

            let migrator = IOxMigrator::try_new([migration_2.clone()]).unwrap();

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(HashSet::from([2]), applied);

            let migrator = IOxMigrator::try_new([migration_1, migration_2]).unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: new migration (1) goes before last applied migration (2), this should not have been merged!",
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_new_migration_before_dirty() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migration_1 = IOxMigration {
                version: 1,
                description: "".into(),
                steps: [].into(),
                checksum: [1, 2, 3].into(),
                other_compatible_checksums: [].into(),
            };
            let migration_2 = IOxMigration {
                version: 2,
                description: "".into(),
                steps: [IOxMigrationStep::SqlStatement {
                    sql: "foo".into(),
                    in_transaction: false,
                }]
                .into(),
                checksum: [4, 5, 6].into(),
                other_compatible_checksums: [].into(),
            };

            let migrator = IOxMigrator::try_new([migration_2.clone()]).unwrap();

            migrator.run_direct(conn).await.unwrap_err();

            let migrator = IOxMigrator::try_new([migration_1, migration_2]).unwrap();

            let err = migrator.run_direct(conn).await.unwrap_err();

            assert_eq!(
                err.to_string(),
                "while resolving migrations: new migration (1) goes before dirty version (2), this should not have been merged!",
            );
        }

        async fn setup_pool() -> HotSwapPool<Postgres> {
            maybe_start_logging();

            setup_db_no_migration().await.into_pool()
        }

        async fn setup() -> PoolConnection<Postgres> {
            let pool = setup_pool().await;
            pool.acquire().await.unwrap()
        }

        #[track_caller]
        fn assert_starts_with(s: &str, prefix: &str) {
            if !s.starts_with(prefix) {
                panic!("'{s}' does not start with '{prefix}'");
            }
        }
    }
}
