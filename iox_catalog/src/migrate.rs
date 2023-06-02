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
//! ## Transactions
//! Each step will be executed in its own transaction. However you can opt-out of this:
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
//! ## Non-SQL steps
//! At the moment, we only support SQL-based migrationsteps but other step types can easily be added.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::Deref,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use observability_deps::tracing::info;
use siphasher::sip::SipHasher13;
use sqlx::{
    migrate::{AppliedMigration, Migrate, MigrateError, Migration, MigrationType, Migrator},
    query, query_scalar, Acquire, Connection, Executor, PgConnection,
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
            Self::SqlStatement {
                sql,
                in_transaction,
            } => {
                if *in_transaction {
                    conn.exec_with_transaction(sql).await?;
                } else {
                    conn.exec_without_transaction(sql).await?;
                }
            }
        }

        Ok(())
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
    pub steps: Vec<IOxMigrationStep>,

    /// Checksum of the given steps.
    pub checksum: Cow<'static, [u8]>,
}

impl IOxMigration {
    /// Apply migration and return elapsed wall-clock time (measured locally).
    async fn apply<C>(&self, conn: &mut C) -> Result<Duration, MigrateError>
    where
        C: IOxMigrate,
    {
        info!(
            version = self.version,
            description = self.description.as_ref(),
            steps = self.steps.len(),
            "applying migration"
        );
        let start = Instant::now();

        conn.start_migration(self).await?;

        for (i, step) in self.steps.iter().enumerate() {
            info!(
                version = self.version,
                step = i + 1,
                "applying migration step"
            );
            step.apply(conn).await?;
        }

        let elapsed = start.elapsed();
        conn.finish_migration(self, elapsed).await?;

        info!(
            version = self.version,
            description = self.description.as_ref(),
            steps = self.steps.len(),
            elapsed_secs = elapsed.as_secs_f64(),
            "migration applied"
        );

        Ok(elapsed)
    }
}

impl From<&Migration> for IOxMigration {
    fn from(migration: &Migration) -> Self {
        assert!(
            migration.migration_type == MigrationType::Simple,
            "migration type has to be simple but is {:?}",
            migration.migration_type,
        );

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
        Self {
            version: migration.version,
            description: migration.description.clone(),
            steps,
            // Keep original (unprocessed) checksum for backwards compatibility.
            checksum: migration.checksum.clone(),
        }
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
    /// # Panics
    /// Panics if migrations are not sorted or if there are duplication [versions](IOxMigration::version).
    pub fn new(migrations: impl IntoIterator<Item = IOxMigration>) -> Self {
        let migrations = migrations.into_iter().collect::<Vec<_>>();

        if let Some(m) = migrations.windows(2).find(|m| m[0].version > m[1].version) {
            panic!(
                "migrations are not sorted: version {} is before {} but should not",
                m[0].version, m[1].version,
            );
        }
        if let Some(m) = migrations.windows(2).find(|m| m[0].version == m[1].version) {
            panic!(
                "migrations are not not unique: version {} found twice",
                m[0].version,
            );
        }

        Self { migrations }
    }

    /// Run migragtor on connection/pool.
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

    /// Run migragtor on open connection.
    ///
    /// See docs for [run](Self::run).
    async fn run_direct<C>(&self, conn: &mut C) -> Result<HashSet<i64>, MigrateError>
    where
        C: IOxMigrate,
    {
        let lock_id = conn.generate_lock_id().await?;
        <C as IOxMigrate>::lock(conn, lock_id).await?;

        // creates [_migrations] table only if needed
        // eventually this will likely migrate previous versions of the table
        conn.ensure_migrations_table().await?;

        let version = conn.dirty_version().await?;
        if let Some(version) = version {
            // We currently assume that migrations are NOT idempotent and hence we cannot re-apply them.
            return Err(MigrateError::Dirty(version));
        }

        let applied_migrations = conn.list_applied_migrations().await?;
        validate_applied_migrations(&applied_migrations, self)?;

        let applied_migrations: HashSet<_> =
            applied_migrations.into_iter().map(|m| m.version).collect();

        let mut new_migrations = HashSet::new();
        for migration in &self.migrations {
            if applied_migrations.contains(&migration.version) {
                continue;
            }
            migration.apply(conn).await?;
            new_migrations.insert(migration.version);
        }

        // unlock the migrator to allow other migrators to run
        // but do nothing as we already migrated
        <C as IOxMigrate>::unlock(conn, lock_id).await?;

        Ok(new_migrations)
    }
}

impl From<&Migrator> for IOxMigrator {
    fn from(migrator: &Migrator) -> Self {
        assert!(
            !migrator.ignore_missing,
            "`Migragtor::ignore_missing` MUST NOT be set"
        );
        assert!(migrator.locking, "`Migrator::locking` MUST be set");

        let migrations = migrator
            .migrations
            .iter()
            .map(|migration| migration.into())
            .collect::<Vec<_>>();

        Self::new(migrations)
    }
}

/// Validate an already-applied migration.
///
/// Checks that:
///
/// - applied migration is known
/// - checksum of applied migration and known migration match
fn validate_applied_migrations(
    applied_migrations: &[AppliedMigration],
    migrator: &IOxMigrator,
) -> Result<(), MigrateError> {
    let migrations: HashMap<_, _> = migrator.migrations.iter().map(|m| (m.version, m)).collect();

    for applied_migration in applied_migrations {
        match migrations.get(&applied_migration.version) {
            None => {
                return Err(MigrateError::VersionMissing(applied_migration.version));
            }
            Some(migration) => {
                if migration.checksum != applied_migration.checksum {
                    return Err(MigrateError::VersionMismatch(migration.version));
                }
            }
        }
    }

    Ok(())
}

/// Interface of a specific database implementation (like Postgres) and the IOx migration system.
///
/// This mostly delegates to the SQLx [`Migrate`] interface but also has some extra methods.
#[async_trait]
pub trait IOxMigrate: Migrate + Send {
    /// Generate a lock ID that is used for [`lock`](Self::lock) and [`unlock`](Self::unlock).
    async fn generate_lock_id(&mut self) -> Result<i64, MigrateError>;

    /// Lock database for migrations.
    async fn lock(&mut self, lock_id: i64) -> Result<(), MigrateError>;

    /// Unlock database after migration.
    async fn unlock(&mut self, lock_id: i64) -> Result<(), MigrateError>;

    /// Start a migration and mark it as "not finished".
    async fn start_migration(&mut self, migration: &IOxMigration) -> Result<(), MigrateError>;

    /// Finish a migration and register the elapsed time.
    async fn finish_migration(
        &mut self,
        migration: &IOxMigration,
        elapsed: Duration,
    ) -> Result<(), MigrateError>;

    /// Execute a SQL statement (that may contain multiple sub-statements) within a transaction block.
    ///
    /// Note that the SQL text can in theory contain `BEGIN`/`COMMIT` commands but shouldn't.
    async fn exec_with_transaction(&mut self, sql: &str) -> Result<(), MigrateError>;

    /// Execute a SQL statement (that may contain multiple sub-statements) without a transaction block.
    async fn exec_without_transaction(&mut self, sql: &str) -> Result<(), MigrateError>;
}

#[async_trait]
impl IOxMigrate for PgConnection {
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

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn unlock(&mut self, lock_id: i64) -> Result<(), MigrateError> {
        let _ = query("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .execute(self)
            .await?;

        Ok(())
    }

    async fn start_migration(&mut self, migration: &IOxMigration) -> Result<(), MigrateError> {
        let _ = query(
            r#"
INSERT INTO _sqlx_migrations ( version, description, success, checksum, execution_time )
VALUES ( $1, $2, FALSE, $3, -1 )
            "#,
        )
        .bind(migration.version)
        .bind(&*migration.description)
        .bind(&*migration.checksum)
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

    async fn exec_with_transaction(&mut self, sql: &str) -> Result<(), MigrateError> {
        let mut tx = <Self as Connection>::begin(self).await?;
        let _ = tx.execute(sql).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn exec_without_transaction(&mut self, sql: &str) -> Result<(), MigrateError> {
        let _ = self.execute(sql).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod generic {
        use super::*;

        #[test]
        #[should_panic(
            expected = "migrations are not sorted: version 2 is before 1 but should not"
        )]
        fn test_migrator_new_panic_not_sorted() {
            IOxMigrator::new([
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: vec![],
                    checksum: vec![].into(),
                },
                IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: vec![],
                    checksum: vec![].into(),
                },
            ]);
        }

        #[test]
        #[should_panic(expected = "migrations are not not unique: version 2 found twice")]
        fn test_migrator_new_panic_not_unique() {
            IOxMigrator::new([
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: vec![],
                    checksum: vec![].into(),
                },
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: vec![],
                    checksum: vec![].into(),
                },
            ]);
        }

        #[test]
        #[should_panic(expected = "`Migrator::locking` MUST be set")]
        fn test_convert_migrator_from_sqlx_panic_no_locking() {
            let _ = IOxMigrator::from(&Migrator {
                migrations: vec![].into(),
                ignore_missing: false,
                locking: false,
            });
        }

        #[test]
        #[should_panic(expected = "`Migragtor::ignore_missing` MUST NOT be set")]
        fn test_convert_migrator_from_sqlx_panic_ignore_missing() {
            let _ = IOxMigrator::from(&Migrator {
                migrations: vec![].into(),
                ignore_missing: true,
                locking: true,
            });
        }

        #[test]
        #[should_panic(expected = "migration type has to be simple but is ReversibleUp")]
        fn test_convert_migrator_from_sqlx_panic_invalid_migration_type_rev_up() {
            let _ = IOxMigrator::from(&Migrator {
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
            });
        }

        #[test]
        #[should_panic(expected = "migration type has to be simple but is ReversibleDown")]
        fn test_convert_migrator_from_sqlx_panic_invalid_migration_type_rev_down() {
            let _ = IOxMigrator::from(&Migrator {
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
            });
        }

        #[test]
        fn test_convert_migrator_from_sqlx_ok() {
            let actual = IOxMigrator::from(&Migrator {
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
                ]
                .into(),
                ignore_missing: false,
                locking: true,
            });

            let expected = IOxMigrator {
                migrations: vec![
                    IOxMigration {
                        version: 1,
                        description: "some descr".into(),
                        steps: vec![IOxMigrationStep::SqlStatement {
                            sql: "SELECT 1;".into(),
                            in_transaction: true,
                        }],
                        checksum: vec![1, 2, 3].into(),
                    },
                    IOxMigration {
                        version: 10,
                        description: "more descr".into(),
                        steps: vec![
                            IOxMigrationStep::SqlStatement {
                                sql: "SELECT 2;".into(),
                                in_transaction: true,
                            },
                            IOxMigrationStep::SqlStatement {
                                sql: "-- IOX_NO_TRANSACTION\nSELECT 3;".into(),
                                in_transaction: false,
                            },
                        ],
                        checksum: vec![4, 5, 6].into(),
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
        use test_helpers::maybe_start_logging;

        use crate::postgres::test_utils::{maybe_skip_integration, setup_db_no_migration};

        use super::*;

        #[tokio::test]
        async fn test_step_sql_statement_no_transaction() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            conn.execute("CREATE TABLE t (x INT);").await.unwrap();

            let create_index_concurrently = "CREATE INDEX CONCURRENTLY i ON t (x);";

            // `CREATE INDEX CONCURRENTLY` is NOT possible w/ a transaction. Verify that.
            IOxMigrationStep::SqlStatement {
                sql: create_index_concurrently.into(),
                in_transaction: true,
            }
            .apply(conn)
            .await
            .unwrap_err();

            // ... but it IS possible w/o a transaction.
            IOxMigrationStep::SqlStatement {
                sql: create_index_concurrently.into(),
                in_transaction: false,
            }
            .apply(conn)
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn test_migrator_happy_path() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::new([
                IOxMigration {
                    version: 1,
                    description: "".into(),
                    steps: vec![
                        IOxMigrationStep::SqlStatement {
                            sql: "CREATE TABLE t (x INT);".into(),
                            in_transaction: false,
                        },
                        IOxMigrationStep::SqlStatement {
                            sql: "INSERT INTO t (x) VALUES (1); INSERT INTO t (x) VALUES (10);"
                                .into(),
                            in_transaction: true,
                        },
                    ],
                    checksum: vec![].into(),
                },
                IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: vec![IOxMigrationStep::SqlStatement {
                        sql: "INSERT INTO t (x) VALUES (100);".into(),
                        in_transaction: true,
                    }],
                    checksum: vec![].into(),
                },
            ]);

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1, 2]));

            let r = sqlx::query_as::<_, Res>("SELECT SUM(x)::INT AS r FROM t;")
                .fetch_one(conn)
                .await
                .unwrap()
                .r;

            assert_eq!(r, 111);
        }

        #[tokio::test]
        async fn test_migrator_only_apply_new_migrations() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![IOxMigrationStep::SqlStatement {
                    // NOT idempotent!
                    sql: "CREATE TABLE t (x INT);".into(),
                    in_transaction: false,
                }],
                checksum: vec![].into(),
            }]);

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([1]));

            let migrator =
                IOxMigrator::new(migrator.migrations.iter().cloned().chain([IOxMigration {
                    version: 2,
                    description: "".into(),
                    steps: vec![IOxMigrationStep::SqlStatement {
                        // NOT idempotent!
                        sql: "CREATE TABLE s (x INT);".into(),
                        in_transaction: false,
                    }],
                    checksum: vec![].into(),
                }]));

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([2]));

            let applied = migrator.run_direct(conn).await.unwrap();
            assert_eq!(applied, HashSet::from([]));
        }

        #[tokio::test]
        async fn test_migrator_fail_migration_missing() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![],
                checksum: vec![].into(),
            }]);

            migrator.run_direct(conn).await.unwrap();

            let migrator = IOxMigrator::new([IOxMigration {
                version: 2,
                description: "".into(),
                steps: vec![],
                checksum: vec![].into(),
            }]);

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but is missing in the resolved migrations"
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_checksum_mismatch() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![],
                checksum: vec![1, 2, 3].into(),
            }]);

            migrator.run_direct(conn).await.unwrap();

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![],
                checksum: vec![4, 5, 6].into(),
            }]);

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 was previously applied but has been modified"
            );
        }

        #[tokio::test]
        async fn test_migrator_fail_dirty() {
            maybe_skip_integration!();
            let mut conn = setup().await;
            let conn = &mut *conn;

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![IOxMigrationStep::SqlStatement {
                    sql: "foo".into(),
                    in_transaction: false,
                }],
                checksum: vec![1, 2, 3].into(),
            }]);

            migrator.run_direct(conn).await.unwrap_err();

            let migrator = IOxMigrator::new([IOxMigration {
                version: 1,
                description: "".into(),
                steps: vec![],
                // same checksum, but now w/ valid steps (to simulate a once failed SQL statement)
                checksum: vec![1, 2, 3].into(),
            }]);

            let err = migrator.run_direct(conn).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                "migration 1 is partially applied; fix and remove row from `_sqlx_migrations` table"
            );
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
            let pool = setup_db_no_migration().await.into_pool();

            let migrator = Arc::new(IOxMigrator::new((0..N_TABLES_AND_INDICES).map(|i| {
                IOxMigration {
                    version: i as i64,
                    description: "".into(),
                    steps: vec![
                        IOxMigrationStep::SqlStatement {
                            sql: format!("CREATE TABLE t{i} (x INT);").into(),
                            in_transaction: false,
                        },
                        IOxMigrationStep::SqlStatement {
                            sql: format!("CREATE INDEX CONCURRENTLY i{i} ON t{i} (x);").into(),
                            in_transaction: false,
                        },
                    ],
                    checksum: vec![].into(),
                }
            })));

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

        async fn setup() -> PoolConnection<Postgres> {
            maybe_start_logging();

            let pool = setup_db_no_migration().await.into_pool();
            pool.acquire().await.unwrap()
        }

        #[derive(sqlx::FromRow)]
        struct Res {
            r: i32,
        }
    }
}
