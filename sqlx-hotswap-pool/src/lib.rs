#![doc = include_str!("../README.md")]
#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro
)]
#![allow(clippy::missing_docs_in_private_items, clippy::type_complexity)]

use std::sync::{Arc, RwLock};

use either::Either;
use futures::{future::BoxFuture, prelude::stream::BoxStream};
use sqlx::{
    database::HasStatement, pool::PoolConnection, Acquire, Database, Describe, Error, Execute,
    Executor, Pool, Transaction,
};

/// A `HotSwapPool` is a Pool that wraps another Pool and it allows the pool to
/// be replaced at runtime.
#[derive(Debug)]
pub struct HotSwapPool<DB>
where
    DB: Database,
{
    pool: Arc<RwLock<Arc<Pool<DB>>>>,
}

impl<DB> HotSwapPool<DB>
where
    DB: Database,
{
    /// Creates a new [`HotSwapPool`] from a [`Pool`].
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            pool: Arc::new(RwLock::new(Arc::new(pool))),
        }
    }

    /// Replaces the underlying [`Pool`] with `new_pool`, returning
    /// the previous pool
    ///
    /// Existing connections obtained by performing operations on the pool
    /// before the call to `replace` are still valid.
    ///
    /// This method affects new operations only.
    pub fn replace(&self, new_pool: Pool<DB>) -> Arc<Pool<DB>> {
        let mut t = Arc::new(new_pool);
        let mut pool = self.pool.write().expect("poisoned");
        // swap the new pool for the old pool
        std::mem::swap(&mut t, &mut *pool);
        t
    }
}

impl<DB> Clone for HotSwapPool<DB>
where
    DB: Database,
{
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
        }
    }
}

impl<'a, DB> Acquire<'a> for &'_ HotSwapPool<DB>
where
    DB: Database,
{
    type Database = DB;

    type Connection = PoolConnection<DB>;

    fn acquire(self) -> BoxFuture<'static, Result<Self::Connection, Error>> {
        let pool = self.pool.read().expect("poisoned");
        Box::pin(pool.acquire())
    }

    fn begin(self) -> BoxFuture<'static, Result<Transaction<'a, DB>, Error>> {
        let pool = self.pool.read().expect("poisoned");
        let pool = Arc::clone(&pool);
        Box::pin(async move { pool.begin().await })
    }
}

impl<'p, DB> Executor<'p> for &'_ HotSwapPool<DB>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    type Database = DB;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<DB::QueryResult, DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
    {
        let pool = self.pool.read().expect("poisoned");
        pool.fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
    {
        let pool = self.pool.read().expect("poisoned");
        pool.fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as HasStatement<'q>>::Statement, Error>> {
        let pool = self.pool.read().expect("poisoned");
        pool.prepare_with(sql, parameters)
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>> {
        let pool = self.pool.read().expect("poisoned");
        pool.describe(sql)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;

    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use sqlx::{postgres::PgPoolOptions, Postgres};

    // Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment variables
    // are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenvy::dotenv().ok();

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
                return;
            }
        }};
    }

    // test helper to create a regular (non hotswapping) DB connection
    async fn connect_db() -> Result<Pool<Postgres>, sqlx::Error> {
        // create a random schema for this particular pool
        let schema_name = {
            // use scope to make it clear to clippy / rust that `rng` is
            // not carried past await points
            let mut rng = rand::thread_rng();
            (&mut rng)
                .sample_iter(Alphanumeric)
                .filter(|c| c.is_ascii_alphabetic())
                .take(20)
                .map(char::from)
                .collect::<String>()
        };
        let dsn = std::env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").unwrap();
        let captured_schema_name = schema_name.clone();
        PgPoolOptions::new()
            .min_connections(1)
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(2))
            .idle_timeout(Duration::from_secs(500))
            .test_before_acquire(true)
            .after_connect(move |c, _meta| {
                let captured_schema_name = captured_schema_name.clone();
                Box::pin(async move {
                    // Tag the connection with the provided application name.
                    c.execute(sqlx::query("SET application_name = 'test';"))
                        .await?;

                    // Note can only bind data values, not schema names
                    let query = format!("CREATE SCHEMA IF NOT EXISTS {}", &captured_schema_name);
                    c.execute(sqlx::query(&query))
                        .await
                        .expect("failed to create schema");

                    let search_path_query = format!("SET search_path TO {captured_schema_name}");
                    c.execute(sqlx::query(&search_path_query)).await?;
                    Ok(())
                })
            })
            .connect(&dsn)
            .await
    }

    // The goal of this test is to verify that the hotswap pool can indeed replace
    // the pool. In the real world one would replace pools in order to use new
    // credentials. Here in our tests though just testing that it actually uses
    // a different pool handed over by the test utilities will be good enough.
    // The test utilities return an isolated namespace schema for every test. We'll
    // leverage that in order to verify that we indeed have swapped the pool at
    // runtime.
    #[tokio::test]
    async fn test_replace() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();
        println!("tests are running");

        let db = connect_db().await.unwrap();
        let db = HotSwapPool::new(db);

        sqlx::query("CREATE TABLE IF NOT EXISTS test (id int)")
            .execute(&db)
            .await
            .expect("executed");

        sqlx::query("INSERT INTO test (id) VALUES (1)")
            .execute(&db)
            .await
            .expect("executed");

        // Acquire a connection before replacing the pool. We'll use this later to prove
        // that having a connection from the old pool doesn't block our ability to
        // replace the pool.
        let mut conn = db.acquire().await.unwrap();

        // hot swap a new pool. The maybe_skip_postgres_integration_no_hotswap creates a
        // new schema namespace and sets it as the default namespace for all the
        // connections in that pool, this effectively means we won't find this table
        // anymore.
        let new_pool = connect_db().await.unwrap();
        db.replace(new_pool);

        // create the table so we can test if the row doesn't exist, which is nicer than
        // testing that we got a "relation not found" error.
        sqlx::query("CREATE TABLE IF NOT EXISTS test (id int)")
            .execute(&db)
            .await
            .expect("executed");

        // Perform an actual query on the previous pool.
        conn.fetch_one("SELECT id FROM test")
            .await
            .expect("got result");

        // Perform a query on the new pool. This pool uses the schema whose test table
        // has no rows.
        let res = sqlx::query("SELECT id FROM test")
            .fetch_optional(&db)
            .await
            .expect("got result");

        assert!(res.is_none());
    }

    #[tokio::test]
    async fn test_replace_with_outstanding_future() {
        maybe_skip_integration!();
        println!("tests are running");

        let db = connect_db().await.unwrap();
        let db = HotSwapPool::new(db);

        sqlx::query("CREATE TABLE IF NOT EXISTS test (id int)")
            .execute(&db)
            .await
            .expect("executed");

        // create a future from the pool but don't execute it yet
        let query_future = sqlx::query("CREATE TABLE IF NOT EXISTS test (id int)").execute(&db);

        // hot swap a new pool. This should not deadlock
        let new_pool = connect_db().await.unwrap();
        let old_pool = db.replace(new_pool);

        // Ensure there are outstanding references to the old pool
        // captured in the future
        assert_eq!(Arc::strong_count(&old_pool), 1);

        // resolve the future created prior to the previous
        // connection (executes against the new connection)
        query_future.await.expect("executed");

        // can also run queries successfully against the new pool
        sqlx::query("CREATE TABLE IF NOT EXISTS test (id int)")
            .execute(&db)
            .await
            .expect("executed");
    }
}
