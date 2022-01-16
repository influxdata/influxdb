//! A Postgres backed implementation of the Catalog

use crate::interface::{
    Column, ColumnRepo, ColumnSchema, ColumnType, Error, KafkaTopic, KafkaTopicRepo, Namespace,
    NamespaceRepo, NamespaceSchema, QueryPool, QueryPoolRepo, RepoCollection, Result, Sequencer,
    SequencerRepo, Table, TableRepo, TableSchema,
};
use async_trait::async_trait;
use observability_deps::tracing::info;
use sqlx::{postgres::PgPoolOptions, Executor, Pool, Postgres};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

const MAX_CONNECTIONS: u32 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(500);
#[allow(dead_code)]
const SCHEMA_NAME: &str = "iox_catalog";

/// Connect to the catalog store.
pub async fn connect_catalog_store(
    app_name: &'static str,
    schema_name: &'static str,
    dsn: &str,
) -> Result<Pool<Postgres>, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(MAX_CONNECTIONS)
        .connect_timeout(CONNECT_TIMEOUT)
        .idle_timeout(IDLE_TIMEOUT)
        .test_before_acquire(true)
        .after_connect(move |c| {
            Box::pin(async move {
                // Tag the connection with the provided application name.
                c.execute(sqlx::query("SET application_name = '$1';").bind(app_name))
                    .await?;
                let search_path_query = format!("SET search_path TO {}", schema_name);
                c.execute(sqlx::query(&search_path_query)).await?;

                Ok(())
            })
        })
        .connect(dsn)
        .await?;

    // Log a connection was successfully established and include the application
    // name for cross-correlation between Conductor logs & database connections.
    info!(application_name=%app_name, "connected to catalog store");

    Ok(pool)
}

struct PostgresCatalog {
    pool: Pool<Postgres>,
}

impl RepoCollection for Arc<PostgresCatalog> {
    fn kafka_topic(&self) -> Arc<dyn KafkaTopicRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn KafkaTopicRepo + Sync + Send>
    }

    fn query_pool(&self) -> Arc<dyn QueryPoolRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn QueryPoolRepo + Sync + Send>
    }

    fn namespace(&self) -> Arc<dyn NamespaceRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn NamespaceRepo + Sync + Send>
    }

    fn table(&self) -> Arc<dyn TableRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn TableRepo + Sync + Send>
    }

    fn column(&self) -> Arc<dyn ColumnRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn ColumnRepo + Sync + Send>
    }

    fn sequencer(&self) -> Arc<dyn SequencerRepo + Sync + Send> {
        Self::clone(self) as Arc<dyn SequencerRepo + Sync + Send>
    }
}

#[async_trait]
impl KafkaTopicRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str) -> Result<KafkaTopic> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
INSERT INTO kafka_topic ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT kafka_topic_name_unique
DO UPDATE SET name = kafka_topic.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl QueryPoolRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT query_pool_name_unique
DO UPDATE SET name = query_pool.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl NamespaceRepo for PostgresCatalog {
    async fn create(
        &self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: i32,
        query_pool_id: i16,
    ) -> Result<NamespaceSchema> {
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
        .fetch_one(&self.pool)
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

        Ok(NamespaceSchema::new(rec.id, kafka_topic_id, query_pool_id))
    }

    async fn get_by_name(&self, name: &str) -> Result<Option<NamespaceSchema>> {
        // TODO: maybe get all the data in a single call to Postgres?
        let rec = sqlx::query_as::<_, Namespace>(
            r#"
SELECT * FROM namespace WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(&self.pool)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let namespace = rec.map_err(|e| Error::SqlxError { source: e })?;
        // get the columns first just in case someone else is creating schema while we're doing this.
        let columns = ColumnRepo::list_by_namespace_id(self, namespace.id).await?;
        let tables = TableRepo::list_by_namespace_id(self, namespace.id).await?;

        let mut namespace = NamespaceSchema::new(
            namespace.id,
            namespace.kafka_topic_id,
            namespace.query_pool_id,
        );

        let mut table_id_to_schema = BTreeMap::new();
        for t in tables {
            table_id_to_schema.insert(t.id, (t.name, TableSchema::new(t.id)));
        }

        for c in columns {
            let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
            match ColumnType::try_from(c.column_type) {
                Ok(column_type) => {
                    t.columns.insert(
                        c.name,
                        ColumnSchema {
                            id: c.id,
                            column_type,
                        },
                    );
                }
                _ => {
                    return Err(Error::UnknownColumnType {
                        data_type: c.column_type,
                        name: c.name.to_string(),
                    });
                }
            }
        }

        for (_, (table_name, schema)) in table_id_to_schema {
            namespace.tables.insert(table_name, schema);
        }

        return Ok(Some(namespace));
    }
}

#[async_trait]
impl TableRepo for PostgresCatalog {
    async fn create_or_get(&self, name: &str, namespace_id: i32) -> Result<Table> {
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
        .fetch_one(&self.pool)
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

    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Table>(
            r#"
SELECT * FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl ColumnRepo for PostgresCatalog {
    async fn create_or_get(
        &self,
        name: &str,
        table_id: i32,
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
        .fetch_one(&self.pool)
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

    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Column>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::SqlxError { source: e })?;

        Ok(rec)
    }
}

#[async_trait]
impl SequencerRepo for PostgresCatalog {
    async fn create_or_get(&self, topic: &KafkaTopic, partition: i32) -> Result<Sequencer> {
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
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn list(&self) -> Result<Vec<Sequencer>> {
        sqlx::query_as::<_, Sequencer>(r#"SELECT * FROM sequencer;"#)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::SqlxError { source: e })
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
    use crate::{create_or_get_default_records, validate_or_insert_schema};
    use futures::{stream::FuturesOrdered, StreamExt};
    use influxdb_line_protocol::parse_lines;
    use std::env;

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

    async fn setup_db() -> (Arc<PostgresCatalog>, KafkaTopic, QueryPool) {
        let dsn = std::env::var("DATABASE_URL").unwrap();
        let pool = connect_catalog_store("test", SCHEMA_NAME, &dsn)
            .await
            .unwrap();
        let postgres_catalog = Arc::new(PostgresCatalog { pool });

        let (kafka_topic, query_pool, _) = create_or_get_default_records(2, &postgres_catalog)
            .await
            .unwrap();
        (postgres_catalog, kafka_topic, query_pool)
    }

    #[tokio::test]
    async fn test_catalog() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let (postgres, kafka_topic, query_pool) = setup_db().await;
        clear_schema(&postgres.pool).await;

        let namespace = NamespaceRepo::create(postgres.as_ref(), "foo", "inf", 0, 0).await;
        assert!(matches!(
            namespace.unwrap_err(),
            Error::ForeignKeyViolation { source: _ }
        ));
        let namespace = NamespaceRepo::create(
            postgres.as_ref(),
            "foo",
            "inf",
            kafka_topic.id,
            query_pool.id,
        )
        .await
        .unwrap();
        assert!(namespace.id > 0);
        assert_eq!(namespace.kafka_topic_id, kafka_topic.id);
        assert_eq!(namespace.query_pool_id, query_pool.id);

        // test that we can create or get a table
        let t = TableRepo::create_or_get(postgres.as_ref(), "foo", namespace.id)
            .await
            .unwrap();
        let tt = TableRepo::create_or_get(postgres.as_ref(), "foo", namespace.id)
            .await
            .unwrap();
        assert!(t.id > 0);
        assert_eq!(t, tt);

        // test that we can craete or get a column
        let c = ColumnRepo::create_or_get(postgres.as_ref(), "foo", t.id, ColumnType::I64)
            .await
            .unwrap();
        let cc = ColumnRepo::create_or_get(postgres.as_ref(), "foo", t.id, ColumnType::I64)
            .await
            .unwrap();
        assert!(c.id > 0);
        assert_eq!(c, cc);

        // test that attempting to create an already defined column of a different type returns error
        let err = ColumnRepo::create_or_get(postgres.as_ref(), "foo", t.id, ColumnType::F64)
            .await
            .expect_err("should error with wrong column type");
        assert!(matches!(
            err,
            Error::ColumnTypeMismatch {
                name: _,
                existing: _,
                new: _
            }
        ));

        // now test with a new namespace
        let namespace = NamespaceRepo::create(
            postgres.as_ref(),
            "asdf",
            "inf",
            kafka_topic.id,
            query_pool.id,
        )
        .await
        .unwrap();
        let data = r#"
m1,t1=a,t2=b f1=2i,f2=2.0 1
m1,t1=a f1=3i 2
m2,t3=b f1=true 1
        "#;

        // test that new schema gets returned
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let schema = Arc::new(NamespaceSchema::new(
            namespace.id,
            namespace.kafka_topic_id,
            namespace.query_pool_id,
        ));
        let new_schema = validate_or_insert_schema(lines, &schema, &postgres)
            .await
            .unwrap();
        let new_schema = new_schema.unwrap();

        // ensure new schema is in the db
        let schema_from_db = NamespaceRepo::get_by_name(postgres.as_ref(), "asdf")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema_from_db);

        // test that a new table will be created
        let data = r#"
m1,t1=c f1=1i 2
new_measurement,t9=a f10=true 1
        "#;
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, &schema_from_db, &postgres)
            .await
            .unwrap()
            .unwrap();
        let new_table = new_schema.tables.get("new_measurement").unwrap();
        assert_eq!(
            ColumnType::Bool,
            new_table.columns.get("f10").unwrap().column_type
        );
        assert_eq!(
            ColumnType::Tag,
            new_table.columns.get("t9").unwrap().column_type
        );
        let schema = NamespaceRepo::get_by_name(postgres.as_ref(), "asdf")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);

        // test that a new column for an existing table will be created
        // test that a new table will be created
        let data = r#"
m1,new_tag=c new_field=1i 2
        "#;
        let lines: Vec<_> = parse_lines(data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, &schema, &postgres)
            .await
            .unwrap()
            .unwrap();
        let table = new_schema.tables.get("m1").unwrap();
        assert_eq!(
            ColumnType::I64,
            table.columns.get("new_field").unwrap().column_type
        );
        assert_eq!(
            ColumnType::Tag,
            table.columns.get("new_tag").unwrap().column_type
        );
        let schema = NamespaceRepo::get_by_name(postgres.as_ref(), "asdf")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);
    }

    #[tokio::test]
    async fn test_sequencers() {
        maybe_skip_integration!();

        let (postgres, kafka_topic, _query_pool) = setup_db().await;
        clear_schema(&postgres.pool).await;

        // Create 10 sequencers
        let created = (1..=10)
            .map(|partition| {
                SequencerRepo::create_or_get(postgres.as_ref(), &kafka_topic, partition)
            })
            .collect::<FuturesOrdered<_>>()
            .map(|v| {
                let v = v.expect("failed to create sequencer");
                (v.id, v)
            })
            .collect::<BTreeMap<_, _>>()
            .await;

        // List them and assert they match
        let listed = SequencerRepo::list(postgres.as_ref())
            .await
            .expect("failed to list sequencers")
            .into_iter()
            .map(|v| (v.id, v))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(created, listed);
    }

    async fn clear_schema(pool: &Pool<Postgres>) {
        sqlx::query("delete from column_name;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from table_name;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from namespace;")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("delete from sequencer;")
            .execute(pool)
            .await
            .unwrap();
    }
}
