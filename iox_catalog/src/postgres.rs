//! A Postgres backed implementation of the Catalog

use crate::TIME_COLUMN;
use influxdb_line_protocol::{FieldValue, ParsedLine};
use snafu::{ResultExt, Snafu};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Executor};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use observability_deps::tracing::info;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("Unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("Foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("Column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: String,
        new: String,
    },

    #[snafu(display(
        "Column type {} is in the db for column {}, which is unknown",
        data_type,
        name
    ))]
    UnknownColumnType { data_type: i16, name: String },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

const MAX_CONNECTIONS: u32 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(500);
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
                c.execute(sqlx::query("SET application_name = '$1';").bind(app_name)).await?;
                let search_path_query = format!("SET search_path TO {}", schema_name);
                c.execute(sqlx::query(&search_path_query))
                    .await?;

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

#[derive(Debug, sqlx::FromRow)]
struct KafkaTopic {
    id: i32,
    name: String,
}

impl KafkaTopic {
    async fn create_or_get(name: &str, pool: &Pool<Postgres>) -> Result<KafkaTopic> {
        let rec = sqlx::query_as::<_, KafkaTopic>(
            r#"
INSERT INTO kafka_topic ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT kafka_topic_name_unique
DO UPDATE SET name = kafka_topic.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(pool)
        .await
        .context(SqlxSnafu)?;

        Ok(rec)
    }
}

#[derive(Debug, sqlx::FromRow)]
struct QueryPool {
    id: i16,
    name: String,
}

impl QueryPool {
    async fn create_or_get(
        name: &str,
        pool: &Pool<Postgres>,
    ) -> Result<QueryPool> {
        let rec = sqlx::query_as::<_, QueryPool>(
            r#"
INSERT INTO query_pool ( name )
VALUES ( $1 )
ON CONFLICT ON CONSTRAINT query_pool_name_unique
DO UPDATE SET name = query_pool.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(pool)
        .await
        .context(SqlxSnafu)?;

        Ok(rec)
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Namespace {
    id: i32,
    name: String,
    #[sqlx(default)]
    retention_duration: Option<String>,
    kafka_topic_id: i32,
    query_pool_id: i32,
}

impl Namespace {
    async fn create(
        name: &str,
        retention_duration: &str,
        kafka_topic_id: i32,
        query_pool_id: i16,
        pool: &Pool<Postgres>,
    ) -> Result<Self> {
        let rec = sqlx::query_as::<_, Self>(
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
        .fetch_one(pool)
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

    async fn get_by_name(name: &str, pool: &Pool<Postgres>) -> Result<Option<Self>> {
        let rec = sqlx::query_as::<_, Self>(
            r#"
SELECT * FROM namespace WHERE name = $1;
        "#,
        )
        .bind(&name) // $1
        .fetch_one(pool)
        .await;

        if let Err(sqlx::Error::RowNotFound) = rec {
            return Ok(None);
        }

        let rec = rec.context(SqlxSnafu)?;
        Ok(Some(rec))
    }
}

#[derive(Debug, sqlx::FromRow, Eq, PartialEq)]
pub struct Table {
    id: i32,
    namespace_id: i32,
    name: String,
}

impl Table {
    async fn create_or_get(name: &str, namespace_id: i32, pool: &Pool<Postgres>) -> Result<Self> {
        let rec = sqlx::query_as::<_, Self>(
            r#"
INSERT INTO table_name ( name, namespace_id )
VALUES ( $1, $2 )
ON CONFLICT ON CONSTRAINT table_name_unique
DO UPDATE SET name = table_name.name RETURNING *;
        "#,
        )
        .bind(&name) // $1
        .bind(&namespace_id) // $2
        .fetch_one(pool)
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

    async fn get_by_namespace_id(namespace_id: i32, pool: &Pool<Postgres>) -> Result<Vec<Table>> {
        let rec = sqlx::query_as::<_, Self>(
            r#"
SELECT * FROM table_name
WHERE namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(pool)
        .await
        .context(SqlxSnafu)?;

        Ok(rec)
    }
}

#[derive(Debug, sqlx::FromRow, Eq, PartialEq)]
pub struct Column {
    id: i32,
    table_id: i32,
    name: String,
    column_type: i16,
}

impl Column {
    fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag as i16
    }

    fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        match field_value {
            FieldValue::I64(_) => self.column_type == ColumnType::I64 as i16,
            FieldValue::U64(_) => self.column_type == ColumnType::U64 as i16,
            FieldValue::F64(_) => self.column_type == ColumnType::F64 as i16,
            FieldValue::String(_) => self.column_type == ColumnType::String as i16,
            FieldValue::Boolean(_) => self.column_type == ColumnType::Bool as i16,
        }
    }

    async fn create_or_get(
        name: &str,
        table_id: i32,
        column_type: ColumnType,
        pool: &Pool<Postgres>,
    ) -> Result<Self> {
        let ct = column_type as i16;

        let rec = sqlx::query_as::<_, Self>(
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
        .fetch_one(pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })?;

        if rec.column_type != ct {
            return ColumnTypeMismatchSnafu {
                name,
                existing: rec.name,
                new: column_type.as_str(),
            }
            .fail();
        }

        Ok(rec)
    }

    async fn get_by_namespace_id(namespace_id: i32, pool: &Pool<Postgres>) -> Result<Vec<Column>> {
        let rec = sqlx::query_as::<_, Self>(
            r#"
SELECT column_name.* FROM table_name
INNER JOIN column_name on column_name.table_id = table_name.id
WHERE table_name.namespace_id = $1;
            "#,
        )
        .bind(&namespace_id)
        .fetch_all(pool)
        .await
        .context(SqlxSnafu)?;

        Ok(rec)
    }
}

/// Schema collection for a namespace
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamespaceSchema {
    /// the namespace id
    pub id: i32,
    /// the tables in the namespace by name
    pub tables: BTreeMap<String, TableSchema>,
}

impl NamespaceSchema {
    pub fn new(id: i32) -> Self {
        Self {
            id,
            tables: BTreeMap::new(),
        }
    }

    pub async fn get_by_name(name: &str, pool: &Pool<Postgres>) -> Result<Option<Self>> {
        // TODO: maybe get all the data in a single call to Postgres?
        let namespace = Namespace::get_by_name(name, pool).await?;
        if let Some(namespace) = namespace {
            // get the columns first just in case someone else is creating schema while we're doing this.
            let columns = Column::get_by_namespace_id(namespace.id, pool).await?;
            let tables = Table::get_by_namespace_id(namespace.id, pool).await?;

            let mut namespace = NamespaceSchema::new(namespace.id);

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
                        return UnknownColumnTypeSnafu {
                            data_type: c.column_type,
                            name: c.name.to_string(),
                        }
                        .fail()
                    }
                }
            }

            for (_, (table_name, schema)) in table_id_to_schema {
                namespace.tables.insert(table_name, schema);
            }

            return Ok(Some(namespace));
        }

        Ok(None)
    }

    fn add_tables_and_columns(
        &mut self,
        new_tables: BTreeMap<String, i32>,
        new_columns: BTreeMap<i32, BTreeMap<String, ColumnSchema>>,
    ) {
        for (table_name, table_id) in new_tables {
            self.tables
                .entry(table_name)
                .or_insert_with(|| TableSchema::new(table_id));
        }

        for (table_id, new_columns) in new_columns {
            let table = self
                .get_table_mut(table_id)
                .expect("table must be in namespace to add columns");
            table.add_columns(new_columns);
        }
    }

    fn get_table_mut(&mut self, table_id: i32) -> Option<&mut TableSchema> {
        for (_, table) in self.tables.iter_mut() {
            if table.id == table_id {
                return Some(table);
            }
        }

        None
    }
}

/// Column definitions for a table
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// the table id
    pub id: i32,
    /// the table's columns by their name
    pub columns: BTreeMap<String, ColumnSchema>,
}

impl TableSchema {
    pub fn new(id: i32) -> Self {
        Self {
            id,
            columns: BTreeMap::new(),
        }
    }

    fn add_columns(&mut self, columns: BTreeMap<String, ColumnSchema>) {
        for (name, column) in columns {
            self.columns.insert(name, column);
        }
    }
}

/// The column id and its type for a column
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// the column id
    pub id: i32,
    /// the column type
    pub column_type: ColumnType,
}

impl ColumnSchema {
    fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag
    }

    fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        match (field_value, self.column_type) {
            (FieldValue::I64(_), ColumnType::I64) => true,
            (FieldValue::U64(_), ColumnType::U64) => true,
            (FieldValue::F64(_), ColumnType::F64) => true,
            (FieldValue::String(_), ColumnType::String) => true,
            (FieldValue::Boolean(_), ColumnType::Bool) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Sequencer {
    pub id: i16,
    pub kafka_topic_id: i32,
    pub kafka_partition: i32,
    pub min_unpersisted_sequence_number: i64,
}

impl Sequencer {
    async fn create(topic: &KafkaTopic, partition: i32, pool: &Pool<Postgres>) -> Result<Self> {
        sqlx::query_as::<_, Self>(
            r#"
        INSERT INTO sequencer
            ( kafka_topic_id, kafka_partition, min_unpersisted_sequence_number )
        VALUES
            ( $1, $2, 0 )
        RETURNING *;
        "#,
        )
        .bind(&topic.id) // $1
        .bind(&partition) // $2
        .fetch_one(pool)
        .await
        .map_err(|e| {
            if is_fk_violation(&e) {
                Error::ForeignKeyViolation { source: e }
            } else {
                Error::SqlxError { source: e }
            }
        })
    }

    async fn list(pool: &Pool<Postgres>) -> Result<Vec<Self>> {
        sqlx::query_as::<_, Self>(r#"SELECT * FROM sequencer;"#)
            .fetch_all(pool)
            .await
            .context(SqlxSnafu)
    }
}

/// The column data type
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ColumnType {
    I64 = 1,
    U64 = 2,
    F64 = 3,
    Bool = 4,
    String = 5,
    Time = 6,
    Tag = 7,
}

impl ColumnType {
    fn as_str(&self) -> &'static str {
        match self {
            ColumnType::I64 => "i64",
            ColumnType::U64 => "u64",
            ColumnType::F64 => "f64",
            ColumnType::Bool => "bool",
            ColumnType::String => "string",
            ColumnType::Time => "time",
            ColumnType::Tag => "tag",
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();

        write!(f, "{}", s)
    }
}

impl TryFrom<i16> for ColumnType {
    type Error = ();

    fn try_from(value: i16) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        match value {
            x if x == ColumnType::I64 as i16 => Ok(ColumnType::I64),
            x if x == ColumnType::U64 as i16 => Ok(ColumnType::U64),
            x if x == ColumnType::F64 as i16 => Ok(ColumnType::F64),
            x if x == ColumnType::Bool as i16 => Ok(ColumnType::Bool),
            x if x == ColumnType::String as i16 => Ok(ColumnType::String),
            x if x == ColumnType::Time as i16 => Ok(ColumnType::Time),
            x if x == ColumnType::Tag as i16 => Ok(ColumnType::Tag),
            _ => Err(()),
        }
    }
}

/// Given the lines of a write request and an in memory schema, this will validate the write
/// against the schema, or if new schema is defined, attempt to insert it into the Postgres
/// catalog. If any new schema is created or found, this function will return a new
/// `NamespaceSchema` struct which can replace the passed in one in cache.
///
/// If another writer attempts to create a column of the same name with a different
/// type at the same time and beats this caller to it, an error will be returned. If another
/// writer adds the same schema before this one, then this will load that schema here.
pub async fn validate_or_insert_schema(
    lines: Vec<ParsedLine<'_>>,
    schema: Arc<NamespaceSchema>,
    pool: &Pool<Postgres>,
) -> Result<Option<NamespaceSchema>> {
    // table name to table_id
    let mut new_tables: BTreeMap<String, i32> = BTreeMap::new();
    // table_id to map of column name to column
    let mut new_columns: BTreeMap<i32, BTreeMap<String, ColumnSchema>> = BTreeMap::new();

    for line in &lines {
        let table_name = line.series.measurement.as_str();
        match schema.tables.get(table_name) {
            Some(table) => {
                // validate existing tags or insert in new
                if let Some(tagset) = &line.series.tag_set {
                    for (key, _) in tagset {
                        match table.columns.get(key.as_str()) {
                            Some(c) => {
                                if !c.is_tag() {
                                    return ColumnTypeMismatchSnafu {
                                        name: key.to_string(),
                                        existing: c.column_type.to_string(),
                                        new: ColumnType::Tag.to_string(),
                                    }
                                    .fail();
                                };
                            }
                            None => {
                                let entry = new_columns.entry(table.id).or_default();
                                if entry.get(key.as_str()).is_none() {
                                    let column = Column::create_or_get(
                                        key.as_str(),
                                        table.id,
                                        ColumnType::Tag,
                                        &pool,
                                    )
                                    .await?;
                                    entry.insert(
                                        column.name,
                                        ColumnSchema {
                                            id: column.id,
                                            column_type: ColumnType::Tag,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }

                // validate existing fields or insert
                for (key, value) in &line.field_set {
                    if let Some(column) = table.columns.get(key.as_str()) {
                        if !column.matches_field_type(&value) {
                            return ColumnTypeMismatchSnafu {
                                name: key.to_string(),
                                existing: column.column_type.as_str().to_string(),
                                new: column_type_from_field(&value).to_string(),
                            }
                            .fail();
                        }
                    } else {
                        let entry = new_columns.entry(table.id).or_default();
                        if entry.get(key.as_str()).is_none() {
                            let data_type = column_type_from_field(&value);
                            let column =
                                Column::create_or_get(key.as_str(), table.id, data_type, &pool)
                                    .await?;
                            entry.insert(
                                column.name,
                                ColumnSchema {
                                    id: column.id,
                                    column_type: data_type,
                                },
                            );
                        }
                    }
                }
            }
            None => {
                let new_table = Table::create_or_get(table_name, schema.id, &pool).await?;
                let new_table_columns = new_columns.entry(new_table.id).or_default();

                if let Some(tagset) = &line.series.tag_set {
                    for (key, _) in tagset {
                        let new_column = Column::create_or_get(
                            key.as_str(),
                            new_table.id,
                            ColumnType::Tag,
                            &pool,
                        )
                        .await?;
                        new_table_columns.insert(
                            new_column.name,
                            ColumnSchema {
                                id: new_column.id,
                                column_type: ColumnType::Tag,
                            },
                        );
                    }
                }
                for (key, value) in &line.field_set {
                    let data_type = column_type_from_field(&value);
                    let new_column =
                        Column::create_or_get(key.as_str(), new_table.id, data_type, &pool).await?;
                    new_table_columns.insert(
                        new_column.name,
                        ColumnSchema {
                            id: new_column.id,
                            column_type: data_type,
                        },
                    );
                }
                let time_column =
                    Column::create_or_get(TIME_COLUMN, new_table.id, ColumnType::Time, &pool)
                        .await?;
                new_table_columns.insert(
                    time_column.name,
                    ColumnSchema {
                        id: time_column.id,
                        column_type: ColumnType::Time,
                    },
                );

                new_tables.insert(new_table.name, new_table.id);
            }
        };
    }

    if !new_tables.is_empty() || !new_columns.is_empty() {
        let mut new_schema = schema.as_ref().clone();
        new_schema.add_tables_and_columns(new_tables, new_columns);
        return Ok(Some(new_schema));
    }

    Ok(None)
}

fn column_type_from_field(field_value: &FieldValue) -> ColumnType {
    match field_value {
        FieldValue::I64(_) => ColumnType::I64,
        FieldValue::U64(_) => ColumnType::U64,
        FieldValue::F64(_) => ColumnType::F64,
        FieldValue::String(_) => ColumnType::String,
        FieldValue::Boolean(_) => ColumnType::Bool,
    }
}

/// The error code returned by Postgres for a unique constraint violation.
///
/// See <https://www.postgresql.org/docs/9.2/errcodes-appendix.html>
pub const PG_UNIQUE_VIOLATION: &str = "23505";

/// Returns true if `e` is a unique constraint violation error.
pub fn is_unique_violation(e: &sqlx::Error) -> bool {
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
pub const PG_FK_VIOLATION: &str = "23503";

pub fn is_fk_violation(e: &sqlx::Error) -> bool {
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
    use crate::{SHARED_KAFKA_TOPIC, SHARED_QUERY_POOL};
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

    async fn setup_db() -> (Pool<Postgres>, KafkaTopic, QueryPool) {
        let dsn = std::env::var("DATABASE_URL").unwrap();
        let pool = connect_catalog_store("test", SCHEMA_NAME, &dsn)
            .await
            .unwrap();
        let kafka_topic = KafkaTopic::create_or_get(SHARED_KAFKA_TOPIC, &pool)
            .await
            .unwrap();
        let query_pool = QueryPool::create_or_get(SHARED_QUERY_POOL, &pool)
            .await
            .unwrap();

        (pool, kafka_topic, query_pool)
    }

    #[tokio::test]
    async fn test_catalog() {
        // If running an integration test on your laptop, this requires that you have Postgres
        // running and that you've done the sqlx migrations. See the README in this crate for
        // info to set it up.
        maybe_skip_integration!();

        let (pool, kafka_topic, query_pool) = setup_db().await;
        clear_schema(&pool).await;

        let namespace = Namespace::create("foo", "inf", 0, 0, &pool).await;
        assert!(matches!(
            namespace.unwrap_err(),
            Error::ForeignKeyViolation { source: _ }
        ));
        let namespace = Namespace::create("foo", "inf", kafka_topic.id, query_pool.id, &pool)
            .await
            .unwrap();
        assert!(namespace.id > 0);
        assert_eq!(&namespace.name, "foo");

        // test that we can create or get a table
        let t = Table::create_or_get("foo", namespace.id, &pool)
            .await
            .unwrap();
        let tt = Table::create_or_get("foo", namespace.id, &pool)
            .await
            .unwrap();
        assert!(t.id > 0);
        assert_eq!(t, tt);

        // test that we can craete or get a column
        let c = Column::create_or_get("foo", t.id, ColumnType::I64, &pool)
            .await
            .unwrap();
        let cc = Column::create_or_get("foo", t.id, ColumnType::I64, &pool)
            .await
            .unwrap();
        assert!(c.id > 0);
        assert_eq!(c, cc);

        // test that attempting to create an already defined column of a different type returns error
        let err = Column::create_or_get("foo", t.id, ColumnType::F64, &pool)
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
        let namespace = Namespace::create("asdf", "inf", kafka_topic.id, query_pool.id, &pool)
            .await
            .unwrap();
        let data = r#"
m1,t1=a,t2=b f1=2i,f2=2.0 1
m1,t1=a f1=3i 2
m2,t3=b f1=true 1
        "#;

        // test that new schema gets returned
        let lines: Vec<_> = parse_lines(&data).map(|l| l.unwrap()).collect();
        let schema = Arc::new(NamespaceSchema::new(namespace.id));
        let new_schema = validate_or_insert_schema(lines, Arc::clone(&schema), &pool)
            .await
            .unwrap();
        let new_schema = new_schema.unwrap();

        // ensure new schema is in the db
        let schema_from_db = NamespaceSchema::get_by_name("asdf", &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema_from_db);

        // test that a new table will be created
        let data = r#"
m1,t1=c f1=1i 2
new_measurement,t9=a f10=true 1
        "#;
        let lines: Vec<_> = parse_lines(&data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, Arc::new(schema_from_db), &pool)
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
        let schema = NamespaceSchema::get_by_name("asdf", &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);

        // test that a new column for an existing table will be created
        // test that a new table will be created
        let data = r#"
m1,new_tag=c new_field=1i 2
        "#;
        let lines: Vec<_> = parse_lines(&data).map(|l| l.unwrap()).collect();
        let new_schema = validate_or_insert_schema(lines, Arc::new(schema), &pool)
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
        let schema = NamespaceSchema::get_by_name("asdf", &pool)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(new_schema, schema);
    }

    #[tokio::test]
    async fn test_sequencers() {
        maybe_skip_integration!();

        let (pool, kafka_topic, _query_pool) = setup_db().await;
        clear_schema(&pool).await;

        // Create 10 sequencers
        let created = (1..=10)
            .map(|partition| Sequencer::create(&kafka_topic, partition, &pool))
            .collect::<FuturesOrdered<_>>()
            .map(|v| {
                let v = v.expect("failed to create sequencer");
                (v.id, v)
            })
            .collect::<BTreeMap<_, _>>()
            .await;

        // List them and assert they match
        let listed = Sequencer::list(&pool)
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
