//! Helpers for initializing the shared database connection

use assert_cmd::Command;
use observability_deps::tracing::info;
use once_cell::sync::Lazy;
use sqlx::{migrate::MigrateDatabase, Postgres};
use std::collections::BTreeSet;
use tokio::sync::Mutex;

// I really do want to block everything until the database is initialized...
static DB_INITIALIZED: Lazy<Mutex<BTreeSet<String>>> = Lazy::new(|| Mutex::new(BTreeSet::new()));

/// Performs once-per-process database initialization, if necessary
pub async fn initialize_db(dsn: &str, schema_name: &str) {
    let mut init = DB_INITIALIZED.lock().await;

    // already done
    if init.contains(schema_name) {
        return;
    }

    info!(%dsn, %schema_name, "Initializing database...");

    let dsn = iox_catalog::postgres::parse_dsn(dsn).unwrap();
    let dsn = &dsn;

    // Create the catalog database if it doesn't exist
    if dsn.starts_with("postgres") && !Postgres::database_exists(dsn).await.unwrap() {
        info!("Creating postgres database...");
        match Postgres::create_database(dsn).await {
            Err(e) => {
                panic!("Database initialization failed: {e}.");
            }
            Ok(_) => {
                info!("Database initialization succeeded");
            }
        }
    }

    // Set up the catalog
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("catalog")
        .arg("setup")
        .env("INFLUXDB_IOX_CATALOG_DSN", dsn)
        .env("INFLUXDB_IOX_CATALOG_POSTGRES_SCHEMA_NAME", schema_name)
        .ok()
        .unwrap();

    init.insert(schema_name.into());
}
