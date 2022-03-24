//! Helpers for initializing the shared database connection

use assert_cmd::Command;
use once_cell::sync::Lazy;
use sqlx::{migrate::MigrateDatabase, Postgres};
use std::sync::Mutex;

// I really do want to block everything until the database is initialized...
#[allow(clippy::await_holding_lock)]
#[allow(clippy::mutex_atomic)]
static DB_INITIALIZED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

/// Performs once-per-process database initialization, if necessary
pub async fn initialize_db(dsn: &str) {
    let mut init = DB_INITIALIZED.lock().expect("Mutex poisoned");

    // already done
    if *init {
        return;
    }

    println!("Initializing database...");

    // Create the catalog database if it doesn't exist
    if !Postgres::database_exists(dsn).await.unwrap() {
        println!("Creating database...");
        Postgres::create_database(dsn).await.unwrap();
    }

    // Set up the catalog
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("catalog")
        .arg("setup")
        .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
        .ok()
        .unwrap();

    // Create the shared Kafka topic in the catalog
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("catalog")
        .arg("topic")
        .arg("update")
        .arg("iox-shared")
        .env("INFLUXDB_IOX_CATALOG_DSN", &dsn)
        .ok()
        .unwrap();

    *init = true;
}
