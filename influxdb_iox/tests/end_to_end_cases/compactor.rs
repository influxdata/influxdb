use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use datafusion::datasource::object_store::ObjectStoreUrl;
use futures::TryStreamExt;
use object_store::{local::LocalFileSystem, path::Path as ObjectStorePath, ObjectStore};
use parquet_to_line_protocol::ParquetFileReader;
use predicates::prelude::*;
use std::sync::Arc;
use test_helpers_end_to_end::maybe_skip_integration;

#[tokio::test]
async fn compactor_generate_has_defaults() {
    let database_url = maybe_skip_integration!();
    let dir = tempfile::tempdir()
        .expect("could not get temporary directory")
        .into_path();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(&database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(&dir)
        .assert()
        .success();
    let data_generation_spec = dir.join("compactor_data/spec.toml");
    assert!(data_generation_spec.exists());
}

#[tokio::test]
async fn compactor_generate_zeroes_are_invalid() {
    let database_url = maybe_skip_integration!();
    let dir = tempfile::tempdir().expect("could not get temporary directory");

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(&database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(dir.path())
        .arg("--num-partitions")
        .arg("0")
        .arg("--num-files")
        .arg("0")
        .arg("--num-cols")
        .arg("0")
        .arg("--num-rows")
        .arg("0")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "number would be zero for non-zero type",
        ));
}

#[tokio::test]
async fn compactor_generate_creates_files_and_catalog_entries() {
    let database_url = maybe_skip_integration!();
    let dir = tempfile::tempdir().expect("could not get temporary directory");

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(&database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success();

    let data_generation_spec = dir.path().join("compactor_data/spec.toml");
    assert!(data_generation_spec.exists());
}

#[tokio::test]
async fn running_compactor_generate_twice_overwrites_existing_files() {
    let database_url = maybe_skip_integration!();
    let dir = tempfile::tempdir().expect("could not get temporary directory");

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(&database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success();

    let first_run_data_path = dir
        .path()
        .join("compactor_data/parquet/data_0_measure.parquet");
    let first_run_record_batches = read_record_batches(&first_run_data_path).await;
    assert_eq!(first_run_record_batches.len(), 1);

    let first_run_record_batch = &first_run_record_batches[0];
    let first_run_num_lines = first_run_record_batch.num_rows();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("compactor")
        .arg("generate")
        .arg("--catalog-dsn")
        .arg(&database_url)
        .arg("--object-store")
        .arg("file")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success();

    let second_run_data_path = dir
        .path()
        .join("compactor_data/parquet/data_0_measure.parquet");
    let second_run_record_batches = read_record_batches(&second_run_data_path).await;
    assert_eq!(second_run_record_batches.len(), 1);

    let second_run_record_batch = &second_run_record_batches[0];
    let second_run_num_lines = second_run_record_batch.num_rows();

    // If generation is appending instead of overwriting, this will fail.
    assert_eq!(first_run_num_lines, second_run_num_lines);

    // If generation isn't creating different data every time it's invoked, this will fail.
    assert_ne!(first_run_record_batch, second_run_record_batch);
}

async fn read_record_batches(path: impl AsRef<std::path::Path>) -> Vec<RecordBatch> {
    let object_store_path = ObjectStorePath::from_filesystem_path(path).unwrap();
    let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let object_meta = object_store.head(&object_store_path).await.unwrap();

    let reader = ParquetFileReader::try_new(object_store, object_store_url, object_meta)
        .await
        .unwrap();

    reader.read().await.unwrap().try_collect().await.unwrap()
}
