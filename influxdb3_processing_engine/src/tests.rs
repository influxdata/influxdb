use crate::ProcessingEngineManagerImpl;
use crate::TriggerSpecificationDefinition;
use crate::WalEvent;
use crate::environment::TestManager;
use crate::plugins::ProcessingEngineEnvironmentManager;
use crate::virtualenv::init_pyo3;
use crate::write::InProcessWriteEndpoint;
use datafusion_util::config::register_iox_object_store;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::{
    ApiNodeSpec, Catalog, DeletionScope, HardDeletionTime, TriggerSettings,
};
use influxdb3_id::{DbId, TriggerId};
use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
use influxdb3_py_api::cache::{CacheStore, PyCache};
use influxdb3_shutdown::ShutdownManager;
use influxdb3_sys_events::SysEventStore;
use influxdb3_types::DatabaseName;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::{
    N_SNAPSHOTS_TO_LOAD_ON_START, WriteBufferImpl, WriteBufferImplArgs,
};
use iox_query::exec::{
    DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext, PerQueryMemoryPoolConfig,
};
use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use parking_lot::Mutex;
use parquet_file::storage::{ParquetStorage, StorageId};
use pyo3::Python;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

#[test_log::test(tokio::test)]
async fn test_trigger_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let test_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let (pem, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB by inserting a line.
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await?;

    // Create an enabled trigger
    let file_name = pem
        .validate_plugin_filename(file_name.as_str())
        .await
        .unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            file_name,
            ApiNodeSpec::All,
            "all_tables",
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();

    // Verify trigger is not disabled in schema
    let schema = pem.catalog.db_schema("foo").unwrap();
    let trigger = schema
        .processing_engine_triggers
        .get_by_name("test_trigger")
        .unwrap();
    assert!(!trigger.disabled);

    // Disable the trigger
    pem.catalog
        .disable_processing_engine_trigger("foo", "test_trigger")
        .await
        .unwrap();

    // Verify trigger is disabled in schema
    let schema = pem.catalog.db_schema("foo").unwrap();
    let trigger = schema
        .processing_engine_triggers
        .get_by_name("test_trigger")
        .unwrap();
    assert!(trigger.disabled);

    // Enable the trigger
    pem.catalog
        .enable_processing_engine_trigger("foo", "test_trigger")
        .await
        .unwrap();

    // Verify trigger is enabled and running
    let schema = pem.catalog.db_schema("foo").unwrap();
    let trigger = schema
        .processing_engine_triggers
        .get_by_name("test_trigger")
        .unwrap();
    assert!(!trigger.disabled);
    Ok(())
}

#[tokio::test]
async fn test_create_disabled_trigger() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let test_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let (pem, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB by inserting a line.
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await?;

    let file_name = pem.validate_plugin_filename(&file_name).await.unwrap();
    // Create a disabled trigger
    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            file_name,
            ApiNodeSpec::All,
            "all_tables",
            TriggerSettings::default(),
            &None,
            true,
        )
        .await
        .unwrap();

    // Verify trigger is created but disabled
    let schema = pem.catalog.db_schema("foo").unwrap();
    let trigger = schema
        .processing_engine_triggers
        .get_by_name("test_trigger")
        .unwrap();
    assert!(trigger.disabled);

    // Verify trigger is not in active triggers list
    assert!(pem.catalog.active_triggers().is_empty());
    Ok(())
}

#[tokio::test]
async fn test_enable_nonexistent_trigger() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let test_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let (pem, _file_name) = setup(start_time, test_store, wal_config).await;

    // Create the DB by inserting a line.
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await?;

    let Err(CatalogError::NotFound(_)) = pem
        .catalog
        .enable_processing_engine_trigger("foo", "nonexistent_trigger")
        .await
    else {
        panic!("should receive not found error for non existent trigger on enable");
    };

    Ok(())
}

async fn setup(
    start: Time,
    object_store: Arc<dyn ObjectStore>,
    wal_config: WalConfig,
) -> (Arc<ProcessingEngineManagerImpl>, NamedTempFile) {
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
    let metric_registry = Arc::new(Registry::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&object_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::clone(&metric_registry),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();
    let ctx = IOxSessionContext::with_testing();
    let runtime_env = ctx.inner().runtime_env();
    register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

    let qe = Arc::new(UnimplementedQueryExecutor);

    let mut file = NamedTempFile::new().unwrap();
    let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#;
    writeln!(file, "{code}").unwrap();
    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(file.path().parent().unwrap().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(crate::environment::TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));

    (
        ProcessingEngineManagerImpl::new(
            environment_manager,
            catalog,
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
        file,
    )
}

pub(crate) fn make_exec() -> Arc<Executor> {
    let metrics = Arc::new(metric::Registry::default());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let parquet_store = ParquetStorage::new(
        Arc::clone(&object_store),
        StorageId::from("test_exec_storage"),
    );
    Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            // Default to 1gb
            mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            heap_memory_limit: None,
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
        },
        DedicatedExecutor::new_testing(),
    ))
}

fn construct_plugin_url(plugin_repo: Option<&str>, plugin_path: &str) -> String {
    let repo = plugin_repo
        .unwrap_or("https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/");
    if repo.ends_with('/') {
        format!("{repo}{plugin_path}")
    } else {
        format!("{repo}/{plugin_path}")
    }
}

#[test]
fn test_plugin_repo_url_construction_default() {
    // Test URL construction with default repo
    let plugin_repo: Option<String> = None;
    let plugin_path = "my_plugin.py";
    let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
    assert_eq!(
        url,
        "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/my_plugin.py"
    );
}

#[test]
fn test_plugin_repo_url_construction_custom() {
    // Test URL construction with custom repo
    let plugin_repo = Some("https://custom-repo.example.com/plugins/".to_string());
    let plugin_path = "my_plugin.py";
    let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
    assert_eq!(url, "https://custom-repo.example.com/plugins/my_plugin.py");
}

#[test]
fn test_plugin_repo_url_construction_custom_without_trailing_slash() {
    // Test URL construction with custom repo without trailing slash
    let plugin_repo = Some("https://custom-repo.example.com/plugins".to_string());
    let plugin_path = "my_plugin.py";
    let url = construct_plugin_url(plugin_repo.as_deref(), plugin_path);
    // Automatic slash insertion creates correct URL regardless of input format
    assert_eq!(url, "https://custom-repo.example.com/plugins/my_plugin.py");
}

#[tokio::test]
async fn test_read_multifile_plugin() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path().join("my_plugin");
    std::fs::create_dir(&plugin_dir).unwrap();

    let init_code = r#"
from .utils import helper_function

def process_writes(influxdb3_local, table_batches, args=None):
    helper_function()
    influxdb3_local.info("done")
"#;
    std::fs::write(plugin_dir.join("__init__.py"), init_code).unwrap();

    let utils_code = r#"
def helper_function():
    return "helper"
"#;
    std::fs::write(plugin_dir.join("utils.py"), utils_code).unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = ProcessingEngineManagerImpl::new(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::new(InProcessWriteEndpoint::new(wbuf)),
        qe,
        time_provider,
        sys_event_store,
    )
    .await
    .unwrap();

    let plugin_code = pem.read_plugin_code("my_plugin").await.unwrap();

    match plugin_code {
        crate::PluginCode::LocalDirectory(dir) => {
            assert!(dir.plugin_root.ends_with("my_plugin"));
            assert!(dir.entry_point.ends_with("__init__.py"));
            let code = dir.read_entry_point_if_modified();
            assert!(code.contains("helper_function"));
        }
        _ => panic!("Expected LocalDirectory variant"),
    }
}

#[tokio::test]
async fn test_missing_init_py() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path().join("my_plugin");
    std::fs::create_dir(&plugin_dir).unwrap();

    std::fs::write(plugin_dir.join("utils.py"), "def helper(): pass").unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = ProcessingEngineManagerImpl::new(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::new(InProcessWriteEndpoint::new(wbuf)),
        qe,
        time_provider,
        sys_event_store,
    )
    .await
    .unwrap();

    let result = pem.read_plugin_code("my_plugin").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, crate::plugins::PluginError::ReadPluginError(_)),
        "Expected ReadPluginError"
    );
}

#[test]
fn test_hot_reload_multifile_plugin() {
    use std::thread::sleep;

    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_root = temp_dir.path().join("my_plugin");
    std::fs::create_dir(&plugin_root).unwrap();

    let init_path = plugin_root.join("__init__.py");
    std::fs::write(&init_path, "def process_writes(): pass").unwrap();

    let plugin = crate::LocalPluginDirectory {
        plugin_root: plugin_root.clone(),
        entry_point: init_path.clone(),
        last_read_and_code: Mutex::new((SystemTime::now(), Arc::from("initial"))),
    };

    let first_modified = plugin.find_latest_modified_time();
    assert!(first_modified.is_some());

    sleep(Duration::from_millis(100));

    std::fs::write(plugin_root.join("utils.py"), "def helper(): pass").unwrap();

    let second_modified = plugin.find_latest_modified_time();
    assert!(second_modified.is_some());
    assert!(
        second_modified.unwrap() > first_modified.unwrap(),
        "Modification time should be newer after adding file"
    );
}

#[test]
fn test_pycache_ignored() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_root = temp_dir.path().join("my_plugin");
    std::fs::create_dir(&plugin_root).unwrap();

    let init_path = plugin_root.join("__init__.py");
    std::fs::write(&init_path, "def process_writes(): pass").unwrap();

    let pycache_dir = plugin_root.join("__pycache__");
    std::fs::create_dir(&pycache_dir).unwrap();
    std::fs::write(pycache_dir.join("__init__.cpython-39.pyc"), "bytecode").unwrap();

    let plugin = crate::LocalPluginDirectory {
        plugin_root: plugin_root.clone(),
        entry_point: init_path.clone(),
        last_read_and_code: Mutex::new((SystemTime::now(), Arc::from("initial"))),
    };

    let first_modified = plugin.find_latest_modified_time().unwrap();

    std::thread::sleep(Duration::from_millis(100));

    std::fs::write(pycache_dir.join("utils.cpython-39.pyc"), "more bytecode").unwrap();

    let second_modified = plugin.find_latest_modified_time().unwrap();

    assert_eq!(
        first_modified, second_modified,
        "Modification time should not change when only __pycache__ is modified"
    );
}

#[tokio::test]
async fn test_atomic_directory_replacement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path();

    // Create initial plugin directory with some files
    let initial_plugin = plugin_dir.join("test_plugin");
    std::fs::create_dir(&initial_plugin).unwrap();
    std::fs::write(initial_plugin.join("__init__.py"), "def process_v1(): pass").unwrap();
    std::fs::write(
        initial_plugin.join("old_file.py"),
        "def old_function(): pass",
    )
    .unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(plugin_dir.to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger first
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await
        .unwrap();

    let plugin_filename = pem.validate_plugin_filename("test_plugin").await.unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            plugin_filename,
            ApiNodeSpec::All,
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();

    // Prepare new files for atomic replacement
    let new_files = vec![
        (
            "__init__.py".to_string(),
            "def process_v2(): pass".to_string(),
        ),
        ("utils.py".to_string(), "def helper(): pass".to_string()),
        (
            "models/processor.py".to_string(),
            "class Processor: pass".to_string(),
        ),
    ];

    // Perform atomic replacement
    pem.replace_plugin_directory("test_trigger", new_files)
        .await
        .unwrap();

    // Verify the new directory structure
    assert!(initial_plugin.join("__init__.py").exists());
    assert!(initial_plugin.join("utils.py").exists());
    assert!(initial_plugin.join("models").join("processor.py").exists());

    // Verify old file was deleted
    assert!(!initial_plugin.join("old_file.py").exists());

    // Verify content is correct
    let init_content = std::fs::read_to_string(initial_plugin.join("__init__.py")).unwrap();
    assert_eq!(init_content, "def process_v2(): pass");

    let utils_content = std::fs::read_to_string(initial_plugin.join("utils.py")).unwrap();
    assert_eq!(utils_content, "def helper(): pass");

    // Verify old directory was cleaned up
    assert!(!plugin_dir.join("test_plugin.old").exists());
    assert!(!plugin_dir.join("test_plugin.tmp").exists());
}

// Path traversal vulnerability tests
#[test]
fn test_validate_path_within_plugin_dir_basic() {
    use crate::validate_path_within_plugin_dir;

    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path();

    // Valid paths should work
    let result = validate_path_within_plugin_dir(plugin_dir, "plugin.py");
    assert!(result.is_ok());

    let result = validate_path_within_plugin_dir(plugin_dir, "my_plugin/utils.py");
    assert!(result.is_ok());

    // Parent directory traversal should fail
    let result = validate_path_within_plugin_dir(plugin_dir, "../evil.py");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::plugins::PluginError::PathTraversal(_)
    ));

    // Absolute path should fail
    let result = validate_path_within_plugin_dir(plugin_dir, "/etc/passwd");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::plugins::PluginError::PathTraversal(_)
    ));

    // Nested traversal should fail
    let result = validate_path_within_plugin_dir(plugin_dir, "subdir/../../evil.py");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::plugins::PluginError::PathTraversal(_)
    ));
}

#[tokio::test]
async fn test_create_plugin_file_path_traversal_parent_dir() {
    let temp_dir = tempfile::tempdir().unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Try to create a file with path traversal
    let result = pem.create_plugin_file("../evil.py", "malicious code").await;
    assert!(result.is_err());

    // Verify no file was created outside plugin directory
    assert!(!temp_dir.path().parent().unwrap().join("evil.py").exists());
}

#[tokio::test]
async fn test_create_plugin_file_path_traversal_absolute() {
    let temp_dir = tempfile::tempdir().unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Try to create a file with absolute path
    let result = pem
        .create_plugin_file("/tmp/evil_absolute.py", "malicious code")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_plugin_file_path_traversal_nested() {
    let temp_dir = tempfile::tempdir().unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Try to create a file with nested traversal
    let result = pem
        .create_plugin_file("subdir/../../evil.py", "malicious code")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_plugin_file_valid_nested_path() {
    let temp_dir = tempfile::tempdir().unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(temp_dir.path().to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Valid nested path should work
    let result = pem
        .create_plugin_file("my_plugin/utils/helper.py", "def helper(): pass")
        .await;
    assert!(result.is_ok());

    // Verify file was created in the correct location
    assert!(temp_dir.path().join("my_plugin/utils/helper.py").exists());
}

#[tokio::test]
async fn test_replace_plugin_directory_path_traversal_in_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path();

    // Create initial plugin directory
    let initial_plugin = plugin_dir.join("test_plugin");
    std::fs::create_dir(&initial_plugin).unwrap();
    std::fs::write(initial_plugin.join("__init__.py"), "def process_v1(): pass").unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(plugin_dir.to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger first
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await
        .unwrap();

    let plugin_filename = pem.validate_plugin_filename("test_plugin").await.unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            plugin_filename,
            ApiNodeSpec::All,
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();

    // Try to replace with files containing path traversal
    let malicious_files = vec![
        ("__init__.py".to_string(), "def process(): pass".to_string()),
        ("../../../evil.py".to_string(), "malicious code".to_string()),
    ];

    let result = pem
        .replace_plugin_directory("test_trigger", malicious_files)
        .await;
    assert!(result.is_err());

    // Verify no file was created outside the temp directory
    assert!(!temp_dir.path().parent().unwrap().join("evil.py").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn test_create_plugin_file_symlink_escape() {
    use std::os::unix::fs::symlink;

    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path().join("plugins");
    std::fs::create_dir(&plugin_dir).unwrap();

    // Create a symlink inside the plugin dir that points outside
    let outside_dir = temp_dir.path().join("outside");
    std::fs::create_dir(&outside_dir).unwrap();
    let evil_link = plugin_dir.join("evil_link");
    symlink(&outside_dir, &evil_link).unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(plugin_dir.clone()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Try to create a file through the symlink
    let result = pem
        .create_plugin_file("evil_link/escaped.py", "malicious code")
        .await;
    assert!(result.is_err());

    // Verify no file was created in the outside directory
    assert!(!outside_dir.join("escaped.py").exists());
}

/// Tests that update_plugin_file properly validates paths.
/// Note: Path traversal via update_plugin_file is blocked by:
/// 1. Trigger creation validates plugin_filename (primary protection)
/// 2. update_plugin_file calls validate_path_within_plugin_dir (defense-in-depth)
#[tokio::test]
async fn test_update_plugin_file_validates_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_dir = temp_dir.path();

    // Create a valid single-file plugin
    std::fs::write(plugin_dir.join("test_plugin.py"), "def process(): pass").unwrap();

    let environment_manager = ProcessingEngineEnvironmentManager {
        plugin_dir: Some(plugin_dir.to_path_buf()),
        virtual_env_location: None,
        package_manager: Arc::new(TestManager),
        plugin_dir_only: false,
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Catalog::new(
        "test_host",
        Arc::clone(&test_store),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let qe = Arc::new(UnimplementedQueryExecutor);
    let persister = Arc::new(Persister::new(
        Arc::clone(&test_store),
        "test_host".to_string(),
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let shutdown = ShutdownManager::new_testing();
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
        persister,
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: make_exec(),
        wal_config,
        parquet_cache: None,
        metric_registry: Arc::new(Registry::new()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        shutdown: shutdown.register("test"),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: NonZeroUsize::new(10).unwrap(),
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
        )
        .await
        .unwrap();

    let plugin_filename = pem
        .validate_plugin_filename("test_plugin.py")
        .await
        .unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            plugin_filename,
            ApiNodeSpec::All,
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();

    // Update should succeed for valid trigger
    let result = pem
        .update_plugin_file("test_trigger", "def process_v2(): pass")
        .await;
    assert!(result.is_ok());

    // Verify content was updated
    let content = std::fs::read_to_string(plugin_dir.join("test_plugin.py")).unwrap();
    assert_eq!(content, "def process_v2(): pass");
}

/// Sets up a database with a trigger and its channel for deletion tests.
///
/// Creates a "test_db" database, registers a trigger of the given kind in the
/// catalog, and inserts the corresponding channel into PluginChannels. Returns
/// the processing engine manager and the database ID (needed for hard delete).
///
/// The `_file` return keeps the NamedTempFile alive so the plugin path remains valid.
async fn setup_db_with_trigger(
    trigger_kind: &str,
) -> Result<
    (
        Arc<ProcessingEngineManagerImpl>,
        DbId,
        TriggerId,
        NamedTempFile,
    ),
    Box<dyn std::error::Error>,
> {
    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let test_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let (pem, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB
    pem.write_endpoint
        .write_lp(
            DatabaseName::new("test_db").unwrap(),
            "cpu,host=a val=1\n",
            start_time,
            false,
        )
        .await?;

    let db_id = pem.catalog.db_schema("test_db").unwrap().id;

    let (trigger_name, trigger_spec) = match trigger_kind {
        "wal" => ("wal_trigger", "all_tables"),
        "schedule" => ("schedule_trigger", "every:1s"),
        "request" => (
            "request_trigger",
            "request:/api/v3/engine/test_db/test_endpoint",
        ),
        other => panic!("unknown trigger kind: {other}"),
    };

    let validated = pem.validate_plugin_filename(&file_name).await.unwrap();
    pem.catalog
        .create_processing_engine_trigger(
            "test_db",
            trigger_name,
            validated,
            ApiNodeSpec::All,
            trigger_spec,
            TriggerSettings::default(),
            &None,
            false,
        )
        .await?;

    let trigger_id = pem
        .catalog
        .db_schema("test_db")
        .unwrap()
        .processing_engine_triggers
        .get_by_name(trigger_name)
        .unwrap()
        .trigger_id;

    // Insert the channel to simulate a running trigger. run_trigger requires
    // enterprise node registration, so we populate the channel map directly.
    {
        let mut channels = pem.plugin_event_tx.write().await;
        match trigger_kind {
            "wal" => {
                channels.add_wal_trigger(db_id, trigger_id);
            }
            "schedule" => {
                channels.add_schedule_trigger(db_id, trigger_id, CancellationToken::new());
            }
            "request" => {
                channels.add_request_trigger(
                    db_id,
                    trigger_id,
                    "/api/v3/engine/test_db/test_endpoint".to_string(),
                );
            }
            _ => unreachable!(),
        }
    }

    Ok((pem, db_id, trigger_id, file))
}

#[test_log::test(tokio::test)]
async fn test_soft_delete_stops_wal_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("wal").await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id))
        );
    }

    // HardDeletionTime::Never means no hard delete follows, so soft delete
    // performs full cleanup (shutdown + remove + cache drop) immediately.
    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Never, DeletionScope::default())
        .await?;

    // Catalog subscriptions are synchronous (ACK-on-drop), so by the time
    // soft_delete_database returns, the background handler has processed it.
    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "WAL triggers should be fully removed after soft delete with HardDeletionTime::Never"
        );
    }
    // Triggers should be marked disabled in the catalog
    {
        let db = pem.catalog.db_schema_by_id(&db_id).unwrap();
        let trigger = db
            .processing_engine_triggers
            .get_by_name("wal_trigger")
            .unwrap();
        assert!(
            trigger.disabled,
            "WAL trigger should be disabled in the catalog after soft delete"
        );
    }
    Ok(())
}

/// Seed a per-trigger (local) Python cache entry for `(db_id, trigger_id)`.
/// Requires `init_pyo3()` to have run.
fn seed_trigger_cache(cache: &Arc<Mutex<CacheStore>>, db_id: DbId, trigger_id: TriggerId) {
    let pc = PyCache::new_trigger_cache(Arc::clone(cache), db_id, trigger_id);
    let value = Python::attach(|py| py.None());
    // use_global = None -> writes the per-trigger (local) cache, which is what
    // drop_trigger_cache / drop_all_trigger_caches_for_db remove.
    pc.put("k".to_string(), value, None, None).unwrap();
}

#[test_log::test(tokio::test)]
async fn test_soft_delete_drops_trigger_cache() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("wal").await?;
    init_pyo3();

    seed_trigger_cache(&pem.cache, db_id, trigger_id);
    // Control entry under an unrelated db/trigger; it must survive, proving the
    // drop is correctly db-scoped.
    let (cdb, ctrig) = (DbId::new(987654), TriggerId::new(987654));
    seed_trigger_cache(&pem.cache, cdb, ctrig);

    // HardDeletionTime::Never means no hard delete follows, so soft delete
    // performs full cleanup (including dropping all trigger caches for the db)
    // immediately. Catalog subscriptions are synchronous, so the handler has
    // run by the time this returns.
    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Never, DeletionScope::default())
        .await?;

    assert!(
        !pem.cache.lock().drop_trigger_cache(db_id, trigger_id),
        "soft delete should have dropped the trigger's cache"
    );
    assert!(
        pem.cache.lock().drop_trigger_cache(cdb, ctrig),
        "control cache under an unrelated db should survive soft delete"
    );
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_stop_trigger_drops_trigger_cache() -> Result<(), Box<dyn std::error::Error>> {
    // Reuse the helper only to obtain a ready `pem`; we register our own trigger
    // ids below. stop_trigger does not consult the catalog, so the catalog state
    // is irrelevant to it.
    let (pem, _db_id, _trigger_id, _file) = setup_db_with_trigger("wal").await?;
    init_pyo3();

    // Fresh ids, distinct from the helper's.
    let db_id = DbId::new(7);
    let trigger_id = TriggerId::new(7);

    // Register a WAL trigger and keep the receiver alive so send_shutdown
    // succeeds.
    let rx = pem
        .plugin_event_tx
        .write()
        .await
        .add_wal_trigger(db_id, trigger_id);

    // Complete the shutdown handshake so stop_trigger's shutdown_rx.await
    // resolves Ok and the trigger is removed.
    let responder = tokio::spawn(async move {
        let mut rx = rx;
        if let Some(WalEvent::Shutdown(tx)) = rx.recv().await {
            let _ = tx.send(());
        }
    });

    seed_trigger_cache(&pem.cache, db_id, trigger_id);
    // Control entry; must survive, proving the drop is id-scoped.
    seed_trigger_cache(&pem.cache, DbId::new(8), TriggerId::new(8));

    pem.stop_trigger(db_id, trigger_id).await.unwrap();
    responder.await.unwrap();

    assert!(
        !pem.cache.lock().drop_trigger_cache(db_id, trigger_id),
        "stop_trigger should have dropped the trigger's cache"
    );
    assert!(
        pem.cache
            .lock()
            .drop_trigger_cache(DbId::new(8), TriggerId::new(8)),
        "control cache under unrelated ids should survive stop_trigger"
    );
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_soft_delete_stops_schedule_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("schedule").await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id))
        );
    }

    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Never, DeletionScope::default())
        .await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Schedule triggers should be fully removed after soft delete with HardDeletionTime::Never"
        );
    }
    // Triggers should be marked disabled in the catalog
    {
        let db = pem.catalog.db_schema_by_id(&db_id).unwrap();
        let trigger = db
            .processing_engine_triggers
            .get_by_name("schedule_trigger")
            .unwrap();
        assert!(
            trigger.disabled,
            "Schedule trigger should be disabled in the catalog after soft delete"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_soft_delete_stops_request_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("request").await?;

    let path = "/api/v3/engine/test_db/test_endpoint";
    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id))
        );
        assert!(channels.request_paths.contains_key(path));
    }

    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Never, DeletionScope::default())
        .await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Request triggers should be fully removed after soft delete with HardDeletionTime::Never"
        );
        assert!(
            !channels.request_paths.contains_key(path),
            "Request path index should be cleared after soft delete with HardDeletionTime::Never"
        );
    }
    // Triggers should be marked disabled in the catalog
    {
        let db = pem.catalog.db_schema_by_id(&db_id).unwrap();
        let trigger = db
            .processing_engine_triggers
            .get_by_name("request_trigger")
            .unwrap();
        assert!(
            trigger.disabled,
            "Request trigger should be disabled in the catalog after soft delete"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_removes_wal_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("wal").await?;

    // Use HardDeletionTime::Now so soft delete leaves the trigger in the
    // registry for the hard delete handler instead of cleaning up immediately.
    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Now, DeletionScope::default())
        .await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "WAL triggers should still be in the map after soft delete (pending hard delete)"
        );
    }

    pem.catalog.hard_delete_database(&db_id).await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "WAL triggers should be fully removed after hard delete"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_removes_schedule_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("schedule").await?;

    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Now, DeletionScope::default())
        .await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Schedule triggers should still be in the map after soft delete (pending hard delete)"
        );
    }

    pem.catalog.hard_delete_database(&db_id).await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Schedule triggers should be fully removed after hard delete"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_removes_request_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, trigger_id, _file) = setup_db_with_trigger("request").await?;

    let path = "/api/v3/engine/test_db/test_endpoint";

    pem.catalog
        .soft_delete_database("test_db", HardDeletionTime::Now, DeletionScope::default())
        .await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Request triggers should still be in the map after soft delete (pending hard delete)"
        );
        assert!(
            channels.request_paths.contains_key(path),
            "Request path index should still be present after soft delete (pending hard delete)"
        );
    }

    pem.catalog.hard_delete_database(&db_id).await?;

    {
        let channels = pem.plugin_event_tx.read().await;
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "Request triggers should be fully removed after hard delete"
        );
        assert!(
            !channels.request_paths.contains_key(path),
            "Request path index should be cleared after hard delete"
        );
    }
    Ok(())
}

/// Unit tests for the id-keyed running-trigger registry. These exercise
/// `PluginChannels` directly, without a catalog, to prove shutdown and request
/// routing are driven entirely by `(DbId, TriggerId)`.
#[cfg(test)]
mod plugin_channels_tests {
    use crate::{PluginChannels, Request, RequestEvent, WalEvent};
    use bytes::Bytes;
    use hashbrown::HashMap;
    use influxdb3_id::{DbId, TriggerId};
    use iox_http_util::Response;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn add_wal_trigger_is_keyed_by_id() {
        let mut channels = PluginChannels::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let _rx = channels.add_wal_trigger(db_id, trigger_id);
        assert!(
            channels
                .triggers
                .get(&db_id)
                .unwrap()
                .contains_key(&trigger_id)
        );
    }

    #[tokio::test]
    async fn shutdown_by_id_without_catalog_spec() {
        let mut channels = PluginChannels::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let mut event_rx = channels.add_wal_trigger(db_id, trigger_id);

        let rx = channels
            .send_shutdown(db_id, trigger_id)
            .await
            .expect("send_shutdown should not error")
            .expect("a receiver should be returned for a running trigger");

        // The trigger should have received a Shutdown event.
        assert!(
            matches!(event_rx.recv().await, Some(WalEvent::Shutdown(_))),
            "expected WalEvent::Shutdown"
        );

        // send_shutdown for an unknown trigger yields Ok(None).
        assert!(
            channels
                .send_shutdown(db_id, TriggerId::new(99))
                .await
                .unwrap()
                .is_none()
        );

        drop(rx);
    }

    #[tokio::test]
    async fn remove_request_trigger_clears_path_index() {
        let mut channels = PluginChannels::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let _rx = channels.add_request_trigger(db_id, trigger_id, "/p".to_string());
        assert!(channels.request_paths.contains_key("/p"));

        channels.remove_trigger(db_id, trigger_id);

        assert!(
            !channels.request_paths.contains_key("/p"),
            "removing a request trigger must clear its path index"
        );
        assert!(
            !channels
                .triggers
                .get(&db_id)
                .is_some_and(|m| m.contains_key(&trigger_id)),
            "removing a request trigger must drop it from the registry"
        );
    }

    #[tokio::test]
    async fn send_request_routes_by_path() {
        let mut channels = PluginChannels::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let mut event_rx = channels.add_request_trigger(db_id, trigger_id, "/p".to_string());

        let (response_tx, _response_rx) = oneshot::channel::<Response>();
        let request = Request {
            query_params: HashMap::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            response_tx,
        };
        channels
            .send_request("/p", request)
            .await
            .expect("send_request should route by path");

        assert!(
            matches!(event_rx.recv().await, Some(RequestEvent::Request(_))),
            "expected RequestEvent::Request"
        );

        // An unknown path is reported as not found.
        let (response_tx, _response_rx) = oneshot::channel::<Response>();
        let request = Request {
            query_params: HashMap::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            response_tx,
        };
        assert!(channels.send_request("/missing", request).await.is_err());
    }

    #[tokio::test]
    async fn shutdown_all_for_db_by_id() {
        let mut channels = PluginChannels::default();
        let db_id = DbId::new(1);
        let wal_trigger = TriggerId::new(2);
        let req_trigger = TriggerId::new(3);
        let mut wal_rx = channels.add_wal_trigger(db_id, wal_trigger);
        let _req_rx = channels.add_request_trigger(db_id, req_trigger, "/p".to_string());

        let receivers = channels.shutdown_all_for_db(db_id).await;
        assert_eq!(
            receivers.len(),
            2,
            "shutdown_all_for_db should return one receiver per running trigger"
        );

        // The WAL trigger should have received a Shutdown event.
        assert!(
            matches!(wal_rx.recv().await, Some(WalEvent::Shutdown(_))),
            "expected WalEvent::Shutdown"
        );
    }

    /// Stopping a scheduled trigger must cancel its per-trigger token, so an
    /// in-flight run_async=false run is interrupted (the schedule loop races this
    /// token in run_at_time). Without it the disable/`delete --force` hang recurs.
    #[tokio::test]
    async fn send_shutdown_cancels_schedule_trigger_token() {
        let mut channels = PluginChannels::default();
        let (db_id, trigger_id) = (DbId::new(1), TriggerId::new(1));
        let token = CancellationToken::new();
        let _rx = channels.add_schedule_trigger(db_id, trigger_id, token.clone());
        assert!(!token.is_cancelled());

        let shutdown_rx = channels.send_shutdown(db_id, trigger_id).await.unwrap();
        assert!(shutdown_rx.is_some(), "expected a shutdown receiver");
        assert!(
            token.is_cancelled(),
            "stopping a scheduled trigger must cancel its per-trigger token"
        );
    }

    /// Deleting a database must also cancel its scheduled triggers' tokens — a
    /// blocking run_async=false run would otherwise hang the database deletion the
    /// same way it hangs a single-trigger disable/delete.
    #[tokio::test]
    async fn shutdown_all_for_db_cancels_schedule_trigger_token() {
        let mut channels = PluginChannels::default();
        let (db_id, trigger_id) = (DbId::new(1), TriggerId::new(1));
        let token = CancellationToken::new();
        let _rx = channels.add_schedule_trigger(db_id, trigger_id, token.clone());
        assert!(!token.is_cancelled());

        let receivers = channels.shutdown_all_for_db(db_id).await;
        assert_eq!(
            receivers.len(),
            1,
            "expected one shutdown receiver for the schedule trigger"
        );
        assert!(
            token.is_cancelled(),
            "deleting a database must cancel its schedule triggers' tokens"
        );
    }
}

// Property-based tests using proptest
use proptest::prelude::*;

proptest! {
    /// Paths starting with "../" (1-10 repetitions) must always be rejected.
    /// Example: "../../../evil.py" should fail regardless of depth.
    #[test]
    fn prop_test_parent_traversal_always_rejected(
        depth in 1usize..10,
        suffix in "[a-z]{1,10}"
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let dots = "../".repeat(depth);
        let malicious = format!("{}{}.py", dots, suffix);
        let result = crate::validate_path_within_plugin_dir(
            temp_dir.path(),
            &malicious,
        );
        prop_assert!(result.is_err());
    }

    /// Paths that descend into a directory then traverse out must be rejected.
    /// Example: "subdir/../../../evil.py" should fail even with a valid prefix.
    #[test]
    fn prop_test_nested_traversal_always_rejected(
        prefix in "[a-z]{1,5}",
        depth in 1usize..10,
        suffix in "[a-z]{1,10}"
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let dots = "../".repeat(depth);
        let malicious = format!("{}/{}{}.py", prefix, dots, suffix);
        let result = crate::validate_path_within_plugin_dir(
            temp_dir.path(),
            &malicious,
        );
        prop_assert!(result.is_err());
    }

    /// Valid nested paths without traversal components must be accepted.
    /// Example: "My_Plugin/Utils/Helper.py" should succeed.
    #[test]
    fn prop_test_valid_paths_accepted(
        segments in prop::collection::vec("[a-zA-Z0-9_]{1,12}", 1..10),
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let valid_path = format!("{}.py", segments.join("/"));
        let result = crate::validate_path_within_plugin_dir(
            temp_dir.path(),
            &valid_path,
        );
        prop_assert!(result.is_ok());
    }

    /// Absolute paths starting with "/" must always be rejected.
    /// Example: "/etc/passwd" or "/tmp/evil.py" should fail.
    #[test]
    fn prop_test_absolute_paths_rejected(
        path in "/[a-z/]{1,20}"
    ) {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = crate::validate_path_within_plugin_dir(
            temp_dir.path(),
            &path,
        );
        prop_assert!(result.is_err());
    }
}
