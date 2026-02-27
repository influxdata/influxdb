use crate::ProcessingEngineManagerImpl;
use crate::TriggerSpecificationDefinition;
use crate::environment::TestManager;
use crate::plugins::ProcessingEngineEnvironmentManager;
use data_types::NamespaceName;
use datafusion_util::config::register_iox_object_store;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::log::TriggerSettings;
use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
use influxdb3_shutdown::ShutdownManager;
use influxdb3_sys_events::SysEventStore;
use influxdb3_wal::{Gen1Duration, WalConfig};
use influxdb3_write::Precision;
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
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;

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
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
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
            Arc::clone(&pem.node_id),
            file_name,
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
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
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
            false,
        )
        .await?;

    let file_name = pem.validate_plugin_filename(&file_name).await.unwrap();
    // Create a disabled trigger
    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            Arc::clone(&pem.node_id),
            file_name,
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
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
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
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
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
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
        plugin_repo: None,
    };

    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));

    (
        ProcessingEngineManagerImpl::new(
            environment_manager,
            catalog,
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = ProcessingEngineManagerImpl::new(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = ProcessingEngineManagerImpl::new(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger first
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

    let plugin_filename = pem.validate_plugin_filename("test_plugin").await.unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            Arc::clone(&pem.node_id),
            plugin_filename,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger first
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();

    let plugin_filename = pem.validate_plugin_filename("test_plugin").await.unwrap();

    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            Arc::clone(&pem.node_id),
            plugin_filename,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
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
        plugin_repo: None,
    };

    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
    let test_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(
        Catalog::new(
            "test_host",
            Arc::clone(&test_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

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
        shutdown: shutdown.register(),
        n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
        wal_replay_concurrency_limit: 1,
    })
    .await
    .unwrap();

    let pem = Arc::new(
        ProcessingEngineManagerImpl::new(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            wbuf as _,
            qe,
            time_provider,
            sys_event_store,
        )
        .await
        .unwrap(),
    );

    // Create the DB and trigger
    pem.write_buffer
        .write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
            start_time,
            false,
            Precision::Nanosecond,
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
            Arc::clone(&pem.node_id),
            plugin_filename,
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
