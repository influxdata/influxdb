use crate::TriggerSpecificationDefinition;
use crate::environment::TestManager;
use crate::plugins::ProcessingEngineEnvironmentManager;
use crate::query::UnimplementedQueryEndpoint;
use crate::virtualenv::init_pyo3;
use crate::write::InProcessWriteEndpoint;
use crate::{ProcessingEngineManagerImpl, ProcessingEngineManagerOptions};
use datafusion_util::config::register_iox_object_store;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::{
    ApiNodeSpec, Catalog, DeletionScope, ErrorBehavior, HardDeletionTime, TriggerSettings,
};
use influxdb3_id::{DbId, TriggerId};
use influxdb3_py_api::cache::{CacheStore, PyCache};
use influxdb3_py_api::write::{WriteEndpoint, WriteTarget};
use influxdb3_shutdown::ShutdownManager;
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
    let (pem, write_endpoint, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB by inserting a line.
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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
async fn test_scheduler_error_behavior_disable_persists_and_cleans_registry()
-> Result<(), Box<dyn std::error::Error>> {
    let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
    let test_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        gen1_duration: Gen1Duration::new_1m(),
        max_write_buffer_size: 100,
        flush_interval: Duration::from_millis(10),
        snapshot_size: 1,
        ..Default::default()
    };
    let (pem, write_endpoint, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
            "cpu,warehouse=us-east reading=37\n",
            start_time,
            false,
        )
        .await?;

    let file_name = pem.validate_plugin_filename(&file_name).await.unwrap();
    pem.catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            file_name,
            ApiNodeSpec::All,
            "all_tables",
            TriggerSettings {
                run_async: false,
                error_behavior: ErrorBehavior::Disable,
            },
            &None,
            false,
        )
        .await?;

    let db_id = pem.catalog.db_schema("foo").unwrap().id;
    let trigger = pem
        .catalog
        .db_schema("foo")
        .unwrap()
        .processing_engine_triggers
        .get_by_name("test_trigger")
        .unwrap();
    let trigger_id = trigger.trigger_id;
    assert!(!trigger.disabled);

    let cancel = CancellationToken::new();
    let manager = Arc::clone(&pem);
    let trigger_for_disable = Arc::clone(&trigger);
    let scheduler = crate::scheduler::Scheduler::new(Arc::<str>::from("scheduler"), |completion| {
        vec![Arc::new(AlwaysFailTriggerWorker { completion })]
    });
    let key = crate::scheduler::TriggerKey { db_id, trigger_id };
    scheduler
        .register_trigger(crate::scheduler::TriggerRegistration {
            key,
            trigger_definition: Arc::clone(&trigger),
            cancel,
            config: crate::scheduler::SchedulerConfig::new(16, false, std::num::NonZeroUsize::MAX),
            auto_disable: Arc::new(move || {
                let manager = Arc::clone(&manager);
                let trigger_for_disable = Arc::clone(&trigger_for_disable);
                Box::pin(async move {
                    manager
                        .disable_trigger_from_scheduler(trigger_for_disable)
                        .await
                }) as crate::scheduler::AutoDisableFuture
            }) as crate::scheduler::AutoDisable,
        })
        .await;
    pem.trigger_registry.write().await.add_wal_trigger(
        key,
        Arc::clone(&trigger.database_name),
        crate::WalRouteFilter::AllTables,
    );

    scheduler
        .enqueue(crate::scheduler::TriggerInvocation::new(
            key,
            crate::scheduler::TriggerPayload::Schedule {
                scheduled_at: chrono::Utc::now(),
            },
        ))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let disabled = pem
                .catalog
                .db_schema("foo")
                .unwrap()
                .processing_engine_triggers
                .get_by_name("test_trigger")
                .unwrap()
                .disabled;
            let still_routed = pem.trigger_registry.read().await.routes.contains_key(&key);
            if disabled && !still_routed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("error-behavior=disable should persist and clean route");

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
    let (pem, write_endpoint, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB by inserting a line.
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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
    let (pem, write_endpoint, _file_name) = setup(start_time, test_store, wal_config).await;

    // Create the DB by inserting a line.
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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
) -> (
    Arc<ProcessingEngineManagerImpl>,
    Arc<dyn WriteEndpoint>,
    NamedTempFile,
) {
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
    let metric_registry = Arc::new(Registry::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "test_host",
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

    let write_endpoint: Arc<dyn WriteEndpoint> = Arc::new(InProcessWriteEndpoint::new(wbuf));
    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        catalog,
        "test_node",
        Arc::clone(&write_endpoint),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
    )
    .await
    .unwrap();

    (pem, write_endpoint, file)
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

    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::new(InProcessWriteEndpoint::new(wbuf)),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
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

    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::new(InProcessWriteEndpoint::new(wbuf)),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
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

    let write_endpoint: Arc<dyn WriteEndpoint> = Arc::new(InProcessWriteEndpoint::new(wbuf));
    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::clone(&write_endpoint),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
    )
    .await
    .unwrap();

    // Create the DB and trigger first
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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
        ProcessingEngineManagerImpl::new_with_options(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            Arc::new(UnimplementedQueryEndpoint),
            time_provider,
            ProcessingEngineManagerOptions::new(),
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
        ProcessingEngineManagerImpl::new_with_options(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            Arc::new(UnimplementedQueryEndpoint),
            time_provider,
            ProcessingEngineManagerOptions::new(),
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
        ProcessingEngineManagerImpl::new_with_options(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            Arc::new(UnimplementedQueryEndpoint),
            time_provider,
            ProcessingEngineManagerOptions::new(),
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
        ProcessingEngineManagerImpl::new_with_options(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            Arc::new(UnimplementedQueryEndpoint),
            time_provider,
            ProcessingEngineManagerOptions::new(),
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

    let write_endpoint: Arc<dyn WriteEndpoint> = Arc::new(InProcessWriteEndpoint::new(wbuf));
    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::clone(&write_endpoint),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
    )
    .await
    .unwrap();

    // Create the DB and trigger first
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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
        ProcessingEngineManagerImpl::new_with_options(
            environment_manager,
            Arc::clone(&catalog),
            "test_node",
            Arc::new(InProcessWriteEndpoint::new(wbuf)),
            Arc::new(UnimplementedQueryEndpoint),
            time_provider,
            ProcessingEngineManagerOptions::new(),
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

    let write_endpoint: Arc<dyn WriteEndpoint> = Arc::new(InProcessWriteEndpoint::new(wbuf));
    let pem = ProcessingEngineManagerImpl::new_with_options(
        environment_manager,
        Arc::clone(&catalog),
        "test_node",
        Arc::clone(&write_endpoint),
        Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        ProcessingEngineManagerOptions::new(),
    )
    .await
    .unwrap();

    // Create the DB and trigger
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("foo").unwrap()),
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

struct NoopTriggerWorker {
    completion: Arc<dyn crate::scheduler_worker_protocol::TriggerScheduler>,
}

impl crate::scheduler_worker_protocol::TriggerWorker for NoopTriggerWorker {
    fn node_id(&self) -> Arc<str> {
        Arc::<str>::from("worker")
    }

    fn submit_work(
        self: Arc<Self>,
        _scheduler_node_id: Arc<str>,
        work: crate::scheduler_worker_protocol::TriggerWork,
    ) {
        let work_id = work.id;
        let result = match work.payload {
            crate::scheduler_worker_protocol::TriggerWorkPayload::Request(_) => Ok(
                crate::scheduler_worker_protocol::TriggerWorkOutput::RequestResponse(
                    crate::scheduler_worker_protocol::TriggerResponse {
                        status_code: hyper::StatusCode::OK.as_u16(),
                        headers: Default::default(),
                        body: "ok".to_string(),
                    },
                ),
            ),
            crate::scheduler_worker_protocol::TriggerWorkPayload::Wal { .. }
            | crate::scheduler_worker_protocol::TriggerWorkPayload::Schedule { .. } => {
                Ok(crate::scheduler_worker_protocol::TriggerWorkOutput::Complete)
            }
        };
        self.completion
            .work_progressed(Arc::<str>::from("worker"), work_id);
        self.completion.work_finished(
            Arc::<str>::from("worker"),
            crate::scheduler_worker_protocol::TriggerWorkResult { work_id, result },
        );
    }

    fn cancel_work(
        self: Arc<Self>,
        _scheduler_node_id: Arc<str>,
        _work_id: crate::scheduler_worker_protocol::TriggerWorkId,
    ) {
    }
}

struct AlwaysFailTriggerWorker {
    completion: Arc<dyn crate::scheduler_worker_protocol::TriggerScheduler>,
}

impl crate::scheduler_worker_protocol::TriggerWorker for AlwaysFailTriggerWorker {
    fn node_id(&self) -> Arc<str> {
        Arc::<str>::from("worker")
    }

    fn submit_work(
        self: Arc<Self>,
        _scheduler_node_id: Arc<str>,
        work: crate::scheduler_worker_protocol::TriggerWork,
    ) {
        let completion = Arc::clone(&self.completion);
        tokio::spawn(async move {
            completion.work_progressed(Arc::<str>::from("worker"), work.id);
            completion.work_finished(
                Arc::<str>::from("worker"),
                crate::scheduler_worker_protocol::TriggerWorkResult {
                    work_id: work.id,
                    result: Err(
                        crate::scheduler_worker_protocol::TriggerExecutionError::new("boom"),
                    ),
                },
            );
        });
    }

    fn cancel_work(
        self: Arc<Self>,
        _scheduler_node_id: Arc<str>,
        _work_id: crate::scheduler_worker_protocol::TriggerWorkId,
    ) {
    }
}

fn trigger_key(db_id: DbId, trigger_id: TriggerId) -> crate::scheduler::TriggerKey {
    crate::scheduler::TriggerKey { db_id, trigger_id }
}

fn successful_auto_disable() -> crate::scheduler::AutoDisable {
    Arc::new(|| Box::pin(async { true }) as crate::scheduler::AutoDisableFuture)
}

const REQUEST_TRIGGER_PATH: &str = "/api/v3/engine/test_db/test_endpoint";

fn trigger_name_for_kind(trigger_kind: &str) -> &'static str {
    match trigger_kind {
        "wal" => "wal_trigger",
        "schedule" => "schedule_trigger",
        "request" => "request_trigger",
        other => panic!("unknown trigger kind: {other}"),
    }
}

fn trigger_spec_for_kind(trigger_kind: &str) -> &'static str {
    match trigger_kind {
        "wal" => "all_tables",
        "schedule" => "every:1s",
        "request" => "request:/api/v3/engine/test_db/test_endpoint",
        other => panic!("unknown trigger kind: {other}"),
    }
}

fn request_path_for_kind(trigger_kind: &str) -> Option<&'static str> {
    (trigger_kind == "request").then_some(REQUEST_TRIGGER_PATH)
}

/// Sets up a database with a trigger and its registry for deletion tests.
///
/// Creates a "test_db" database, registers a trigger of the given kind in the
/// catalog, and starts a scheduler runtime for it. WAL and request triggers also
/// get routing metadata. Returns the token so tests can assert shutdown.
///
/// The `_file` return keeps the NamedTempFile alive so the plugin path remains valid.
async fn setup_db_with_trigger(
    trigger_kind: &str,
) -> Result<
    (
        Arc<ProcessingEngineManagerImpl>,
        DbId,
        TriggerId,
        CancellationToken,
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
    let (pem, write_endpoint, file) = setup(start_time, test_store, wal_config).await;
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Create the DB
    write_endpoint
        .write_lp(
            WriteTarget::User(DatabaseName::new("test_db").unwrap()),
            "cpu,host=a val=1\n",
            start_time,
            false,
        )
        .await?;

    let db_id = pem.catalog.db_schema("test_db").unwrap().id;

    let trigger_name = trigger_name_for_kind(trigger_kind);
    let trigger_spec = trigger_spec_for_kind(trigger_kind);

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

    let trigger = pem
        .catalog
        .db_schema("test_db")
        .unwrap()
        .processing_engine_triggers
        .get_by_name(trigger_name)
        .unwrap();
    let trigger_id = trigger.trigger_id;

    // Simulate a running trigger without going through run_trigger, which requires
    // enterprise node registration.
    let key = trigger_key(db_id, trigger_id);
    let cancel = CancellationToken::new();
    pem.scheduler
        .register_trigger(crate::scheduler::TriggerRegistration {
            key,
            trigger_definition: Arc::clone(&trigger),
            cancel: cancel.clone(),
            config: crate::scheduler::SchedulerConfig::new(
                16,
                trigger.trigger_settings.run_async,
                std::num::NonZeroUsize::MAX,
            ),
            auto_disable: successful_auto_disable(),
        })
        .await;

    // Insert only the routing metadata needed for externally delivered events.
    // Schedule triggers are driven by their event-source task and do not need a route.
    {
        let mut registry = pem.trigger_registry.write().await;
        match trigger_kind {
            "wal" => {
                registry.add_wal_trigger(
                    key,
                    Arc::clone(&trigger.database_name),
                    crate::WalRouteFilter::AllTables,
                );
            }
            "schedule" => {}
            "request" => {
                registry.add_request_trigger(key, REQUEST_TRIGGER_PATH.to_string());
            }
            _ => unreachable!(),
        }
    }

    Ok((pem, db_id, trigger_id, cancel, file))
}

async fn assert_external_route(
    pem: &ProcessingEngineManagerImpl,
    key: crate::scheduler::TriggerKey,
    request_path: Option<&str>,
    expected: bool,
) {
    let registry = pem.trigger_registry.read().await;
    assert_eq!(registry.routes.contains_key(&key), expected);
    if let Some(path) = request_path {
        assert_eq!(registry.request_paths.contains_key(path), expected);
    }
}

fn assert_trigger_disabled(pem: &ProcessingEngineManagerImpl, db_id: DbId, trigger_kind: &str) {
    let db = pem.catalog.db_schema_by_id(&db_id).unwrap();
    let trigger = db
        .processing_engine_triggers
        .get_by_name(trigger_name_for_kind(trigger_kind))
        .unwrap();
    assert!(
        trigger.disabled,
        "{trigger_kind} trigger should be disabled in the catalog after soft delete"
    );
}

#[test_log::test(tokio::test)]
async fn test_soft_delete_stops_external_triggers() -> Result<(), Box<dyn std::error::Error>> {
    for trigger_kind in ["wal", "request"] {
        let (pem, db_id, trigger_id, _cancel, _file) = setup_db_with_trigger(trigger_kind).await?;
        let key = trigger_key(db_id, trigger_id);
        let request_path = request_path_for_kind(trigger_kind);

        assert_external_route(&pem, key, request_path, true).await;

        // HardDeletionTime::Never means no hard delete follows, so soft delete
        // performs full cleanup (shutdown + remove + cache drop) immediately.
        pem.catalog
            .soft_delete_database(
                "test_db",
                HardDeletionTime::Never,
                DeletionScope::DataAndCatalog,
            )
            .await?;

        // Catalog subscriptions are synchronous (ACK-on-drop), so by the time
        // soft_delete_database returns, the background handler has processed it.
        assert_external_route(&pem, key, request_path, false).await;
        assert_trigger_disabled(&pem, db_id, trigger_kind);
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
    let (pem, db_id, trigger_id, _cancel, _file) = setup_db_with_trigger("wal").await?;
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
        .soft_delete_database(
            "test_db",
            HardDeletionTime::Never,
            DeletionScope::DataAndCatalog,
        )
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
    let (pem, db_id, trigger_id, _cancel, _file) = setup_db_with_trigger("wal").await?;
    init_pyo3();

    seed_trigger_cache(&pem.cache, db_id, trigger_id);
    // Control entry; must survive, proving the drop is id-scoped.
    seed_trigger_cache(&pem.cache, DbId::new(8), TriggerId::new(8));

    pem.stop_trigger(db_id, trigger_id).await.unwrap();

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
    let (pem, db_id, _trigger_id, cancel, _file) = setup_db_with_trigger("schedule").await?;
    assert!(!cancel.is_cancelled());

    pem.catalog
        .soft_delete_database(
            "test_db",
            HardDeletionTime::Never,
            DeletionScope::DataAndCatalog,
        )
        .await?;

    assert!(
        cancel.is_cancelled(),
        "Schedule trigger runtime should stop after soft delete with HardDeletionTime::Never"
    );
    assert_trigger_disabled(&pem, db_id, "schedule");
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_removes_external_triggers() -> Result<(), Box<dyn std::error::Error>> {
    for trigger_kind in ["wal", "request"] {
        let (pem, db_id, trigger_id, _cancel, _file) = setup_db_with_trigger(trigger_kind).await?;
        let key = trigger_key(db_id, trigger_id);
        let request_path = request_path_for_kind(trigger_kind);

        // Use HardDeletionTime::Now so soft delete leaves the trigger in the
        // routing table for the hard delete handler instead of cleaning up immediately.
        pem.catalog
            .soft_delete_database(
                "test_db",
                HardDeletionTime::Now,
                DeletionScope::DataAndCatalog,
            )
            .await?;

        assert_external_route(&pem, key, request_path, true).await;

        pem.catalog.hard_delete_database(&db_id).await?;

        assert_external_route(&pem, key, request_path, false).await;
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_removes_schedule_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let (pem, db_id, _trigger_id, cancel, _file) = setup_db_with_trigger("schedule").await?;
    assert!(!cancel.is_cancelled());

    pem.catalog
        .soft_delete_database(
            "test_db",
            HardDeletionTime::Now,
            DeletionScope::DataAndCatalog,
        )
        .await?;

    assert!(
        cancel.is_cancelled(),
        "Schedule trigger runtime should stop on soft delete even when hard delete is pending"
    );

    pem.catalog.hard_delete_database(&db_id).await?;
    Ok(())
}

/// Unit tests for the id-keyed trigger routing table. These exercise
/// `TriggerRegistry` directly, without a catalog, to prove WAL and request
/// routing are driven by `TriggerKey`.
#[cfg(test)]
mod trigger_registry_tests {
    use crate::{TriggerRegistry, WalRouteFilter};
    use bytes::Bytes;
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::{
        NodeSpec, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
    };
    use influxdb3_id::{DbId, TableId, TriggerId};
    use influxdb3_py_api::wal::WalFlushElement;
    use iox_http_util::Response;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    fn trigger(db_id: DbId, trigger_id: TriggerId) -> Arc<TriggerDefinition> {
        let _ = db_id;
        Arc::new(TriggerDefinition {
            trigger_id,
            trigger_name: Arc::from("trig"),
            plugin_filename: "plugin.py".to_string(),
            database_name: Arc::from("db"),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings::default(),
            trigger_arguments: None,
            disabled: false,
        })
    }

    fn key(db_id: DbId, trigger_id: TriggerId) -> crate::scheduler::TriggerKey {
        crate::scheduler::TriggerKey { db_id, trigger_id }
    }

    async fn register_trigger(
        scheduler: &crate::scheduler::Scheduler,
        db_id: DbId,
        trigger_id: TriggerId,
        token: CancellationToken,
    ) {
        scheduler
            .register_trigger(crate::scheduler::TriggerRegistration {
                key: key(db_id, trigger_id),
                trigger_definition: trigger(db_id, trigger_id),
                cancel: token,
                config: crate::scheduler::SchedulerConfig::new(
                    16,
                    false,
                    std::num::NonZeroUsize::MAX,
                ),
                auto_disable: super::successful_auto_disable(),
            })
            .await;
    }

    struct CountingWorker {
        attempts: Arc<AtomicUsize>,
        completion: Arc<dyn crate::scheduler_worker_protocol::TriggerScheduler>,
    }

    impl crate::scheduler_worker_protocol::TriggerWorker for CountingWorker {
        fn node_id(&self) -> Arc<str> {
            Arc::<str>::from("worker")
        }

        fn submit_work(
            self: Arc<Self>,
            _scheduler_node_id: Arc<str>,
            work: crate::scheduler_worker_protocol::TriggerWork,
        ) {
            let completion = Arc::clone(&self.completion);
            tokio::spawn(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                completion.work_progressed(Arc::<str>::from("worker"), work.id);
                completion.work_finished(
                    Arc::<str>::from("worker"),
                    crate::scheduler_worker_protocol::TriggerWorkResult {
                        work_id: work.id,
                        result: Ok(crate::scheduler_worker_protocol::TriggerWorkOutput::Complete),
                    },
                );
            });
        }

        fn cancel_work(
            self: Arc<Self>,
            _scheduler_node_id: Arc<str>,
            _work_id: crate::scheduler_worker_protocol::TriggerWorkId,
        ) {
        }
    }

    async fn counting_wal_scheduler(
        db_id: DbId,
        trigger_id: TriggerId,
        attempts: Arc<AtomicUsize>,
    ) -> crate::scheduler::Scheduler {
        let scheduler =
            crate::scheduler::Scheduler::new(Arc::<str>::from("scheduler"), |completion| {
                vec![Arc::new(CountingWorker {
                    attempts,
                    completion,
                })]
            });
        register_trigger(&scheduler, db_id, trigger_id, CancellationToken::new()).await;
        scheduler
    }

    #[tokio::test]
    async fn add_wal_trigger_is_keyed_by_id() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let key = key(db_id, trigger_id);

        registry.add_wal_trigger(key, Arc::from("db"), WalRouteFilter::AllTables);

        assert!(registry.routes.contains_key(&key));
    }

    #[tokio::test]
    async fn single_table_wal_routing_skips_unrelated_flushes() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let key = key(db_id, trigger_id);
        let attempts = Arc::new(AtomicUsize::new(0));
        let scheduler = counting_wal_scheduler(db_id, trigger_id, Arc::clone(&attempts)).await;
        registry.add_wal_trigger(
            key,
            Arc::from("db"),
            WalRouteFilter::SingleTable("home".to_string()),
        );

        let unrelated: Arc<[WalFlushElement]> = Arc::from(vec![WalFlushElement {
            table_id: TableId::new(1),
            table_name: Arc::from("sensors"),
            data: vec![],
        }]);
        crate::ProcessingEngineManagerImpl::enqueue_wal_invocations(
            &scheduler,
            registry.wal_invocations(Arc::from("db"), unrelated),
        )
        .await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);

        let related: Arc<[WalFlushElement]> = Arc::from(vec![WalFlushElement {
            table_id: TableId::new(2),
            table_name: Arc::from("home"),
            data: vec![],
        }]);
        crate::ProcessingEngineManagerImpl::enqueue_wal_invocations(
            &scheduler,
            registry.wal_invocations(Arc::from("db"), related),
        )
        .await;
        while attempts.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        scheduler.shutdown_trigger(key).await;
    }

    #[tokio::test]
    async fn remove_all_for_db_removes_matching_routes_only() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let trigger_key = key(db_id, trigger_id);
        let other_key = key(DbId::new(2), TriggerId::new(3));
        registry.add_wal_trigger(trigger_key, Arc::from("db"), WalRouteFilter::AllTables);
        registry.add_wal_trigger(other_key, Arc::from("other"), WalRouteFilter::AllTables);

        registry.remove_all_for_db(db_id);

        assert!(!registry.routes.contains_key(&trigger_key));
        assert!(registry.routes.contains_key(&other_key));
    }

    #[tokio::test]
    async fn remove_request_trigger_clears_path_index() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let key = key(db_id, trigger_id);
        registry.add_request_trigger(key, "/p".to_string());
        assert!(registry.request_paths.contains_key("/p"));

        registry.remove_trigger(key);

        assert!(
            !registry.request_paths.contains_key("/p"),
            "removing a request trigger must clear its path index"
        );
        assert!(
            !registry.routes.contains_key(&key),
            "removing a request trigger must drop it from the routes"
        );
    }

    #[tokio::test]
    async fn send_request_routes_by_path() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let key = key(db_id, trigger_id);
        let scheduler =
            crate::scheduler::Scheduler::new(Arc::<str>::from("scheduler"), |completion| {
                vec![Arc::new(super::NoopTriggerWorker { completion })]
            });
        register_trigger(&scheduler, db_id, trigger_id, CancellationToken::new()).await;
        registry.add_request_trigger(key, "/p".to_string());

        let (response_tx, response_rx) = oneshot::channel::<Response>();
        let payload = crate::scheduler::RequestPayload::new(
            HashMap::new(),
            HashMap::new(),
            Bytes::new(),
            response_tx,
        );
        let invocation = registry
            .request_invocation("/p", payload)
            .expect("request_invocation should route by path");
        scheduler
            .enqueue(invocation)
            .await
            .expect("enqueue request");
        let response = response_rx
            .await
            .expect("request should receive a response");
        assert_eq!(response.status(), hyper::StatusCode::OK);

        // An unknown path is reported as not found.
        let (response_tx, _response_rx) = oneshot::channel::<Response>();
        let payload = crate::scheduler::RequestPayload::new(
            HashMap::new(),
            HashMap::new(),
            Bytes::new(),
            response_tx,
        );
        assert!(registry.request_invocation("/missing", payload).is_err());
        scheduler.shutdown_trigger(key).await;
    }

    #[tokio::test]
    async fn registering_schedule_trigger_replaces_and_cancels_old_runtime() {
        let scheduler =
            crate::scheduler::Scheduler::new(Arc::<str>::from("scheduler"), |completion| {
                vec![Arc::new(super::NoopTriggerWorker { completion })]
            });
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(2);
        let old_token = CancellationToken::new();
        let new_token = CancellationToken::new();

        register_trigger(&scheduler, db_id, trigger_id, old_token.clone()).await;
        register_trigger(&scheduler, db_id, trigger_id, new_token.clone()).await;

        assert!(old_token.is_cancelled());
        assert!(!new_token.is_cancelled());
        scheduler.shutdown_trigger(key(db_id, trigger_id)).await;
        assert!(new_token.is_cancelled());
    }

    #[tokio::test]
    async fn remove_all_for_db_clears_request_path_index() {
        let mut registry = TriggerRegistry::default();
        let db_id = DbId::new(1);
        let wal_trigger = TriggerId::new(2);
        let req_trigger = TriggerId::new(3);
        let wal_key = key(db_id, wal_trigger);
        let req_key = key(db_id, req_trigger);
        registry.add_wal_trigger(wal_key, Arc::from("db"), WalRouteFilter::AllTables);
        registry.add_request_trigger(req_key, "/p".to_string());

        registry.remove_all_for_db(db_id);

        assert!(!registry.routes.contains_key(&wal_key));
        assert!(!registry.routes.contains_key(&req_key));
        assert!(
            !registry.request_paths.contains_key("/p"),
            "removing request routes for a DB must clear the path index"
        );
    }

    /// Stopping a trigger must cancel its per-trigger token so an in-flight
    /// plugin run is interrupted (the run loop races this token).
    #[tokio::test]
    async fn scheduler_shutdown_trigger_cancels_trigger_token() {
        let token = CancellationToken::new();
        let scheduler =
            crate::scheduler::Scheduler::new(Arc::<str>::from("scheduler"), |completion| {
                vec![Arc::new(super::NoopTriggerWorker { completion })]
            });
        let db_id = DbId::new(1);
        let trigger_id = TriggerId::new(1);
        register_trigger(&scheduler, db_id, trigger_id, token.clone()).await;
        assert!(!token.is_cancelled());

        scheduler.shutdown_trigger(key(db_id, trigger_id)).await;
        assert!(
            token.is_cancelled(),
            "stopping a trigger must cancel its per-trigger token"
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
