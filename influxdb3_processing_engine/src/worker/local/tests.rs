use super::*;
use crate::environment::TestManager;
use crate::query::UnimplementedQueryEndpoint;
use influxdb3_catalog::catalog::{ApiNodeSpec, TriggerSettings, ValidPluginFilename};
use influxdb3_py_api::write::WriteAccumulator;
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::memory::InMemory;
use std::io::Write as _;
use std::time::Duration;
use tempfile::NamedTempFile;

struct NoopTriggerScheduler(Arc<str>);

impl TriggerScheduler for NoopTriggerScheduler {
    fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.0)
    }

    fn work_progressed(&self, _worker_node_id: Arc<str>, _work_id: TriggerWorkId) {}

    fn work_finished(&self, _worker_node_id: Arc<str>, _result: TriggerWorkResult) {}
}

#[tokio::test]
async fn python_worker_reuses_cached_trigger_plugin_tracks_schedulers_and_drops_unknown_work() {
    let now = Time::from_timestamp_nanos(1);
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::clone(&time_provider),
        Duration::from_secs(10),
    )));
    let catalog = Catalog::new(
        "test_host",
        Arc::new(InMemory::new()),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();
    catalog.create_database("foo").await.unwrap();

    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "def process_scheduled_call(influxdb3_local, call_time, args=None): pass"
    )
    .unwrap();
    let plugin_dir = file.path().parent().unwrap().to_path_buf();
    let file_name = file
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let trigger_definition = catalog
        .create_processing_engine_trigger(
            "foo",
            "test_trigger",
            ValidPluginFilename::from_validated_name(&file_name),
            ApiNodeSpec::All,
            "every:1s",
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();
    let db_schema = catalog.db_schema("foo").unwrap();
    let work_key = TriggerKey {
        db_id: db_schema.id,
        trigger_id: trigger_definition.trigger_id,
    };
    let worker = Arc::new(PythonTriggerWorker {
        environment_manager: ProcessingEngineEnvironmentManager {
            plugin_dir: Some(plugin_dir),
            virtual_env_location: None,
            package_manager: Arc::new(TestManager),
            plugin_dir_only: false,
            plugin_repo: None,
        },
        catalog,
        node_id: Arc::from("test_node"),
        write_endpoint: Arc::new(WriteAccumulator::default()),
        query_endpoint: Arc::new(UnimplementedQueryEndpoint),
        time_provider,
        cache,
        plugin_shutdown: CancellationToken::new(),
        plugin_trigger_invocation_registry: None,
        plugins: Default::default(),
        active_work: Default::default(),
        schedulers: Default::default(),
    });
    let scheduler_one: Arc<dyn TriggerScheduler> =
        Arc::new(NoopTriggerScheduler(Arc::from("scheduler-one")));
    let scheduler_two: Arc<dyn TriggerScheduler> =
        Arc::new(NoopTriggerScheduler(Arc::from("scheduler-two")));
    worker.register_scheduler(Arc::clone(&scheduler_one));
    worker.register_scheduler(Arc::clone(&scheduler_two));
    assert_eq!(
        worker.scheduler_for("scheduler-one").unwrap().node_id(),
        Arc::<str>::from("scheduler-one")
    );
    assert_eq!(
        worker.scheduler_for("scheduler-two").unwrap().node_id(),
        Arc::<str>::from("scheduler-two")
    );

    let first = worker.plugin_for_key(work_key).await.unwrap();
    let second = worker.plugin_for_key(work_key).await.unwrap();

    assert!(
        Arc::ptr_eq(&first, &second),
        "the same trigger should reuse its cached TriggerPlugin"
    );
    assert_eq!(worker.plugins.lock().len(), 1);

    let work_id = TriggerWorkId::next();
    Arc::clone(&worker).submit_work(
        Arc::from("unknown-scheduler"),
        TriggerWork {
            id: work_id,
            key: work_key,
            payload: TriggerWorkPayload::Schedule {
                scheduled_at: Utc::now(),
            },
        },
    );
    Arc::clone(&worker).cancel_work(Arc::from("unknown-scheduler"), work_id);
    assert!(!worker.active_work.contains(work_id));
}

#[test]
fn duplicate_work_id_stays_active_until_its_matching_completion() {
    let active_work = ActiveWorkRegistry::default();
    let work_id = TriggerWorkId::next();
    let cancel = CancellationToken::new();

    let generation = active_work
        .submit(work_id, cancel.clone())
        .expect("first work submission should be accepted");
    assert!(
        active_work
            .submit(work_id, CancellationToken::new())
            .is_none(),
        "a duplicate delivery must not start a second execution"
    );
    active_work.cancel(work_id);
    assert!(cancel.is_cancelled());

    active_work.finish(work_id, generation.wrapping_add(1));
    assert!(
        active_work.contains(work_id),
        "a stale execution must not remove the active work"
    );

    active_work.finish(work_id, generation);
    assert!(!active_work.contains(work_id));
}
