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

use super::test_support::{NoopTriggerScheduler, gated_plugin_repo, test_worker_with_plugin_repo};

async fn create_gh_trigger(catalog: &Arc<Catalog>) -> TriggerKey {
    create_gh_trigger_in_db(catalog, "foo").await
}

async fn create_gh_trigger_in_db(catalog: &Arc<Catalog>, db_name: &str) -> TriggerKey {
    catalog.create_database(db_name).await.unwrap();
    let trigger_definition = catalog
        .create_processing_engine_trigger(
            db_name,
            "test_trigger",
            ValidPluginFilename::from_validated_name("gh:test/plugin.py"),
            ApiNodeSpec::All,
            "every:1s",
            TriggerSettings::default(),
            &None,
            false,
        )
        .await
        .unwrap();
    TriggerKey {
        db_id: catalog.db_schema(db_name).unwrap().id,
        trigger_id: trigger_definition.trigger_id,
    }
}

#[tokio::test]
async fn forget_all_for_db_evicts_only_that_dbs_entries() {
    let mut repo = mockito::Server::new_async().await;
    let mock = repo
        .mock("GET", "/test/plugin.py")
        .with_body("# v1")
        .expect(2)
        .create_async()
        .await;
    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo.url())).await;
    let key1 = create_gh_trigger_in_db(&catalog, "db_one").await;
    let key2 = create_gh_trigger_in_db(&catalog, "db_two").await;
    worker.plugin_for_key(key1).await.unwrap();
    worker.plugin_for_key(key2).await.unwrap();
    mock.assert_async().await;
    assert_eq!(worker.plugins.lock().len(), 2);

    worker.forget_all_for_db(key1.db_id);
    {
        let plugins = worker.plugins.lock();
        assert!(!plugins.contains_key(&key1));
        assert!(plugins.get(&key2).unwrap().plugin.is_some());
    }

    worker.forget_all_for_db(key2.db_id);
    assert_eq!(
        worker.plugins.lock().len(),
        0,
        "eviction must remove entries, not leave tombstones"
    );
}

#[tokio::test]
async fn load_in_flight_across_a_db_eviction_is_not_cached() {
    let (repo_url, request_arrived, release_response) = gated_plugin_repo(&["# stale"]).await;
    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo_url)).await;
    let key = create_gh_trigger(&catalog).await;

    let load_worker = Arc::clone(&worker);
    let load = tokio::spawn(async move { load_worker.plugin_for_key(key).await });
    request_arrived.notified().await;
    worker.forget_all_for_db(key.db_id);
    release_response.notify_one();

    let plugin = load.await.unwrap().unwrap();
    assert_eq!(plugin.plugin_code.code().as_ref(), "# stale");
    assert!(
        !worker.plugins.lock().contains_key(&key),
        "a db eviction must invalidate a load that was in flight"
    );
}

/// Pins the per-db granularity for which a map-level epoch was rejected:
/// evicting one db must not invalidate another db's in-flight load.
#[tokio::test]
async fn db_eviction_does_not_invalidate_another_dbs_in_flight_load() {
    let (repo_url, request_arrived, release_response) =
        gated_plugin_repo(&["# db1", "# db2"]).await;
    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo_url)).await;
    let key1 = create_gh_trigger_in_db(&catalog, "db_one").await;
    let key2 = create_gh_trigger_in_db(&catalog, "db_two").await;

    // db_one cached up front so the eviction has something to remove.
    let load1 = {
        let worker = Arc::clone(&worker);
        tokio::spawn(async move { worker.plugin_for_key(key1).await })
    };
    request_arrived.notified().await;
    release_response.notify_one();
    load1.await.unwrap().unwrap();

    // db_two's load is held mid-fetch across the db_one eviction.
    let load2 = {
        let worker = Arc::clone(&worker);
        tokio::spawn(async move { worker.plugin_for_key(key2).await })
    };
    request_arrived.notified().await;
    worker.forget_all_for_db(key1.db_id);
    release_response.notify_one();

    load2.await.unwrap().unwrap();
    let plugins = worker.plugins.lock();
    assert!(!plugins.contains_key(&key1));
    assert!(
        plugins.get(&key2).unwrap().plugin.is_some(),
        "evicting db_one must not invalidate db_two's in-flight load"
    );
}

#[tokio::test]
async fn forget_trigger_evicts_cached_plugin_so_gh_source_is_refetched() {
    let mut repo = mockito::Server::new_async().await;
    let mock_v1 = repo
        .mock("GET", "/test/plugin.py")
        .with_body("# v1")
        .expect(1)
        .create_async()
        .await;

    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo.url())).await;
    let key = create_gh_trigger(&catalog).await;

    let first = worker.plugin_for_key(key).await.unwrap();
    assert_eq!(first.plugin_code.code().as_ref(), "# v1");
    mock_v1.assert_async().await;

    let mock_v2 = repo
        .mock("GET", "/test/plugin.py")
        .with_body("# v2")
        .expect(1)
        .create_async()
        .await;
    worker.forget_trigger(key);

    let second = worker.plugin_for_key(key).await.unwrap();
    assert!(
        !Arc::ptr_eq(&first, &second),
        "eviction must rebuild the TriggerPlugin"
    );
    assert_eq!(second.plugin_code.code().as_ref(), "# v2");
    mock_v2.assert_async().await;
}

#[tokio::test]
async fn load_in_flight_across_an_eviction_is_not_cached() {
    let (repo_url, request_arrived, release_response) = gated_plugin_repo(&["# stale"]).await;
    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo_url)).await;
    let key = create_gh_trigger(&catalog).await;

    let load_worker = Arc::clone(&worker);
    let load = tokio::spawn(async move { load_worker.plugin_for_key(key).await });
    request_arrived.notified().await;
    worker.forget_trigger(key);
    release_response.notify_one();

    // The load still hands its plugin to the (cancelled) work that wanted it,
    // but must not re-cache source fetched before the trigger stopped.
    let plugin = load.await.unwrap().unwrap();
    assert_eq!(plugin.plugin_code.code().as_ref(), "# stale");
    assert!(
        !worker.plugins.lock().contains_key(&key),
        "an eviction must invalidate a load that was in flight"
    );
}

/// Guards the ABA hole that makes removal-based eviction safe: a slot
/// recreated after an eviction gets a globally unique generation, so a load
/// that reserved the *old* slot can never cache into the new one.
#[tokio::test]
async fn a_load_reserved_before_eviction_never_caches_into_a_recreated_slot() {
    let (repo_url, request_arrived, release_response) =
        gated_plugin_repo(&["# stale", "# fresh"]).await;
    let (worker, catalog, _shutdown) =
        test_worker_with_plugin_repo("test_node", None, Some(repo_url)).await;
    let key = create_gh_trigger(&catalog).await;

    let w1 = Arc::clone(&worker);
    let l1 = tokio::spawn(async move { w1.plugin_for_key(key).await });
    request_arrived.notified().await;
    worker.forget_trigger(key);

    // L2 recreates the slot, capturing its fresh generation under the map lock
    // *before* its HTTP connection is accepted (the gated repo serves
    // connections sequentially), so waiting for the slot to reappear orders
    // L2's reservation ahead of L1's insert.
    let w2 = Arc::clone(&worker);
    let l2 = tokio::spawn(async move { w2.plugin_for_key(key).await });
    while !worker.plugins.lock().contains_key(&key) {
        tokio::task::yield_now().await;
    }

    release_response.notify_one();
    let stale = l1.await.unwrap().unwrap();
    assert_eq!(stale.plugin_code.code().as_ref(), "# stale");
    assert!(
        worker.plugins.lock().get(&key).unwrap().plugin.is_none(),
        "a recreated slot must reject a load reserved before the eviction"
    );

    request_arrived.notified().await;
    release_response.notify_one();
    let fresh = l2.await.unwrap().unwrap();
    assert_eq!(fresh.plugin_code.code().as_ref(), "# fresh");
    assert!(
        worker.plugins.lock().get(&key).unwrap().plugin.is_some(),
        "the load that owns the recreated slot caches normally"
    );
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
        next_plugin_generation: Default::default(),
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
