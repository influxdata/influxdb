use super::*;
use iox_time::MockProvider;

fn store() -> CacheStore {
    let tp = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    CacheStore::new(tp, Duration::from_secs(60))
}

fn insert_trigger(store: &mut CacheStore, db_id: DbId, trigger_id: TriggerId) {
    store.namespaces.insert(
        CacheId::Trigger { db_id, trigger_id },
        ExpiringCache::new(Arc::clone(&store.time_provider), None),
    );
}

fn insert_global(store: &mut CacheStore) {
    store.namespaces.insert(
        CacheId::Global(),
        ExpiringCache::new(Arc::clone(&store.time_provider), None),
    );
}

#[test]
fn drop_trigger_cache_removes_only_that_trigger() {
    let mut store = store();
    let db_id = DbId::new(1);
    let trigger_id_a = TriggerId::new(10);
    let trigger_id_b = TriggerId::new(20);

    insert_trigger(&mut store, db_id, trigger_id_a);
    insert_trigger(&mut store, db_id, trigger_id_b);
    assert_eq!(store.namespaces.len(), 2);

    // First removal returns true and removes only A.
    assert!(store.drop_trigger_cache(db_id, trigger_id_a));
    assert_eq!(store.namespaces.len(), 1);
    assert!(store.namespaces.contains_key(&CacheId::Trigger {
        db_id,
        trigger_id: trigger_id_b
    }));

    // Second removal of the same id returns false.
    assert!(!store.drop_trigger_cache(db_id, trigger_id_a));
}

#[test]
fn drop_all_trigger_caches_for_db_is_db_scoped() {
    let mut store = store();
    let db1 = DbId::new(1);
    let db2 = DbId::new(2);

    insert_trigger(&mut store, db1, TriggerId::new(10));
    insert_trigger(&mut store, db1, TriggerId::new(11));
    insert_trigger(&mut store, db2, TriggerId::new(20));
    insert_global(&mut store);
    assert_eq!(store.namespaces.len(), 4);

    store.drop_all_trigger_caches_for_db(db1);

    // Both db1 triggers removed; db2 trigger and global remain.
    assert_eq!(store.namespaces.len(), 2);
    assert!(store.namespaces.contains_key(&CacheId::Trigger {
        db_id: db2,
        trigger_id: TriggerId::new(20)
    }));
    assert!(store.namespaces.contains_key(&CacheId::Global()));
}

#[test]
fn new_trigger_cache_builds_id_keyed_namespace() {
    let store = Arc::new(Mutex::new(store()));
    let db_id = DbId::new(42);
    let trigger_id = TriggerId::new(7);

    let cache = PyCache::new_trigger_cache(Arc::clone(&store), db_id, trigger_id);

    assert_eq!(cache.local_cache_id, CacheId::Trigger { db_id, trigger_id });
    assert_eq!(cache.global_cache_id, CacheId::Global());
}
