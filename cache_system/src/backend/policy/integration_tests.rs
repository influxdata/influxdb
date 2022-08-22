//! Test integration between different policies.

use std::{collections::HashMap, sync::Arc, time::Duration};

use iox_time::{MockProvider, Time};
use tokio::sync::Notify;

use crate::backend::{policy::refresh::test_util::NotifyExt, CacheBackend};

use super::{
    refresh::{
        test_util::{TestLoader, TestRefreshDurationProvider},
        RefreshPolicy,
    },
    ttl::{test_util::TestTtlProvider, TtlPolicy},
    PolicyBackend,
};

#[tokio::test]
async fn test_refresh_can_prevent_expiration() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new().await;

    loader.mock_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));
    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(2)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // no expired because refresh resets the timer
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // we don't request a 2nd refresh (refresh duration is None), so this finally expires
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_refresh_sets_new_expiration_after_it_finishes() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new().await;

    let barrier = loader.block_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(3)));
    refresh_duration_provider.set_refresh_in(1, String::from("foo"), None);
    ttl_provider.set_expires_in(1, String::from("foo"), Some(Duration::from_secs(3)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("a")));

    time_provider.inc(Duration::from_secs(1));
    barrier.wait().await;
    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // no expired because refresh resets the timer after it was ready (now), not when it started (1s ago)
    time_provider.inc(Duration::from_secs(2));
    assert_eq!(backend.get(&1), Some(String::from("foo")));

    // we don't request a 2nd refresh (refresh duration is None), so this finally expires
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), None);
}

#[tokio::test]
async fn test_if_refresh_to_slow_then_expire() {
    let TestStateTtlAndRefresh {
        mut backend,
        refresh_duration_provider,
        ttl_provider,
        time_provider,
        loader,
        notify_idle,
        ..
    } = TestStateTtlAndRefresh::new().await;

    let _barrier = loader.block_next(1, String::from("foo"));
    refresh_duration_provider.set_refresh_in(1, String::from("a"), Some(Duration::from_secs(1)));
    ttl_provider.set_expires_in(1, String::from("a"), Some(Duration::from_secs(2)));
    backend.set(1, String::from("a"));

    // perform refresh
    time_provider.inc(Duration::from_secs(1));
    assert_eq!(backend.get(&1), Some(String::from("a")));

    notify_idle.notified_with_timeout().await;
    assert_eq!(backend.get(&1), Some(String::from("a")));

    time_provider.inc(Duration::from_secs(1));
    notify_idle.not_notified().await;
    assert_eq!(backend.get(&1), None);
}

/// Test setup that integrates the TTL policy with a refresh.
struct TestStateTtlAndRefresh {
    backend: PolicyBackend<u8, String>,
    ttl_provider: Arc<TestTtlProvider>,
    refresh_duration_provider: Arc<TestRefreshDurationProvider>,
    time_provider: Arc<MockProvider>,
    loader: Arc<TestLoader>,
    notify_idle: Arc<Notify>,
}

impl TestStateTtlAndRefresh {
    async fn new() -> Self {
        let refresh_duration_provider = Arc::new(TestRefreshDurationProvider::new());
        let ttl_provider = Arc::new(TestTtlProvider::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metric_registry = metric::Registry::new();
        let loader = Arc::new(TestLoader::default());
        let notify_idle = Arc::new(Notify::new());

        let mut backend = PolicyBackend::new(Box::new(HashMap::<u8, String>::new()));
        backend.add_policy(
            RefreshPolicy::new_inner(
                Arc::clone(&refresh_duration_provider) as _,
                Arc::clone(&time_provider) as _,
                Arc::clone(&loader) as _,
                "my_cache",
                &metric_registry,
                Arc::clone(&notify_idle),
            )
            .await,
        );
        backend.add_policy(TtlPolicy::new(
            Arc::clone(&ttl_provider) as _,
            Arc::clone(&time_provider) as _,
            "my_cache",
            &metric_registry,
        ));

        Self {
            backend,
            refresh_duration_provider,
            ttl_provider,
            time_provider,
            loader,
            notify_idle,
        }
    }
}
