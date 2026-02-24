//! Client for performing quorum catalog reads/writes

use crate::api::client::{CatalogCacheClient, Error as ClientError};
use crate::local::CatalogCache;
use crate::{CacheKey, CacheValue};
use futures::channel::oneshot;
use futures::future::{Either, select};
use futures::{StreamExt, pin_mut};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

fn display_remote_generations(g: &[RemoteGeneration; 2]) -> impl std::fmt::Display {
    let mut out = String::new();

    out.push('[');

    for (idx, (url, res)) in g.iter().enumerate() {
        if idx > 0 {
            write!(&mut out, ", ").unwrap();
        }

        write!(&mut out, "'{url}'=>").unwrap();

        match res.as_ref() {
            Ok(Some(generation)) => {
                write!(&mut out, "Ok({generation})").unwrap();
            }
            Ok(None) => {
                write!(&mut out, "Ok(MISSING)").unwrap();
            }
            Err(e) => {
                write!(&mut out, "Err({e})").unwrap();
            }
        }
    }

    out.push(']');

    out
}

/// Generation that we got from a remote endpoint.
///
/// Used for [error reporting](Error).
pub type RemoteGeneration = (Url, Result<Option<u64>, ClientError>);

/// Error for [`QuorumCatalogCache`]
#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to communicate with any remote replica: {source}"))]
    NoRemote { source: ClientError },

    #[snafu(display("Write task was aborted"))]
    Cancelled,

    #[snafu(display("Join Error: {source}"))]
    Join { source: JoinError },

    #[snafu(display(
        "Failed to establish a read quorum: local={}, remote={}",
        local_generation.map(|g| g.to_string()).unwrap_or_else(|| "MISSING".to_owned()),
        display_remote_generations(remote_generations),
    ))]
    Quorum {
        local_generation: Option<u64>,
        remote_generations: Box<[RemoteGeneration; 2]>,
    },

    #[snafu(display("Failed to list replica: {source}"))]
    List { source: crate::api::list::Error },

    #[snafu(display("Local cache error: {source}"), context(false))]
    Local { source: crate::local::Error },
}

/// Result for [`QuorumCatalogCache`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Performs quorum reads and writes across a local [`CatalogCache`] and two [`CatalogCacheClient`]
#[derive(Debug)]
pub struct QuorumCatalogCache {
    local: Arc<CatalogCache>,
    replicas: Arc<[CatalogCacheClient; 2]>,
    shutdown: CancellationToken,
}

impl Drop for QuorumCatalogCache {
    fn drop(&mut self) {
        self.shutdown.cancel()
    }
}

impl QuorumCatalogCache {
    /// Create a new [`QuorumCatalogCache`]
    pub fn new(local: Arc<CatalogCache>, replicas: Arc<[CatalogCacheClient; 2]>) -> Self {
        Self {
            local,
            replicas,
            shutdown: CancellationToken::new(),
        }
    }

    /// Retrieve the given value from the remote cache
    ///
    /// Returns `None` if value is not present in a quorum of replicas
    /// Returns [`Error::Quorum`] if cannot establish a read quorum
    pub async fn get(&self, key: CacheKey) -> Result<Option<CacheValue>> {
        let local = self.local.get(key);

        let local_generation = local.as_ref().map(|x| x.generation);
        let local_etag = local.as_ref().and_then(|x| x.etag());
        let fut1 = self.replicas[0].get_if_modified(key, local_generation, local_etag.cloned());
        let fut2 = self.replicas[1].get_if_modified(key, local_generation, local_etag.cloned());
        pin_mut!(fut1);
        pin_mut!(fut2);

        match select(fut1, fut2).await {
            Either::Left((result, fut)) | Either::Right((result, fut)) => match (local, result) {
                (None, Ok(None)) => Ok(None),
                (Some(l), Err(ClientError::NotModified)) => Ok(Some(l)),
                (Some(l), Ok(Some(r))) if l.generation <= r.generation => {
                    // preempt write from remote to local that arrives late
                    if l.generation < r.generation {
                        self.local.insert(key, r.clone())?;
                    }
                    Ok(Some(r))
                }
                (local, r1) => {
                    // r1 either failed or did not return anything
                    let r2 = fut.await;
                    match (local, r1, r2) {
                        (Some(l), _, Err(ClientError::NotModified)) => Ok(Some(l)),
                        (None, _, Ok(None)) | (_, Ok(None), Ok(None)) => Ok(None),
                        (Some(l), _, Ok(Some(r))) if l.generation <= r.generation => {
                            // preempt write from remote to local that arrives late
                            if l.generation < r.generation {
                                self.local.insert(key, r.clone())?;
                            }
                            Ok(Some(r))
                        }
                        (local, Ok(Some(l)), Ok(Some(r))) if l.generation == r.generation => {
                            if local.map(|x| x.generation < l.generation).unwrap_or(true) {
                                self.local.insert(key, l.clone())?;
                            }
                            Ok(Some(l))
                        }
                        (l, r1, r2) => Err(Error::Quorum {
                            local_generation: l.map(|x| x.generation()),
                            remote_generations: Box::new([
                                (
                                    self.replicas[0].endpoint().clone(),
                                    r1.map(|x| x.map(|x| x.generation)),
                                ),
                                (
                                    self.replicas[1].endpoint().clone(),
                                    r2.map(|x| x.map(|x| x.generation)),
                                ),
                            ]),
                        }),
                    }
                }
            },
        }
    }

    /// Upsert the given key-value pair
    ///
    /// Returns Ok if able to replicate the write to a quorum
    pub async fn put(&self, key: CacheKey, value: CacheValue) -> Result<()> {
        self.local.insert(key, value.clone())?;

        let replicas = Arc::clone(&self.replicas);
        let (sender, receiver) = oneshot::channel();

        let fut = async move {
            let fut1 = replicas[0].put(key, &value);
            let fut2 = replicas[1].put(key, &value);
            pin_mut!(fut1);
            pin_mut!(fut2);

            match select(fut1, fut2).await {
                Either::Left((r, fut)) | Either::Right((r, fut)) => {
                    let _ = sender.send(r);
                    fut.await
                }
            }
        };

        // We spawn a tokio task so that we can potentially continue to replicate
        // to the second replica asynchronously once we receive an ok response
        let cancel = self.shutdown.child_token();
        let handle = tokio::spawn(async move {
            let cancelled = cancel.cancelled();
            pin_mut!(fut);
            pin_mut!(cancelled);
            match select(cancelled, fut).await {
                Either::Left(_) => Err(Error::Cancelled),
                Either::Right((Ok(_), _)) => Ok(()),
                Either::Right((Err(source), _)) => Err(Error::NoRemote { source }),
            }
        });

        match receiver.await {
            Ok(Ok(_)) => Ok(()),
            _ => match handle.await {
                Ok(r) => r,
                Err(source) => Err(Error::Join { source }),
            },
        }
    }

    /// Warm the local cache by performing quorum reads from the other two replicas.
    ///
    /// From the first replica we will only fetch the version. From the second one we will fetch the actual payload. The
    /// payload size is limited by the given `max_value_size`. If [`None`] is given, this will default to [`MAX_VALUE_SIZE`].
    ///
    /// This method should be called after this server has been participating in the write quorum
    /// for a period of time, e.g. 1 minute. This avoids an issue where a quorum cannot be
    /// established for in-progress writes.
    ///
    ///
    /// [`MAX_VALUE_SIZE`]: crate::api::list::MAX_VALUE_SIZE
    pub async fn warm(&self, max_value_size: Option<usize>) -> Result<WarmupStats> {
        // List doesn't return keys in any particular order
        //
        // We therefore build a hashmap with the keys from one replica and compare
        // this against those returned by the other
        //
        // We don't need to consult the local `CatalogCache`, as we only need to insert
        // if a read quorum can be established between the replicas and isn't present locally
        let mut generations = HashMap::with_capacity(128);
        let mut list = self.replicas[0].list(Some(0));
        while let Some(entry) = list.next().await.transpose().context(ListSnafu)? {
            if let Some(k) = entry.key() {
                generations.insert(k, entry);
            }
        }

        info!(
            count = generations.len(),
            "Collected version information from first replica"
        );

        let mut second_replicate_elements_with_payload = 0;
        let mut second_replicate_elements_without_payload = 0;
        let mut second_replicate_bytes = 0;
        let mut inserted_elements = 0;
        let mut inserted_bytes = 0;

        let mut list = self.replicas[1].list(max_value_size);
        while let Some(entry) = list.next().await.transpose().context(ListSnafu)? {
            let Some(k) = entry.key() else {
                continue;
            };

            let v = match entry.value() {
                Some(v) => {
                    second_replicate_elements_with_payload += 1;
                    second_replicate_bytes += v.len();
                    v
                }
                None => {
                    second_replicate_elements_without_payload += 1;
                    continue;
                }
            };

            let Some(first) = generations.get(&k) else {
                continue;
            };

            if first.generation() == entry.generation()
                || matches!((first.etag(), entry.etag()), (Some(a), Some(b)) if a == b)
            {
                inserted_elements += 1;
                inserted_bytes += v.len();

                let value = CacheValue::new(v.clone(), first.generation())
                    .with_etag_opt(entry.etag().cloned());
                // In the case that local already has the given version
                // this will be a no-op
                self.local.insert(k, value)?;
            }
        }

        info!(
            count = inserted_elements,
            total_bytes = inserted_elements,
            "Finished warmup"
        );
        Ok(WarmupStats {
            first_replicate_elements: generations.len(),
            second_replicate_elements_with_payload,
            second_replicate_elements_without_payload,
            second_replicate_bytes,
            inserted_elements,
            inserted_bytes,
        })
    }

    /// Returns a reference to the local [`CatalogCache`]
    pub fn local(&self) -> &Arc<CatalogCache> {
        &self.local
    }

    /// Returns a reference to the peers of this [`QuorumCatalogCache`]
    pub fn peers(&self) -> &[CatalogCacheClient; 2] {
        &self.replicas
    }
}

/// Statistics for [warm-up](QuorumCatalogCache::warm)
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(missing_copy_implementations)] // allow extensions
pub struct WarmupStats {
    /// Number elements pulled from the first replica.
    ///
    /// These elements only provide a version, NOT any form of payload.
    pub first_replicate_elements: usize,

    /// Number of elements with payload that we got from the second replica.
    pub second_replicate_elements_with_payload: usize,

    /// Number of elements without payload that we got from the second replica.
    ///
    /// Elements in this category did not get any payload due to the provided `max_value_size`.
    pub second_replicate_elements_without_payload: usize,

    /// Number of payload bytes transferred from the second replica.
    pub second_replicate_bytes: usize,

    /// Inserted elements.
    ///
    /// For these, both replicas agreed on a version and we also got payload data.
    pub inserted_elements: usize,

    /// Inserted bytes.
    ///
    /// For these, both replicas agreed on a version and we also got payload data.
    pub inserted_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::client::Error as ClientError;
    use crate::api::server::test_util::TestCacheServer;
    use std::future::Future;
    use std::task::Context;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic() {
        let local = Arc::new(CatalogCache::default());
        let metric_registry = Arc::new(metric::Registry::new());
        let r1 = TestCacheServer::bind_ephemeral(&metric_registry).await;
        let r2 = TestCacheServer::bind_ephemeral(&metric_registry).await;

        let replicas = Arc::new([r1.client(), r2.client()]);
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        assert_eq!(quorum.get(CacheKey::Table(1)).await.unwrap(), None);

        let k1 = CacheKey::Table(1);
        let k2 = CacheKey::Table(2);
        let k3 = CacheKey::Table(3);

        let v1 = CacheValue::new("foo".into(), 2);
        quorum.put(k1, v1.clone()).await.unwrap();
        quorum.put(k2, v1.clone()).await.unwrap();

        let r = quorum.get(k2).await.unwrap().unwrap();
        assert_eq!(r, v1);

        // New value
        let v2 = CacheValue::new("foo".into(), 4);
        quorum.put(k2, v2.clone()).await.unwrap();

        let r = quorum.get(k1).await.unwrap().unwrap();
        assert_eq!(r, v1);

        let r = quorum.get(k2).await.unwrap().unwrap();
        assert_eq!(r, v2);

        // Both replicas should have a value for k2 before proceeding
        let mut attempts = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if r1.cache().get(k2).is_some() && r2.cache().get(k2).is_some() {
                break;
            } else {
                assert!(attempts < 100);
                attempts += 1;
            }
        }

        // Can remove value from one replica and still get quorum
        r2.cache().delete(k2).unwrap();
        let r = quorum.get(k2).await.unwrap().unwrap();
        assert_eq!(r, v2);

        // Loss of two copies results in not found
        r1.cache().delete(k2).unwrap();
        let r = quorum.get(k2).await.unwrap();
        assert_eq!(r, None);

        // Simulate stale value in r1
        r1.cache().insert(k2, v1.clone()).unwrap();
        let err = quorum.get(k2).await.unwrap_err();
        assert!(matches!(err, Error::Quorum { .. }), "{err}");

        // If quorum has stale value follows quorum
        r2.cache().delete(k2);
        r2.cache().insert(k2, v1.clone()).unwrap();
        let r = quorum.get(k2).await.unwrap().unwrap();
        assert_eq!(r, v1);

        // Simulate loss of replica 2
        r2.shutdown().await;

        // Can still establish a write quorum
        quorum.put(k3, v1.clone()).await.unwrap();

        // Can read newly inserted value
        let r = quorum.get(k3).await.unwrap().unwrap();
        assert_eq!(r, v1);

        // Can still read from quorum of k1
        let r = quorum.get(k1).await.unwrap().unwrap();
        assert_eq!(r, v1);

        // Cannot get quorum as lost single node and local disagrees with replica 1
        let err = quorum.get(k2).await.unwrap_err();
        assert!(matches!(err, Error::Quorum { .. }), "{err}");

        // Can establish quorum following write
        quorum.put(k2, v2.clone()).await.unwrap();
        let r = quorum.get(k2).await.unwrap().unwrap();
        assert_eq!(r, v2);

        // Still cannot establish quorum
        r1.cache().delete(k2);
        let err = quorum.get(k2).await.unwrap_err();
        assert!(matches!(err, Error::Quorum { .. }), "{err}");

        // k2 is now no longer present anywhere, can establish quorum
        local.delete(k2);
        let r = quorum.get(k2).await.unwrap();
        assert_eq!(r, None);

        // Simulate loss of replica 1 (in addition to replica 2)
        r1.shutdown().await;

        // Can no longer get quorum for anything
        let err = quorum.get(k1).await.unwrap_err();
        assert!(matches!(err, Error::Quorum { .. }), "{err}");
    }

    #[tokio::test]
    async fn test_read_through() {
        let metric_registry = Arc::new(metric::Registry::new());
        let local = Arc::new(CatalogCache::default());
        let r1 = TestCacheServer::bind_ephemeral(&metric_registry).await;
        let r2 = TestCacheServer::bind_ephemeral(&metric_registry).await;

        let replicas = Arc::new([r1.client(), r2.client()]);
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        let key = CacheKey::Table(1);
        let v0 = CacheValue::new("v0".into(), 0);

        r1.cache().insert(key, v0.clone()).unwrap();
        r2.cache().insert(key, v0.clone()).unwrap();

        let result = quorum.get(key).await.unwrap().unwrap();
        assert_eq!(result, v0);

        // Should have read-through to local
        assert_eq!(local.get(key).unwrap(), v0);

        let v1 = CacheValue::new("v1".into(), 1);
        let v2 = CacheValue::new("v2".into(), 2);

        r1.cache().insert(key, v1.clone()).unwrap();
        r2.cache().insert(key, v2.clone()).unwrap();

        // A quorum request will get either v1 or v2 depending on which it contacts first
        let result = quorum.get(key).await.unwrap().unwrap();
        assert!(result == v1 || result == v2, "{result:?}");

        // Should read-through
        assert_eq!(local.get(key).unwrap(), result);

        // Update r1 with version 2
        r1.cache().insert(key, v2.clone()).unwrap();

        let result = quorum.get(key).await.unwrap().unwrap();
        assert_eq!(result, v2);

        // Should read-through
        assert_eq!(local.get(key).unwrap(), v2);

        let v3 = CacheValue::new("v3".into(), 3);
        local.insert(key, v3.clone()).unwrap();

        // Should establish quorum for v2 even though local is v3
        let result = quorum.get(key).await.unwrap().unwrap();
        assert_eq!(result, v2);

        // Should not read-through
        assert_eq!(local.get(key).unwrap(), v3);

        let v4 = CacheValue::new("v4".into(), 4);
        let v5 = CacheValue::new("v5".into(), 5);

        local.insert(key, v5.clone()).unwrap();
        r1.cache().insert(key, v4.clone()).unwrap();

        // Should fail as cannot establish quorum of three different versions of `[5, 4, 2]`
        // and has latest version locally
        let err = quorum.get(key).await.unwrap_err();
        assert!(matches!(err, Error::Quorum { .. }), "{err}");
        assert_eq!(local.get(key).unwrap(), v5);

        let v6 = CacheValue::new("v6".into(), 6);
        r1.cache().insert(key, v6.clone()).unwrap();

        // Should succeed as r1 has newer version than local
        let result = quorum.get(key).await.unwrap().unwrap();
        assert_eq!(result, v6);

        // Should read-through
        assert_eq!(local.get(key).unwrap(), v6);
    }

    #[tokio::test]
    async fn test_warm() {
        let metric_registry = Arc::new(metric::Registry::new());
        let local = Arc::new(CatalogCache::default());
        let r1 = TestCacheServer::bind_ephemeral(&metric_registry).await;
        let r2 = TestCacheServer::bind_ephemeral(&metric_registry).await;

        let replicas = Arc::new([r1.client(), r2.client()]);
        let quorum = QuorumCatalogCache::new(local, Arc::clone(&replicas));

        let k1 = CacheKey::Table(1);
        let v1 = CacheValue::new("v1".into(), 1).with_etag("etag1");
        quorum.put(k1, v1.clone()).await.unwrap();

        let k2 = CacheKey::Table(2);
        let v2 = CacheValue::new("v2".into(), 1).with_etag("etag2");
        quorum.put(k2, v2.clone()).await.unwrap();

        // Simulate local restart
        let local = Arc::new(CatalogCache::default());
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        assert_eq!(local.list().count(), 0);

        assert_eq!(
            quorum.warm(None).await.unwrap(),
            WarmupStats {
                first_replicate_elements: 2,
                second_replicate_elements_with_payload: 2,
                second_replicate_elements_without_payload: 0,
                second_replicate_bytes: 4,
                inserted_elements: 2,
                inserted_bytes: 4,
            },
        );

        // Should populate both entries
        let mut entries: Vec<_> = local.list().collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        assert_eq!(entries, vec![(k1, v1.clone()), (k2, v2.clone())]);

        // Simulate local restart
        let local = Arc::new(CatalogCache::default());
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        // Simulate in-progress write
        let v3 = CacheValue::new("v3".into(), 2).with_etag("etag3");
        assert!(r1.cache().insert(k2, v3.clone()).unwrap());

        // Cannot establish quorum for k1 so should skip over
        assert_eq!(
            quorum.warm(None).await.unwrap(),
            WarmupStats {
                first_replicate_elements: 2,
                second_replicate_elements_with_payload: 2,
                second_replicate_elements_without_payload: 0,
                second_replicate_bytes: 4,
                inserted_elements: 1,
                inserted_bytes: 2,
            },
        );
        let entries: Vec<_> = local.list().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (k1, v1.clone()));

        // If r2 updated warming should pick up new quorum
        assert!(r2.cache().insert(k2, v3.clone()).unwrap());
        assert_eq!(
            quorum.warm(None).await.unwrap(),
            WarmupStats {
                first_replicate_elements: 2,
                second_replicate_elements_with_payload: 2,
                second_replicate_elements_without_payload: 0,
                second_replicate_bytes: 4,
                inserted_elements: 2,
                inserted_bytes: 4,
            },
        );
        let mut entries: Vec<_> = local.list().collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        assert_eq!(entries, vec![(k1, v1.clone()), (k2, v3)]);

        // Simulate local restart
        let local = Arc::new(CatalogCache::default());
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        // Simulate in-progress write that increments generation but doesn't update payload
        let v3 = CacheValue::new("v3".into(), 3).with_etag("etag3");
        assert!(r1.cache().insert(k2, v3.clone()).unwrap());

        // Can establish quorum for k1 despite generation mismatch because etag matches
        assert_eq!(
            quorum.warm(None).await.unwrap(),
            WarmupStats {
                first_replicate_elements: 2,
                second_replicate_elements_with_payload: 2,
                second_replicate_elements_without_payload: 0,
                second_replicate_bytes: 4,
                inserted_elements: 2,
                inserted_bytes: 4,
            },
        );
        let mut entries: Vec<_> = local.list().collect();
        entries.sort_unstable_by_key(|(k, _)| *k);
        assert_eq!(entries, vec![(k1, v1.clone()), (k2, v3)]);

        // Test cancellation safety
        let k3 = CacheKey::Table(3);
        let fut = quorum.put(k3, v2.clone());
        {
            // `fut` is dropped (cancelled) on exit from this code block
            pin_mut!(fut);

            let noop_waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&noop_waker);
            assert!(fut.poll(&mut cx).is_pending());
        }

        // Write should still propagate asynchronously
        let mut attempts = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            match quorum.get(k3).await {
                Ok(Some(_)) => break,
                _ => {
                    assert!(attempts < 100);
                    attempts += 1;
                }
            }
        }

        // test max size
        let local = Arc::new(CatalogCache::default());
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));
        assert_eq!(local.list().count(), 0);
        assert_eq!(
            quorum.warm(Some(0)).await.unwrap(),
            WarmupStats {
                first_replicate_elements: 3,
                second_replicate_elements_with_payload: 0,
                second_replicate_elements_without_payload: 3,
                second_replicate_bytes: 0,
                inserted_elements: 0,
                inserted_bytes: 0,
            },
        );
        assert_eq!(local.list().count(), 0);
    }

    #[tokio::test]
    async fn test_network_timeout() {
        let local = Arc::new(CatalogCache::default());
        let metric_registry = Arc::new(metric::Registry::new());

        // Create a client with an invalid address that will cause timeouts
        // Instead of using a real TestCacheServer
        let invalid_client1 = CatalogCacheClient::builder(
            "http://non-exist-iox-shared-catalog-1:9090"
                .parse()
                .unwrap(),
            Arc::clone(&metric_registry),
        )
        .build()
        .expect("Failed to create client with invalid address");
        let invalid_client2 = CatalogCacheClient::builder(
            "http://non-exist-iox-shared-catalog-2:9090"
                .parse()
                .unwrap(),
            Arc::clone(&metric_registry),
        )
        .build()
        .expect("Failed to create client with invalid address");

        let replicas = Arc::new([invalid_client1, invalid_client2]);
        let quorum = QuorumCatalogCache::new(Arc::clone(&local), Arc::clone(&replicas));

        // Attempt to put a value should result in NoRemote error due to timeout
        let key = CacheKey::Root;
        let value = CacheValue::new("test_namespace".into(), 1);
        let err = tokio::time::timeout(
            // Mimic a timeout scenario
            std::time::Duration::from_secs(2), // the default client timeout limit is 1s
            quorum.put(key, value),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(
            matches!(&err, Error::NoRemote { source: ClientError::Put { source } } if source.is_timeout() || source.is_connect()),
            "Expected NoRemote error with timeout or connection issue, got: {err:?}"
        );
        assert!(
            format!("{:?}", err.to_string()).contains("error sending request"),
            "Unexpected error message: Failed to communicate with any remote replica: Put Reqwest error: {err:?}"
        );

        // Get should also fail with a Quorum error
        let err = quorum.get(key).await.unwrap_err();
        assert!(
            matches!(err, Error::Quorum { .. }),
            "Expected Quorum error, got: {err}"
        );
    }

    #[test]
    fn quorum_err_display() {
        assert_eq!(
            Error::Quorum {
                local_generation: None,
                remote_generations: Box::new([
                    ("http://foo".parse().unwrap(), Ok(None)),
                    ("http://bar".parse().unwrap(), Ok(None)),
                ]),
            }
            .to_string(),
            "Failed to establish a read quorum: local=MISSING, remote=['http://foo/'=>Ok(MISSING), 'http://bar/'=>Ok(MISSING)]",
        );
        assert_eq!(
            Error::Quorum {
                local_generation: Some(1),
                remote_generations: Box::new([
                    ("http://foo".parse().unwrap(), Ok(Some(1))),
                    (
                        "http://bar".parse().unwrap(),
                        Err(ClientError::MissingGeneration)
                    ),
                ]),
            }
            .to_string(),
            "Failed to establish a read quorum: local=1, remote=['http://foo/'=>Ok(1), 'http://bar/'=>Err(Missing generation header)]",
        );
    }
}
