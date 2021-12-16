//! This module contains the IOx implementation for wrapping existing object store types into an artificial "sleep" wrapper.
use std::{convert::TryInto, sync::Mutex};

use crate::{GetResult, ListResult, ObjectStoreApi, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use tokio::time::{sleep, Duration};

/// Configuration settings for throttled store
#[derive(Debug, Default, Clone, Copy)]
pub struct ThrottleConfig {
    /// Sleep duration for every call to [`delete`](ThrottledStore::delete).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation.
    pub wait_delete_per_call: Duration,

    /// Sleep duration for every byte received during [`get`](ThrottledStore::get).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful gets. The
    /// sleep duration is additive to [`wait_get_per_call`](Self::wait_get_per_call).
    ///
    /// Note that the per-byte sleep only happens as the user consumes the output bytes. Should
    /// there be an intermediate failure (i.e. after partly consuming the output bytes), the
    /// resulting sleep time will be partial as well.
    pub wait_get_per_byte: Duration,

    /// Sleep duration for every call to [`get`](ThrottledStore::get).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_get_per_byte`](Self::wait_get_per_byte).
    pub wait_get_per_call: Duration,

    /// Sleep duration for every call to [`list`](ThrottledStore::list).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_list_per_entry`](Self::wait_list_per_entry).
    pub wait_list_per_call: Duration,

    /// Sleep duration for every entry received during [`list`](ThrottledStore::list).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful lists.
    /// The sleep duration is additive to [`wait_list_per_call`](Self::wait_list_per_call).
    ///
    /// Note that the per-entry sleep only happens as the user consumes the output entries. Should
    /// there be an intermediate failure (i.e. after partly consuming the output entries), the
    /// resulting sleep time will be partial as well.
    pub wait_list_per_entry: Duration,

    /// Sleep duration for every call to
    /// [`list_with_delimiter`](ThrottledStore::list_with_delimiter).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_list_with_delimiter_per_entry`](Self::wait_list_with_delimiter_per_entry).
    pub wait_list_with_delimiter_per_call: Duration,

    /// Sleep duration for every entry received during
    /// [`list_with_delimiter`](ThrottledStore::list_with_delimiter).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful gets. The
    /// sleep duration is additive to
    /// [`wait_list_with_delimiter_per_call`](Self::wait_list_with_delimiter_per_call).
    pub wait_list_with_delimiter_per_entry: Duration,

    /// Sleep duration for every call to [`put`](ThrottledStore::put).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation.
    pub wait_put_per_call: Duration,
}

/// Store wrapper that wraps an inner store with some `sleep` calls.
///
/// This can be used for performance testing.
///
/// **Note that the behavior of the wrapper is deterministic and might not reflect real-world
/// conditions!**
#[derive(Debug)]
pub struct ThrottledStore<T: ObjectStoreApi> {
    inner: T,
    config: Mutex<ThrottleConfig>,
}

impl<T: ObjectStoreApi> ThrottledStore<T> {
    /// Create new wrapper with zero waiting times.
    pub fn new(inner: T, config: ThrottleConfig) -> Self {
        Self {
            inner,
            config: Mutex::new(config),
        }
    }

    /// Mutate config.
    pub fn config_mut<F>(&self, f: F)
    where
        F: Fn(&mut ThrottleConfig),
    {
        let mut guard = self.config.lock().expect("lock poissened");
        f(&mut guard)
    }

    /// Return copy of current config.
    pub fn config(&self) -> ThrottleConfig {
        *self.config.lock().expect("lock poissened")
    }
}

#[async_trait]
impl<T: ObjectStoreApi> ObjectStoreApi for ThrottledStore<T> {
    type Path = T::Path;

    type Error = T::Error;

    fn new_path(&self) -> Self::Path {
        self.inner.new_path()
    }

    fn path_from_raw(&self, raw: &str) -> Self::Path {
        self.inner.path_from_raw(raw)
    }

    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<(), Self::Error> {
        sleep(self.config().wait_put_per_call).await;

        self.inner.put(location, bytes).await
    }

    async fn get(&self, location: &Self::Path) -> Result<GetResult<Self::Error>, Self::Error> {
        sleep(self.config().wait_get_per_call).await;

        // need to copy to avoid moving / referencing `self`
        let wait_get_per_byte = self.config().wait_get_per_byte;

        self.inner.get(location).await.map(|result| {
            let s = match result {
                GetResult::Stream(s) => s,
                GetResult::File(_, _) => unimplemented!(),
            };

            GetResult::Stream(
                s.then(move |bytes_result| async move {
                    match bytes_result {
                        Ok(bytes) => {
                            let bytes_len: u32 = usize_to_u32_saturate(bytes.len());
                            sleep(wait_get_per_byte * bytes_len).await;
                            Ok(bytes)
                        }
                        Err(err) => Err(err),
                    }
                })
                .boxed(),
            )
        })
    }

    async fn delete(&self, location: &Self::Path) -> Result<(), Self::Error> {
        sleep(self.config().wait_delete_per_call).await;

        self.inner.delete(location).await
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>, Self::Error>>, Self::Error> {
        sleep(self.config().wait_list_per_call).await;

        // need to copy to avoid moving / referencing `self`
        let wait_list_per_entry = self.config().wait_list_per_entry;

        self.inner.list(prefix).await.map(|stream| {
            stream
                .then(move |entries_result| async move {
                    match entries_result {
                        Ok(entries) => {
                            let entries_len = usize_to_u32_saturate(entries.len());
                            sleep(wait_list_per_entry * entries_len).await;
                            Ok(entries)
                        }
                        Err(err) => Err(err),
                    }
                })
                .boxed()
        })
    }

    async fn list_with_delimiter(
        &self,
        prefix: &Self::Path,
    ) -> Result<ListResult<Self::Path>, Self::Error> {
        sleep(self.config().wait_list_with_delimiter_per_call).await;

        match self.inner.list_with_delimiter(prefix).await {
            Ok(list_result) => {
                let entries_len = usize_to_u32_saturate(list_result.objects.len());
                sleep(self.config().wait_list_with_delimiter_per_entry * entries_len).await;
                Ok(list_result)
            }
            Err(err) => Err(err),
        }
    }
}

/// Saturated `usize` to `u32` cast.
fn usize_to_u32_saturate(x: usize) -> u32 {
    x.try_into().unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::InMemory,
        path::ObjectStorePath,
        tests::{list_uses_directories_correctly, list_with_delimiter, put_get_delete_list},
        ObjectStore,
    };
    use bytes::Bytes;
    use futures::TryStreamExt;
    use tokio::time::Duration;
    use tokio::time::Instant;

    const WAIT_TIME: Duration = Duration::from_millis(100);
    const ZERO: Duration = Duration::from_millis(0); // Duration::default isn't constant

    macro_rules! assert_bounds {
        ($d:expr, $lower:expr) => {
            assert_bounds!($d, $lower, $lower + 1);
        };
        ($d:expr, $lower:expr, $upper:expr) => {
            let d = $d;
            let lower = $lower * WAIT_TIME;
            let upper = $upper * WAIT_TIME;
            assert!(d >= lower, "{:?} must be >= than {:?}", d, lower);
            assert!(d < upper, "{:?} must be < than {:?}", d, upper);
        };
    }

    #[tokio::test]
    async fn throttle_test() {
        let config = ThrottleConfig::default();
        let integration = ObjectStore::new_in_memory_throttled(config);

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn delete_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_delete(&store, None).await, 0);
        assert_bounds!(measure_delete(&store, Some(0)).await, 0);
        assert_bounds!(measure_delete(&store, Some(10)).await, 0);

        store.config_mut(|cfg| cfg.wait_delete_per_call = WAIT_TIME);
        assert_bounds!(measure_delete(&store, None).await, 1);
        assert_bounds!(measure_delete(&store, Some(0)).await, 1);
        assert_bounds!(measure_delete(&store, Some(10)).await, 1);
    }

    #[tokio::test]
    async fn get_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_get(&store, None).await, 0);
        assert_bounds!(measure_get(&store, Some(0)).await, 0);
        assert_bounds!(measure_get(&store, Some(10)).await, 0);

        store.config_mut(|cfg| cfg.wait_get_per_call = WAIT_TIME);
        assert_bounds!(measure_get(&store, None).await, 1);
        assert_bounds!(measure_get(&store, Some(0)).await, 1);
        assert_bounds!(measure_get(&store, Some(10)).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_get_per_call = ZERO;
            cfg.wait_get_per_byte = WAIT_TIME;
        });
        assert_bounds!(measure_get(&store, Some(2)).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_get_per_call = WAIT_TIME;
            cfg.wait_get_per_byte = WAIT_TIME;
        });
        assert_bounds!(measure_get(&store, Some(2)).await, 3);
    }

    #[tokio::test]
    async fn list_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_list(&store, 0).await, 0);
        assert_bounds!(measure_list(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_list_per_call = WAIT_TIME);
        assert_bounds!(measure_list(&store, 0).await, 1);
        assert_bounds!(measure_list(&store, 10).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_list_per_call = ZERO;
            cfg.wait_list_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list(&store, 2).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_list_per_call = WAIT_TIME;
            cfg.wait_list_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list(&store, 2).await, 3);
    }

    #[tokio::test]
    async fn list_with_delimiter_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_list_with_delimiter(&store, 0).await, 0);
        assert_bounds!(measure_list_with_delimiter(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_list_with_delimiter_per_call = WAIT_TIME);
        assert_bounds!(measure_list_with_delimiter(&store, 0).await, 1);
        assert_bounds!(measure_list_with_delimiter(&store, 10).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_list_with_delimiter_per_call = ZERO;
            cfg.wait_list_with_delimiter_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list_with_delimiter(&store, 2).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_list_with_delimiter_per_call = WAIT_TIME;
            cfg.wait_list_with_delimiter_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list_with_delimiter(&store, 2).await, 3);
    }

    #[tokio::test]
    async fn put_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_put(&store, 0).await, 0);
        assert_bounds!(measure_put(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_put_per_call = WAIT_TIME);
        assert_bounds!(measure_put(&store, 0).await, 1);
        assert_bounds!(measure_put(&store, 10).await, 1);

        store.config_mut(|cfg| cfg.wait_put_per_call = ZERO);
        assert_bounds!(measure_put(&store, 0).await, 0);
    }

    async fn place_test_object(
        store: &ThrottledStore<InMemory>,
        n_bytes: Option<usize>,
    ) -> <ThrottledStore<InMemory> as ObjectStoreApi>::Path {
        let mut path = store.new_path();
        path.set_file_name("foo");

        if let Some(n_bytes) = n_bytes {
            let data: Vec<_> = std::iter::repeat(1u8).take(n_bytes).collect();
            let bytes = Bytes::from(data);
            store.put(&path, bytes).await.unwrap();
        } else {
            // ensure object is absent
            store.delete(&path).await.unwrap();
        }

        path
    }

    async fn place_test_objects(
        store: &ThrottledStore<InMemory>,
        n_entries: usize,
    ) -> <ThrottledStore<InMemory> as ObjectStoreApi>::Path {
        let mut prefix = store.new_path();
        prefix.push_dir("foo");

        // clean up store
        for entry in store
            .list(Some(&prefix))
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap()
        {
            store.delete(&entry).await.unwrap();
        }

        // create new entries
        for i in 0..n_entries {
            let mut path = prefix.clone();
            path.set_file_name(&i.to_string());

            let data = Bytes::from("bar");
            store.put(&path, data).await.unwrap();
        }

        prefix
    }

    async fn measure_delete(store: &ThrottledStore<InMemory>, n_bytes: Option<usize>) -> Duration {
        let path = place_test_object(store, n_bytes).await;

        let t0 = Instant::now();
        store.delete(&path).await.unwrap();

        t0.elapsed()
    }

    async fn measure_get(store: &ThrottledStore<InMemory>, n_bytes: Option<usize>) -> Duration {
        let path = place_test_object(store, n_bytes).await;

        let t0 = Instant::now();
        let res = store.get(&path).await;
        if n_bytes.is_some() {
            // need to consume bytes to provoke sleep times
            let s = match res.unwrap() {
                GetResult::Stream(s) => s,
                GetResult::File(_, _) => unimplemented!(),
            };

            s.map_ok(|b| bytes::BytesMut::from(&b[..]))
                .try_concat()
                .await
                .unwrap();
        } else {
            assert!(res.is_err());
        }

        t0.elapsed()
    }

    async fn measure_list(store: &ThrottledStore<InMemory>, n_entries: usize) -> Duration {
        let prefix = place_test_objects(store, n_entries).await;

        let t0 = Instant::now();
        store
            .list(Some(&prefix))
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();

        t0.elapsed()
    }

    async fn measure_list_with_delimiter(
        store: &ThrottledStore<InMemory>,
        n_entries: usize,
    ) -> Duration {
        let prefix = place_test_objects(store, n_entries).await;

        let t0 = Instant::now();
        store.list_with_delimiter(&prefix).await.unwrap();

        t0.elapsed()
    }

    async fn measure_put(store: &ThrottledStore<InMemory>, n_bytes: usize) -> Duration {
        let mut path = store.new_path();
        path.set_file_name("foo");

        let data: Vec<_> = std::iter::repeat(1u8).take(n_bytes).collect();
        let bytes = Bytes::from(data);

        let t0 = Instant::now();
        store.put(&path, bytes).await.unwrap();

        t0.elapsed()
    }
}
