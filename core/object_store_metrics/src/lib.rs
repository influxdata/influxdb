//! A metric instrumentation wrapper over [`ObjectStore`] implementations.

use log::LogContext;
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{borrow::Cow, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iox_time::TimeProvider;

use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};

use crate::{
    metrics::{
        Metrics, MetricsRecorder, MetricsWithBytes, MetricsWithBytesAndTtfb,
        MetricsWithBytesAndTtfbRecorder, MetricsWithBytesRecorder, MetricsWithCount,
        MetricsWithCountRecorder, OpResult,
    },
    multipart_upload::MultipartUploadWrapper,
    stream::{BytesStreamDelegate, CountStreamDelegate, StreamMetricRecorder},
};

mod cache_metrics;
pub use cache_metrics::{FilterArgs, ObjectStoreCacheMetrics};
pub mod cache_state;
mod log;
mod metrics;
mod multipart_upload;
mod stream;
#[cfg(test)]
mod test_utils;

/// A typed name of a scope / type to report the metrics under.
#[derive(Debug, Clone)]
pub struct StoreType(Cow<'static, str>);

impl<T> From<T> for StoreType
where
    T: Into<Cow<'static, str>>,
{
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

/// An instrumentation decorator, wrapping an underlying [`ObjectStore`]
/// implementation and recording bytes transferred and call latency.
///
/// # Stream Duration
///
/// The [`ObjectStore::get()`] call can return a [`Stream`] which is polled
/// by the caller and may yield chunks of a file over a series of polls (as
/// opposed to all of the file data in one go). Because the caller drives the
/// polling and therefore fetching of data from the object store over the
/// lifetime of the [`Stream`], the duration of a [`ObjectStore::get()`]
/// request is measured to be the wall clock difference between the moment the
/// caller executes the [`ObjectStore::get()`] call, up until the last chunk
/// of data is yielded to the caller.
///
/// This means the duration metrics measuring consumption of returned streams
/// are recording the rate at which the application reads the data, as opposed
/// to the duration of time taken to fetch that data.
///
/// # Stream Errors
///
/// The [`ObjectStore::get()`] method can return a [`Stream`] of [`Result`]
/// instances, and returning an error when polled is not necessarily a terminal
/// state. The metric recorder allows for a caller to observe a transient error
/// and subsequently go on to complete reading the stream, recording this read
/// in the "success" histogram.
///
/// If a stream is not polled again after observing an error, the operation is
/// recorded in the "error" histogram.
///
/// A stream can return an arbitrary sequence of success and error states before
/// terminating, with the last observed poll result that yields a [`Result`]
/// dictating which histogram the operation is recorded in.
///
/// # Bytes Transferred
///
/// The metric recording bytes transferred accounts for only object data, and
/// not object metadata (such as that returned by list methods).
///
/// The total data transferred will be greater than the metric value due to
/// metadata queries, read errors, etc. The metric tracks the amount of object
/// data successfully yielded to the caller.
///
/// # Backwards Clocks
///
/// If the system clock is observed as moving backwards in time, call durations
/// are not recorded. The bytes transferred metric is not affected.
///
///
/// [`Stream`]: futures::stream
#[derive(Debug)]
pub struct ObjectStoreMetrics {
    inner: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    bucket: Option<String>,

    put: Arc<MetricsWithBytes>,
    put_opts: Arc<MetricsWithBytes>,
    put_multipart: Arc<MetricsWithBytes>,
    put_multipart_opts: Arc<MetricsWithBytes>,
    get: Arc<MetricsWithBytesAndTtfb>,
    get_opts: Arc<MetricsWithBytesAndTtfb>,
    get_range: Arc<MetricsWithBytes>,
    get_ranges: Arc<MetricsWithBytes>,
    head: Arc<Metrics>,
    delete: Arc<Metrics>,
    delete_stream: Arc<MetricsWithCount>,
    list: Arc<MetricsWithCount>,
    list_with_offset: Arc<MetricsWithCount>,
    list_with_delimiter: Arc<MetricsWithCount>,
    copy: Arc<Metrics>,
    rename: Arc<Metrics>,
    copy_if_not_exists: Arc<Metrics>,
    rename_if_not_exists: Arc<Metrics>,
}

impl ObjectStoreMetrics {
    /// Instrument `T`, pushing to `registry`.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        store_type: impl Into<StoreType>,
        registry: &metric::Registry,
        bucket: Option<impl Into<String>>,
    ) -> Self {
        let store_type = store_type.into();
        let bucket = bucket.map(Into::into);

        Self {
            inner,
            time_provider,

            put: Arc::new(MetricsWithBytes::new(registry, &store_type, "put", &bucket)),
            put_opts: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "put_opts",
                &bucket,
            )),
            put_multipart: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "put_multipart",
                &bucket,
            )),
            put_multipart_opts: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "put_multipart_opts",
                &bucket,
            )),
            get: Arc::new(MetricsWithBytesAndTtfb::new(
                registry,
                &store_type,
                "get",
                &bucket,
            )),
            get_opts: Arc::new(MetricsWithBytesAndTtfb::new(
                registry,
                &store_type,
                "get_opts",
                &bucket,
            )),
            get_range: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "get_range",
                &bucket,
            )),
            get_ranges: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "get_ranges",
                &bucket,
            )),
            head: Arc::new(Metrics::new(registry, &store_type, "head", &bucket)),
            delete: Arc::new(Metrics::new(registry, &store_type, "delete", &bucket)),
            delete_stream: Arc::new(MetricsWithCount::new(
                registry,
                &store_type,
                "delete_stream",
                &bucket,
            )),
            list: Arc::new(MetricsWithCount::new(
                registry,
                &store_type,
                "list",
                &bucket,
            )),
            list_with_offset: Arc::new(MetricsWithCount::new(
                registry,
                &store_type,
                "list_with_offset",
                &bucket,
            )),
            list_with_delimiter: Arc::new(MetricsWithCount::new(
                registry,
                &store_type,
                "list_with_delimiter",
                &bucket,
            )),
            copy: Arc::new(Metrics::new(registry, &store_type, "copy", &bucket)),
            rename: Arc::new(Metrics::new(registry, &store_type, "rename", &bucket)),
            copy_if_not_exists: Arc::new(Metrics::new(
                registry,
                &store_type,
                "copy_if_not_exists",
                &bucket,
            )),
            rename_if_not_exists: Arc::new(Metrics::new(
                registry,
                &store_type,
                "rename_if_not_exists",
                &bucket,
            )),

            bucket,
        }
    }
}

impl std::fmt::Display for ObjectStoreMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreMetrics({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ObjectStoreMetrics {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let mut recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.put),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let size = payload.content_length();
        let res = self.inner.put(location, payload).await;
        recorder.submit((&res).into(), Some(size as _));
        res
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let mut recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.put_opts),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let size = bytes.content_length();
        let res = self.inner.put_opts(location, bytes, opts).await;
        recorder.submit((&res).into(), Some(size as _));
        res
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.put_multipart),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.put_multipart(location).await;
        wrap_multipart_upload_res(res, recorder)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.put_multipart_opts),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.put_multipart_opts(location, opts).await;
        wrap_multipart_upload_res(res, recorder)
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&self.get),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.get(location).await;
        wrap_getresult_res(res, recorder).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let recorder = MetricsWithBytesAndTtfbRecorder::new(
            Arc::clone(&self.get_opts),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                get_range: options.range.clone(),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.get_opts(location, options).await;
        wrap_getresult_res(res, recorder).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let mut recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.get_range),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                get_range: Some(range.clone().into()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.get_range(location, range).await;
        recorder.submit((&res).into(), res.as_ref().ok().map(|b| b.len() as _));
        res
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut recorder = MetricsWithBytesRecorder::new(
            Arc::clone(&self.get_ranges),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.get_ranges(location, ranges).await;
        recorder.submit(
            (&res).into(),
            res.as_ref()
                .ok()
                .map(|b| b.iter().map(|b| b.len() as u64).sum()),
        );
        res
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.head),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.head(location).await;
        recorder.submit((&res).into());
        res
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.delete),
            &self.time_provider,
            LogContext {
                location: Some(location.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.delete(location).await;
        recorder.submit((&res).into());
        res
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let recorder = MetricsWithCountRecorder::new(
            Arc::clone(&self.delete_stream),
            &self.time_provider,
            LogContext::default(),
            &self.bucket,
        );

        let s = self.inner.delete_stream(locations);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(s, CountStreamDelegate::new(recorder))
            .fuse()
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let recorder = MetricsWithCountRecorder::new(
            Arc::clone(&self.list),
            &self.time_provider,
            LogContext {
                prefix: prefix.cloned(),
                ..Default::default()
            },
            &self.bucket,
        );

        let s = self.inner.list(prefix);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(s, CountStreamDelegate::new(recorder))
            .fuse()
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let recorder = MetricsWithCountRecorder::new(
            Arc::clone(&self.list_with_offset),
            &self.time_provider,
            LogContext {
                prefix: prefix.cloned(),
                offset: Some(offset.clone()),
                ..Default::default()
            },
            &self.bucket,
        );

        let s = self.inner.list_with_offset(prefix, offset);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(s, CountStreamDelegate::new(recorder))
            .fuse()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut recorder = MetricsWithCountRecorder::new(
            Arc::clone(&self.list_with_delimiter),
            &self.time_provider,
            LogContext {
                prefix: prefix.cloned(),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.list_with_delimiter(prefix).await;
        recorder.submit(
            (&res).into(),
            res.as_ref().ok().map(|res| res.objects.len() as _),
        );
        res
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.copy),
            &self.time_provider,
            LogContext {
                from: Some(from.clone()),
                to: Some(to.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.copy(from, to).await;
        recorder.submit((&res).into());
        res
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.rename),
            &self.time_provider,
            LogContext {
                from: Some(from.clone()),
                to: Some(to.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.rename(from, to).await;
        recorder.submit((&res).into());
        res
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.copy_if_not_exists),
            &self.time_provider,
            LogContext {
                from: Some(from.clone()),
                to: Some(to.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.copy_if_not_exists(from, to).await;
        recorder.submit((&res).into());
        res
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let mut recorder = MetricsRecorder::new(
            Arc::clone(&self.rename_if_not_exists),
            &self.time_provider,
            LogContext {
                from: Some(from.clone()),
                to: Some(to.clone()),
                ..Default::default()
            },
            &self.bucket,
        );
        let res = self.inner.rename_if_not_exists(from, to).await;
        recorder.submit((&res).into());
        res
    }
}

fn wrap_multipart_upload_res(
    res: Result<Box<dyn MultipartUpload>>,
    mut recorder: MetricsWithBytesRecorder,
) -> Result<Box<dyn MultipartUpload>> {
    match res {
        Ok(inner) => {
            let multipart_upload = MultipartUploadWrapper::new(inner, recorder);
            Ok(Box::new(multipart_upload))
        }
        Err(e) => {
            recorder.submit(OpResult::Error, None);
            Err(e)
        }
    }
}

async fn wrap_getresult_res(
    res: Result<GetResult>,
    mut recorder: MetricsWithBytesAndTtfbRecorder,
) -> Result<GetResult> {
    match res {
        Ok(mut res) => {
            res.payload = match res.payload {
                GetResultPayload::File(file, path) => {
                    let file = tokio::fs::File::from_std(file);
                    let size = file.metadata().await.ok().map(|m| m.len());
                    let file = file.into_std().await;

                    // headers & first byte wasn't really measured, so take "end" instead
                    let end_time = recorder.freeze();
                    recorder.submit(Some(end_time), Some(end_time), OpResult::Success, size);
                    GetResultPayload::File(file, path)
                }
                GetResultPayload::Stream(s) => {
                    // Wrap the object store data stream in a decorator to track the
                    // yielded data / wall clock, inclusive of the inner call above.
                    let t_headers = recorder.time_provider().now();
                    GetResultPayload::Stream(Box::pin(Box::new(
                        StreamMetricRecorder::new(s, BytesStreamDelegate::new(recorder, t_headers))
                            .fuse(),
                    )))
                }
            };
            Ok(res)
        }
        Err(e) => {
            // headers are measured
            let end_time = recorder.freeze();
            recorder.submit(Some(end_time), None, OpResult::Error, None);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek, Write},
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use futures::{FutureExt, TryStreamExt};
    use futures_test_utils::AssertFutureExt;
    use iox_time::{MockProvider, Time};
    use object_store_mock::{
        DATA, MockCall, MockStore, err, get_result_stream, multipart_upload_err,
        multipart_upload_ok, object_meta, path, path2,
    };
    use test_helpers::tracing::TracingCapture;
    use tokio::sync::Barrier;

    use object_store::UploadPart;

    use crate::test_utils::{
        assert_counter_value, assert_histogram_hit, assert_histogram_not_hit,
        assert_u64histogram_hits, assert_u64histogram_total,
    };

    use super::*;

    /// Alias for helping the compiler when using `None` in tests that do not use a bucket below:
    type Bucket = Option<String>;

    #[tokio::test]
    async fn test_put() {
        let capture = capture();
        let path = path();
        let payload = PutPayload::from([42_u8, 42, 42, 42, 42].as_slice());
        let store = MockStore::new()
            .mock_next(MockCall::Put {
                params: (path.clone(), payload.clone().into()),
                barriers: vec![],
                res: Ok(PutResult {
                    e_tag: None,
                    version: None,
                }),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.put(&path, payload).await.expect("put should succeed");

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "success"),
            ],
            5,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "success"),
            ],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 5
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_fails() {
        let capture = capture();
        let path = path();
        let payload = PutPayload::from([42_u8, 42, 42, 42, 42].as_slice());
        let store = MockStore::new()
            .mock_next(MockCall::Put {
                params: (path.clone(), payload.clone().into()),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .put(&path, payload)
            .await
            .expect_err("put should error");

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "error"),
            ],
            5,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "error"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "error"),
            ],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 5
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_opts() {
        let capture = capture();
        let path = path();
        let payload = PutPayload::from([42_u8, 42, 42, 42, 42].as_slice());
        let store = MockStore::new()
            .mock_next(MockCall::PutOpts {
                params: (path.clone(), payload.clone().into(), Default::default()),
                barriers: vec![],
                res: Ok(PutResult {
                    e_tag: None,
                    version: None,
                }),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .put_opts(&path, payload, Default::default())
            .await
            .expect("put should succeed");

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_opts"),
                ("result", "success"),
            ],
            5,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_opts"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_opts"),
                ("result", "success"),
            ],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_opts"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 5
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_opts\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_ok()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // demonstrate that it sums across bytes
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42]))
                .await
                .is_ok()
        );
        multipart_upload.complete().await.unwrap();
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
            8,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
            8,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 8
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_cancel() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_ok()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // demonstrate that it sums across bytes
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42]))
                .await
                .is_ok()
        );
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            8,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            8,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 8
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"canceled\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_abort() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_ok()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // demonstrate that it sums across bytes
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42]))
                .await
                .is_ok()
        );
        multipart_upload.abort().await.unwrap();
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            8,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            8,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 8
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"canceled\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_abort_fail() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        multipart_upload.abort().await.unwrap_err();
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 0
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_drop_store() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_ok()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");

        // drop store
        drop(store);

        // yield back to tokio to drop tasks in `JoinSet` contained in store
        tokio::time::sleep(Duration::from_millis(10)).await;

        // should NOT panic
        drop(multipart_upload);

        // nothing accounted
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
            0,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
            0,
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "canceled"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 0
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"canceled\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_immediate_error() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.put_multipart(&path).await.unwrap_err();

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            0,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_fails() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(multipart_upload_err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        assert!(
            multipart_upload
                .put_part(PutPayload::from(Bytes::from(vec![42_u8, 42, 42, 42, 42])))
                .await
                .is_err()
        );
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            5,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 5
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[derive(Default, Debug)]
    struct WriteOnceMultipartUpload(AtomicBool /* has previous write */);

    #[async_trait]
    impl MultipartUpload for WriteOnceMultipartUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            let has_previous_writes = self.0.load(Ordering::Acquire);
            self.0.fetch_or(true, Ordering::AcqRel);
            async move {
                if has_previous_writes {
                    Err(object_store::Error::NotImplemented)
                } else {
                    Ok(())
                }
            }
            .boxed()
        }

        async fn complete(&mut self) -> Result<PutResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn abort(&mut self) -> Result<()> {
            Err(object_store::Error::NotImplemented)
        }
    }

    #[tokio::test]
    async fn test_put_multipart_delayed_write_failure() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(Box::new(WriteOnceMultipartUpload::default())),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        // FAILURE: one write ok, one write failure.
        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        // first write succeeds
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // second write fails
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_err()
        );
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            10,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            10,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 10
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_delayed_flush_failure() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipart {
                params: path.clone(),
                barriers: vec![],
                res: Ok(Box::new(WriteOnceMultipartUpload::default())),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        // FAILURE: one write ok, one flush failure.
        let mut multipart_upload = store
            .put_multipart(&path)
            .await
            .expect("should get multipart upload");
        // first write succeeds
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // flush fails
        assert!(multipart_upload.complete().await.is_err());
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            5,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
            5,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 5
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_put_multipart_opts() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::PutMultipartOptions {
                params: (path.clone(), Default::default()),
                barriers: vec![],
                res: Ok(multipart_upload_ok()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut multipart_upload = store
            .put_multipart_opts(&path, Default::default())
            .await
            .expect("should get multipart upload");
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
                .await
                .is_ok()
        );
        // demonstrate that it sums across bytes
        assert!(
            multipart_upload
                .put_part(PutPayload::from_static(&[42_u8, 42, 42]))
                .await
                .is_ok()
        );
        multipart_upload.complete().await.unwrap();
        drop(multipart_upload);

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart_opts"),
                ("result", "success"),
            ],
            8,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart_opts"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart_opts"),
                ("result", "success"),
            ],
            8,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "put_multipart_opts"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 8
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"put_multipart_opts\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_list() {
        let capture = capture();
        let store = MockStore::new()
            .mock_next(MockCall::List {
                params: None,
                barriers: vec![],
                res: futures::stream::iter([Ok(object_meta()), Ok(object_meta())])
                    .boxed()
                    .into(),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.list(None).try_collect::<Vec<_>>().await.unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "success"),
            ],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "success"),
            ],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - count: 2
          duration_secs: 0
          level: DEBUG
          message: object store operation
          op: "\"list\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::ListWithOffset {
                params: (None, path.clone()),
                barriers: vec![],
                res: futures::stream::iter([Ok(object_meta()), Ok(object_meta())])
                    .boxed()
                    .into(),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .list_with_offset(None, &path)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [
                ("store_type", "bananas"),
                ("op", "list_with_offset"),
                ("result", "success"),
            ],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list_with_offset"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list_with_offset"),
                ("result", "success"),
            ],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list_with_offset"),
                ("result", "success"),
            ],
        );

        // NOT raw `list` call
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - count: 2
          duration_secs: 0
          level: DEBUG
          message: object store operation
          offset: "\"path\""
          op: "\"list_with_offset\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_list_fails() {
        let capture = capture();
        let store = MockStore::new()
            .mock_next(MockCall::List {
                params: None,
                barriers: vec![],
                res: futures::stream::iter([Err(err())]).boxed().into(),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        assert!(
            store.list(None).try_collect::<Vec<_>>().await.is_err(),
            "mock configured to fail"
        );

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "error"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list"),
                ("result", "error"),
            ],
            1,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - count: 0
          duration_secs: 0
          level: DEBUG
          message: object store operation
          op: "\"list\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::ListWithDelimiter {
                params: Some(path.clone()),
                barriers: vec![],
                res: Ok(ListResult {
                    common_prefixes: vec![],
                    objects: vec![object_meta(), object_meta()],
                }),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .list_with_delimiter(Some(&path))
            .await
            .expect("list should succeed");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "success"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "success"),
            ],
            2,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "success"),
            ],
            2,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - count: 2
          duration_secs: 0
          level: DEBUG
          message: object store operation
          op: "\"list_with_delimiter\""
          prefix: "\"path\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_list_with_delimiter_fails() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::ListWithDelimiter {
                params: Some(path.clone()),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        assert!(
            store.list_with_delimiter(Some(&path)).await.is_err(),
            "mock configured to fail"
        );

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "error"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "list_with_delimiter"),
                ("result", "error"),
            ],
            0,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          message: object store operation
          op: "\"list_with_delimiter\""
          prefix: "\"path\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_head() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Head {
                params: path.clone(),
                barriers: vec![],
                res: Ok(object_meta()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.head(&path).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "head"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"head\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_head_fails() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Head {
                params: path.clone(),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .head(&path)
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "head"),
                ("result", "error"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"head\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_get_cancel() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Get {
                params: path.clone(),
                barriers: vec![Arc::new(Barrier::new(2))],
                res: Err(err()),
            })
            .as_store();

        let time = Arc::new(MockProvider::new(Time::MIN));
        let metrics = Arc::new(metric::Registry::default());
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let mut f = store.get(&path);
        f.assert_pending().await;
        drop(f);

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "canceled"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "canceled"),
            ],
            0,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get\""
          result: "\"canceled\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_get_fails() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Get {
                params: path.clone(),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.get(&path).await.expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
            0,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
            0,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "error"),
            ],
            0,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          headers_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_getrange() {
        let capture = capture();
        let path = path();
        let range = 0..1000;
        let store = MockStore::new()
            .mock_next(MockCall::GetRange {
                params: (path.clone(), range.clone()),
                barriers: vec![],
                res: Ok(Bytes::from_static(DATA)),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let res = store.get_range(&path, range).await.unwrap();
        assert_eq!(res, DATA);

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "success"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 11
          duration_secs: 0
          get_range: bytes=0-999
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get_range\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_getrange_fails() {
        let capture = capture();
        let path = path();
        let range = 0..1000;
        let store = MockStore::new()
            .mock_next(MockCall::GetRange {
                params: (path.clone(), range.clone()),
                barriers: vec![],
                res: Err(err()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .get_range(&path, range)
            .await
            .expect_err("mock configured to fail");

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "error"),
            ],
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "error"),
            ],
            0,
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          get_range: bytes=0-999
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get_range\""
          result: "\"error\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_getranges() {
        let capture = capture();
        let path = path();
        let ranges_usize = [0..2usize, 1..2, 0..1];
        let ranges_u64 = ranges_usize
            .clone()
            .map(|r| (r.start as u64)..(r.end as u64));
        let store = MockStore::new()
            .mock_next(MockCall::GetRanges {
                params: (path.clone(), ranges_u64.to_vec()),
                barriers: vec![],
                res: Ok(ranges_usize
                    .iter()
                    .map(|r| Bytes::from_static(&DATA[r.clone()]))
                    .collect()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.get_ranges(&path, &ranges_u64).await.unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get_ranges"),
                ("result", "success"),
            ],
            4,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_ranges"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_ranges"),
                ("result", "success"),
            ],
            4,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get_ranges"),
                ("result", "success"),
            ],
        );

        // NO `get_range` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get_range"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 4
          duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get_ranges\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_copy() {
        let capture = capture();
        let path = path();
        let path2 = path2();
        let store = MockStore::new()
            .mock_next(MockCall::Copy {
                params: (path.clone(), path2.clone()),
                barriers: vec![],
                res: Ok(()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.copy(&path, &path2).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "copy"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          from: "\"path\""
          level: DEBUG
          message: object store operation
          op: "\"copy\""
          result: "\"success\""
          store_type: "\"bananas\""
          to: "\"path2\""
        "#);
    }

    #[tokio::test]
    async fn test_copy_if_not_exists() {
        let capture = capture();
        let path = path();
        let path2 = path2();
        let store = MockStore::new()
            .mock_next(MockCall::CopyIfNotExists {
                params: (path.clone(), path2.clone()),
                barriers: vec![],
                res: Ok(()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.copy_if_not_exists(&path, &path2).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "copy_if_not_exists"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          from: "\"path\""
          level: DEBUG
          message: object store operation
          op: "\"copy_if_not_exists\""
          result: "\"success\""
          store_type: "\"bananas\""
          to: "\"path2\""
        "#);
    }

    #[tokio::test]
    async fn test_rename() {
        let capture = capture();
        let path = path();
        let path2 = path2();
        let store = MockStore::new()
            .mock_next(MockCall::Rename {
                params: (path.clone(), path2.clone()),
                barriers: vec![],
                res: Ok(()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.rename(&path, &path2).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "rename"),
                ("result", "success"),
            ],
        );

        // NO `copy`/`delete` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "copy"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "delete"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          from: "\"path\""
          level: DEBUG
          message: object store operation
          op: "\"rename\""
          result: "\"success\""
          store_type: "\"bananas\""
          to: "\"path2\""
        "#);
    }

    #[tokio::test]
    async fn test_rename_if_not_exists() {
        let capture = capture();
        let path = path();
        let path2 = path2();
        let store = MockStore::new()
            .mock_next(MockCall::RenameIfNotExists {
                params: (path.clone(), path2.clone()),
                barriers: vec![],
                res: Ok(()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.rename_if_not_exists(&path, &path2).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "rename_if_not_exists"),
                ("result", "success"),
            ],
        );

        // NO `copy`/`copy_if_not_exists`/`delete` used!
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "copy"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "copy_if_not_exists"),
                ("result", "success"),
            ],
        );
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "delete"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          from: "\"path\""
          level: DEBUG
          message: object store operation
          op: "\"rename_if_not_exists\""
          result: "\"success\""
          store_type: "\"bananas\""
          to: "\"path2\""
        "#);
    }

    #[tokio::test]
    async fn test_delete_stream() {
        let capture = capture();
        let path = path();
        let path2 = path2();
        let store = MockStore::new()
            .mock_next(MockCall::DeleteStream {
                params: (),
                barriers: vec![],
                res: futures::stream::iter([path.clone(), path2.clone()])
                    .map(Ok)
                    .boxed()
                    .into(),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store
            .delete_stream(futures::stream::iter([path, path2]).map(Ok).boxed())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_counter_value(
            &metrics,
            "object_store_transfer_objects",
            [
                ("store_type", "bananas"),
                ("op", "delete_stream"),
                ("result", "success"),
            ],
            2,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "delete_stream"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_objects_hist",
            [
                ("store_type", "bananas"),
                ("op", "delete_stream"),
                ("result", "success"),
            ],
            2,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "delete_stream"),
                ("result", "success"),
            ],
        );

        // NOT raw `delete` call
        assert_histogram_not_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "delete"),
                ("result", "success"),
            ],
        );

        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - count: 2
          duration_secs: 0
          level: DEBUG
          message: object store operation
          op: "\"delete_stream\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_delete() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Delete {
                params: path.clone(),
                barriers: vec![],
                res: Ok(()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        store.delete(&path).await.unwrap();

        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "delete"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - duration_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"delete\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_get_stream() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Get {
                params: path.clone(),
                barriers: vec![],
                res: Ok(get_result_stream()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let got = store.get(&path).await.expect("should read stream");
        match got.payload {
            GetResultPayload::Stream(mut stream) => while (stream.next().await).is_some() {},
            v => panic!("not a stream: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 11
          duration_secs: 0
          first_byte_secs: 0
          headers_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_get_file() {
        let capture = capture();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(DATA).unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();

        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Get {
                params: path.clone(),
                barriers: vec![],
                res: Ok(GetResult {
                    payload: GetResultPayload::File(file, PathBuf::from(path.to_string())),
                    meta: object_meta(),
                    range: 0..(DATA.len() as u64),
                    attributes: Default::default(),
                }),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let got = store.get(&path).await.expect("should read file");
        match got.payload {
            GetResultPayload::File(mut file, _) => {
                let mut contents = vec![];
                file.read_to_end(&mut contents)
                    .expect("failed to read file data");
                assert_eq!(Bytes::from(contents), DATA);
            }
            v => panic!("not a file: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "get"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 11
          duration_secs: 0
          first_byte_secs: 0
          headers_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_get_opts() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::GetOpts {
                params: (path.clone(), Default::default()),
                barriers: vec![],
                res: Ok(get_result_stream()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics, Bucket::None);

        let got = store
            .get_opts(&path, Default::default())
            .await
            .expect("should read stream");
        match got.payload {
            GetResultPayload::Stream(mut stream) => while (stream.next().await).is_some() {},
            v => panic!("not a stream: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
            DATA.len() as u64,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "bananas"),
                ("op", "get_opts"),
                ("result", "success"),
            ],
        );
        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bytes: 11
          duration_secs: 0
          first_byte_secs: 0
          headers_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get_opts\""
          result: "\"success\""
          store_type: "\"bananas\""
        "#);
    }

    #[tokio::test]
    async fn test_bucket_label() {
        let capture = capture();
        let path = path();
        let store = MockStore::new()
            .mock_next(MockCall::Get {
                params: path.clone(),
                barriers: vec![],
                res: Ok(get_result_stream()),
            })
            .as_store();

        let metrics = Arc::new(metric::Registry::default());
        let time = Arc::new(MockProvider::new(Time::MIN));
        let store = ObjectStoreMetrics::new(store, time, "buckets", &metrics, Some("my_bucket"));

        let got = store.get(&path).await.expect("should read stream");
        match got.payload {
            GetResultPayload::Stream(mut stream) => while (stream.next().await).is_some() {},
            v => panic!("not a stream: {v:?}"),
        }

        assert_counter_value(
            &metrics,
            "object_store_transfer_bytes",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
            DATA.len() as u64,
        );
        assert_u64histogram_hits(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
            1,
        );
        assert_u64histogram_total(
            &metrics,
            "object_store_transfer_bytes_hist",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
            DATA.len() as u64,
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_duration",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_headers",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
        );
        assert_histogram_hit(
            &metrics,
            "object_store_op_ttfb",
            [
                ("store_type", "buckets"),
                ("op", "get"),
                ("result", "success"),
                ("bucket", "my_bucket"),
            ],
        );

        insta::assert_yaml_snapshot!(
            capture.lines_as_maps(),
            @r#"
        - bucket: "\"my_bucket\""
          bytes: 11
          duration_secs: 0
          first_byte_secs: 0
          headers_secs: 0
          level: DEBUG
          location: "\"path\""
          message: object store operation
          op: "\"get\""
          result: "\"success\""
          store_type: "\"buckets\""
        "#);
    }

    fn capture() -> TracingCapture {
        TracingCapture::builder()
            .filter_target("object_store_metrics::log")
            .build()
    }
}
