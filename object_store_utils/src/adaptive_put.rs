//! Adaptive PUT for object store: multipart for large files, single-part for small ones.

use bytes::Bytes;
use object_store::{ObjectStore, PutResult, WriteMultipart, path::Path};
use std::num::NonZeroUsize;

/// Default chunk size for multipart object store uploads (16 MiB).
///
/// Chosen from observed production file sizes in influxdata/influxdb_pro#2472:
/// typical compacted pacha_tree files land in the 10-30 MB band, so 16 MiB
/// splits the common case into two parts while keeping smaller files on the
/// single-part fast path. Large enough to keep per-part HTTP round trips
/// low, small enough to stay well under the 30s per-request HTTP timeout on
/// any realistic cloud network.
pub const DEFAULT_MULTIPART_CHUNK_SIZE: NonZeroUsize = match NonZeroUsize::new(16 * 1024 * 1024) {
    Some(n) => n,
    None => panic!("default multipart chunk size must be non-zero"),
};

/// Extension trait that adds a size-gated `put_adaptive` method to any
/// [`ObjectStore`] implementation.
///
/// At or below [`DEFAULT_MULTIPART_CHUNK_SIZE`] the method routes through
/// single-part [`ObjectStore::put`]; strictly above, through multipart
/// upload with that chunk size. At or below the threshold multipart cannot
/// produce more than one part anyway, so the three extra round trips
/// (CreateMultipartUpload + UploadPart + CompleteMultipartUpload) would be
/// pure overhead. Above it, each multipart part gets its own per-request
/// HTTP-timeout budget, eliminating the cliff that triggered
/// influxdata/influxdb_pro#2998.
///
/// # Errors
///
/// Returns any error produced by `put`, `put_multipart`, `put_part`, or the
/// multipart `complete` step. On a `complete` failure, [`WriteMultipart::finish`]
/// issues an `abort`; on other failures, orphaned parts rely on bucket
/// lifecycle rules for cleanup.
#[async_trait::async_trait]
pub trait AdaptivePutExt: ObjectStore {
    /// Size-gated PUT using [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    async fn put_adaptive(
        &self,
        path: &Path,
        bytes: Bytes,
    ) -> Result<PutResult, object_store::Error> {
        put_adaptive_impl(self, path, bytes, DEFAULT_MULTIPART_CHUNK_SIZE).await
    }
}

impl<T: ObjectStore + ?Sized> AdaptivePutExt for T {}

/// The underlying implementation, taking an explicit `chunk_size` for
/// test coverage at small sizes. Production callers should use
/// [`AdaptivePutExt::put_adaptive`] instead, which bakes in
/// [`DEFAULT_MULTIPART_CHUNK_SIZE`].
pub(crate) async fn put_adaptive_impl<S: ObjectStore + ?Sized>(
    store: &S,
    path: &Path,
    bytes: Bytes,
    chunk_size: NonZeroUsize,
) -> Result<PutResult, object_store::Error> {
    let chunk = chunk_size.get();
    // `<=` (not `<`): a payload of exactly chunk_size bytes produces only
    // one part in the multipart branch, so single-part is strictly cheaper
    // (1 round trip vs CreateMultipartUpload + UploadPart + CompleteMultipartUpload).
    if bytes.len() <= chunk {
        store.put(path, bytes.into()).await
    } else {
        let upload = store.put_multipart(path).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk);
        writer.put(bytes);
        writer.finish().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorConfig, OperationKind, TestObjectStore};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[derive(Copy, Clone, Debug)]
    enum Branch {
        SinglePart,
        Multipart,
    }

    fn chunk(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    /// Assert that `put_adaptive_impl` routes `payload` through `expected`
    /// AND round-trips the bytes intact.
    ///
    /// Wraps `InMemory` in a `TestObjectStore` that fails on the *opposite*
    /// branch's operation. If the code under test takes the expected branch,
    /// the failure predicate never fires and the data lands in the inner
    /// store; if it takes the wrong branch, the failure surfaces as an error
    /// and the test fails. This guards against silent regressions that would
    /// flip the `<=` comparison -- a round-trip-only test would continue to
    /// pass because `InMemory` is indifferent to which API the caller used.
    async fn assert_branch(
        label: &str,
        payload: Bytes,
        chunk_size: NonZeroUsize,
        expected: Branch,
    ) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fail_kind = match expected {
            Branch::SinglePart => OperationKind::PutMultipart,
            Branch::Multipart => OperationKind::Put,
        };
        let test_store = Arc::new(
            TestObjectStore::new(inner)
                .with_error_config(ErrorConfig::FirstCallFails)
                .with_failure_predicate(move |ctx| ctx.kind == fail_kind),
        );
        let path = Path::from(label);

        put_adaptive_impl(test_store.as_ref(), &path, payload.clone(), chunk_size)
            .await
            .unwrap_or_else(|e| {
                panic!("{label}: put_adaptive took the wrong branch (expected {expected:?}): {e}")
            });

        assert_eq!(
            test_store.get_injected_failure_count(),
            0,
            "{label}: put_adaptive touched the {fail_kind:?} path (expected {expected:?})",
        );

        let got = test_store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, payload, "{label}: round-trip mismatch");
    }

    #[tokio::test]
    async fn empty_payload_takes_single_part_branch() {
        assert_branch(
            "empty",
            Bytes::new(),
            chunk(16 * 1024 * 1024),
            Branch::SinglePart,
        )
        .await;
    }

    #[tokio::test]
    async fn small_payload_takes_single_part_branch() {
        assert_branch(
            "small",
            Bytes::from(vec![0xAB; 1024]),
            chunk(16 * 1024 * 1024),
            Branch::SinglePart,
        )
        .await;
    }

    #[tokio::test]
    async fn just_below_boundary_takes_single_part_branch() {
        let chunk_size = chunk(4 * 1024 * 1024);
        assert_branch(
            "just-below",
            Bytes::from(vec![0x7F; 4 * 1024 * 1024 - 1]),
            chunk_size,
            Branch::SinglePart,
        )
        .await;
    }

    #[tokio::test]
    async fn exact_boundary_takes_single_part_branch() {
        // `len == chunk_size` must stay on the single-part branch: the
        // multipart path would produce one part and pay 3 round trips
        // (CreateMultipartUpload + UploadPart + CompleteMultipartUpload)
        // for no benefit. Pins the `<=` comparison at the boundary.
        let chunk_size = chunk(4 * 1024 * 1024);
        assert_branch(
            "boundary",
            Bytes::from(vec![0xEF; 4 * 1024 * 1024]),
            chunk_size,
            Branch::SinglePart,
        )
        .await;
    }

    #[tokio::test]
    async fn cross_boundary_takes_multipart_branch() {
        // `chunk_size + small_tail` produces two multipart parts where the
        // second is smaller than chunk_size, exercising the tail-flushing
        // path in WriteMultipart::finish.
        let chunk_size = chunk(2 * 1024 * 1024);
        let mut payload_vec = vec![0x11; 2 * 1024 * 1024];
        payload_vec.extend(std::iter::repeat_n(0x22, 512 * 1024));
        assert_branch(
            "cross",
            Bytes::from(payload_vec),
            chunk_size,
            Branch::Multipart,
        )
        .await;
    }

    #[tokio::test]
    async fn large_payload_takes_multipart_branch() {
        // 8 MiB payload / 3 MiB chunk_size => 3 parts (3 + 3 + 2).
        assert_branch(
            "large",
            Bytes::from(vec![0xCD; 8 * 1024 * 1024]),
            chunk(3 * 1024 * 1024),
            Branch::Multipart,
        )
        .await;
    }

    #[tokio::test]
    async fn put_multipart_failure_propagates() {
        // Orthogonal to assert_branch: here we DO want the failure to
        // surface, to verify errors from the multipart path propagate
        // unchanged through put_adaptive_impl.
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let test_store = Arc::new(
            TestObjectStore::new(inner)
                .with_error_config(ErrorConfig::FirstCallFails)
                .with_failure_predicate(|ctx| ctx.kind == OperationKind::PutMultipart),
        );

        let path = Path::from("multipart-err");
        let payload = Bytes::from(vec![0; 10 * 1024 * 1024]);

        let err = put_adaptive_impl(test_store.as_ref(), &path, payload, chunk(3 * 1024 * 1024))
            .await
            .expect_err("put_multipart error should propagate");

        assert!(
            matches!(err, object_store::Error::Generic { .. }),
            "unexpected error variant: {err:?}"
        );
    }

    #[tokio::test]
    async fn trait_method_delegates_to_impl_with_default_chunk_size() {
        // Verifies that AdaptivePutExt::put_adaptive is wired up correctly
        // and dispatches through `Arc<dyn ObjectStore>` as production code
        // does. A small payload should round-trip via the single-part
        // branch (far below DEFAULT_MULTIPART_CHUNK_SIZE).
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("trait-small");
        let payload = Bytes::from(vec![0x42; 4096]);

        store
            .put_adaptive(&path, payload.clone())
            .await
            .expect("trait method succeeds on small payload");

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, payload);
    }
}
