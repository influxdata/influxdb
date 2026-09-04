//! Adaptive GET for object store: ranged/chunked reads for large objects,
//! single-part GET for small ones.

use bytes::{Bytes, BytesMut};
use object_store::{ObjectStore, path::Path};
use std::num::{NonZeroU64, NonZeroUsize};

/// Object size at or below which [`AdaptiveGetExt::get_adaptive`] issues a
/// single [`ObjectStore::get`]; strictly above, it chunks into
/// [`DEFAULT_GET_CHUNK_SIZE`] ranged reads.
///
/// Set higher than the chunk size: a single GET has no protocol overhead, so
/// chunking only pays off once one GET risks exceeding the per-request HTTP
/// timeout (`--object-store-request-timeout`, default 30s) and would restart
/// the whole download from byte 0 — the multi-GiB case.
pub const SINGLE_GET_THRESHOLD: NonZeroU64 =
    const { NonZeroU64::new(128 * 1024 * 1024).expect("single-GET threshold must be non-zero") };

/// Range size for the chunked branch (16 MiB), matching
/// `object_store_utils::DEFAULT_MULTIPART_CHUNK_SIZE`. Sized to sit well under
/// the 30s per-request HTTP timeout so each ranged GET has ample budget.
pub const DEFAULT_GET_CHUNK_SIZE: NonZeroU64 =
    const { NonZeroU64::new(16 * 1024 * 1024).expect("default get chunk size must be non-zero") };

/// Number of [`DEFAULT_GET_CHUNK_SIZE`] ranged GETs the chunked branch issues
/// concurrently per batch. Bounds both in-flight requests and the transient
/// memory held on top of the reassembly buffer to
/// `MAX_CHUNKS_PER_BATCH * DEFAULT_GET_CHUNK_SIZE` (here 8 * 16 MiB = 128 MiB)
/// instead of buffering every chunk of a multi-GiB object at once, which would
/// roughly double peak RSS.
pub const MAX_CHUNKS_PER_BATCH: NonZeroUsize =
    const { NonZeroUsize::new(8).expect("max chunks per batch must be non-zero") };

/// Extension trait that adds a size-gated `get_adaptive` method to any
/// [`ObjectStore`] implementation.
///
/// At or below [`SINGLE_GET_THRESHOLD`] it does a single [`ObjectStore::get`]
/// (unchanged behavior); above, it fetches [`DEFAULT_GET_CHUNK_SIZE`] chunks via
/// per-chunk [`ObjectStore::get_range`] ([`MAX_CHUNKS_PER_BATCH`] at a time) and
/// concatenates them. This is the read-side counterpart to `put_adaptive`: each
/// chunk gets its own request-timeout and retry budget, so a transient failure
/// retries one range rather than restarting a whole-object GET from byte 0.
/// (`get_range`, not `get_ranges`, which would coalesce the contiguous ranges
/// back into one request.)
///
/// `size` is the object's byte length. Pass `Some` when the caller already
/// knows it (e.g. from a prior `list()` / `ObjectMeta`) to skip size discovery;
/// pass `None` to have the method discover it with a ranged GET of the first
/// chunk (which also returns the whole object in one request when it is small).
///
/// # Correctness
///
/// The chunked branch reads the object across multiple requests, so it assumes
/// the object is immutable for the duration of the read. Snapshot manifests and
/// checkpoints are write-once (a new sequence number is a new path), so this
/// holds for their load paths.
#[async_trait::async_trait]
pub trait AdaptiveGetExt: ObjectStore {
    /// Size-gated GET using [`SINGLE_GET_THRESHOLD`] / [`DEFAULT_GET_CHUNK_SIZE`].
    async fn get_adaptive(
        &self,
        path: &Path,
        size: Option<u64>,
    ) -> Result<Bytes, object_store::Error> {
        get_adaptive_impl(
            self,
            path,
            size,
            SINGLE_GET_THRESHOLD,
            DEFAULT_GET_CHUNK_SIZE,
            MAX_CHUNKS_PER_BATCH,
        )
        .await
    }
}

impl<T: ObjectStore + ?Sized> AdaptiveGetExt for T {}

/// The underlying implementation, taking explicit `threshold` / `chunk_size`
/// for test coverage at small sizes. Production callers should use
/// [`AdaptiveGetExt::get_adaptive`], which bakes in the defaults.
pub(crate) async fn get_adaptive_impl<S: ObjectStore + ?Sized>(
    store: &S,
    path: &Path,
    size: Option<u64>,
    threshold: NonZeroU64,
    chunk_size: NonZeroU64,
    max_chunks_per_batch: NonZeroUsize,
) -> Result<Bytes, object_store::Error> {
    let chunk = chunk_size.get();

    // `first_chunk` holds the leading chunk when size discovery fetched it (the
    // size=None path), so the range walk can reuse it instead of re-fetching.
    let (size, first_chunk): (u64, Option<Bytes>) = match size {
        Some(size) => (size, None),
        None => {
            // Discover the size with a ranged get_opts rather than a HEAD:
            // GetResult.meta.size is the object's *total* size, so one request
            // yields both the size and the first chunk's bytes. Avoids HEAD,
            // which on S3 has no error response body and is opaque on failure
            // (influxdata/influxdb_pro#4423). Range one chunk (not the whole
            // threshold) so this request stays within the per-request timeout.
            let options = object_store::GetOptions {
                range: Some((0..chunk).into()),
                ..Default::default()
            };
            let result = store.get_opts(path, options).await?;
            let size = result.meta.size;
            let bytes = result.bytes().await?;
            if size <= chunk {
                // The object fits in the first chunk — return it, one request.
                return Ok(bytes);
            }
            (size, Some(bytes))
        }
    };

    // Up to the threshold a single GET fits the timeout budget and is cheaper
    // than several — but only if discovery didn't already fetch the first chunk.
    // If it did, fall through to the range walk, which reuses that chunk and
    // fetches only the remainder rather than re-downloading the whole object.
    if size <= threshold.get() && first_chunk.is_none() {
        return store.get(path).await?.bytes().await;
    }

    // Reassembly buffer is contiguous (callers use serde_json::from_slice); a
    // checked cast surfaces objects larger than usize on 32-bit rather than
    // truncating the capacity.
    let capacity = usize::try_from(size).map_err(|_| object_store::Error::Generic {
        store: "adaptive_get",
        source: format!("object size {size} exceeds addressable memory on this platform").into(),
    })?;

    // If size discovery already fetched range 0, seed the buffer with it and
    // start the range walk at the second chunk to avoid re-fetching.
    let first_offset = if first_chunk.is_some() { chunk } else { 0 };
    let ranges: Vec<std::ops::Range<u64>> = (first_offset..size)
        .step_by(chunk as usize)
        .map(|offset| offset..(offset + chunk).min(size))
        .collect();

    // Per-chunk get_range, NOT get_ranges: get_ranges coalesces ranges within
    // OBJECT_STORE_COALESCE_DEFAULT (1 MiB), so our contiguous ranges would
    // merge back into one whole-object GET, re-exposing the timeout cliff this
    // path avoids. Guarded by the test
    // `chunked_branch_issues_one_get_range_per_chunk_not_coalesced`. Batching
    // bounds concurrency and holds at most one batch on top of the buffer.
    let mut buf = BytesMut::with_capacity(capacity);
    if let Some(first) = first_chunk {
        buf.extend_from_slice(&first);
    }
    for batch in ranges.chunks(max_chunks_per_batch.get()) {
        let parts = futures::future::try_join_all(
            batch
                .iter()
                .map(|range| store.get_range(path, range.clone())),
        )
        .await?;
        for part in parts {
            buf.extend_from_slice(&part);
        }
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_object_store::{ErrorConfig, OperationKind, TestObjectStore};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[derive(Copy, Clone, Debug)]
    enum Branch {
        SingleGet,
        Chunked,
    }

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap()
    }

    fn nzu(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    /// Assert that `get_adaptive_impl` routes a `payload`-sized object through
    /// `expected` AND returns the bytes intact.
    ///
    /// Wraps `InMemory` in a `TestObjectStore` that fails on the *opposite*
    /// branch's operation, mirroring `adaptive_put`'s `assert_branch`: if the
    /// code under test takes the expected branch the failure predicate never
    /// fires; if it takes the wrong branch the failure surfaces as an error and
    /// the test fails. Guards against a silent flip of the `<=` comparison that
    /// a round-trip-only test (indifferent `InMemory`) would not catch.
    async fn assert_branch(
        label: &str,
        payload: Bytes,
        threshold: NonZeroU64,
        chunk_size: NonZeroU64,
        expected: Branch,
    ) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(label);
        inner.put(&path, payload.clone().into()).await.unwrap();

        let fail_kind = match expected {
            // Single-GET branch must not touch the ranged path.
            Branch::SingleGet => OperationKind::GetRange,
            // Chunked branch must not issue a whole-object GET.
            Branch::Chunked => OperationKind::Get,
        };
        let test_store = TestObjectStore::new(inner)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(move |ctx| ctx.kind == fail_kind);

        let size = Some(payload.len() as u64);
        // Large batch: branch-routing tests don't care about batching.
        let got = get_adaptive_impl(&test_store, &path, size, threshold, chunk_size, nzu(1024))
            .await
            .unwrap_or_else(|e| {
                panic!("{label}: get_adaptive took the wrong branch (expected {expected:?}): {e}")
            });

        assert_eq!(
            test_store.get_injected_failure_count(),
            0,
            "{label}: get_adaptive touched the {fail_kind:?} path (expected {expected:?})",
        );
        assert_eq!(got, payload, "{label}: round-trip mismatch");
    }

    #[tokio::test]
    async fn small_object_takes_single_get_branch() {
        assert_branch(
            "small",
            Bytes::from(vec![1u8; 100]),
            nz(1024),
            nz(256),
            Branch::SingleGet,
        )
        .await;
    }

    #[tokio::test]
    async fn exact_threshold_takes_single_get_branch() {
        // An object of exactly the threshold stays on the single-GET fast path.
        assert_branch(
            "exact",
            Bytes::from(vec![2u8; 1024]),
            nz(1024),
            nz(256),
            Branch::SingleGet,
        )
        .await;
    }

    #[tokio::test]
    async fn above_threshold_takes_chunked_branch() {
        // threshold+1 crosses into the ranged path.
        assert_branch(
            "above",
            Bytes::from(vec![3u8; 1025]),
            nz(1024),
            nz(256),
            Branch::Chunked,
        )
        .await;
    }

    #[tokio::test]
    async fn chunked_branch_reassembles_multiple_chunks_intact() {
        // 1000 bytes / 256-byte chunks -> 4 ranges (256,256,256,232); the
        // reassembled bytes must equal the original exactly.
        let payload: Bytes = (0..1000u32).map(|i| i as u8).collect::<Vec<u8>>().into();
        assert_branch("multi", payload, nz(512), nz(256), Branch::Chunked).await;
    }

    #[tokio::test]
    async fn size_none_small_object_served_by_ranged_get_no_head() {
        // Size unknown + object smaller than one chunk: a single get_range of
        // the first chunk returns the whole object (short read), so no HEAD is
        // issued. A store that fails HEAD must still succeed.
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("small-none");
        let payload = Bytes::from(vec![7u8; 50]);
        inner.put(&path, payload.clone().into()).await.unwrap();

        let head_fails = TestObjectStore::new(inner)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::Head);
        // chunk=256 > payload=50, so the ranged GET short-reads the whole object.
        let got = get_adaptive_impl(&head_fails, &path, None, nz(1024), nz(256), nzu(4))
            .await
            .expect("small object with size=None must not require a HEAD");
        assert_eq!(got, payload);
        assert_eq!(
            head_fails.get_injected_failure_count(),
            0,
            "size=None small-object path must not issue a HEAD"
        );
    }

    #[tokio::test]
    async fn size_none_large_object_discovers_size_without_head() {
        // Size unknown + object larger than one chunk: the first ranged GET
        // (get_opts) yields the object's true size via GetResult.meta, so the
        // impl learns the size AND the first chunk in one request and never
        // issues a HEAD (a foot-gun on S3, see #4423). Result byte-identical.
        let payload: Bytes = (0..1000u32).map(|i| i as u8).collect::<Vec<u8>>().into();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("large-none");
        inner.put(&path, payload.clone().into()).await.unwrap();
        // Fail any HEAD so the test proves the discovery path avoids it.
        let store = TestObjectStore::new(inner)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::Head);

        // threshold=500 < size(1000) -> chunked; chunk=100 -> first get_opts
        // returns a full 100-byte chunk plus meta.size=1000.
        let got = get_adaptive_impl(&store, &path, None, nz(500), nz(100), nzu(4))
            .await
            .expect("size discovery for a large object must not require a HEAD");
        assert_eq!(got, payload);
        assert_eq!(
            store.get_injected_failure_count(),
            0,
            "size=None large-object path must not issue a HEAD"
        );
    }

    #[tokio::test]
    async fn size_none_midsize_object_reuses_first_chunk_no_full_get() {
        // Size unknown + object between one chunk and the threshold (chunk <
        // size <= threshold): discovery already fetched the first chunk via
        // get_opts, so the impl must fetch only the remainder and concat, NOT
        // re-download the whole object with a plain get() (which would waste the
        // prefetched chunk). Fail whole-object Get to prove it is not used.
        let payload: Bytes = (0..300u32).map(|i| i as u8).collect::<Vec<u8>>().into();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("midsize-none");
        inner.put(&path, payload.clone().into()).await.unwrap();
        let store = TestObjectStore::new(inner)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(|ctx| {
                matches!(ctx.kind, OperationKind::Get | OperationKind::Head)
            });

        // chunk=100, threshold=500 -> size(300) is in (100, 500]: threshold
        // branch, but discovery already holds bytes 0..100.
        let got = get_adaptive_impl(&store, &path, None, nz(500), nz(100), nzu(4))
            .await
            .expect("mid-size size=None path must not issue a whole-object get or HEAD");
        assert_eq!(got, payload);
        assert_eq!(
            store.get_injected_failure_count(),
            0,
            "mid-size size=None path must reuse the prefetched chunk, not re-get the whole object"
        );
    }

    #[tokio::test]
    async fn chunked_branch_issues_one_get_range_per_chunk_not_coalesced() {
        // 1000 bytes / 100-byte chunks -> 10 contiguous ranges. The impl must
        // issue 10 separate get_range requests (one per chunk), NOT a single
        // get_ranges: object_store's get_ranges coalesces adjacent ranges (gap
        // <= 1 MiB) and would merge all 10 back into one whole-object GET,
        // defeating chunking and re-exposing the request-timeout cliff.
        //
        // Fail on GetRanges so any coalescing path surfaces as an error, and
        // count the per-chunk get_range calls (the only op in this branch).
        let payload: Bytes = (0..1000u32).map(|i| i as u8).collect::<Vec<u8>>().into();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("perchunk");
        inner.put(&path, payload.clone().into()).await.unwrap();
        let store = TestObjectStore::new(inner)
            .with_error_config(ErrorConfig::PercentageError(100.0))
            .with_failure_predicate(|ctx| ctx.kind == OperationKind::GetRanges);

        let got = get_adaptive_impl(
            &store,
            &path,
            Some(payload.len() as u64),
            nz(500), // threshold < size -> chunked
            nz(100), // chunk size -> 10 ranges
            nzu(4),  // batches of 4 (concurrency/memory bound), still 10 get_range total
        )
        .await
        .unwrap_or_else(|e| {
            panic!("chunked read must not use the coalescing get_ranges path: {e}")
        });

        assert_eq!(got, payload, "reassembly across batches must be intact");
        assert_eq!(
            store.get_injected_failure_count(),
            0,
            "chunked read touched the coalescing get_ranges path",
        );
        assert_eq!(
            store.get_call_count(),
            10,
            "expected 10 per-chunk get_range calls, got {}",
            store.get_call_count()
        );
    }
}
