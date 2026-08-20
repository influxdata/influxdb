//! Nonce-tagged conditional creates, so a writer can recognise its own object
//! when a retried `PutMode::Create` collides with its own hidden success.
//!
//! Background and the reasoning behind each choice: influxdata/influxdb_pro#4776.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use metric::{Registry, U64Counter};
use object_store::{
    Attribute, Attributes, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    path::Path,
};
use observability_deps::tracing::{debug, warn};

use crate::{RetryParams, RetryableObjectStore};

/// Object metadata key carrying the writer's nonce.
///
/// Lowercase and underscore-separated: Azure requires metadata names to be
/// valid C# identifiers, and S3 and GCS store keys lowercased.
pub const PUT_NONCE_METADATA_KEY: &str = "influxdb3_put_nonce";

/// Identifies one logical write. Stable across a caller's own retries when the
/// caller mints it outside its retry loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutNonce(String);

impl PutNonce {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Marks a `PutMode::Create` as verifiable against whatever object is already
/// at the path. Placed in `PutOptions::extensions`, so a store built without
/// the verifying layer drops it and behaves as it always has.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SelfVerifyingCreate {
    nonce: Option<PutNonce>,
}

impl SelfVerifyingCreate {
    /// Verify against a caller-owned nonce. Minting it outside the caller's
    /// retry loop is what extends coverage to that loop.
    pub fn with_nonce(nonce: PutNonce) -> Self {
        Self { nonce: Some(nonce) }
    }

    /// Verify against a nonce minted per call, covering only the object store
    /// client's internal retries.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_options(self) -> PutOptions {
        let mut opts = PutOptions::from(PutMode::Create);
        opts.extensions.insert(self);
        opts
    }

    pub(crate) fn into_nonce(self) -> PutNonce {
        self.nonce.unwrap_or_else(PutNonce::generate)
    }
}

/// Verdict on the nonce stored against an existing object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceCheck {
    /// The stored nonce is ours: this is our own write.
    Ours,
    /// A different nonce: another writer holds the path.
    Foreign,
    /// No nonce at all. Cannot be distinguished from a foreign writer that
    /// does not tag, an older build, or a store that discards metadata.
    Absent,
}

pub fn nonce_check(attributes: &Attributes, nonce: &PutNonce) -> NonceCheck {
    match attributes.get(&Attribute::Metadata(PUT_NONCE_METADATA_KEY.into())) {
        None => NonceCheck::Absent,
        Some(stored) if stored.as_ref() == nonce.as_str() => NonceCheck::Ours,
        Some(_) => NonceCheck::Foreign,
    }
}

pub const SELF_WRITE_METRIC_NAME: &str = "influxdb3_conditional_create_self_write_verified";
pub const CONFLICT_METRIC_NAME: &str = "influxdb3_conditional_create_conflict_confirmed";
pub const UNTAGGED_CONFLICT_METRIC_NAME: &str = "influxdb3_conditional_create_conflict_untagged";
pub const UNVERIFIED_CONFLICT_METRIC_NAME: &str =
    "influxdb3_conditional_create_conflict_unverified";

/// Recorders are taken at construction so every series exists from startup,
/// reading as zero until something is recorded to it.
#[derive(Debug)]
struct SelfVerifyMetrics {
    self_writes: U64Counter,
    conflicts: U64Counter,
    untagged_conflicts: U64Counter,
    unverified_conflicts: U64Counter,
}

impl SelfVerifyMetrics {
    fn new(registry: &Registry) -> Self {
        Self {
            self_writes: registry
                .register_metric::<U64Counter>(
                    SELF_WRITE_METRIC_NAME,
                    "conditional creates that failed but were confirmed to be this writer's own object",
                )
                .recorder(&[]),
            conflicts: registry
                .register_metric::<U64Counter>(
                    CONFLICT_METRIC_NAME,
                    "conditional creates that failed against an object carrying another writer's nonce",
                )
                .recorder(&[]),
            untagged_conflicts: registry
                .register_metric::<U64Counter>(
                    UNTAGGED_CONFLICT_METRIC_NAME,
                    "conditional creates that failed against an object carrying no nonce",
                )
                .recorder(&[]),
            unverified_conflicts: registry
                .register_metric::<U64Counter>(
                    UNVERIFIED_CONFLICT_METRIC_NAME,
                    "conditional creates that failed and could not be read back to check the nonce",
                )
                .recorder(&[]),
        }
    }
}

/// Read-back attempts inside one verification, covering a transient failure on
/// the GET without waiting out a lasting one.
const READ_BACK_ATTEMPTS: usize = 3;

fn read_back_retry_params() -> RetryParams {
    RetryParams {
        max_retries: READ_BACK_ATTEMPTS - 1,
        min_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(500),
        ..Default::default()
    }
}

/// Report a collision as `AlreadyExists` whatever shape the backend gave it,
/// so a caller that keys on that variant sees it on every backend.
fn conflict(location: &Path, source: object_store::Error) -> object_store::Error {
    object_store::Error::AlreadyExists {
        path: location.to_string(),
        source: Box::new(source),
    }
}

#[derive(Debug)]
pub struct SelfVerifyingCreateStore {
    inner: Arc<dyn ObjectStore>,
    metrics: SelfVerifyMetrics,
}

impl SelfVerifyingCreateStore {
    pub fn new(inner: Arc<dyn ObjectStore>, registry: &Registry) -> Self {
        Self {
            inner,
            metrics: SelfVerifyMetrics::new(registry),
        }
    }

    /// Decide whether the object already at `location` is our own write.
    async fn verify(
        &self,
        location: &Path,
        nonce: &PutNonce,
        collision: object_store::Error,
    ) -> Result<PutResult> {
        // Metadata only: the nonce rides in the object's attributes, and a
        // ranged read is rejected outright against a zero-length object.
        let opts = GetOptions {
            head: true,
            ..Default::default()
        };

        let existing = match self
            .inner
            .get_opts_with_retries(
                location,
                opts,
                format!("verifying conditional create for {location}"),
                read_back_retry_params(),
            )
            .await
        {
            Ok(existing) => existing,
            Err(source) => {
                // There is some special handling of NotFound here; if we are doing verification in
                // this method, it is because we encoutered a 412/409 error when doing a PUT, i.e.,
                // the object we tried to create reported as already exists.
                //
                // How would a NotFound arise when doing this GET request? Perhaps:
                // - the object was persisted, then deleted due to a snapshot; that is not likely,
                //   since the snapshot-triggered deletion gives some grace period before deleting,
                //   but it is one conceivable way in which a NotFound could be encountered here.
                // - a NotFound is improperly reported by the store due to cosmic anomoly or act of
                //   God.
                //
                // The special handling is a difference in warning message to distinguish the
                // underlying error when viewing the logs. For *all* errors in this scenario, we
                // return AlreadyExists. The effect of this would be that the verification fails.
                //
                // For example, in the Parquet WAL, this would be treated as another process having
                // persisted a WAL file in place of the current process, and the current process
                // would exit. That outcome is appropriate (terminating the process) as allowing it
                // to continue (and continue ingesting/persisting WAL) could lead to undefined
                // behaviour or data corruption/loss; in which case, having the control plane
                // restart the node (or if there _actually is_ another process running in place of
                // this one, just letting the node shutdown) is better.
                self.metrics.unverified_conflicts.inc(1);
                if matches!(source, object_store::Error::NotFound { .. }) {
                    warn!(
                        %location,
                        %source,
                        "attempt to verify object after write conflict failed: the object was \
                         gone before it could be checked; another process may have already \
                         written and removed it"
                    );
                } else {
                    warn!(
                        %location,
                        %source,
                        "attempt to verify object after write conflict failed: the object \
                         could not be fetched"
                    );
                }
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: format!(
                        "write conflict ({collision}), and the attempt to verify the object \
                         failed: {source}"
                    )
                    .into(),
                });
            }
        };

        match nonce_check(&existing.attributes, nonce) {
            NonceCheck::Ours => {
                self.metrics.self_writes.inc(1);
                debug!(
                    %location,
                    "conditional create reported a conflict against this writer's own object; \
                     the write is durable, treating as success"
                );
                Ok(PutResult {
                    e_tag: existing.meta.e_tag.clone(),
                    version: existing.meta.version.clone(),
                })
            }
            NonceCheck::Foreign => {
                self.metrics.conflicts.inc(1);
                warn!(
                    %location,
                    "conditional create reported a conflict against an object carrying another \
                     writer's nonce"
                );
                Err(conflict(location, collision))
            }
            NonceCheck::Absent => {
                self.metrics.untagged_conflicts.inc(1);
                warn!(
                    %location,
                    "conditional create reported a conflict against an object carrying no nonce; \
                     the store may not retain object metadata, or the object predates tagging"
                );
                Err(conflict(location, collision))
            }
        }
    }
}

impl std::fmt::Display for SelfVerifyingCreateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SelfVerifyingCreateStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for SelfVerifyingCreateStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let Some(marker) = opts.extensions.remove::<SelfVerifyingCreate>() else {
            return self.inner.put_opts(location, payload, opts).await;
        };

        // The marker only means anything on a create. Any other mode may be
        // writing over an object this writer created earlier, so its nonce
        // would still be at the path and a lost race would read back as ours.
        if !matches!(opts.mode, PutMode::Create) {
            return self.inner.put_opts(location, payload, opts).await;
        }

        let nonce = marker.into_nonce();
        opts.attributes.insert(
            Attribute::Metadata(PUT_NONCE_METADATA_KEY.into()),
            nonce.as_str().to_string().into(),
        );

        match self.inner.put_opts(location, payload, opts).await {
            // A conditional create that collided. Backends disagree on the
            // shape: S3 and GCS translate the 412 into `AlreadyExists`, Azure
            // passes the status through, so an `If-None-Match: *` collision
            // there arrives as `Precondition` (412) or `AlreadyExists` (409).
            // Within a marked create all three mean the same thing.
            Err(
                e @ (object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }),
            ) => self.verify(location, &nonce, e).await,
            other => other,
        }
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests;
