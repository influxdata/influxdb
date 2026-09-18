//! Object-store wrappers for exercising compare-and-swap / conditional-put
//! logic (ambiguous-success retries, version-keyed backends, transient errors).

use std::sync::Arc;

use object_store::ObjectStore;

/// Which error a lost response surfaces as. A `PutMode::Create` collision
/// reaches the caller as `AlreadyExists`; `PutMode::Update` as `Precondition`;
/// a dropped connection on a committed write as `Generic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostResponseError {
    Precondition,
    AlreadyExists,
    Generic,
}

/// Store wrapper simulating an ambiguous conditional-put success: the write
/// lands on the inner store, but the caller receives a failure — as happens
/// when the client's internal retry layer re-sends a PUT whose response was
/// lost and collides with its own earlier success.
#[derive(Debug)]
pub struct LostResponseStore {
    inner: Arc<dyn ObjectStore>,
    lose_next_put_response: std::sync::Mutex<Option<LostResponseError>>,
    collision_shape: std::sync::Mutex<Option<LostResponseError>>,
    last_get: std::sync::Mutex<Option<(Option<object_store::GetRange>, bool)>>,
}

/// The error a lost response, or a reshaped collision, surfaces as.
fn lost_response_error(
    error: LostResponseError,
    location: &object_store::path::Path,
) -> object_store::Error {
    match error {
        LostResponseError::Precondition => object_store::Error::Precondition {
            path: location.to_string(),
            source: "simulated lost response + retry collision".into(),
        },
        LostResponseError::AlreadyExists => object_store::Error::AlreadyExists {
            path: location.to_string(),
            source: "simulated lost response + retry collision".into(),
        },
        LostResponseError::Generic => object_store::Error::Generic {
            store: "LostResponseStore",
            source: "simulated lost response on a committed write".into(),
        },
    }
}

impl LostResponseStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            lose_next_put_response: std::sync::Mutex::new(None),
            collision_shape: std::sync::Mutex::new(None),
            last_get: std::sync::Mutex::new(None),
        }
    }

    /// Report every `PutMode::Create` collision as `error`, as Azure does when
    /// an `If-None-Match: *` put comes back 412 rather than 409.
    pub fn report_collisions_as(&self, error: LostResponseError) {
        *self.collision_shape.lock().expect("lock not poisoned") = Some(error);
    }

    /// The `range` and `head` of the most recent `get_opts`, for pinning the
    /// shape of a read-back.
    pub fn last_get(&self) -> Option<(Option<object_store::GetRange>, bool)> {
        self.last_get.lock().expect("lock not poisoned").clone()
    }

    /// Lose the next put response as a `Precondition`, the shape a
    /// `PutMode::Update` collision takes.
    pub fn lose_next_put_response(&self) {
        self.lose_next_put_response_with(LostResponseError::Precondition);
    }

    pub fn lose_next_put_response_with(&self, error: LostResponseError) {
        *self
            .lose_next_put_response
            .lock()
            .expect("lock not poisoned") = Some(error);
    }
}

impl std::fmt::Display for LostResponseStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LostResponseStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LostResponseStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let result = match self.inner.put_opts(location, payload, opts).await {
            Ok(result) => result,
            Err(e @ object_store::Error::AlreadyExists { .. }) => {
                let shape = *self.collision_shape.lock().expect("lock not poisoned");
                return Err(shape.map_or(e, |shape| lost_response_error(shape, location)));
            }
            Err(e) => return Err(e),
        };
        // The write landed, but the caller sees the failure its own retry got.
        let taken = self
            .lose_next_put_response
            .lock()
            .expect("lock not poisoned")
            .take();
        match taken {
            Some(error) => Err(lost_response_error(error, location)),
            None => Ok(result),
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        *self.last_get.lock().expect("lock not poisoned") =
            Some((options.range.clone(), options.head));
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 object_store::path::Path>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<object_store::ListResult>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_with_delimiter(prefix)
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Store wrapper simulating a version-keyed backend like GCS: a conditional
/// `Update` put must carry an object `version` (GCS sends it as
/// `x-goog-if-generation-match` and returns `MissingVersion` when it is
/// absent), and every read/write surfaces a fresh version. Code under test must
/// capture and pass this version through, or every conditional `Update` after
/// the first `Create` fails against a version-keyed store.
#[derive(Debug)]
pub struct VersionKeyedStore {
    inner: Arc<dyn ObjectStore>,
    version: std::sync::atomic::AtomicU64,
}

impl VersionKeyedStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            version: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn current_version(&self) -> String {
        self.version
            .load(std::sync::atomic::Ordering::SeqCst)
            .to_string()
    }
}

impl std::fmt::Display for VersionKeyedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionKeyedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for VersionKeyedStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if let object_store::PutMode::Update(uv) = &opts.mode
            && uv.version.is_none()
        {
            // GCS keys conditional updates on the object generation and
            // rejects the put outright when it is missing.
            return Err(object_store::Error::Generic {
                store: "VersionKeyedStore",
                source: "conditional update requires an object version (MissingVersion)".into(),
            });
        }
        let result = self.inner.put_opts(location, payload, opts).await?;
        let version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        Ok(object_store::PutResult {
            e_tag: result.e_tag,
            version: Some(version.to_string()),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta.version = Some(self.current_version());
        Ok(result)
    }

    async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 object_store::path::Path>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<object_store::ListResult>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_with_delimiter(prefix)
    }

    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
