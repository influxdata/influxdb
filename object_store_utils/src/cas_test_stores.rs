//! Object-store wrappers for exercising compare-and-swap / conditional-put
//! logic (ambiguous-success retries, version-keyed backends, transient errors).

use std::sync::Arc;

use object_store::ObjectStore;

/// Store wrapper simulating an ambiguous conditional-put success: the write
/// lands on the inner store, but the caller receives `Precondition` — as
/// happens when the client's internal retry layer re-sends a PUT whose
/// response was lost and collides with its own earlier success.
#[derive(Debug)]
pub struct LostResponseStore {
    inner: Arc<dyn ObjectStore>,
    lose_next_put_response: std::sync::atomic::AtomicBool,
}

impl LostResponseStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            lose_next_put_response: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn lose_next_put_response(&self) {
        self.lose_next_put_response
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
        let result = self.inner.put_opts(location, payload, opts).await?;
        if self
            .lose_next_put_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // The write landed, but the caller sees the 412 its own retry got.
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "simulated lost response + retry collision".into(),
            });
        }
        Ok(result)
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
