use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use futures::stream::{BoxStream, StreamExt};
use object_store::{
    Error as ObjectStoreError, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore,
    PutOptions, PutPayload, PutResult, Result, path::Path,
};
use observability_deps::tracing::warn;

type RetryIfCond = Arc<dyn Fn(&ObjectStoreError) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct RetryParams {
    pub max_retries: usize,
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub factor: f32,
    pub with_jitter: bool,
    /// Optional predicate to determine if an error should be retried.
    /// If None, all errors are retried. If Some, only errors for which the predicate returns true are retried.
    pub when: Option<RetryIfCond>,
}

impl std::fmt::Debug for RetryParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryParams")
            .field("max_retries", &self.max_retries)
            .field("min_delay", &self.min_delay)
            .field("max_delay", &self.max_delay)
            .field("factor", &self.factor)
            .field("with_jitter", &self.with_jitter)
            .field("when", &self.when.as_ref().map(|_| "<predicate>"))
            .finish()
    }
}

impl Default for RetryParams {
    fn default() -> Self {
        Self {
            max_retries: 5,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            factor: 2.0,
            with_jitter: true,
            when: None,
        }
    }
}

impl RetryParams {
    fn exponential_builder(&self) -> ExponentialBuilder {
        let retry_builder = ExponentialBuilder::default()
            .with_factor(self.factor)
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries);

        if self.with_jitter {
            retry_builder.with_jitter()
        } else {
            retry_builder
        }
    }
}
static DEFAULT_PARAMS: OnceLock<RetryParams> = OnceLock::new();

/// Set the default retry parameters globally. This must be called before any retryable operations.
/// If not called, default values will be used.
pub fn set_default_retry_params(params: RetryParams) -> Result<()> {
    DEFAULT_PARAMS
        .set(params)
        .map_err(|_| object_store::Error::Generic {
            store: "object_store_utils",
            source: "Default retry params have already been set".into(),
        })
}

/// Get the current default retry parameters
fn get_default_retry_params() -> RetryParams {
    DEFAULT_PARAMS.get().cloned().unwrap_or_default()
}

async fn retry_operation<T, F, Fut>(
    retry_params: RetryParams,
    context_message: String,
    operation_label: &'static str,
    path: &Path,
    default_should_retry: Option<fn(&ObjectStoreError) -> bool>,
    op: F,
) -> Result<T>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<T>> + Send,
{
    let retry_builder = retry_params.exponential_builder();

    let retryable = op.retry(&retry_builder).notify({
        move |err: &ObjectStoreError, dur: Duration| {
            warn!(
                "{context_message}: Retrying object store {operation_label} operation for {path} after error: {err}. Retry after {}ms",
                dur.as_millis()
            );
        }
    });

    if let Some(when_fn) = retry_params.when {
        retryable.when(move |err| when_fn(err)).await
    } else if let Some(default_should_retry) = default_should_retry {
        retryable.when(default_should_retry).await
    } else {
        retryable.await
    }
}

/// Extension trait for ObjectStore that provides automatic retry capabilities with exponential backoff.
///
/// This trait adds retry variants for common ObjectStore operations, allowing for resilient
/// interactions with object storage systems that may experience transient failures.
///
/// The advantage of an extension trait over an `Arc<dyn ObjectStore>` here is that this lets us
/// use additional parameters on top of those supported by the `ObjectStore` trait to contextualize
/// error messages or adjust retry parameters on a per-call basis if desired.
#[async_trait::async_trait]
pub trait RetryableObjectStore: ObjectStore {
    async fn get_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<GetResult> {
        self.get_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn get_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<GetResult> {
        let store = self;

        retry_operation(
            retry_params,
            context_message,
            "get",
            path,
            Some(|err| !matches!(err, ObjectStoreError::NotFound { .. })),
            || async { store.get(path).await },
        )
        .await
    }

    async fn get_opts_with_default_retries(
        &self,
        path: &Path,
        options: GetOptions,
        context_message: String,
    ) -> Result<GetResult> {
        self.get_opts_with_retries(path, options, context_message, get_default_retry_params())
            .await
    }

    async fn get_opts_with_retries(
        &self,
        path: &Path,
        options: GetOptions,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<GetResult> {
        let store = self;

        retry_operation(
            retry_params,
            context_message,
            "get_opts",
            path,
            Some(|err| {
                !matches!(
                    err,
                    ObjectStoreError::NotFound { .. } | ObjectStoreError::Precondition { .. }
                )
            }),
            || async { store.get_opts(path, options.clone()).await },
        )
        .await
    }

    async fn head_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<ObjectMeta> {
        self.head_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn head_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<ObjectMeta> {
        let store = self;

        retry_operation(
            retry_params,
            context_message,
            "head",
            path,
            Some(|err| !matches!(err, ObjectStoreError::NotFound { .. })),
            || async { store.head(path).await },
        )
        .await
    }

    async fn put_with_default_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        context_message: String,
    ) -> Result<PutResult> {
        self.put_with_retries(path, payload, context_message, get_default_retry_params())
            .await
    }

    async fn put_opts_with_default_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
        context_message: String,
    ) -> Result<PutResult> {
        self.put_opts_with_retries(
            path,
            payload,
            options,
            context_message,
            get_default_retry_params(),
        )
        .await
    }

    async fn put_opts_with_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<PutResult> {
        let store = self;

        retry_operation(
            retry_params,
            context_message,
            "put_opts",
            path,
            Some(|err| {
                !matches!(
                    err,
                    ObjectStoreError::NotFound { .. } | ObjectStoreError::Precondition { .. }
                )
            }),
            || async { store.put_opts(path, payload.clone(), options.clone()).await },
        )
        .await
    }

    async fn put_with_retries(
        &self,
        path: &Path,
        payload: PutPayload,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<PutResult> {
        let path_clone = path.clone();
        let store = self;

        retry_operation(retry_params, context_message, "put", path, None, || async {
            store.put(&path_clone, payload.clone()).await
        })
        .await
    }

    async fn raw_delete_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<()> {
        let path_clone = path.clone();
        let store = self;

        retry_operation(
            retry_params,
            context_message,
            "delete",
            path,
            Some(|err| !matches!(err, ObjectStoreError::NotFound { .. })),
            || async { store.delete(&path_clone).await },
        )
        .await
    }

    async fn raw_delete_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<()> {
        self.raw_delete_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn delete_with_default_retries(
        &self,
        path: &Path,
        context_message: String,
    ) -> Result<()> {
        self.delete_with_retries(path, context_message, get_default_retry_params())
            .await
    }

    async fn delete_with_retries(
        &self,
        path: &Path,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<()> {
        match self
            .raw_delete_with_retries(path, context_message, retry_params)
            .await
        {
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            other => other,
        }
    }

    async fn list_with_delimiter_with_default_retries(
        &self,
        prefix: Option<&Path>,
        context_message: String,
    ) -> Result<ListResult> {
        self.list_with_delimiter_with_retries(prefix, context_message, get_default_retry_params())
            .await
    }

    async fn list_with_delimiter_with_retries(
        &self,
        prefix: Option<&Path>,
        context_message: String,
        retry_params: RetryParams,
    ) -> Result<ListResult> {
        let prefix_clone = prefix.cloned();
        let prefix_str = prefix
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<root>".to_string());
        let retry_builder = retry_params.exponential_builder();
        let store = self;

        if let Some(when_fn) = retry_params.when {
            (|| async { store.list_with_delimiter(prefix_clone.as_ref()).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{context_message}: Retrying object store list_with_delimiter operation for prefix '{prefix_str}' after error: {err}. Retry after {}ms",
                        dur.as_millis()
                    );
                })
                .when(move |err| when_fn(err))
                .await
        } else {
            (|| async { store.list_with_delimiter(prefix_clone.as_ref()).await })
                .retry(&retry_builder)
                .notify(|err: &ObjectStoreError, dur: Duration| {
                    warn!(
                        "{context_message}: Retrying object store list_with_delimiter operation for prefix '{prefix_str}' after error: {err}. Retry after {}ms",
                        dur.as_millis()
                    );
                })
                .when(|err| {
                    !matches!(
                        err,
                        ObjectStoreError::NotFound { .. }
                    )
                })
                .await
        }
    }

    fn list_with_default_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
    ) -> BoxStream<'static, Result<ObjectMeta>>
    where
        Self: Clone + Send + Sync + 'static,
    {
        self.list_with_retries(prefix, offset, context_message, get_default_retry_params())
    }

    fn list_with_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
        retry_params: RetryParams,
    ) -> BoxStream<'static, Result<ObjectMeta>>
    where
        Self: Clone + Send + Sync + 'static,
    {
        let prefix_str = prefix
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<root>".to_string());

        // Clone self to move into async block
        let prefix_clone = prefix.cloned();
        let offset_clone = offset.cloned();
        let inner = self.clone();
        let retry_builder = retry_params.exponential_builder();
        let fut = async move {
            let prefix_clone = prefix_clone.clone();
            let offset_clone = offset_clone.clone();
            let inner = inner.clone();
            let f = async || -> Result<BoxStream<'static, Result<ObjectMeta>>> {
                let mut stream = if let Some(offset) = &offset_clone {
                    inner.list_with_offset(prefix_clone.as_ref(), offset)
                } else {
                    inner.list(prefix_clone.as_ref())
                }
                .peekable();

                // Because peek only gives us a borrowed Err(e) value, we can only use that peek to
                // check if the value is an Err, then get the actual owned value as an error from
                // the stream if it is
                if Pin::new(&mut stream)
                    .peek()
                    .await
                    .is_some_and(|v| v.is_err())
                    // the following condition is redundant, but we need to do it to get an owned
                    // ObjectStoreError
                    && let Some(Err(err)) = stream.next().await
                {
                    return Err(err);
                }

                Ok(stream.boxed())
            };
            let result: Result<BoxStream<'static, Result<ObjectMeta>>> = f
            .retry(&retry_builder)
            .notify(|err: &ObjectStoreError, dur: Duration| {
                warn!(
                    "{context_message}: Retrying object store list_with_offset operation for prefix '{prefix_str}' offset '{offset_clone:?}' after error: {err}. Retry after {}ms",
                    dur.as_millis()
                );
            })
            .await;

            match result {
                Ok(s) => s,
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }
}

// Special implementation for Arc<dyn ObjectStore> to handle dynamic dispatch
#[async_trait::async_trait]
impl RetryableObjectStore for Arc<dyn ObjectStore> {
    fn list_with_retries(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
        context_message: String,
        retry_params: RetryParams,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<root>".to_string());

        // Clone self to move into async block
        let prefix_clone = prefix.cloned();
        let offset_clone = offset.cloned();
        let inner = Arc::clone(self);
        let retry_builder = retry_params.exponential_builder();
        let fut = async move {
            let prefix_clone = prefix_clone.clone();
            let offset_clone = offset_clone.clone();
            let inner = Arc::clone(&inner);
            let f = async || -> Result<BoxStream<'static, Result<ObjectMeta>>> {
                let mut stream = if let Some(offset) = &offset_clone {
                    inner.list_with_offset(prefix_clone.as_ref(), offset)
                } else {
                    inner.list(prefix_clone.as_ref())
                }
                .peekable();

                // Because peek only gives us a borrowed Err(e) value, we can only use that peek to
                // check if the value is an Err, then get the actual owned value as an error from
                // the stream if it is
                if Pin::new(&mut stream)
                    .peek()
                    .await
                    .is_some_and(|v| v.is_err())
                    // the following condition is redundant, but we need to do it to get an owned
                    // ObjectStoreError
                    && let Some(Err(err)) = stream.next().await
                {
                    return Err(err);
                }

                Ok(stream.boxed())
            };
            let result: Result<BoxStream<'static, Result<ObjectMeta>>> = f
            .retry(&retry_builder)
            .notify(|err: &ObjectStoreError, dur: Duration| {
                warn!(
                    "{context_message}: Retrying object store list_with_offset operation for prefix '{prefix_str}' offset '{offset_clone:?}' after error: {err}. Retry after {}ms",
                    dur.as_millis()
                );
            })
            .await;

            match result {
                Ok(s) => s,
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }
}

#[cfg(test)]
mod tests;
