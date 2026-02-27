use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use futures::FutureExt;
use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};

use crate::metrics::{MetricsWithBytesRecorder, OpResult};

/// Able to perform metrics tracking bytes written
/// with object_store::put_multipart.
#[derive(Debug)]
pub(crate) struct MultipartUploadWrapper {
    /// current inner [`MultipartUpload`]
    inner: Box<dyn MultipartUpload>,

    /// current subtotaled size, regardless of success or failure
    bytes_attempted_so_far: Arc<AtomicU64>,

    /// current outcome
    outcome: Arc<AtomicBool>,

    /// was [`complete`](Self::complete) called?
    completed: bool,

    /// metric recorder
    recorder: MetricsWithBytesRecorder,
}

impl MultipartUploadWrapper {
    pub(crate) fn new(inner: Box<dyn MultipartUpload>, recorder: MetricsWithBytesRecorder) -> Self {
        Self {
            inner,
            bytes_attempted_so_far: Arc::new(AtomicU64::new(0)),
            outcome: Arc::new(AtomicBool::new(true)),
            completed: false,
            recorder,
        }
    }
}

#[async_trait]
impl MultipartUpload for MultipartUploadWrapper {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.completed = false;
        let attempted_size = data.content_length();
        let res = self.inner.as_mut().put_part(data);
        let bytes_attempted_so_far = Arc::clone(&self.bytes_attempted_so_far);
        let outcome = Arc::clone(&self.outcome);

        async move {
            match res.await {
                Ok(_) => {
                    bytes_attempted_so_far.fetch_add(attempted_size as u64, Ordering::AcqRel);
                    Ok(())
                }
                Err(e) => {
                    outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
                    bytes_attempted_so_far.fetch_add(attempted_size as u64, Ordering::AcqRel);
                    Err(e)
                }
            }
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.completed = false;
        let res = self.inner.complete().await;
        self.completed = true;
        if res.is_err() {
            self.outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
        }
        res
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.completed = false;
        let res = self.inner.abort().await;
        if res.is_err() {
            self.outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
        }
        res
    }
}

impl Drop for MultipartUploadWrapper {
    fn drop(&mut self) {
        let outcome = self.outcome.load(Ordering::Acquire);
        let bytes = self.bytes_attempted_so_far.load(Ordering::Acquire);

        let op_res = match (outcome, self.completed) {
            (false, _) => OpResult::Error,
            (true, true) => OpResult::Success,
            (true, false) => OpResult::Canceled,
        };
        self.recorder.submit(op_res, Some(bytes));
    }
}
