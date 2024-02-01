use std::sync::Arc;

use object_store::{GetResult, GetResultPayload, ObjectMeta, ObjectStore};
use observability_deps::tracing::warn;

use crate::data_types::WriteHint;

use super::{store::LocalStore, DataError};

/// Handles the WRITE requests (`/write-hint`)
#[derive(Debug, Clone)]
pub struct WriteHandler {
    cache: Arc<LocalStore>,
    direct_store: Arc<dyn ObjectStore>,
}

impl WriteHandler {
    pub fn new(cache: Arc<LocalStore>, direct_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cache,
            direct_store,
        }
    }

    pub async fn write_local(
        &self,
        location: &str,
        write_hint: &WriteHint,
    ) -> Result<ObjectMeta, DataError> {
        // get from remote
        let WriteHint {
            file_size_bytes, ..
        } = write_hint;
        let GetResult { meta, payload, .. } = self
            .direct_store
            .get(&location.into())
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => DataError::DoesNotExist,
                _ => DataError::Stream(e.to_string()),
            })?;

        if !(meta.size as i64).eq(file_size_bytes) {
            warn!(
                "failed to perform writeback due to file size mismatch: {} != {}",
                meta.size, file_size_bytes
            );
            return Err(DataError::BadRequest(
                "failed to perform writeback due to file size mismatch".to_string(),
            ));
        }

        // write local
        match payload {
            GetResultPayload::File(_, pathbuf) => self
                .cache
                .move_file_to_cache(pathbuf, &location.into())
                .await
                .map_err(|e| DataError::File(e.to_string()))?,
            GetResultPayload::Stream(stream) => self
                .cache
                .write_object(&location.into(), *file_size_bytes, stream)
                .await
                .map_err(|e| DataError::Stream(e.to_string()))?,
        };

        Ok(meta)
    }
}
