use std::sync::Arc;

use super::store::{LocalStore, StreamedObject};
use super::DataError;

/// Service that handles the READ requests (`GET /object`).
#[derive(Debug, Clone)]
pub struct ReadHandler {
    cache: Arc<LocalStore>,
}

impl ReadHandler {
    pub fn new(cache: Arc<LocalStore>) -> Self {
        Self { cache }
    }

    pub async fn read_local(&self, location: &String) -> Result<StreamedObject, DataError> {
        self.cache
            .read_object(location)
            .await
            .map_err(|e| DataError::Read(e.to_string()))
    }
}
