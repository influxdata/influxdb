use arrow_array::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryEndpointError {
    #[error("Cannot query: {err} (query: {query})")]
    Fail {
        err: Box<dyn std::error::Error + Send + Sync>,
        query: String,
    },
}

#[async_trait]
pub trait QueryEndpoint: std::fmt::Debug + Send + Sync + 'static {
    async fn query(
        &self,
        db: &str,
        query: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryEndpointError>;
}
