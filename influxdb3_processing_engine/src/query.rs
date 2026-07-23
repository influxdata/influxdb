use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_py_api::query::{QueryEndpoint, QueryEndpointError};
use iox_query_params::StatementParams;
use std::{collections::HashMap, sync::Arc};

/// [`QueryEndpoint`] that directly forwards queries to a [`QueryExecutor`].
#[derive(Debug)]
pub struct InProcessQueryEndpoint {
    query_executor: Arc<dyn QueryExecutor>,
}

impl InProcessQueryEndpoint {
    pub fn new(query_executor: Arc<dyn QueryExecutor>) -> Self {
        Self { query_executor }
    }
}

#[async_trait]
impl QueryEndpoint for InProcessQueryEndpoint {
    async fn query(
        &self,
        db: &str,
        query: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryEndpointError> {
        let mut stmt_params = StatementParams::new();
        for (key, value) in params {
            stmt_params.insert(key, value.as_str());
        }

        let _permit = self.query_executor.acquire_execution_semaphore(None).await;
        let res = self
            .query_executor
            .query_sql(db, query, Some(stmt_params), None, None)
            .await
            .map_err(|e| QueryEndpointError::Fail {
                query: query.to_owned(),
                err: Box::new(e),
            })?;

        let batches =
            res.try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| QueryEndpointError::Fail {
                    query: query.to_owned(),
                    err: Box::new(e),
                })?;
        Ok(batches)
    }
}

/// [`QueryEndpoint`] that is unimplemented.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnimplementedQueryEndpoint;

#[async_trait]
impl QueryEndpoint for UnimplementedQueryEndpoint {
    async fn query(
        &self,
        _db: &str,
        query: &str,
        _params: &HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>, QueryEndpointError> {
        Err(QueryEndpointError::Fail {
            query: query.to_owned(),
            err: Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "QueryEndpoint::query is not implemented",
            )),
        })
    }
}
