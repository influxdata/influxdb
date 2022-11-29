use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use observability_deps::tracing::*;
use trace::span::{Span, SpanRecorder};

use super::{QueryError, QueryExec};
use crate::query::response::QueryResponse;

#[derive(Debug)]
pub(crate) struct QueryRunner;

impl QueryRunner {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl QueryExec for QueryRunner {
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        let mut _span_recorder = SpanRecorder::new(span);

        info!(
            namespace_id=%namespace_id,
            table_id=%table_id,
            columns=?columns,
            "executing query"
        );

        unimplemented!();
    }
}
