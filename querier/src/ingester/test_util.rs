use super::IngesterConnection;
use crate::cache::namespace::CachedTable;
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::prelude::Expr;
use parking_lot::Mutex;
use schema::Schema as IOxSchema;
use std::{any::Any, collections::HashSet, sync::Arc};
use trace::span::Span;

/// IngesterConnection for testing
#[derive(Debug, Default)]
pub struct MockIngesterConnection {
    next_response: Mutex<Option<super::Result<Vec<super::IngesterPartition>>>>,
}

impl MockIngesterConnection {
    /// Create connection w/ an empty response.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set next response for this connection.
    #[cfg(test)]
    pub fn next_response(&self, response: super::Result<Vec<super::IngesterPartition>>) {
        *self.next_response.lock() = Some(response);
    }
}

#[async_trait]
impl IngesterConnection for MockIngesterConnection {
    async fn partitions(
        &self,
        _namespace_id: NamespaceId,
        _cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        _filters: &[Expr],
        _span: Option<Span>,
    ) -> super::Result<Vec<super::IngesterPartition>> {
        let Some(partitions) = self.next_response.lock().take() else {
            return Ok(vec![]);
        };
        let partitions = partitions?;

        let cols = columns.into_iter().collect::<HashSet<_>>();

        // do pruning
        let cols = &cols;
        let partitions = partitions
            .into_iter()
            .map(|mut p| async move {
                let chunks = p
                    .chunks
                    .into_iter()
                    .map(|ic| async move {
                        // restrict selection to available columns
                        let schema = &ic.schema;
                        let projection = schema
                            .as_arrow()
                            .fields()
                            .iter()
                            .enumerate()
                            .filter(|(_idx, f)| cols.contains(f.name()))
                            .map(|(idx, _f)| idx)
                            .collect::<Vec<_>>();
                        let batches: Vec<_> = ic
                            .batches
                            .iter()
                            .map(|batch| batch.project(&projection).unwrap())
                            .collect();

                        assert!(!batches.is_empty(), "Error: empty batches");
                        let schema = IOxSchema::try_from(batches[0].schema()).unwrap();
                        super::IngesterChunk {
                            batches,
                            schema,
                            ..ic
                        }
                    })
                    .collect::<Vec<_>>();

                p.chunks = futures::future::join_all(chunks).await;
                p
            })
            .collect::<Vec<_>>();

        let partitions = futures::future::join_all(partitions).await;
        Ok(partitions)
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
