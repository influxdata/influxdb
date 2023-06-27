use super::IngesterConnection;
use crate::cache::namespace::CachedTable;
use async_trait::async_trait;
use data_types::NamespaceId;
use parking_lot::Mutex;
use schema::{Projection, Schema as IOxSchema};
use std::{any::Any, sync::Arc};
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
        _predicate: &predicate::Predicate,
        _span: Option<Span>,
    ) -> super::Result<Vec<super::IngesterPartition>> {
        // see if we want to do projection pushdown
        let mut prune_columns = true;
        let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let selection = Projection::Some(&cols);
        match selection {
            Projection::All => prune_columns = false,
            Projection::Some(val) => {
                if val.is_empty() {
                    prune_columns = false;
                }
            }
        }
        if !prune_columns {
            return self
                .next_response
                .lock()
                .take()
                .unwrap_or_else(|| Ok(vec![]));
        }

        // no partitions
        let partitions = self.next_response.lock().take();
        if partitions.is_none() {
            return Ok(vec![]);
        }

        // do pruning
        let partitions = partitions.unwrap().unwrap();
        let partitions = partitions
            .into_iter()
            .map(|mut p| async move {
                let chunks = p
                    .chunks
                    .into_iter()
                    .map(|ic| async move {
                        let batches: Vec<_> = ic
                            .batches
                            .iter()
                            .map(|batch| match ic.schema.df_projection(selection).unwrap() {
                                Some(projection) => batch.project(&projection).unwrap(),
                                None => batch.clone(),
                            })
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
