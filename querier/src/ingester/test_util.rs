use crate::chunk::util::create_basic_summary;

use super::IngesterConnection;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::ShardIndex;
use futures::StreamExt;
use generated_types::influxdata::iox::ingester::v1::GetWriteInfoResponse;
use iox_query::{exec::IOxSessionContext, QueryChunk};
use parking_lot::Mutex;
use schema::selection::Selection;
use schema::Schema as IOxSchema;
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
    #[allow(dead_code)]
    pub fn next_response(&self, response: super::Result<Vec<super::IngesterPartition>>) {
        *self.next_response.lock() = Some(response);
    }
}

#[async_trait]
impl IngesterConnection for MockIngesterConnection {
    async fn partitions(
        &self,
        _shard_indexes: &[ShardIndex],
        _namespace_name: Arc<str>,
        _table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &predicate::Predicate,
        _expected_schema: Arc<schema::Schema>,
        _span: Option<Span>,
    ) -> super::Result<Vec<super::IngesterPartition>> {
        // see if we want to do projection pushdown
        let mut prune_columns = true;
        let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let selection = Selection::Some(&cols);
        match selection {
            Selection::All => prune_columns = false,
            Selection::Some(val) => {
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
                        let mut batches: Vec<RecordBatch> = vec![];
                        let mut stream = ic
                            .read_filter(IOxSessionContext::with_testing(), predicate, selection)
                            .expect("Error in read_filter");
                        while let Some(b) = stream.next().await {
                            let b = b.expect("Error in stream");
                            batches.push(b)
                        }

                        assert!(!batches.is_empty(), "Error: empty batches");
                        let new_schema = IOxSchema::try_from(batches[0].schema()).unwrap();
                        let total_row_count =
                            batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

                        super::IngesterChunk {
                            chunk_id: ic.chunk_id,
                            table_name: ic.table_name,
                            partition_id: ic.partition_id,
                            schema: Arc::new(new_schema.clone()),
                            partition_sort_key: ic.partition_sort_key,
                            batches,
                            ts_min_max: ic.ts_min_max,
                            summary: Arc::new(create_basic_summary(
                                total_row_count,
                                &new_schema,
                                ic.ts_min_max,
                            )),
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

    async fn get_write_info(&self, _write_token: &str) -> super::Result<GetWriteInfoResponse> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
