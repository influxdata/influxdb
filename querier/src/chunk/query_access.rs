use crate::chunk::{QuerierParquetChunk, QuerierRBChunk};
use arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch};
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary, TimestampMinMax,
};
use datafusion::physical_plan::RecordBatchStream;
use iox_query::{QueryChunk, QueryChunkError, QueryChunkMeta};
use observability_deps::tracing::debug;
use predicate::PredicateMatch;
use read_buffer::ReadFilterResults;
use schema::{sort::SortKey, Schema};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Parquet File Error in chunk {}: {}", chunk_id, source))]
    ParquetFileChunk {
        source: parquet_file::storage::ReadError,
        chunk_id: ChunkId,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    RBChunk {
        source: read_buffer::Error,
        chunk_id: ChunkId,
    },

    #[snafu(display(
        "Could not find column name '{}' in read buffer column_values results for chunk {}",
        column_name,
        chunk_id,
    ))]
    ColumnNameNotFound {
        column_name: String,
        chunk_id: ChunkId,
    },
}

impl QueryChunkMeta for QuerierParquetChunk {
    fn summary(&self) -> Option<&TableSummary> {
        Some(self.parquet_chunk.table_summary().as_ref())
    }

    fn schema(&self) -> Arc<Schema> {
        self.parquet_chunk.schema()
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.meta.partition_id())
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &self.delete_predicates
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        self.timestamp_min_max()
    }
}

impl QueryChunk for QuerierParquetChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn table_name(&self) -> &str {
        self.meta().table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn apply_predicate_to_metadata(
        &self,
        predicate: &predicate::Predicate,
    ) -> Result<predicate::PredicateMatch, QueryChunkError> {
        let pred_result = if predicate.has_exprs()
            || self.parquet_chunk.has_timerange(predicate.range.as_ref())
        {
            PredicateMatch::Unknown
        } else {
            PredicateMatch::Zero
        };

        Ok(pred_result)
    }

    fn column_names(
        &self,
        _ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        columns: schema::selection::Selection<'_>,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        if !predicate.is_empty() {
            // if there is anything in the predicate, bail for now and force a full plan
            return Ok(None);
        }
        Ok(self.parquet_chunk.column_names(columns))
    }

    fn column_values(
        &self,
        _ctx: iox_query::exec::IOxSessionContext,
        _column_name: &str,
        _predicate: &predicate::Predicate,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        // Since DataFusion can read Parquet, there is no advantage to
        // manually implementing this vs just letting DataFusion do its thing
        Ok(None)
    }

    fn read_filter(
        &self,
        mut ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, QueryChunkError> {
        let delete_predicates: Vec<_> = self
            .delete_predicates()
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();
        ctx.set_metadata("delete_predicates", delete_predicates.len() as i64);

        // merge the negated delete predicates into the select predicate
        let pred_with_deleted_exprs = predicate.clone().with_delete_predicates(&delete_predicates);
        debug!(?pred_with_deleted_exprs, "Merged negated predicate");

        ctx.set_metadata("predicate", format!("{}", &pred_with_deleted_exprs));
        self.parquet_chunk
            .read_filter(&pred_with_deleted_exprs, selection)
            .context(ParquetFileChunkSnafu {
                chunk_id: self.id(),
            })
            .map_err(|e| Box::new(e) as _)
    }

    fn chunk_type(&self) -> &str {
        "parquet"
    }

    fn order(&self) -> ChunkOrder {
        self.meta().order()
    }
}

impl QueryChunkMeta for QuerierRBChunk {
    fn summary(&self) -> Option<&TableSummary> {
        Some(&self.table_summary)
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.meta.partition_id())
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &self.delete_predicates
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        self.timestamp_min_max
    }
}

impl QueryChunk for QuerierRBChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn table_name(&self) -> &str {
        self.meta().table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn apply_predicate_to_metadata(
        &self,
        predicate: &predicate::Predicate,
    ) -> Result<predicate::PredicateMatch, QueryChunkError> {
        let pred_result = if predicate.has_exprs() || self.has_timerange(predicate.range.as_ref()) {
            PredicateMatch::Unknown
        } else {
            PredicateMatch::Zero
        };

        Ok(pred_result)
    }

    fn column_names(
        &self,
        mut ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        columns: schema::selection::Selection<'_>,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        ctx.set_metadata("storage", "read_buffer");
        ctx.set_metadata("projection", format!("{}", columns));
        ctx.set_metadata("predicate", format!("{}", &predicate));

        let rb_predicate = match to_read_buffer_predicate(predicate) {
            Ok(rb_predicate) => rb_predicate,
            Err(e) => {
                debug!(
                    ?predicate,
                    %e,
                    "read buffer predicate not supported for column_names, falling back"
                );
                return Ok(None);
            }
        };
        ctx.set_metadata("rb_predicate", format!("{}", &rb_predicate));

        // TODO(edd): wire up delete predicates to be pushed down to
        // the read buffer.

        let column_names =
            self.rb_chunk
                .column_names(rb_predicate, vec![], columns, BTreeSet::new());

        let names = match column_names {
            Ok(names) => {
                ctx.set_metadata("output_values", names.len() as i64);
                Some(names)
            }
            Err(read_buffer::Error::TableError {
                source: read_buffer::table::Error::ColumnDoesNotExist { .. },
            }) => {
                ctx.set_metadata("output_values", 0);
                None
            }
            Err(other) => {
                return Err(Box::new(Error::RBChunk {
                    source: other,
                    chunk_id: self.id(),
                }))
            }
        };

        Ok(names)
    }

    fn column_values(
        &self,
        mut ctx: iox_query::exec::IOxSessionContext,
        column_name: &str,
        predicate: &predicate::Predicate,
    ) -> Result<Option<iox_query::exec::stringset::StringSet>, QueryChunkError> {
        ctx.set_metadata("storage", "read_buffer");
        ctx.set_metadata("column_name", column_name.to_string());
        ctx.set_metadata("predicate", format!("{}", &predicate));

        let rb_predicate = match to_read_buffer_predicate(predicate) {
            Ok(rb_predicate) => rb_predicate,
            Err(e) => {
                debug!(
                    ?predicate,
                    %e,
                    "read buffer predicate not supported for column_values, falling back"
                );
                return Ok(None);
            }
        };
        ctx.set_metadata("rb_predicate", format!("{}", &rb_predicate));

        let mut values = self.rb_chunk.column_values(
            rb_predicate,
            schema::selection::Selection::Some(&[column_name]),
            BTreeMap::new(),
        )?;

        // The InfluxRPC frontend only supports getting column values
        // for one column at a time (this is a restriction on the Influx
        // Read gRPC API too). However, the Read Buffer supports multiple
        // columns and will return a map - we just need to pull the
        // column out to get the set of values.
        let values = values
            .remove(column_name)
            .context(ColumnNameNotFoundSnafu {
                chunk_id: self.id(),
                column_name,
            })?;
        ctx.set_metadata("output_values", values.len() as i64);

        Ok(Some(values))
    }

    fn read_filter(
        &self,
        mut ctx: iox_query::exec::IOxSessionContext,
        predicate: &predicate::Predicate,
        selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, QueryChunkError> {
        let delete_predicates: Vec<_> = self
            .delete_predicates()
            .iter()
            .map(|pred| Arc::new(pred.as_ref().clone().into()))
            .collect();
        ctx.set_metadata("delete_predicates", delete_predicates.len() as i64);

        // merge the negated delete predicates into the select predicate
        let pred_with_deleted_exprs = predicate.clone().with_delete_predicates(&delete_predicates);
        debug!(?pred_with_deleted_exprs, "Merged negated predicate");

        ctx.set_metadata("predicate", format!("{}", &pred_with_deleted_exprs));
        ctx.set_metadata("storage", "read_buffer");
        ctx.set_metadata("projection", format!("{}", selection));

        // Only apply pushdownable predicates
        let rb_predicate = self
            .rb_chunk
            // A predicate unsupported by the Read Buffer or against this chunk's schema is
            // replaced with a default empty predicate.
            .validate_predicate(to_read_buffer_predicate(predicate).unwrap_or_default())
            .unwrap_or_default();
        debug!(?rb_predicate, "RB predicate");
        ctx.set_metadata("predicate", format!("{}", &rb_predicate));

        // combine all delete expressions to RB's negated ones
        let negated_delete_exprs = to_read_buffer_negated_predicates(&delete_predicates)?
            .into_iter()
            // Any delete predicates unsupported by the Read Buffer will be elided.
            .filter_map(|p| self.rb_chunk.validate_predicate(p).ok())
            .collect::<Vec<_>>();

        debug!(?negated_delete_exprs, "Negated Predicate pushed down to RB");

        let read_results = self
            .rb_chunk
            .read_filter(rb_predicate, selection, negated_delete_exprs)
            .context(RBChunkSnafu {
                chunk_id: self.id(),
            })?;
        let schema = self
            .rb_chunk
            .read_filter_table_schema(selection)
            .context(RBChunkSnafu {
                chunk_id: self.id(),
            })?;

        Ok(Box::pin(ReadFilterResultsStream::new(
            ctx,
            read_results,
            schema.into(),
        )))
    }

    fn chunk_type(&self) -> &str {
        "read_buffer"
    }

    fn order(&self) -> ChunkOrder {
        self.meta().order()
    }
}

#[derive(Debug)]
struct ReadBufferPredicateConversionError {
    msg: String,
    predicate: predicate::Predicate,
}

impl std::fmt::Display for ReadBufferPredicateConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error translating predicate: {}, {:#?}",
            self.msg, self.predicate
        )
    }
}

impl std::error::Error for ReadBufferPredicateConversionError {}

/// Converts a [`predicate::Predicate`] into [`read_buffer::Predicate`], suitable for evaluating on
/// the ReadBuffer.
///
/// NOTE: a valid Read Buffer predicate is not guaranteed to be applicable to an arbitrary Read
/// Buffer chunk, because the applicability of a predicate depends on the schema of the chunk.
///
/// Callers should validate predicates against chunks they are to be executed against using
/// `read_buffer::Chunk::validate_predicate`
fn to_read_buffer_predicate(
    predicate: &predicate::Predicate,
) -> Result<read_buffer::Predicate, ReadBufferPredicateConversionError> {
    // Try to convert non-time column expressions into binary expressions that are compatible with
    // the read buffer.
    match predicate
        .exprs
        .iter()
        .map(read_buffer::BinaryExpr::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(exprs) => {
            // Construct a `ReadBuffer` predicate with or without InfluxDB-specific expressions on
            // the time column.
            Ok(match predicate.range {
                Some(range) => {
                    read_buffer::Predicate::with_time_range(&exprs, range.start(), range.end())
                }
                None => read_buffer::Predicate::new(exprs),
            })
        }
        Err(e) => Err(ReadBufferPredicateConversionError {
            msg: e,
            predicate: predicate.clone(),
        }),
    }
}

/// NOTE: valid Read Buffer predicates are not guaranteed to be applicable to an arbitrary Read
/// Buffer chunk, because the applicability of a predicate depends on the schema of the chunk.
/// Callers should validate predicates against chunks they are to be executed against using
/// `read_buffer::Chunk::validate_predicate`
fn to_read_buffer_negated_predicates(
    delete_predicates: &[Arc<predicate::Predicate>],
) -> Result<Vec<read_buffer::Predicate>, ReadBufferPredicateConversionError> {
    let mut rb_preds: Vec<read_buffer::Predicate> = vec![];
    for pred in delete_predicates {
        let rb_pred = to_read_buffer_predicate(pred)?;
        rb_preds.push(rb_pred);
    }

    debug!(?rb_preds, "read buffer delete predicates");
    Ok(rb_preds)
}

/// Adapter which will take a ReadFilterResults and make it an async stream
pub struct ReadFilterResultsStream {
    read_results: ReadFilterResults,
    schema: SchemaRef,
    ctx: iox_query::exec::IOxSessionContext,
}

impl ReadFilterResultsStream {
    pub fn new(
        ctx: iox_query::exec::IOxSessionContext,
        read_results: ReadFilterResults,
        schema: SchemaRef,
    ) -> Self {
        Self {
            ctx,
            read_results,
            schema,
        }
    }
}

impl RecordBatchStream for ReadFilterResultsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl futures::Stream for ReadFilterResultsStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut ctx = self.ctx.child_ctx("next_row_group");
        let rb = self.read_results.next();
        if let Some(rb) = &rb {
            ctx.set_metadata("output_rows", rb.num_rows() as i64);
        }

        Poll::Ready(Ok(rb).transpose())
    }

    // TODO is there a useful size_hint to pass?
}
