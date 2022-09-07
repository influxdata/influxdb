use crate::{chunk::QuerierChunk, QuerierChunkLoadSetting};
use arrow::{
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, PartitionId, TableSummary, TimestampMinMax,
};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, TryStreamExt};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use observability_deps::tracing::debug;
use predicate::Predicate;
use read_buffer::ReadFilterResults;
use schema::{
    selection::{select_schema, HalfOwnedSelection, OwnedSelection, Selection},
    sort::SortKey,
    Schema,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use trace::span::SpanRecorder;

use super::ChunkStage;

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

impl From<Error> for ArrowError {
    fn from(e: Error) -> Self {
        Self::ExternalError(Box::new(e))
    }
}

impl QueryChunkMeta for QuerierChunk {
    fn summary(&self) -> Option<Arc<TableSummary>> {
        Some(Arc::clone(self.stage.read().table_summary()))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.meta().partition_id())
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.meta().sort_key()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        &self.delete_predicates
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        Some(self.timestamp_min_max)
    }
}

impl QueryChunk for QuerierChunk {
    fn id(&self) -> ChunkId {
        self.meta().chunk_id
    }

    fn table_name(&self) -> &str {
        self.meta().table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        false
    }

    fn column_names(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        ctx.set_metadata("projection", format!("{}", columns));
        ctx.set_metadata("predicate", format!("{}", &predicate));

        let stage = self.stage.read();
        ctx.set_metadata("storage", stage.name());

        match &*stage {
            ChunkStage::Parquet { parquet_chunk, .. } => {
                if !predicate.is_empty() {
                    // if there is anything in the predicate, bail for now and force a full plan
                    return Ok(None);
                }
                Ok(parquet_chunk.column_names(columns))
            }
            ChunkStage::ReadBuffer { rb_chunk, .. } => {
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
                    rb_chunk.column_names(rb_predicate, vec![], columns, BTreeSet::new());

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
        }
    }

    fn column_values(
        &self,
        mut ctx: IOxSessionContext,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        ctx.set_metadata("column_name", column_name.to_string());
        ctx.set_metadata("predicate", format!("{}", &predicate));

        let stage = self.stage.read();
        ctx.set_metadata("storage", stage.name());

        match &*stage {
            ChunkStage::Parquet { .. } => {
                // Since DataFusion can read Parquet, there is no advantage to
                // manually implementing this vs just letting DataFusion do its thing
                Ok(None)
            }
            ChunkStage::ReadBuffer { rb_chunk, .. } => {
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

                let mut values = rb_chunk.column_values(
                    rb_predicate,
                    Selection::Some(&[column_name]),
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
        }
    }

    fn read_filter(
        &self,
        mut ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, QueryChunkError> {
        let span_recorder = SpanRecorder::new(
            ctx.span()
                .map(|span| span.child("QuerierChunk::read_filter")),
        );
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
        ctx.set_metadata("projection", format!("{}", selection));

        let output_schema = select_schema(selection, &self.schema.as_arrow());

        let load_setting = self.load_setting;
        let chunk_id = self.id();
        let stage = Arc::clone(&self.stage);
        let selection: OwnedSelection = selection.into();
        let predicate = predicate.clone();
        let store = self.store.clone();
        let schema = Arc::clone(&self.schema);
        let catalog_cache = Arc::clone(&self.catalog_cache);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            futures::stream::once(async move {
                if load_setting == QuerierChunkLoadSetting::OnDemand {
                    // maybe load RB
                    let parquet_file = match &*stage.read() {
                        ChunkStage::Parquet { parquet_chunk, .. } => {
                            Some(Arc::clone(parquet_chunk.parquet_file()))
                        }
                        ChunkStage::ReadBuffer { .. } => None,
                    };

                    if let Some(parquet_file) = parquet_file {
                        let rb_chunk = catalog_cache
                            .read_buffer()
                            .get(
                                parquet_file,
                                schema,
                                store,
                                span_recorder.child_span("cache GET read_buffer"),
                            )
                            .await;
                        stage.write().load_to_read_buffer(rb_chunk);
                    }
                }

                let stage = stage.read();
                ctx.set_metadata("storage", stage.name());

                let selection: HalfOwnedSelection<'_> = (&selection).into();
                let selection: Selection<'_> = (&selection).into();

                let stream_res: ArrowResult<SendableRecordBatchStream> = match &*stage {
                    ChunkStage::Parquet { parquet_chunk, .. } => {
                        debug!(?predicate, "parquet read_filter");
                        Ok(parquet_chunk
                            .read_filter(&pred_with_deleted_exprs, selection)
                            .context(ParquetFileChunkSnafu { chunk_id })?)
                    }
                    ChunkStage::ReadBuffer { rb_chunk, .. } => {
                        // Only apply pushdownable predicates
                        let rb_predicate = rb_chunk
                            // A predicate unsupported by the Read Buffer or against this chunk's schema is
                            // replaced with a default empty predicate.
                            .validate_predicate(
                                to_read_buffer_predicate(&predicate).unwrap_or_default(),
                            )
                            .unwrap_or_default();
                        debug!(?rb_predicate, "RB predicate");
                        ctx.set_metadata("rb_predicate", format!("{}", &rb_predicate));

                        // combine all delete expressions to RB's negated ones
                        let negated_delete_exprs =
                            to_read_buffer_negated_predicates(&delete_predicates)?
                                .into_iter()
                                // Any delete predicates unsupported by the Read Buffer will be elided.
                                .filter_map(|p| rb_chunk.validate_predicate(p).ok())
                                .collect::<Vec<_>>();

                        debug!(?negated_delete_exprs, "Negated Predicate pushed down to RB");

                        let read_results = rb_chunk
                            .read_filter(rb_predicate, selection, negated_delete_exprs)
                            .context(RBChunkSnafu { chunk_id })?;
                        let schema = rb_chunk
                            .read_filter_table_schema(selection)
                            .context(RBChunkSnafu { chunk_id })?;

                        Ok(Box::pin(ReadFilterResultsStream::new(
                            ctx,
                            read_results,
                            schema.into(),
                        )) as _)
                    }
                };

                stream_res
            })
            .try_flatten(),
        )))
    }

    fn chunk_type(&self) -> &str {
        self.stage.read().name()
    }

    fn order(&self) -> ChunkOrder {
        self.meta().order()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
struct ReadBufferPredicateConversionError {
    msg: String,
    predicate: Predicate,
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

impl From<ReadBufferPredicateConversionError> for ArrowError {
    fn from(e: ReadBufferPredicateConversionError) -> Self {
        Self::ExternalError(Box::new(e))
    }
}

/// Converts a [`predicate::Predicate`] into [`read_buffer::Predicate`], suitable for evaluating on
/// the ReadBuffer.
///
/// NOTE: a valid Read Buffer predicate is not guaranteed to be applicable to an arbitrary Read
/// Buffer chunk, because the applicability of a predicate depends on the schema of the chunk.
///
/// Callers should validate predicates against chunks they are to be executed against using
/// `read_buffer::Chunk::validate_predicate`
fn to_read_buffer_predicate(
    predicate: &Predicate,
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
    delete_predicates: &[Arc<Predicate>],
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
    ctx: IOxSessionContext,
}

impl ReadFilterResultsStream {
    pub fn new(ctx: IOxSessionContext, read_results: ReadFilterResults, schema: SchemaRef) -> Self {
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

impl Stream for ReadFilterResultsStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut ctx = self.ctx.child_ctx("next_row_group");
        let rb = self.read_results.next();
        if let Some(rb) = &rb {
            ctx.set_metadata("output_rows", rb.num_rows() as i64);
        }

        Poll::Ready(Ok(rb).transpose())
    }

    // TODO is there a useful size_hint to pass?
}
