use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use snafu::{ResultExt, Snafu};

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_util::MemoryStream;
use internal_types::{schema::Schema, selection::Selection};
use mutable_buffer::chunk::snapshot::ChunkSnapshot;
use object_store::path::Path;
use observability_deps::tracing::debug;
use parquet_file::chunk::Chunk as ParquetChunk;
use query::{
    exec::stringset::StringSet,
    predicate::{Predicate, PredicateMatch},
    PartitionChunk,
};
use read_buffer::Chunk as ReadBufferChunk;

use super::{
    catalog::chunk::ChunkMetadata, pred::to_read_buffer_predicate, streams::ReadFilterResultsStream,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::snapshot::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunkError {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, msg))]
    ReadBufferError { chunk_id: u32, msg: String },

    #[snafu(display("Parquet File Error in chunk {}: {}", chunk_id, source))]
    ParquetFileChunkError {
        source: parquet_file::chunk::Error,
        chunk_id: u32,
    },

    #[snafu(display("Internal error restricting schema: {}", source))]
    InternalSelectingSchema {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Predicate conversion error: {}", source))]
    PredicateConversion { source: super::pred::Error },

    #[snafu(display(
        "Internal error: mutable buffer does not support predicate pushdown, but got: {:?}",
        predicate
    ))]
    InternalPredicateNotSupported { predicate: Predicate },

    #[snafu(display("internal error creating plan: {}", source))]
    InternalPlanCreation {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion { source: arrow::error::ArrowError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub struct DbChunk {
    id: u32,
    table_name: Arc<str>,
    state: State,
    meta: Arc<ChunkMetadata>,
}

#[derive(Debug)]
enum State {
    MutableBuffer {
        chunk: Arc<ChunkSnapshot>,
    },
    ReadBuffer {
        chunk: Arc<ReadBufferChunk>,
        partition_key: Arc<str>,
    },
    ParquetFile {
        chunk: Arc<ParquetChunk>,
    },
}

impl DbChunk {
    /// Create a DBChunk snapshot of the catalog chunk
    pub fn snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        let partition_key = Arc::from(chunk.key());

        use super::catalog::chunk::{ChunkStage, ChunkStageFrozenRepr};

        let (state, meta) = match chunk.stage() {
            ChunkStage::Open { mb_chunk, .. } => {
                let snapshot = mb_chunk.snapshot();
                let state = State::MutableBuffer {
                    chunk: Arc::clone(&snapshot),
                };
                let meta = ChunkMetadata {
                    table_summary: Arc::new(mb_chunk.table_summary()),
                    schema: snapshot.full_schema(),
                };
                (state, Arc::new(meta))
            }
            ChunkStage::Frozen {
                representation,
                meta,
                ..
            } => {
                let state = match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(snapshot) => State::MutableBuffer {
                        chunk: Arc::clone(snapshot),
                    },
                    ChunkStageFrozenRepr::ReadBuffer(repr) => State::ReadBuffer {
                        chunk: Arc::clone(repr),
                        partition_key,
                    },
                };
                (state, Arc::clone(&meta))
            }
            ChunkStage::Persisted {
                parquet,
                read_buffer,
                meta,
                ..
            } => {
                let state = if let Some(read_buffer) = &read_buffer {
                    State::ReadBuffer {
                        chunk: Arc::clone(read_buffer),
                        partition_key,
                    }
                } else {
                    State::ParquetFile {
                        chunk: Arc::clone(&parquet),
                    }
                };
                (state, Arc::clone(&meta))
            }
        };

        Arc::new(Self {
            id: chunk.id(),
            table_name: chunk.table_name(),
            state,
            meta,
        })
    }

    /// Return the snapshot of the chunk with type ParquetFile
    /// This function should be only invoked when you know your chunk
    /// is ParquetFile type whose state is  WrittenToObjectStore. The
    /// reason we have this function is because the above snapshot
    /// function always returns the read buffer one for the same state
    pub fn parquet_file_snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        use super::catalog::chunk::ChunkStage;

        let (state, meta) = match chunk.stage() {
            ChunkStage::Persisted { parquet, meta, .. } => {
                let chunk = Arc::clone(&parquet);
                let state = State::ParquetFile { chunk };
                (state, Arc::clone(&meta))
            }
            _ => {
                panic!("Internal error: This chunk's stage is not Persisted");
            }
        };
        Arc::new(Self {
            id: chunk.id(),
            table_name: chunk.table_name(),
            meta,
            state,
        })
    }

    /// Return the Path in ObjectStorage where this chunk is
    /// persisted, if any
    pub fn object_store_path(&self) -> Option<Path> {
        match &self.state {
            State::ParquetFile { chunk } => Some(chunk.table_path()),
            _ => None,
        }
    }

    /// Return the name of the table in this chunk
    pub fn table_name(&self) -> Arc<str> {
        Arc::clone(&self.table_name)
    }
}

impl PartitionChunk for DbChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        self.id
    }

    fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    fn apply_predicate(&self, predicate: &Predicate) -> Result<PredicateMatch> {
        if !predicate.should_include_table(self.table_name().as_ref()) {
            return Ok(PredicateMatch::Zero);
        }

        // TODO apply predicate pruning here...

        let pred_result = match &self.state {
            State::MutableBuffer { chunk, .. } => {
                if predicate.has_exprs() {
                    // TODO: Support more predicates
                    PredicateMatch::Unknown
                } else if chunk.has_timerange(&predicate.range) {
                    // Note: this isn't precise / correct: if the
                    // chunk has the timerange, some other part of the
                    // predicate may rule out the rows, and thus
                    // without further work this clause should return
                    // "Unknown" rather than falsely claiming that
                    // there is at least one row:
                    //
                    // https://github.com/influxdata/influxdb_iox/issues/1590
                    PredicateMatch::AtLeastOne
                } else {
                    PredicateMatch::Zero
                }
            }
            State::ReadBuffer { chunk, .. } => {
                // If not supported, ReadBuffer can't answer with
                // metadata only
                let rb_predicate = match to_read_buffer_predicate(&predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for table_names, falling back");
                        return Ok(PredicateMatch::Unknown);
                    }
                };

                // TODO align API in read_buffer
                let table_names = chunk.table_names(&rb_predicate, &BTreeSet::new());
                if !table_names.is_empty() {
                    // As above, this should really be "Unknown" rather than AtLeastOne
                    // for precision / correctness.
                    PredicateMatch::AtLeastOne
                } else {
                    PredicateMatch::Zero
                }
            }
            State::ParquetFile { chunk, .. } => {
                if predicate.has_exprs() {
                    // TODO: Support more predicates
                    PredicateMatch::Unknown
                } else if chunk.has_timerange(predicate.range.as_ref()) {
                    // As above, this should really be "Unknown" rather than AtLeastOne
                    // for precision / correctness.
                    PredicateMatch::AtLeastOne
                } else {
                    PredicateMatch::Zero
                }
            }
        };

        Ok(pred_result)
    }

    fn table_schema(&self, selection: Selection<'_>) -> Result<Schema, Self::Error> {
        Ok(match selection {
            Selection::All => self.meta.schema.as_ref().clone(),
            Selection::Some(columns) => {
                let columns = self.meta.schema.select(columns).context(SelectColumns)?;
                self.meta.schema.project(&columns)
            }
        })
    }

    fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        let table_name = self.table_name.as_ref();
        // Predicate is not required to be applied for correctness. We only pushed it down
        // when possible for performance gain

        debug!(?predicate, "Input Predicate to read_filter");

        match &self.state {
            State::MutableBuffer { chunk, .. } => {
                let batch = chunk
                    .read_filter(table_name, selection)
                    .context(MutableBufferChunk)?;

                Ok(Box::pin(MemoryStream::new(vec![batch])))
            }
            State::ReadBuffer { chunk, .. } => {
                // Only apply pushdownable predicates
                let rb_predicate =
                    match to_read_buffer_predicate(&predicate).context(PredicateConversion) {
                        Ok(predicate) => predicate,
                        Err(_) => read_buffer::Predicate::default(),
                    };

                debug!(?rb_predicate, "Predicate pushed down to RUB");

                let read_results = chunk
                    .read_filter(table_name, rb_predicate, selection)
                    .context(ReadBufferChunkError {
                        chunk_id: self.id(),
                    })?;

                let schema = chunk
                    .read_filter_table_schema(table_name, selection)
                    .context(ReadBufferChunkError {
                        chunk_id: self.id(),
                    })?;

                Ok(Box::pin(ReadFilterResultsStream::new(
                    read_results,
                    schema.into(),
                )))
            }
            State::ParquetFile { chunk, .. } => chunk
                .read_filter(table_name, predicate, selection)
                .context(ParquetFileChunkError {
                    chunk_id: self.id(),
                }),
        }
    }

    fn column_names(
        &self,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        let table_name = self.table_name.as_ref();
        match &self.state {
            State::MutableBuffer { chunk, .. } => {
                if !predicate.is_empty() {
                    // TODO: Support predicates
                    return Ok(None);
                }
                Ok(chunk.column_names(table_name, columns))
            }
            State::ReadBuffer { chunk, .. } => {
                let rb_predicate = match to_read_buffer_predicate(&predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for column_names, falling back");
                        return Ok(None);
                    }
                };

                Ok(Some(
                    chunk
                        .column_names(table_name, rb_predicate, columns, BTreeSet::new())
                        .context(ReadBufferChunkError {
                            chunk_id: self.id(),
                        })?,
                ))
            }
            State::ParquetFile { chunk, .. } => {
                if !predicate.is_empty() {
                    // TODO: Support predicates when MB supports it
                    return Ok(None);
                }
                Ok(chunk.column_names(columns))
            }
        }
    }

    fn column_values(
        &self,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        let table_name = self.table_name.as_ref();
        match &self.state {
            State::MutableBuffer { .. } => {
                // There is no advantage to manually implementing this
                // vs just letting DataFusion do its thing
                Ok(None)
            }
            State::ReadBuffer { chunk, .. } => {
                let rb_predicate = match to_read_buffer_predicate(predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for column_names, falling back");
                        return Ok(None);
                    }
                };

                let mut values = chunk
                    .column_values(
                        table_name,
                        rb_predicate,
                        Selection::Some(&[column_name]),
                        BTreeMap::new(),
                    )
                    .context(ReadBufferChunkError {
                        chunk_id: self.id(),
                    })?;

                // The InfluxRPC frontend only supports getting column values
                // for one column at a time (this is a restriction on the Influx
                // Read gRPC API too). However, the Read Buffer support multiple
                // columns and will return a map - we just need to pull the
                // column out to get the set of values.
                let values = values
                    .remove(column_name)
                    .ok_or_else(|| Error::ReadBufferError {
                        chunk_id: self.id(),
                        msg: format!(
                            "failed to find column_name {:?} in results of tag_values",
                            column_name
                        ),
                    })?;

                Ok(Some(values))
            }
            State::ParquetFile { .. } => {
                // Since DataFusion can read Parquet, there is no advantage to
                // manually implementing this vs just letting DataFusion do its thing
                Ok(None)
            }
        }
    }
}
