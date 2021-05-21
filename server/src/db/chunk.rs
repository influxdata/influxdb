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
use query::{exec::stringset::StringSet, predicate::Predicate, PartitionChunk};
use read_buffer::Chunk as ReadBufferChunk;

use super::{pred::to_read_buffer_predicate, streams::ReadFilterResultsStream};

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

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion { source: arrow::error::ArrowError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub struct DbChunk {
    id: u32,
    state: State,
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

        use super::catalog::chunk::ChunkState;

        let state = match chunk.state() {
            ChunkState::Invalid => {
                panic!("Invalid internal state");
            }
            ChunkState::Open(chunk) => State::MutableBuffer {
                chunk: chunk.snapshot(),
            },
            ChunkState::Closed(chunk) => State::MutableBuffer {
                chunk: chunk.snapshot(),
            },
            ChunkState::Moved(chunk) => State::ReadBuffer {
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::WrittenToObjectStore(chunk, _) => State::ReadBuffer {
                // Since data exists in both read buffer and object store, we should
                // snapshot the chunk of read buffer
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::ObjectStoreOnly(chunk) => {
                let chunk = Arc::clone(chunk);
                State::ParquetFile { chunk }
            }
        };
        Arc::new(Self {
            id: chunk.id(),
            state,
        })
    }

    /// Return the snapshot of the chunk with type ParquetFile
    /// This function should be only invoked when you know your chunk
    /// is ParquetFile type whose state is  WrittenToObjectStore. The
    /// reason we have this function is because the above snapshot
    /// function always returns the read buffer one for the same state
    pub fn parquet_file_snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        use super::catalog::chunk::ChunkState;

        let state = match chunk.state() {
            ChunkState::WrittenToObjectStore(_, chunk) => {
                let chunk = Arc::clone(chunk);
                State::ParquetFile { chunk }
            }
            _ => {
                panic!("Internal error: This chunk's state is not WrittenToObjectStore");
            }
        };
        Arc::new(Self {
            id: chunk.id(),
            state,
        })
    }

    /// Return object store paths
    pub fn object_store_paths(&self) -> Vec<Path> {
        match &self.state {
            State::ParquetFile { chunk } => chunk.all_paths(),
            _ => vec![],
        }
    }
}

impl PartitionChunk for DbChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        self.id
    }

    fn all_table_names(&self, known_tables: &mut StringSet) {
        match &self.state {
            State::MutableBuffer { chunk, .. } => {
                known_tables.extend(chunk.table_names(None).cloned())
            }
            State::ReadBuffer { chunk, .. } => {
                // TODO - align APIs so they behave in the same way...
                let rb_names = chunk.all_table_names(known_tables);
                for name in rb_names {
                    known_tables.insert(name);
                }
            }
            State::ParquetFile { chunk, .. } => chunk.all_table_names(known_tables),
        }
    }

    fn table_names(
        &self,
        predicate: &Predicate,
        _known_tables: &StringSet, // TODO: Should this be being used?
    ) -> Result<Option<StringSet>, Self::Error> {
        let names = match &self.state {
            State::MutableBuffer { chunk, .. } => {
                if predicate.has_exprs() {
                    // TODO: Support more predicates
                    return Ok(None);
                }
                chunk.table_names(predicate.range).cloned().collect()
            }
            State::ReadBuffer { chunk, .. } => {
                // If not supported, ReadBuffer can't answer with
                // metadata only
                let rb_predicate = match to_read_buffer_predicate(&predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for table_names, falling back");
                        return Ok(None);
                    }
                };

                chunk.table_names(&rb_predicate, &BTreeSet::new())
            }
            State::ParquetFile { chunk, .. } => chunk.table_names(predicate.range).collect(),
        };

        // Prune out tables that should not be
        // present (based on additional table restrictions of the Predicate)
        Ok(Some(
            names
                .into_iter()
                .filter(|table_name| predicate.should_include_table(table_name))
                .collect(),
        ))
    }

    fn table_schema(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<Schema, Self::Error> {
        match &self.state {
            State::MutableBuffer { chunk, .. } => chunk
                .table_schema(table_name, selection)
                .context(MutableBufferChunk),
            State::ReadBuffer { chunk, .. } => {
                // TODO: Andrew -- I think technically this reordering
                // should be happening inside the read buffer, but
                // we'll see when we get to read_filter as the same
                // issue will appear when actually reading columns
                // back
                let needs_sort = matches!(selection, Selection::All);

                // Get the expected output schema for the read_filter operation
                let mut schema = chunk
                    .read_filter_table_schema(table_name, selection)
                    .context(ReadBufferChunkError {
                        chunk_id: self.id(),
                    })?;

                // Ensure the order of the output columns is as
                // specified
                if needs_sort {
                    schema = schema.sort_fields_by_name()
                }

                Ok(schema)
            }
            State::ParquetFile { chunk, .. } => {
                chunk
                    .table_schema(table_name, selection)
                    .context(ParquetFileChunkError {
                        chunk_id: self.id(),
                    })
            }
        }
    }

    fn has_table(&self, table_name: &str) -> bool {
        match &self.state {
            State::MutableBuffer { chunk, .. } => chunk.has_table(table_name),
            State::ReadBuffer { chunk, .. } => chunk.has_table(table_name),
            State::ParquetFile { chunk, .. } => chunk.has_table(table_name),
        }
    }

    fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        // Predicate is not required to be applied for correctness. We only pushed it down
        // when possible for performance gain

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
        table_name: &str,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
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
                Ok(chunk.column_names(table_name, columns))
            }
        }
    }

    fn column_values(
        &self,
        table_name: &str,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
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
