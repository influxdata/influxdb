use arrow_deps::{datafusion::physical_plan::SendableRecordBatchStream, util::MemoryStream};
use internal_types::{schema::Schema, selection::Selection};
use mutable_buffer::chunk::snapshot::ChunkSnapshot;
use object_store::path::Path;
use observability_deps::tracing::debug;
use parquet_file::chunk::Chunk as ParquetChunk;
use query::{exec::stringset::StringSet, predicate::Predicate, PartitionChunk};
use read_buffer::Chunk as ReadBufferChunk;
use snafu::{ResultExt, Snafu};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

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
        source: arrow_deps::datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion {
        source: arrow_deps::arrow::error::ArrowError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DbChunk {
    MutableBuffer {
        chunk: Arc<ChunkSnapshot>,
    },
    ReadBuffer {
        chunk: Arc<ReadBufferChunk>,
        partition_key: Arc<String>,
    },
    ParquetFile {
        chunk: Arc<ParquetChunk>,
    },
}

impl DbChunk {
    /// Create a DBChunk snapshot of the catalog chunk
    pub fn snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        let partition_key = Arc::new(chunk.key().to_string());

        use super::catalog::chunk::ChunkState;

        let db_chunk = match chunk.state() {
            ChunkState::Invalid => {
                panic!("Invalid internal state");
            }
            ChunkState::Open(chunk) | ChunkState::Closed(chunk) => Self::MutableBuffer {
                chunk: chunk.snapshot(),
            },
            ChunkState::Moving(chunk) => Self::MutableBuffer {
                chunk: chunk.snapshot(),
            },
            ChunkState::Moved(chunk) => Self::ReadBuffer {
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::WritingToObjectStore(chunk) => Self::ReadBuffer {
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::WrittenToObjectStore(chunk, _) => Self::ReadBuffer {
                // Since data exists in both read buffer and object store, we should
                // snapshot the chunk of read buffer
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::ObjectStoreOnly(chunk) => {
                let chunk = Arc::clone(chunk);
                Self::ParquetFile { chunk }
            }
        };
        Arc::new(db_chunk)
    }

    /// Return the snapshot of the chunk with type ParquetFile
    /// This function should be only invoked when you know your chunk
    /// is ParquetFile type whose state is  WrittenToObjectStore. The
    /// reason we have this function is because the above snapshot
    /// function always returns the read buffer one for the same state
    pub fn parquet_file_snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        use super::catalog::chunk::ChunkState;

        let db_chunk = match chunk.state() {
            ChunkState::WrittenToObjectStore(_, chunk) => {
                let chunk = Arc::clone(chunk);
                Self::ParquetFile { chunk }
            }
            _ => {
                panic!("Internal error: This chunk's state is not WrittenToObjectStore");
            }
        };
        Arc::new(db_chunk)
    }

    /// Return object store paths
    pub fn object_store_paths(&self) -> Vec<Path> {
        match self {
            Self::ParquetFile { chunk } => chunk.all_paths(),
            _ => vec![],
        }
    }
}

impl PartitionChunk for DbChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.chunk_id(),
            Self::ReadBuffer { chunk, .. } => chunk.id(),
            Self::ParquetFile { chunk, .. } => chunk.id(),
        }
    }

    fn all_table_names(&self, known_tables: &mut StringSet) {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                known_tables.extend(chunk.table_names(None).cloned())
            }
            Self::ReadBuffer { chunk, .. } => {
                // TODO - align APIs so they behave in the same way...
                let rb_names = chunk.all_table_names(known_tables);
                for name in rb_names {
                    known_tables.insert(name);
                }
            }
            Self::ParquetFile { chunk, .. } => chunk.all_table_names(known_tables),
        }
    }

    fn table_names(
        &self,
        predicate: &Predicate,
        _known_tables: &StringSet, // TODO: Should this be being used?
    ) -> Result<Option<StringSet>, Self::Error> {
        let names = match self {
            Self::MutableBuffer { chunk, .. } => {
                if predicate.has_exprs() {
                    // TODO: Support more predicates
                    return Ok(None);
                }
                chunk.table_names(predicate.range).cloned().collect()
            }
            Self::ReadBuffer { chunk, .. } => {
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
            Self::ParquetFile { chunk, .. } => chunk.table_names(predicate.range).collect(),
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
        match self {
            Self::MutableBuffer { chunk, .. } => chunk
                .table_schema(table_name, selection)
                .context(MutableBufferChunk),
            Self::ReadBuffer { chunk, .. } => {
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
                        chunk_id: chunk.id(),
                    })?;

                // Ensure the order of the output columns is as
                // specified
                if needs_sort {
                    schema = schema.sort_fields_by_name()
                }

                Ok(schema)
            }
            Self::ParquetFile { chunk, .. } => {
                chunk
                    .table_schema(table_name, selection)
                    .context(ParquetFileChunkError {
                        chunk_id: chunk.id(),
                    })
            }
        }
    }

    fn has_table(&self, table_name: &str) -> bool {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.has_table(table_name),
            Self::ReadBuffer { chunk, .. } => chunk.has_table(table_name),
            Self::ParquetFile { chunk, .. } => chunk.has_table(table_name),
        }
    }

    fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                if !predicate.is_empty() {
                    return InternalPredicateNotSupported {
                        predicate: predicate.clone(),
                    }
                    .fail();
                }
                let batch = chunk
                    .read_filter(table_name, selection)
                    .context(MutableBufferChunk)?;

                Ok(Box::pin(MemoryStream::new(vec![batch])))
            }
            Self::ReadBuffer { chunk, .. } => {
                // Error converting to a rb_predicate needs to fail
                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(PredicateConversion)?;

                let read_results = chunk
                    .read_filter(table_name, rb_predicate, selection)
                    .context(ReadBufferChunkError {
                        chunk_id: chunk.id(),
                    })?;

                let schema = chunk
                    .read_filter_table_schema(table_name, selection)
                    .context(ReadBufferChunkError {
                        chunk_id: chunk.id(),
                    })?;

                Ok(Box::pin(ReadFilterResultsStream::new(
                    read_results,
                    schema.into(),
                )))
            }
            Self::ParquetFile { chunk, .. } => chunk
                .read_filter(table_name, predicate, selection)
                .context(ParquetFileChunkError {
                    chunk_id: chunk.id(),
                }),
        }
    }

    fn column_names(
        &self,
        table_name: &str,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                if !predicate.is_empty() {
                    // TODO: Support predicates
                    return Ok(None);
                }
                Ok(chunk.column_names(table_name, columns))
            }
            Self::ReadBuffer { chunk, .. } => {
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
                            chunk_id: chunk.id(),
                        })?,
                ))
            }
            Self::ParquetFile { chunk, .. } => {
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
        match self {
            Self::MutableBuffer { .. } => {
                // There is no advantage to manually implementing this
                // vs just letting DataFusion do its thing
                Ok(None)
            }
            Self::ReadBuffer { chunk, .. } => {
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
                        chunk_id: chunk.id(),
                    })?;

                // The InfluxRPC frontend only supports getting column values
                // for one column at a time (this is a restriction on the Influx
                // Read gRPC API too). However, the Read Buffer support multiple
                // columns and will return a map - we just need to pull the
                // column out to get the set of values.
                let values = values
                    .remove(column_name)
                    .ok_or_else(|| Error::ReadBufferError {
                        chunk_id: chunk.id(),
                        msg: format!(
                            "failed to find column_name {:?} in results of tag_values",
                            column_name
                        ),
                    })?;

                Ok(Some(values))
            }
            Self::ParquetFile { .. } => {
                // Since DataFusion can read Parquet, there is no advantage to
                // manually implementing this vs just letting DataFusion do its thing
                Ok(None)
            }
        }
    }
}
