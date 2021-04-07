use arrow_deps::datafusion::physical_plan::SendableRecordBatchStream;
use data_types::chunk::{ChunkStorage, ChunkSummary};
use internal_types::{schema::Schema, selection::Selection};
use mutable_buffer::chunk::Chunk as MBChunk;
use observability_deps::tracing::debug;
use parquet_file::chunk::Chunk as ParquetChunk;
use query::{exec::stringset::StringSet, predicate::Predicate, PartitionChunk};
use read_buffer::Chunk as ReadBufferChunk;
use snafu::{ResultExt, Snafu};

use std::{collections::BTreeSet, sync::Arc};

use super::{
    pred::{to_mutable_buffer_predicate, to_read_buffer_predicate},
    streams::{MutableBufferChunkStream, ReadFilterResultsStream},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunkError {
        source: read_buffer::Error,
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
pub enum DBChunk {
    MutableBuffer {
        chunk: Arc<MBChunk>,
        partition_key: Arc<String>,
        /// is this chunk open for writing?
        open: bool,
    },
    ReadBuffer {
        chunk: Arc<ReadBufferChunk>,
        partition_key: Arc<String>,
    },
    ParquetFile {
        chunk: Arc<ParquetChunk>,
    },
}

impl DBChunk {
    /// Create a DBChunk snapshot of the catalog chunk
    pub fn snapshot(chunk: &super::catalog::chunk::Chunk) -> Arc<Self> {
        let partition_key = Arc::new(chunk.key().to_string());

        use super::catalog::chunk::ChunkState;

        let db_chunk = match chunk.state() {
            ChunkState::Invalid => {
                panic!("Invalid internal state");
            }
            ChunkState::Open(chunk) => {
                // TODO the performance if cloning the chunk is terrible
                // Proper performance is tracked in
                // https://github.com/influxdata/influxdb_iox/issues/635
                let chunk = Arc::new(chunk.clone());
                Self::MutableBuffer {
                    chunk,
                    partition_key,
                    open: true,
                }
            }
            ChunkState::Closing(chunk) => {
                // TODO the performance if cloning the chunk is terrible
                // Proper performance is tracked in
                // https://github.com/influxdata/influxdb_iox/issues/635
                let chunk = Arc::new(chunk.clone());
                Self::MutableBuffer {
                    chunk,
                    partition_key,
                    open: false,
                }
            }
            ChunkState::Moving(chunk) => {
                let chunk = Arc::clone(chunk);
                Self::MutableBuffer {
                    chunk,
                    partition_key,
                    open: false,
                }
            }
            ChunkState::Moved(chunk) => Self::ReadBuffer {
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::WritingToObjectStore(chunk) => Self::ReadBuffer {
                chunk: Arc::clone(chunk),
                partition_key,
            },
            ChunkState::WrittenToObjectStore(_, chunk) => {
                let chunk = Arc::clone(chunk);
                Self::ParquetFile { chunk }
            }
        };
        Arc::new(db_chunk)
    }

    /// Return a partially filled `ChunkSummary` that includes storage
    /// details. Information such as timestamps are provided by the
    /// the catalog
    pub fn summary(&self) -> ChunkSummary {
        match self {
            Self::MutableBuffer {
                chunk,
                partition_key,
                open,
            } => {
                let storage = if *open {
                    ChunkStorage::OpenMutableBuffer
                } else {
                    ChunkStorage::ClosedMutableBuffer
                };
                ChunkSummary::new_without_timestamps(
                    Arc::clone(partition_key),
                    chunk.id(),
                    storage,
                    chunk.size(),
                )
            }
            Self::ReadBuffer {
                chunk,
                partition_key,
            } => {
                let estimated_bytes = chunk.size() as usize;

                ChunkSummary::new_without_timestamps(
                    Arc::clone(&partition_key),
                    chunk.id(),
                    ChunkStorage::ReadBuffer,
                    estimated_bytes,
                )
            }
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file summary not implemented")
            }
        }
    }
}

impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.id(),
            Self::ReadBuffer { chunk, .. } => chunk.id(),
            Self::ParquetFile { .. } => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_summaries(&self) -> Vec<data_types::partition_metadata::TableSummary> {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.table_summaries(),
            Self::ReadBuffer { chunk, .. } => chunk.table_summaries(),
            Self::ParquetFile { .. } => unimplemented!("parquet file not implemented"),
        }
    }

    fn all_table_names(&self, known_tables: &mut StringSet) {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.all_table_names(known_tables),
            Self::ReadBuffer { chunk, .. } => {
                // TODO - align APIs so they behave in the same way...
                let rb_names = chunk.all_table_names(known_tables);
                for name in rb_names {
                    known_tables.insert(name);
                }
            }
            Self::ParquetFile { .. } => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_names(
        &self,
        predicate: &Predicate,
        _known_tables: &StringSet,
    ) -> Result<Option<StringSet>, Self::Error> {
        let names = match self {
            Self::MutableBuffer { chunk, .. } => {
                if chunk.is_empty() {
                    Some(StringSet::new())
                } else {
                    let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                        Ok(chunk_predicate) => chunk_predicate,
                        Err(e) => {
                            debug!(?predicate, %e, "mutable buffer predicate not supported for table_names, falling back");
                            return Ok(None);
                        }
                    };

                    // we don't support arbitrary expressions in chunk predicate yet
                    if !chunk_predicate.chunk_exprs.is_empty() {
                        None
                    } else {
                        let names = chunk
                            .table_names(&chunk_predicate)
                            .context(MutableBufferChunk)?
                            .into_iter()
                            .map(|s| s.to_string())
                            .collect::<StringSet>();

                        Some(names)
                    }
                }
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

                Some(chunk.table_names(&rb_predicate, &BTreeSet::new()))
            }
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented")
            }
        };

        // Prune out tables that should not be
        // present (based on additional table restrictions of the Predicate)
        //
        // This is needed because at time of writing, the ReadBuffer's
        // table_names implementation doesn't include any way to
        // further restrict the tables to a known set of tables
        let names = names.map(|names| {
            names
                .into_iter()
                .filter(|table_name| predicate.should_include_table(table_name))
                .collect()
        });
        Ok(names)
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
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented for table schema")
            }
        }
    }

    fn has_table(&self, table_name: &str) -> bool {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.has_table(table_name),
            Self::ReadBuffer { chunk, .. } => chunk.has_table(table_name),
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented for has_table")
            }
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
                // Note MutableBuffer doesn't support predicate
                // pushdown (other than pruning out the entire chunk
                // via `might_pass_predicate)
                if !predicate.is_empty() {
                    return InternalPredicateNotSupported {
                        predicate: predicate.clone(),
                    }
                    .fail();
                }
                let schema: Schema = self.table_schema(table_name, selection)?;

                Ok(Box::pin(MutableBufferChunkStream::new(
                    Arc::clone(&chunk),
                    schema.as_arrow(),
                    table_name,
                )))
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
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented for scan_data")
            }
        }
    }

    fn could_pass_predicate(&self, _predicate: &Predicate) -> Result<bool> {
        match self {
            Self::MutableBuffer { .. } => {
                // For now, we might get an error if we try and
                // compile a chunk predicate (e.g. for tables that
                // don't exist in the chunk) which could signal the
                // chunk can't pass the predicate.

                // However, we can also get an error if there is some
                // unsupported operation and we need a way to
                // distinguish the two cases.
                Ok(true)
            }
            Self::ReadBuffer { .. } => {
                // TODO: ask Edd how he wants this wired up in the read buffer
                Ok(true)
            }
            Self::ParquetFile { .. } => {
                // TODO proper filtering for parquet files
                Ok(true)
            }
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
                let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                    Ok(chunk_predicate) => chunk_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "mutable buffer predicate not supported for column_names, falling back");
                        return Ok(None);
                    }
                };

                chunk
                    .column_names(table_name, &chunk_predicate, columns)
                    .context(MutableBufferChunk)
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
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented for column_names")
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
            Self::MutableBuffer { chunk, .. } => {
                use mutable_buffer::chunk::Error::UnsupportedColumnTypeForListingValues;

                let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                    Ok(chunk_predicate) => chunk_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "mutable buffer predicate not supported for column_values, falling back");
                        return Ok(None);
                    }
                };

                let values = chunk.tag_column_values(table_name, column_name, &chunk_predicate);

                // if the mutable buffer doesn't support getting
                // values for this kind of column, report back None
                if let Err(UnsupportedColumnTypeForListingValues { .. }) = values {
                    Ok(None)
                } else {
                    values.context(MutableBufferChunk)
                }
            }
            Self::ReadBuffer { .. } => {
                // TODO hook up read buffer API here when ready. Until
                // now, fallback to using a full plan
                // https://github.com/influxdata/influxdb_iox/issues/857
                Ok(None)
            }
            Self::ParquetFile { .. } => {
                unimplemented!("parquet file not implemented for column_values")
            }
        }
    }
}
