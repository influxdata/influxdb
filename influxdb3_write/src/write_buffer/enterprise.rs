use std::sync::Arc;

use datafusion::{catalog::Session, error::DataFusionError};
use influxdb3_catalog::catalog::{DatabaseSchema, TableDefinition};
use iox_query::QueryChunk;

use crate::{ChunkFilter, ParquetFileId};

use super::{parquet_chunk_from_file, WriteBufferImpl};

impl WriteBufferImpl {
    pub fn get_buffer_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let chunks = self
            .buffer
            .get_table_chunks(db_schema, table_def, filter, projection, ctx)?;

        Ok(chunks)
    }

    pub fn get_persisted_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter,
        last_compacted_parquet_file_id: Option<ParquetFileId>,
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut files =
            self.persisted_files
                .get_files_filtered(db_schema.id, table_def.table_id, filter);

        // filter out any files that have been compacted
        if let Some(last_parquet_file_id) = last_compacted_parquet_file_id {
            files.retain(|f| f.id > last_parquet_file_id);
        }

        files
            .into_iter()
            .map(|parquet_file| {
                chunk_order_offset += 1;

                let parquet_chunk = parquet_chunk_from_file(
                    &parquet_file,
                    &table_def.schema,
                    self.persister.object_store_url().clone(),
                    self.persister.object_store(),
                    chunk_order_offset,
                );

                Arc::new(parquet_chunk) as Arc<dyn QueryChunk>
            })
            .collect()
    }
}
