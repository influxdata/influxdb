use std::sync::Arc;

use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::DatabaseSchema;
use iox_query::QueryChunk;
use schema::Schema;

use crate::ParquetFileId;

use super::{parquet_chunk_from_file, WriteBufferImpl};

impl WriteBufferImpl {
    pub fn get_buffer_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let chunks = self
            .buffer
            .get_table_chunks(db_schema, table_name, filters, projection, ctx)?;

        Ok(chunks)
    }

    pub fn get_persisted_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        table_schema: Schema,
        _filters: &[Expr],
        last_compacted_parquet_file_id: Option<ParquetFileId>,
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let Some((db_id, db_schema)) = self.catalog().db_id_and_schema(database_name) else {
            return vec![];
        };
        let Some(table_id) = db_schema.table_name_to_id(table_name) else {
            return vec![];
        };
        let mut files = self.persisted_files.get_files(db_id, table_id);

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
                    &table_schema,
                    self.persister.object_store_url().clone(),
                    self.persister.object_store(),
                    chunk_order_offset,
                );

                Arc::new(parquet_chunk) as Arc<dyn QueryChunk>
            })
            .collect()
    }
}
