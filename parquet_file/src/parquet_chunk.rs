use object_store;

#[derive(Debug, Clone)]
pub struct ParquetChunk {
    /// Partition this chunk belongs to
    pub partition_key: String,
    
    /// The id for this chunk
    pub id: u32,

    /// paths of files in object store, one for each table of this chunk
    pub object_store_paths: Vec<object_store::path::Path>,

    // Note: table names can be extracted from the file and 
    // the table meta data can be extracted from the parquet file so I do not 
    // store them for now but we might need them here for easy-to-use reason. Will see
}

impl ParquetChunk {
    pub fn new(part_key: String, chunk_id: u32) -> Self {
        Self {
            partition_key: part_key,
            id: chunk_id,
            object_store_paths: vec![]
        }
    }
    
}