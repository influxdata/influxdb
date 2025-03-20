use super::*;

#[derive(Debug)]
pub struct ReplicaBufferChunk {
    pub core: BufferChunk,
    pub node_id: Arc<str>,
}

impl QueryChunk for ReplicaBufferChunk {
    fn stats(&self) -> Arc<datafusion::common::Statistics> {
        self.core.stats()
    }

    fn schema(&self) -> &schema::Schema {
        self.core.schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.core.partition_id()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        self.core.sort_key()
    }

    fn id(&self) -> ChunkId {
        self.core.id()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.core.may_contain_pk_duplicates()
    }

    fn data(&self) -> iox_query::QueryChunkData {
        self.core.data()
    }

    fn chunk_type(&self) -> &str {
        self.core.chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.core.order()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.core.as_any()
    }
}

#[derive(Debug)]
pub struct ReplicaParquetChunk {
    pub core: ParquetChunk,
    pub node_id: Arc<str>,
}

impl QueryChunk for ReplicaParquetChunk {
    fn stats(&self) -> Arc<datafusion::common::Statistics> {
        self.core.stats()
    }

    fn schema(&self) -> &schema::Schema {
        self.core.schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.core.partition_id()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        self.core.sort_key()
    }

    fn id(&self) -> ChunkId {
        self.core.id()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.core.may_contain_pk_duplicates()
    }

    fn data(&self) -> iox_query::QueryChunkData {
        self.core.data()
    }

    fn chunk_type(&self) -> &str {
        self.core.chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.core.order()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.core.as_any()
    }
}

#[derive(Debug)]
pub struct CompactedParquetChunk {
    pub core: ParquetChunk,
}

impl QueryChunk for CompactedParquetChunk {
    fn stats(&self) -> Arc<datafusion::common::Statistics> {
        self.core.stats()
    }

    fn schema(&self) -> &schema::Schema {
        self.core.schema()
    }

    fn partition_id(&self) -> &TransitionPartitionId {
        self.core.partition_id()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        self.core.sort_key()
    }

    fn id(&self) -> ChunkId {
        self.core.id()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.core.may_contain_pk_duplicates()
    }

    fn data(&self) -> iox_query::QueryChunkData {
        self.core.data()
    }

    fn chunk_type(&self) -> &str {
        self.core.chunk_type()
    }

    fn order(&self) -> ChunkOrder {
        self.core.order()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.core.as_any()
    }
}
