use std::sync::Arc;

/// The state
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ChunkState {
    /// Chunk can accept new writes
    Open,

    /// Chunk can still accept new writes, but will likely be closed soon
    Closing,

    /// Chunk is closed for new writes and has become read only
    Closed,

    /// Chunk is closed for new writes, and is actively moving to the read
    /// buffer
    Moving,

    /// Chunk has been completely loaded in the read buffer
    Moved,
}

/// The catalog representation of a Chunk in IOx. Note that a chunk
/// may exist in several physical locations at any given time (e.g. in
/// mutable buffer and in read buffer)
#[derive(Debug, PartialEq)]
pub struct Chunk {
    /// What partition does the chunk belong to?
    partition_key: Arc<String>,

    /// The ID of the chunk
    id: u32,

    /// The state of this chunk
    state: ChunkState,
    /* TODO: Additional fields
     * such as object_store_path, etc */
}

impl Chunk {
    /// Create a new chunk in the Open state
    pub(crate) fn new(partition_key: impl Into<String>, id: u32) -> Self {
        let partition_key = Arc::new(partition_key.into());

        Self {
            partition_key,
            id,
            state: ChunkState::Open,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn key(&self) -> &str {
        self.partition_key.as_ref()
    }

    pub fn state(&self) -> ChunkState {
        self.state
    }

    pub fn set_state(&mut self, state: ChunkState) {
        // TODO add state transition validation here?

        self.state = state;
    }
}
