use arrow_deps::parquet::file::writer::TryClone;
use bytes::Bytes;
use object_store::{
    memory::InMemory, path::ObjectStorePath, path::Path, ObjectStore, ObjectStoreApi,
};
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use std::{
    collections::BTreeSet,
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Chunk {
    /// Partition this chunk belongs to
    pub partition_key: String,

    /// The id for this chunk
    pub id: u32,

    /// paths of files in object store, one for each table of this chunk
    pub object_store_paths: Vec<Path>,
    /* Note: table names can be extracted from the file and
     * the table meta data can be extracted from the parquet file so I do not
     * store them for now but we might need them here for easy-to-use reason. Will see */
}

impl Chunk {
    pub fn new(part_key: String, chunk_id: u32) -> Self {
        Self {
            partition_key: part_key,
            id: chunk_id,
            object_store_paths: vec![],
        }
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        // TODO: check if this table exists in the chunk
        if table_name.is_empty() {
            return false;
        }
        true
    }

    pub fn all_table_names(&self, names: &mut BTreeSet<String>) {
        // TODO
        names.insert("todo".to_string());
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        // TODO
        0
    }

    pub async fn write_to_object_store(
        &self,
        db_name: String,
        table_name: String,
        cursor: MemWriter,
    ) -> Result<()> {
        // Full path of the file in object store
        //    <writer id>/<database>/data/<partition key>/<chunk id>/<table
        // name>.parquet

        // TODO: write a function to return an object_store::path::Path's whose
        // root value is already "<writer id>/"
        // This function must be somewhere in server so it has knowledge of the
        // writer_id
        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let mut path = store.new_path();
        path.push_dir("100");

        // Continue building the path
        path.push_dir(db_name);
        path.push_dir("data");
        path.push_dir(self.partition_key.clone());
        path.push_dir(self.id.to_string());
        let file_name = format!("{}.parquet", table_name);
        path.set_file_name(file_name);

        // Put the given file in this file path
        let data = cursor
            .into_inner()
            .expect("Nothing else should have a reference here");

        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        // Now on object store
        store
            .put(
                &path,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)?;

        // TODO: Add this path into this chunk object_store_paths list
        // self.object_store_paths.push(path);  // Not work right now because it is not
        // mutable

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the
    /// Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .map(|mutex| mutex.into_inner().into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: Arc::clone(&self.mem),
        })
    }
}
