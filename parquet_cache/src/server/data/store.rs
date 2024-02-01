use std::{
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{
    stream::{BoxStream, StreamExt},
    FutureExt, TryStreamExt,
};
use pin_project::pin_project;
use tokio::fs::{create_dir_all, remove_dir, remove_file, File};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, Error, ReadBuf};
use tokio_util::codec::{BytesCodec, FramedRead};

/// object_store expected stream IO type
pub type StreamedObject = BoxStream<'static, object_store::Result<Bytes>>;

/// identifier for `object_store::Error::Generic`
const DATA_CACHE: &str = "local store accessor";

/// Access to stored data.
#[derive(Debug)]
pub struct LocalStore {
    dir: PathBuf,
}

impl LocalStore {
    pub fn new(path: Option<impl ToString>) -> Self {
        let dir = path.map(|p| p.to_string()).unwrap_or("/tmp".to_string());
        Self {
            dir: Path::new(dir.as_str()).to_owned(),
        }
    }

    fn local_path(&self, location: &String) -> PathBuf {
        self.dir.join(location)
    }

    /// Move a given file location, into cache
    pub async fn move_file_to_cache(&self, from: PathBuf, location: &String) -> Result<(), Error> {
        let to = self.local_path(location);
        match to.parent() {
            None => {
                return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "object location is not valid",
                ))
            }
            Some(path) => create_dir_all(path).await?,
        };
        std::fs::rename(from, to)
    }

    /// Async write operation
    pub async fn write_object(
        &self,
        location: &String,
        size: i64,
        mut stream: StreamedObject,
    ) -> Result<(), Error> {
        if location.starts_with('/') {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "object location cannot be an absolute path",
            ));
        }
        let path = self.local_path(location);
        let mut obj = AsyncStoreObject::new(path.as_path(), size).await?;

        while let Some(maybe_bytes) = stream.next().await {
            if maybe_bytes.is_err() {
                let _ = obj.delete().await;
                return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "error reading incoming byte stream",
                ));
            }

            match obj.write_all(&maybe_bytes.unwrap()).await {
                Ok(_) => continue,
                Err(e) => {
                    let _ = obj.delete().await;
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Read `GET /object` returns a stream
    pub async fn read_object(&self, location: &String) -> Result<StreamedObject, Error> {
        if location.starts_with('/') {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "object location cannot be an absolute path",
            ));
        }

        // Potential TODO: replace the StreamedObject with sendfile?
        // the the client can return a GetResultPayload::File() through the interface.
        let path = self.dir.join(location);
        Ok(AsyncStoreObject::open(path.as_path()).await?.read_stream())
    }

    /// Delete object in local store, such as on cache eviction.
    pub async fn delete_object(&self, location: &String) -> Result<(), Error> {
        if location.starts_with('/') {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "object location cannot be an absolute path",
            ));
        }

        let path = self.dir.join(location);
        AsyncStoreObject::open(path.as_path()).await?.delete().await
    }
}

#[pin_project]
pub struct AsyncStoreObject<'a> {
    #[pin]
    inner: File,
    path: &'a Path,
}

impl<'a> AsyncStoreObject<'a> {
    /// Create a new AsyncStoreObject, honoring the path provided.
    async fn new(path: &'a Path, size: i64) -> std::io::Result<Self> {
        // The path of the object (in the ObjectStore implementations) is:
        // <namespace_id>/<table_id>/<partition_id>/<object_store_id>.
        //
        // Future cache eviction policies may be mapped to resource allocation per table_id.
        create_dir_all(path.parent().unwrap_or(path)).await?;
        let file = File::create(path).await?;
        file.set_len(size as u64).await?;

        Ok(Self { inner: file, path })
    }

    async fn open(path: &'a Path) -> std::io::Result<Self> {
        Ok(Self {
            inner: File::open(path).await?,
            path,
        })
    }

    fn read_stream(self) -> StreamedObject {
        Box::pin(
            FramedRead::new(self.inner, BytesCodec::new())
                .map_ok(BytesMut::freeze)
                .map_err(|e| object_store::Error::Generic {
                    store: DATA_CACHE,
                    source: Box::new(e),
                }),
        )
    }

    async fn delete(&self) -> std::io::Result<()> {
        remove_file(self.path).await?;
        let dir = self.path.parent().unwrap_or(self.path);
        if dir.read_dir()?.next().is_none() {
            remove_dir(dir).await
        } else {
            Ok(())
        }
    }
}

impl<'a> AsyncRead for AsyncStoreObject<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        this.inner.poll_read(cx, buf)
    }
}

impl<'a> AsyncWrite for AsyncStoreObject<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        this.inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        Box::pin(this.inner.get_mut().sync_all()).poll_unpin(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        Box::pin(this.inner.get_mut().shutdown()).poll_unpin(cx)
    }
}

#[cfg(test)]
mod test {
    use std::{hash::Hasher, io::ErrorKind};

    use assert_matches::assert_matches;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    use super::*;

    async fn create_incoming_stream(file_path: &PathBuf) -> StreamedObject {
        let mut writeable = File::create(file_path)
            .await
            .expect("should create file in tempdir");

        for _ in 0..5 {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(1_000_000)
                .map(char::from)
                .collect();
            writeable
                .write_all(rand_string.as_bytes())
                .await
                .expect("should write to mock incoming");
        }
        writeable
            .sync_all()
            .await
            .expect("should fsync incoming mock data file");

        let readable = File::open(file_path)
            .await
            .expect("file should be readable");
        Box::pin(
            FramedRead::new(readable, BytesCodec::new())
                .map_ok(BytesMut::freeze)
                .map_err(|e| object_store::Error::Generic {
                    store: DATA_CACHE,
                    source: Box::new(e),
                }),
        )
    }

    async fn run_write_read_test() {
        let tempdir = TempDir::new().expect("should make tempdir");
        let incoming_file_path = tempdir.path().join("./incoming-io.txt");
        let obj_stream = create_incoming_stream(&incoming_file_path).await;

        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "obj/to/write.parquet";

        let write_res = local_store
            .write_object(&location.to_string(), 1_000_000 * 5, obj_stream)
            .await;
        assert_matches!(
            write_res,
            Ok(()),
            "write should return ok, instead found {:?}",
            write_res
        );

        let read_res = local_store.read_object(&location.to_string()).await;
        assert!(read_res.is_ok(), "read should return ok");

        // expected == data which was streamed in to WRITE
        let mut expected = Vec::new();
        File::open(incoming_file_path)
            .await
            .expect("should open original incoming data file")
            .read_to_end(&mut expected)
            .await
            .unwrap();
        let mut expected_hash = ahash::AHasher::default();
        expected_hash.write(&expected);

        // got == data that was WRITE then READ
        let mut got = Vec::new();
        tokio_util::io::StreamReader::new(read_res.unwrap())
            .read_to_end(&mut got)
            .await
            .unwrap();
        let mut got_hash = ahash::AHasher::default();
        got_hash.write(&got);

        assert_eq!(
            1_000_000 * 5,
            expected.len(),
            "incoming mock file stream was incorrect"
        );
        assert_eq!(
            expected.len(),
            got.len(),
            "expected {} bytes but found {} bytes",
            expected.len(),
            got.len()
        );
        assert_eq!(
            got_hash.finish(),
            expected_hash.finish(),
            "hash of file contents do not match"
        );

        tempdir.close().expect("should delete tempdir");
    }

    async fn can_duplicate_write_to_key() {
        let tempdir = TempDir::new().expect("should make tempdir");
        let incoming_file_path = tempdir.path().join("./incoming-dupe-writes.txt");

        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "obj/to/write.parquet";

        let write_res = local_store
            .write_object(
                &location.to_string(),
                1_000_000 * 5,
                create_incoming_stream(&incoming_file_path).await,
            )
            .await;
        assert!(
            write_res.is_ok(),
            "first write should succeed, instead found {:?}",
            write_res
        );

        let duplicate_write = local_store
            .write_object(
                &location.to_string(),
                1_000_000 * 5,
                create_incoming_stream(&incoming_file_path).await,
            )
            .await;
        assert!(
            duplicate_write.is_ok(),
            "second write should also succeed, instead found {:?}",
            duplicate_write
        );

        tempdir.close().expect("should delete tempdir");
    }

    async fn run_delete_test() {
        let tempdir = TempDir::new().expect("should make tempdir");
        let incoming_file_path = tempdir.path().join("./incoming-io.txt");
        let obj_stream = create_incoming_stream(&incoming_file_path).await;

        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "obj/to/write.parquet";

        let write_res = local_store
            .write_object(&location.to_string(), 1_000_000 * 5, obj_stream)
            .await;
        assert_matches!(
            write_res,
            Ok(()),
            "write should return ok, instead found {:?}",
            write_res
        );

        // confirm obj is written
        let written_obj_path = tempdir.path().join(location);
        let mut written_obj = Vec::new();
        File::open(written_obj_path.clone())
            .await
            .expect("should open original incoming data file")
            .read_to_end(&mut written_obj)
            .await
            .unwrap();
        assert_eq!(
            1_000_000 * 5,
            written_obj.len(),
            "object should be written to full length"
        );

        // delete obj
        let del_res = local_store.delete_object(&location.to_string()).await;
        assert!(del_res.is_ok(), "should return OK on delete");

        // confirm does not exist
        let should_be_err = File::open(written_obj_path).await;
        assert_matches!(
            should_be_err,
            Err(e) if e.kind() == ErrorKind::NotFound,
            "cache obj should not exist"
        );

        tempdir.close().expect("should delete tempdir");
    }

    async fn error_with_absolute_path_in_obj_key() {
        let tempdir = TempDir::new().expect("should make tempdir");
        let incoming_file_path = tempdir.path().join("./incoming-abs-key-path.txt");

        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "/absolute/pathed/object.parquet";

        let write_res = local_store
            .write_object(
                &location.to_string(),
                1_000_000 * 5,
                create_incoming_stream(&incoming_file_path).await,
            )
            .await;
        assert_matches!(
            write_res,
            Err(e) if e.to_string().contains("object location cannot be an absolute path"),
            "expected write to error, instead found {:?}",
            write_res
        );

        let read_res = local_store.read_object(&location.to_string()).await;
        assert!(read_res.is_err(), "expected read to error",);

        let delete_res = local_store.delete_object(&location.to_string()).await;
        assert!(delete_res.is_err(), "expected delete to error",);

        tempdir.close().expect("should delete tempdir");
    }

    async fn write_aborts_are_handled() {
        let stream_with_partial_write = Box::pin(tokio_stream::iter(vec![Err(
            object_store::Error::Generic {
                store: "error in bytes stream from remote object store",
                source: "delete on first write".into(),
            },
        )])) as StreamedObject;

        let tempdir = TempDir::new().expect("should make tempdir");
        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "obj/to/write.parquet";

        let write_res = local_store
            .write_object(
                &location.to_string(),
                1_000_000 * 5,
                stream_with_partial_write,
            )
            .await;
        assert_matches!(
            write_res,
            Err(e) if e.to_string().contains("error reading incoming byte stream"),
            "expected write to error, instead found {:?}",
            write_res
        );

        tempdir.close().expect("should delete tempdir");
    }

    async fn partial_files_are_deleted_on_write_abort() {
        let stream_with_partial_write = Box::pin(tokio_stream::iter(vec![
            Ok(Bytes::from(&b"good yield"[..])),
            Err(object_store::Error::Generic {
                store: "error in bytes stream from remote object store",
                source: "foobar".into(),
            }),
        ])) as StreamedObject;

        let tempdir = TempDir::new().expect("should make tempdir");
        let local_store = LocalStore::new(tempdir.path().to_str());
        let location = "obj/to/write.parquet";

        let write_res = local_store
            .write_object(
                &location.to_string(),
                1_000_000 * 5,
                stream_with_partial_write,
            )
            .await;
        assert_matches!(
            write_res,
            Err(e) if e.to_string().contains("error reading incoming byte stream"),
            "expected write to error, instead found {:?}",
            write_res
        );

        let incoming_file_path = tempdir.path().join("./incoming-partial.txt");
        let should_not_exist = File::open(incoming_file_path).await;
        assert_matches!(
            should_not_exist,
            Err(e) if e.kind() == ErrorKind::NotFound,
            "file partial should not exist"
        );

        tempdir.close().expect("should delete tempdir");
    }

    #[tokio::test]
    async fn test_write_read_object() {
        futures::join!(run_write_read_test(), can_duplicate_write_to_key(),);
    }

    #[tokio::test]
    async fn test_delete_object() {
        run_delete_test().await;
    }

    #[tokio::test]
    async fn test_error_handling() {
        futures::join!(
            error_with_absolute_path_in_obj_key(),
            write_aborts_are_handled(),
            partial_files_are_deleted_on_write_abort(),
        );
    }
}
