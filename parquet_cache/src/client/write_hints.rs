use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use data_types::ParquetFileParams;
use futures::FutureExt;
use hyper::Method;
use object_store::{limit::LimitStore, path::Path, Error as ObjectStoreError, ObjectStore, Result};
use tower::{Service, ServiceExt};

use crate::data_types::{WriteHint, WriteHintAck, WriteHintRequestBody};
use crate::DataCacheObjectStore;

use super::request::RawRequest;

/// identifier for `object_store::Error::Generic`
const DATA_CACHE: &str = "write hint to data cache";

/// An [`ObjectStore`] which handles write hinting.
///
/// In some cases, the write hinting request does nothing (e.g. for direct-to-store impls).
#[async_trait]
pub trait WriteHintingObjectStore: ObjectStore {
    /// Handle any write hinting performed by the [`ObjectStore`].
    async fn write_hint<'a>(
        &self,
        location: &'a Path,
        new_file: &'a ParquetFileParams,
        ack_setting: WriteHintAck,
    ) -> Result<()>;
}

#[async_trait]
impl WriteHintingObjectStore for DataCacheObjectStore {
    /// Provide write hinting to data cache.
    ///
    /// Response is configuration based on [`WriteHintAck`].
    async fn write_hint<'a>(
        &self,
        location: &'a Path,
        new_file: &'a ParquetFileParams,
        ack_setting: WriteHintAck,
    ) -> Result<()> {
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: location.to_string(),
                hint: WriteHint::from(new_file),
                ack_setting,
            },
        )
        .map_err(|e| ObjectStoreError::Generic {
            store: DATA_CACHE,
            source: Box::new(e),
        })?;

        let key = location.to_string();

        let uri_parts = "/write-hint"
            .parse::<http::Uri>()
            .map(http::uri::Parts::from)
            .expect("should be valid uri");

        let req = RawRequest {
            method: Method::POST,
            uri_parts,
            key: Some(key),
            body: hyper::Body::from(buf.into_inner().freeze()),
            ..Default::default()
        };

        let mut cache = self.cache.clone();
        let service = cache.ready().await.map_err(|e| ObjectStoreError::Generic {
            store: DATA_CACHE,
            source: Box::new(e),
        })?;

        let write_hints = service.call(req);

        match ack_setting {
            WriteHintAck::Sent => {
                write_hints.now_or_never();
                Ok(())
            }
            WriteHintAck::Received => {
                // server responds ok after receipt
                write_hints.await.map_err(|e| ObjectStoreError::Generic {
                    store: DATA_CACHE,
                    source: Box::new(e),
                })?;
                Ok(())
            }
            WriteHintAck::Completed => {
                // server responds ok after downstream actions complete
                write_hints.await.map_err(|e| ObjectStoreError::Generic {
                    store: DATA_CACHE,
                    source: Box::new(e),
                })?;
                Ok(())
            }
        }
    }
}

#[async_trait]
impl<T: ObjectStore> WriteHintingObjectStore for LimitStore<T> {
    /// Enable our store interface to always use `Arc<dyn ObjectStore + WriteHinting>`.
    /// (Aws, Azure, and Gcp [`ObjectStore`] impls are all [`LimitStore`].)
    ///
    /// When data cache is not used, the write hinting does not occur.
    async fn write_hint<'a>(
        &self,
        _location: &'a Path,
        _new_file: &'a ParquetFileParams,
        _ack_setting: WriteHintAck,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use data_types::{
        ColumnId, ColumnSet, CompactionLevel, NamespaceId, ObjectStoreId, PartitionId, TableId,
        Timestamp,
    };
    use object_store::{
        aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
        limit::LimitStore,
    };

    use crate::client::mock::{build_cache_server_client, MockDirectStore};

    use super::*;

    fn new_file() -> ParquetFileParams {
        ParquetFileParams {
            namespace_id: NamespaceId::new(0),
            table_id: TableId::new(0),
            partition_id: PartitionId::new(0),
            partition_hash_id: None,
            object_store_id: ObjectStoreId::new(),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(5),
            file_size_bytes: 0,
            row_count: 0,
            compaction_level: CompactionLevel::Initial,
            created_at: Timestamp::new(1234),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
            max_l0_created_at: Timestamp::new(1234),
        }
    }

    #[tokio::test]
    async fn test_write_hinting_always_available() {
        // This test confirms that any external interfaces can always utilize
        // the object_store, without awareness of whether or not it's the data cache
        // or a direct_to_store.
        //
        //  if object_store.put(&location).await.is_ok() {
        //      object_store.write_hints(&location, new_files, ack_setting).await
        //  }
        //
        // This avoids leaking any configuration details (for conditional checks) across the codebase.

        let location = Path::from("my/scoped/data/file.parquet");
        let new_file = new_file();
        let ack_setting = WriteHintAck::Received;

        // impl with gcp store
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name("foo".to_string());
        let direct_store: Arc<dyn WriteHintingObjectStore> =
            Arc::new(LimitStore::new(builder.build().unwrap(), 10));
        assert!(direct_store
            .write_hint(&location, &new_file, ack_setting)
            .await
            .is_ok());

        // impl with aws store
        let builder = AmazonS3Builder::new()
            .with_bucket_name("foo".to_string())
            .with_region("mars".to_string());
        let direct_store: Arc<dyn WriteHintingObjectStore> =
            Arc::new(LimitStore::new(builder.build().unwrap(), 10));
        assert!(direct_store
            .write_hint(&location, &new_file, ack_setting)
            .await
            .is_ok());

        // impl with azure store
        let builder = MicrosoftAzureBuilder::new()
            .with_container_name("foo".to_string())
            .with_account("dabozz".to_string());
        let direct_store: Arc<dyn WriteHintingObjectStore> =
            Arc::new(LimitStore::new(builder.build().unwrap(), 10));
        assert!(direct_store
            .write_hint(&location, &new_file, ack_setting)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_write_hinting_hits_the_cache() {
        let direct_to_store = Arc::new(MockDirectStore::default());

        let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
        let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

        let location = Path::from("my/scoped/data/file.parquet");
        let new_file = new_file();
        let ack_setting = WriteHintAck::Received;

        assert!(object_store
            .write_hint(&location, &new_file, ack_setting)
            .await
            .is_ok());
        assert!(
            cache_server.was_called(&"/write-hint".to_string()),
            "write-hint should hit the cache server"
        ); // note: payload bytes will be asserted separately with the (non-mock-)server integration tests.
    }
}
