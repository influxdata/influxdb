use self::generated_types::{management_service_client::ManagementServiceClient, *};
use crate::{
    connection::Connection,
    error::Error,
    google::{longrunning::IoxOperation, OptionalField},
};
use bytes::Bytes;
use std::convert::TryInto;
use uuid::Uuid;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::management::v1::*;
    pub use generated_types::influxdata::iox::write_buffer::v1::*;
}

/// An IOx Management API client.
///
/// This client wraps the underlying `tonic` generated client with a
/// more ergonomic interface.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     management::{Client, generated_types::DatabaseRules},
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // Create a new database!
/// client
///     .create_database(DatabaseRules{
///     name: "bananas".to_string(),
///     ..Default::default()
/// })
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: ManagementServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: ManagementServiceClient::new(channel),
        }
    }

    /// Check if databases are loaded and ready for read and write.
    pub async fn get_server_status(&mut self) -> Result<ServerStatus, Error> {
        let response = self
            .inner
            .get_server_status(GetServerStatusRequest {})
            .await?;

        Ok(response
            .into_inner()
            .server_status
            .unwrap_field("server_status")?)
    }

    /// Creates a new IOx database.
    pub async fn create_database(&mut self, rules: DatabaseRules) -> Result<Uuid, Error> {
        let response = self
            .inner
            .create_database(CreateDatabaseRequest { rules: Some(rules) })
            .await?;

        let server_uuid = response.into_inner().uuid;
        let uuid = Uuid::from_slice(&server_uuid)
            .map_err(|e| {
                format!(
                    "Could not create UUID from server value {:?}: {}",
                    server_uuid, e
                )
            })
            .unwrap();

        Ok(uuid)
    }

    /// Updates the configuration for a database.
    pub async fn update_database(&mut self, rules: DatabaseRules) -> Result<DatabaseRules, Error> {
        let response = self
            .inner
            .update_database(UpdateDatabaseRequest { rules: Some(rules) })
            .await?;

        Ok(response.into_inner().rules.unwrap_field("rules")?)
    }

    /// List databases.
    ///
    /// See [`Self::get_database`] for the semanitcs of `omit_defaults`
    pub async fn list_databases(
        &mut self,
        omit_defaults: bool,
    ) -> Result<Vec<DatabaseRules>, Error> {
        let response = self
            .inner
            .list_databases(ListDatabasesRequest { omit_defaults })
            .await?;

        Ok(response.into_inner().rules)
    }

    /// List databases names
    pub async fn list_database_names(&mut self) -> Result<Vec<String>, Error> {
        // doesn't really matter as the name is present in all forms
        // of the config. Pick true to minimize bandwidth.
        let omit_defaults = true;

        let databases = self.list_databases(omit_defaults).await?;

        let names = databases
            .iter()
            .map(|rules| rules.name.to_string())
            .collect::<Vec<_>>();

        Ok(names)
    }

    /// Get database configuration
    ///
    /// If `omit_defaults` is false, return the current configuration
    /// that is being used by the server, with all default values
    /// filled in.
    ///
    /// If `omit_defaults` is true, returns only the persisted configuration (aka only
    /// fields which were was supplied when the database was created
    /// or last modified via UpdateDatabase)
    pub async fn get_database(
        &mut self,
        name: impl Into<String> + Send,
        omit_defaults: bool,
    ) -> Result<DatabaseRules, Error> {
        let response = self
            .inner
            .get_database(GetDatabaseRequest {
                name: name.into(),
                omit_defaults,
            })
            .await?;

        Ok(response.into_inner().rules.unwrap_field("rules")?)
    }

    /// Release database
    pub async fn release_database(
        &mut self,
        db_name: impl Into<String> + Send,
        uuid: Option<Uuid>,
    ) -> Result<Uuid, Error> {
        let db_name = db_name.into();
        let response = self
            .inner
            .release_database(ReleaseDatabaseRequest {
                db_name: db_name.clone(),
                uuid: uuid.map(|u| u.as_bytes().to_vec()).unwrap_or_default(),
            })
            .await?;

        let server_uuid = response.into_inner().uuid;
        let uuid = Uuid::from_slice(&server_uuid)
            .map_err(|e| {
                format!(
                    "Could not create UUID from server value {:?}: {}",
                    server_uuid, e
                )
            })
            .unwrap();

        Ok(uuid)
    }

    /// Claim database
    ///
    /// if `force` is true, forces the server to claim this database, even if it is
    /// ostensibly owned by another server.
    ///
    /// WARNING: If another server is currently writing to this
    /// database, corruption will very likely occur.
    pub async fn claim_database(&mut self, uuid: Uuid, force: bool) -> Result<String, Error> {
        let uuid_bytes = uuid.as_bytes().to_vec();

        let response = self
            .inner
            .claim_database(ClaimDatabaseRequest {
                uuid: uuid_bytes,
                force,
            })
            .await?;

        Ok(response.into_inner().db_name)
    }

    /// List chunks in a database.
    pub async fn list_chunks(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<Vec<Chunk>, Error> {
        let db_name = db_name.into();

        let response = self
            .inner
            .list_chunks(ListChunksRequest { db_name })
            .await?;

        Ok(response.into_inner().chunks)
    }

    /// List all partitions of the database
    pub async fn list_partitions(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<Vec<Partition>, Error> {
        let db_name = db_name.into();
        let response = self
            .inner
            .list_partitions(ListPartitionsRequest { db_name })
            .await?;

        Ok(response.into_inner().partitions)
    }

    /// Get details about a specific partition
    pub async fn get_partition(
        &mut self,
        db_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<Partition, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();

        let response = self
            .inner
            .get_partition(GetPartitionRequest {
                db_name,
                partition_key,
            })
            .await?;

        Ok(response.into_inner().partition.unwrap_field("partition")?)
    }

    /// List chunks in a partition
    pub async fn list_partition_chunks(
        &mut self,
        db_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<Vec<Chunk>, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();

        let response = self
            .inner
            .list_partition_chunks(ListPartitionChunksRequest {
                db_name,
                partition_key,
            })
            .await?;

        Ok(response.into_inner().chunks)
    }

    /// Create a new chunk in a partition
    pub async fn new_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<(), Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .new_partition_chunk(NewPartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
            })
            .await?;

        Ok(())
    }

    /// Creates a dummy job that for each value of the nanos field
    /// spawns a task that sleeps for that number of nanoseconds before
    /// returning
    pub async fn create_dummy_job(&mut self, nanos: Vec<u64>) -> Result<IoxOperation, Error> {
        let response = self
            .inner
            .create_dummy_job(CreateDummyJobRequest { nanos })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Closes the specified chunk in the specified partition and
    /// begins it moving to the read buffer.
    ///
    /// Returns the job tracking the data's movement
    pub async fn close_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_id: Bytes,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let response = self
            .inner
            .close_partition_chunk(ClosePartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Unload chunk from read buffer but keep it in object store.
    pub async fn unload_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_id: Bytes,
    ) -> Result<(), Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .unload_partition_chunk(UnloadPartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            })
            .await?;

        Ok(())
    }

    /// Load a chunk from the object store into the Read Buffer.
    pub async fn load_partition_chunk(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_id: Bytes,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let response = self
            .inner
            .load_partition_chunk(LoadPartitionChunkRequest {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Wipe potential preserved catalog of an uninitialized database.
    pub async fn wipe_preserved_catalog(
        &mut self,
        db_name: impl Into<String> + Send,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();

        let response = self
            .inner
            .wipe_preserved_catalog(WipePreservedCatalogRequest { db_name })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Rebuild preserved catalog of an uninitialized database
    pub async fn rebuild_preserved_catalog(
        &mut self,
        db_name: impl Into<String> + Send,
        force: bool,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();

        let response = self
            .inner
            .rebuild_preserved_catalog(RebuildPreservedCatalogRequest { db_name, force })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Skip replay of an uninitialized database.
    pub async fn skip_replay(&mut self, db_name: impl Into<String> + Send) -> Result<(), Error> {
        let db_name = db_name.into();

        self.inner
            .skip_replay(SkipReplayRequest { db_name })
            .await?;

        Ok(())
    }

    /// Drop partition from memory and (if persisted) from object store.
    pub async fn drop_partition(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<(), Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .drop_partition(DropPartitionRequest {
                db_name,
                partition_key,
                table_name,
            })
            .await?;

        Ok(())
    }

    /// Persist given partition.
    ///
    /// Errors if there is nothing to persist at the moment as per the lifecycle rules. If successful it returns the
    /// chunk that contains the persisted data.
    pub async fn persist_partition(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        force: bool,
    ) -> Result<(), Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        self.inner
            .persist_partition(PersistPartitionRequest {
                db_name,
                partition_key,
                table_name,
                force,
            })
            .await?;

        Ok(())
    }

    /// Compact given object store chunks (db, table, partition, chunks)
    ///
    /// Error if the chunks are not yet compacted and not contiguous
    pub async fn compact_object_store_chunks(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
        chunk_ids: Vec<Bytes>,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let response = self
            .inner
            .compact_object_store_chunks(CompactObjectStoreChunksRequest {
                db_name,
                partition_key,
                table_name,
                chunk_ids,
            })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }

    /// Compact all object store of a give partition
    pub async fn compact_object_store_partition(
        &mut self,
        db_name: impl Into<String> + Send,
        table_name: impl Into<String> + Send,
        partition_key: impl Into<String> + Send,
    ) -> Result<IoxOperation, Error> {
        let db_name = db_name.into();
        let partition_key = partition_key.into();
        let table_name = table_name.into();

        let response = self
            .inner
            .compact_object_store_partition(CompactObjectStorePartitionRequest {
                db_name,
                partition_key,
                table_name,
            })
            .await?;

        Ok(response
            .into_inner()
            .operation
            .unwrap_field("operation")?
            .try_into()?)
    }
}
