//! Catalog preservation and transaction handling.

use crate::{
    catalog::{
        interface::{CatalogParquetInfo, CatalogState, CheckpointData, ChunkAddrWithoutDatabase},
        internals::{
            proto_io::{load_transaction_proto, store_transaction_proto},
            proto_parse,
            types::{FileType, TransactionKey},
        },
    },
    metadata::IoxParquetMetaData,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use generated_types::influxdata::iox::catalog::v1 as proto;
use iox_object_store::{IoxObjectStore, ParquetFilePath, TransactionFilePath};
use object_store::{ObjectStore, ObjectStoreApi};
use observability_deps::tracing::{info, warn};
use parking_lot::RwLock;
use predicate::delete_predicate::DeletePredicate;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap, HashSet,
    },
    convert::TryInto,
    fmt::Debug,
    sync::Arc,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

pub use crate::catalog::internals::proto_io::Error as ProtoIOError;
pub use crate::catalog::internals::proto_parse::Error as ProtoParseError;

/// Current version for serialized transactions.
///
/// For breaking changes, this will change.
pub const TRANSACTION_VERSION: u32 = 19;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error during protobuf IO: {}", source))]
    ProtobufIOError { source: ProtoIOError },

    #[snafu(display("Internal: Error while parsing protobuf: {}", source))]
    ProtobufParseError { source: ProtoParseError },

    #[snafu(display("Error during store write operation: {}", source))]
    Write {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Error during store read operation: {}", source))]
    Read {
        source: <ObjectStore as ObjectStoreApi>::Error,
    },

    #[snafu(display("Missing transaction: {}", revision_counter))]
    MissingTransaction { revision_counter: u64 },

    #[snafu(display(
        "Wrong revision counter in transaction file: expected {} but found {}",
        expected,
        actual
    ))]
    WrongTransactionRevision { expected: u64, actual: u64 },

    #[snafu(display(
        "Wrong UUID for transaction file (revision: {}): expected {} but found {}",
        revision_counter,
        expected,
        actual
    ))]
    WrongTransactionUuid {
        revision_counter: u64,
        expected: Uuid,
        actual: Uuid,
    },

    #[snafu(display(
        "Wrong link to previous UUID in revision {}: expected {:?} but found {:?}",
        revision_counter,
        expected,
        actual
    ))]
    WrongTransactionLink {
        revision_counter: u64,
        expected: Option<Uuid>,
        actual: Option<Uuid>,
    },

    #[snafu(display("Path required but not provided"))]
    PathRequired {},

    #[snafu(display("Fork detected. Revision {} has two UUIDs {} and {}. Maybe two writer instances with the same server ID were running in parallel?", revision_counter, uuid1, uuid2))]
    Fork {
        revision_counter: u64,
        uuid1: Uuid,
        uuid2: Uuid,
    },

    #[snafu(display(
        "Format version of transaction file for revision {} is {} but only {:?} are supported",
        revision_counter,
        actual,
        expected
    ))]
    TransactionVersionMismatch {
        revision_counter: u64,
        actual: u32,
        expected: Vec<u32>,
    },

    #[snafu(display("Upgrade path not implemented/supported: {}", format))]
    UnsupportedUpgrade { format: String },

    #[snafu(display("Cannot decode parquet metadata: {}", source))]
    MetadataDecodingFailed { source: crate::metadata::Error },

    #[snafu(display("Catalog already exists"))]
    AlreadyExists {},

    #[snafu(display(
        "Internal: Found wrong encoding in serialized catalog file: Expected {:?} but got {:?}",
        expected,
        actual
    ))]
    WrongEncodingError {
        expected: proto::transaction::Encoding,
        actual: proto::transaction::Encoding,
    },

    #[snafu(display("Cannot commit transaction: {}", source))]
    CommitError { source: Box<Error> },

    #[snafu(display("Cannot add parquet file during load: {}", source))]
    AddError {
        source: crate::catalog::interface::CatalogStateAddError,
    },

    #[snafu(display("Cannot remove parquet file during load: {}", source))]
    RemoveError {
        source: crate::catalog::interface::CatalogStateRemoveError,
    },

    #[snafu(display("Delete predicate missing"))]
    DeletePredicateMissing,

    #[snafu(display("Cannot deserialize predicate: {}", source))]
    CannotDeserializePredicate {
        source: predicate::serialize::DeserializeError,
    },

    #[snafu(display("Cannot decode chunk id: {}", source))]
    CannotDecodeChunkId {
        source: data_types::chunk_metadata::BytesToChunkIdError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// In-memory view of the preserved catalog.
pub struct PreservedCatalog {
    // We need an RWLock AND a semaphore, so that readers are NOT blocked during an open
    // transactions. Note that this requires a new transaction to:
    //
    // 1. acquire the semaphore
    //
    // 2. get an writer lock (reader-visible critical section start)
    // 3. call `CatalogState::clone_or_keep` to get a new transaction-local state
    // 4. release writer lock (reader-visible critical section ends)
    //
    // 5. perform all transaction edits (e.g. adding parquet files)
    //
    // 6. get an writer lock (reader-visible critical section start)
    // 7. swap transaction-local state w/ global state
    // 8. release writer lock (reader-visible critical section ends)
    //
    // 9. release semaphore
    //
    // Note that there can only be a single transaction that acquires the semaphore.
    previous_tkey: RwLock<Option<TransactionKey>>,
    transaction_semaphore: Semaphore,

    /// Object store that backs this catalog.
    iox_object_store: Arc<IoxObjectStore>,

    /// If set, this UUID will be used for all transactions instead of a fresh UUIDv4.
    ///
    /// This can be useful for testing to achieve deterministic outputs.
    fixed_uuid: Option<Uuid>,

    /// If set, this start time will be used for all transaction instead of "now".
    ///
    /// This can be useful for testing to achieve deterministic outputs.
    fixed_timestamp: Option<DateTime<Utc>>,
}

impl PreservedCatalog {
    /// Checks if a preserved catalog exists.
    pub async fn exists(iox_object_store: &IoxObjectStore) -> Result<bool> {
        let list = iox_object_store
            .catalog_transaction_files()
            .await
            .context(Read)?
            .next()
            .await;
        match list {
            Some(l) => Ok(!l.context(Read)?.is_empty()),
            None => Ok(false),
        }
    }

    /// Find last transaction-start-timestamp.
    ///
    /// This method is designed to read and verify as little as possible and should also work on
    /// most broken catalogs.
    pub async fn find_last_transaction_timestamp(
        iox_object_store: &IoxObjectStore,
    ) -> Result<Option<DateTime<Utc>>> {
        let mut res = None;

        let mut stream = iox_object_store
            .catalog_transaction_files()
            .await
            .context(Read)?;

        while let Some(transaction_file_list) = stream.try_next().await.context(Read)? {
            for transaction_file_path in &transaction_file_list {
                match load_transaction_proto(iox_object_store, transaction_file_path).await {
                    Ok(proto) => match proto_parse::parse_timestamp(&proto.start_timestamp) {
                        Ok(ts) => {
                            res = Some(res.map_or(ts, |res: DateTime<Utc>| res.max(ts)));
                        }
                        Err(e) => warn!(%e, ?transaction_file_path, "Cannot parse timestamp"),
                    },
                    Err(e @ crate::catalog::internals::proto_io::Error::Read { .. }) => {
                        // bubble up IO error
                        return Err(Error::ProtobufIOError { source: e });
                    }
                    Err(e) => warn!(%e, ?transaction_file_path, "Cannot read transaction"),
                }
            }
        }
        Ok(res)
    }

    /// Deletes catalog.
    ///
    /// **Always create a backup before wiping your data!**
    ///
    /// This also works for broken catalogs. Also succeeds if no catalog is present.
    ///
    /// Note that wiping the catalog will NOT wipe any referenced parquet files.
    pub async fn wipe(iox_object_store: &IoxObjectStore) -> Result<()> {
        Ok(iox_object_store.wipe_catalog().await.context(Write)?)
    }

    /// Create new catalog w/o any data.
    ///
    /// An empty transaction will be used to mark the catalog start so that concurrent open but
    /// still-empty catalogs can easily be detected.
    pub async fn new_empty<S>(
        db_name: &str,
        iox_object_store: Arc<IoxObjectStore>,
        state_data: S::EmptyInput,
    ) -> Result<(Self, S)>
    where
        S: CatalogState + Send + Sync,
    {
        Self::new_empty_inner::<S>(db_name, iox_object_store, state_data, None, None).await
    }

    /// Same as [`new_empty`](Self::new_empty) but for testing.
    pub async fn new_empty_for_testing<S>(
        db_name: &str,
        iox_object_store: Arc<IoxObjectStore>,
        state_data: S::EmptyInput,
        fixed_uuid: Uuid,
        fixed_timestamp: DateTime<Utc>,
    ) -> Result<(Self, S)>
    where
        S: CatalogState + Send + Sync,
    {
        Self::new_empty_inner::<S>(
            db_name,
            iox_object_store,
            state_data,
            Some(fixed_uuid),
            Some(fixed_timestamp),
        )
        .await
    }

    pub async fn new_empty_inner<S>(
        db_name: &str,
        iox_object_store: Arc<IoxObjectStore>,
        state_data: S::EmptyInput,
        fixed_uuid: Option<Uuid>,
        fixed_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(Self, S)>
    where
        S: CatalogState + Send + Sync,
    {
        if Self::exists(&iox_object_store).await? {
            return Err(Error::AlreadyExists {});
        }
        let state = S::new_empty(db_name, state_data);

        let catalog = Self {
            previous_tkey: RwLock::new(None),
            transaction_semaphore: Semaphore::new(1),
            iox_object_store,
            fixed_uuid,
            fixed_timestamp,
        };

        // add empty transaction
        let transaction = catalog.open_transaction().await;
        transaction
            .commit()
            .await
            .map_err(Box::new)
            .context(CommitError)?;

        Ok((catalog, state))
    }

    /// Load existing catalog from store, if it exists.
    ///
    /// Loading starts at the latest checkpoint or -- if none exists -- at transaction `0`.
    /// Transactions before that point are neither verified nor are they required to exist.
    pub async fn load<S>(
        db_name: &str,
        iox_object_store: Arc<IoxObjectStore>,
        state_data: S::EmptyInput,
    ) -> Result<Option<(Self, S)>>
    where
        S: CatalogState + Send + Sync,
    {
        // parse all paths into revisions
        let mut transactions: HashMap<u64, Uuid> = HashMap::new();
        let mut max_revision = None;
        let mut last_checkpoint = None;

        let mut stream = iox_object_store
            .catalog_transaction_files()
            .await
            .context(Read)?;

        while let Some(transaction_file_list) = stream.try_next().await.context(Read)? {
            for transaction_file_path in &transaction_file_list {
                // keep track of the max
                max_revision = Some(
                    max_revision
                        .map(|m: u64| m.max(transaction_file_path.revision_counter))
                        .unwrap_or(transaction_file_path.revision_counter),
                );

                // keep track of latest checkpoint
                if transaction_file_path.is_checkpoint() {
                    last_checkpoint = Some(
                        last_checkpoint
                            .map(|m: u64| m.max(transaction_file_path.revision_counter))
                            .unwrap_or(transaction_file_path.revision_counter),
                    );
                }

                // insert but check for duplicates
                match transactions.entry(transaction_file_path.revision_counter) {
                    Occupied(o) => {
                        // sort for determinism
                        let (uuid1, uuid2) = if *o.get() < transaction_file_path.uuid {
                            (*o.get(), transaction_file_path.uuid)
                        } else {
                            (transaction_file_path.uuid, *o.get())
                        };

                        if uuid1 != uuid2 {
                            Fork {
                                revision_counter: transaction_file_path.revision_counter,
                                uuid1,
                                uuid2,
                            }
                            .fail()?;
                        }
                    }
                    Vacant(v) => {
                        v.insert(transaction_file_path.uuid);
                    }
                }
            }
        }

        // Check if there is any catalog stored at all
        if transactions.is_empty() {
            return Ok(None);
        }

        // setup empty state
        let mut state = S::new_empty(db_name, state_data);
        let mut last_tkey = None;

        // detect replay start
        let start_revision = last_checkpoint.unwrap_or(0);

        // detect end of replay process
        let max_revision = max_revision.expect("transactions list is not empty here");

        // read and replay delta revisions
        for rev in start_revision..=max_revision {
            let uuid = transactions.get(&rev).context(MissingTransaction {
                revision_counter: rev,
            })?;
            let tkey = TransactionKey {
                revision_counter: rev,
                uuid: *uuid,
            };
            let file_type = if Some(rev) == last_checkpoint {
                FileType::Checkpoint
            } else {
                FileType::Transaction
            };
            OpenTransaction::load_and_apply(
                &iox_object_store,
                tkey,
                &mut state,
                &last_tkey,
                file_type,
            )
            .await?;
            last_tkey = Some(tkey);
        }

        Ok(Some((
            Self {
                previous_tkey: RwLock::new(last_tkey),
                transaction_semaphore: Semaphore::new(1),
                iox_object_store,
                fixed_uuid: None,
                fixed_timestamp: None,
            },
            state,
        )))
    }

    /// Open a new transaction.
    ///
    /// Note that only a single transaction can be open at any time. This call will `await` until
    /// any outstanding transaction handle is dropped. The newly created transaction will contain
    /// the state after `await` (esp. post-blocking). This system is fair, which means that
    /// transactions are given out in the order they were requested.
    pub async fn open_transaction(&self) -> TransactionHandle<'_> {
        let uuid = self.fixed_uuid.unwrap_or_else(Uuid::new_v4);
        let start_timestamp = self.fixed_timestamp.unwrap_or_else(Utc::now);
        TransactionHandle::new(self, uuid, start_timestamp).await
    }

    /// Get latest revision counter.
    pub fn revision_counter(&self) -> u64 {
        self.previous_tkey
            .read()
            .map(|tkey| tkey.revision_counter)
            .expect("catalog should have at least an empty transaction")
    }

    /// Get latest revision UUID.
    pub fn revision_uuid(&self) -> Uuid {
        self.previous_tkey
            .read()
            .map(|tkey| tkey.uuid)
            .expect("catalog should have at least an empty transaction")
    }

    /// Object store used by this catalog.
    pub fn iox_object_store(&self) -> Arc<IoxObjectStore> {
        Arc::clone(&self.iox_object_store)
    }
}

impl Debug for PreservedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PreservedCatalog{{..}}")
    }
}

/// Tracker for an open, uncommitted transaction.
struct OpenTransaction {
    proto: proto::Transaction,
}

impl OpenTransaction {
    /// Private API to create new transaction, users should always use
    /// [`PreservedCatalog::open_transaction`].
    fn new(
        previous_tkey: &Option<TransactionKey>,
        uuid: Uuid,
        start_timestamp: DateTime<Utc>,
    ) -> Self {
        let (revision_counter, previous_uuid) = match previous_tkey {
            Some(tkey) => (
                tkey.revision_counter + 1,
                tkey.uuid.as_bytes().to_vec().into(),
            ),
            None => (0, Bytes::new()),
        };

        Self {
            proto: proto::Transaction {
                actions: vec![],
                version: TRANSACTION_VERSION,
                uuid: uuid.as_bytes().to_vec().into(),
                revision_counter,
                previous_uuid,
                start_timestamp: Some(start_timestamp.into()),
                encoding: proto::transaction::Encoding::Delta.into(),
            },
        }
    }

    fn tkey(&self) -> TransactionKey {
        TransactionKey {
            revision_counter: self.proto.revision_counter,
            uuid: Uuid::from_slice(&self.proto.uuid).expect("UUID was checked before"),
        }
    }

    /// Handle the given action and populate data to the catalog state.
    ///
    /// This deserializes the action state and passes it to the correct method in [`CatalogState`].
    ///
    /// Note that this method is primarily for replaying transactions and will NOT append the given
    /// action to the current transaction. If you want to store the given action (e.g. during an
    /// in-progress transaction), use [`record_action`](Self::record_action).
    fn handle_action<S>(
        state: &mut S,
        action: proto::transaction::action::Action,
        iox_object_store: &Arc<IoxObjectStore>,
    ) -> Result<()>
    where
        S: CatalogState,
    {
        match action {
            proto::transaction::action::Action::Upgrade(u) => {
                UnsupportedUpgrade { format: u.format }.fail()?;
            }
            proto::transaction::action::Action::AddParquet(a) => {
                let path =
                    proto_parse::parse_dirs_and_filename(a.path.as_ref().context(PathRequired)?)
                        .context(ProtobufParseError)?;
                let file_size_bytes = a.file_size_bytes as usize;

                let metadata = IoxParquetMetaData::from_thrift_bytes(a.metadata.as_ref().to_vec());

                // try to decode to ensure catalog is OK
                metadata.decode().context(MetadataDecodingFailed)?;

                let metadata = Arc::new(metadata);

                state
                    .add(
                        Arc::clone(iox_object_store),
                        CatalogParquetInfo {
                            path,
                            file_size_bytes,
                            metadata,
                        },
                    )
                    .context(AddError)?;
            }
            proto::transaction::action::Action::RemoveParquet(a) => {
                let path =
                    proto_parse::parse_dirs_and_filename(a.path.as_ref().context(PathRequired)?)
                        .context(ProtobufParseError)?;
                state.remove(&path).context(RemoveError)?;
            }
            proto::transaction::action::Action::DeletePredicate(d) => {
                let predicate = Arc::new(
                    predicate::serialize::deserialize(
                        &d.predicate.context(DeletePredicateMissing)?,
                    )
                    .context(CannotDeserializePredicate)?,
                );
                let chunks = d
                    .chunks
                    .into_iter()
                    .map(|chunk| {
                        let addr = ChunkAddrWithoutDatabase {
                            table_name: Arc::from(chunk.table_name),
                            partition_key: Arc::from(chunk.partition_key),
                            chunk_id: chunk.chunk_id.try_into().context(CannotDecodeChunkId)?,
                        };
                        Ok(addr)
                    })
                    .collect::<Result<Vec<ChunkAddrWithoutDatabase>, Error>>()?;
                state.delete_predicate(predicate, chunks);
            }
        };
        Ok(())
    }

    /// Record action to protobuf.
    fn record_action(&mut self, action: proto::transaction::action::Action) {
        self.proto.actions.push(proto::transaction::Action {
            action: Some(action),
        });
    }

    /// Commit to mutable catalog and return previous transaction key.
    fn commit(self, previous_tkey: &mut Option<TransactionKey>) -> Option<TransactionKey> {
        let mut tkey = Some(self.tkey());
        std::mem::swap(previous_tkey, &mut tkey);
        tkey
    }

    /// Abort transaction
    fn abort(self) {}

    async fn store(&self, iox_object_store: &IoxObjectStore) -> Result<()> {
        let path =
            TransactionFilePath::new_transaction(self.tkey().revision_counter, self.tkey().uuid);
        store_transaction_proto(iox_object_store, &path, &self.proto)
            .await
            .context(ProtobufIOError)?;
        Ok(())
    }

    async fn load_and_apply<S>(
        iox_object_store: &Arc<IoxObjectStore>,
        tkey: TransactionKey,
        state: &mut S,
        last_tkey: &Option<TransactionKey>,
        file_type: FileType,
    ) -> Result<()>
    where
        S: CatalogState + Send,
    {
        // recover state from store
        let path = match file_type {
            FileType::Transaction => {
                TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid)
            }
            FileType::Checkpoint => {
                TransactionFilePath::new_checkpoint(tkey.revision_counter, tkey.uuid)
            }
        };
        let proto = load_transaction_proto(iox_object_store, &path)
            .await
            .context(ProtobufIOError)?;

        // sanity-check file content
        if proto.version != TRANSACTION_VERSION {
            TransactionVersionMismatch {
                revision_counter: tkey.revision_counter,
                actual: proto.version,
                // we only support a single version right now
                expected: vec![TRANSACTION_VERSION],
            }
            .fail()?;
        }
        if proto.revision_counter != tkey.revision_counter {
            WrongTransactionRevision {
                actual: proto.revision_counter,
                expected: tkey.revision_counter,
            }
            .fail()?
        }
        let uuid_actual =
            proto_parse::parse_uuid_required(&proto.uuid).context(ProtobufParseError)?;
        if uuid_actual != tkey.uuid {
            WrongTransactionUuid {
                revision_counter: tkey.revision_counter,
                expected: tkey.uuid,
                actual: uuid_actual,
            }
            .fail()?
        }
        let encoding = file_type.encoding();
        if encoding == proto::transaction::Encoding::Delta {
            // only verify chain for delta encodings
            let last_uuid_actual =
                proto_parse::parse_uuid(&proto.previous_uuid).context(ProtobufParseError)?;
            let last_uuid_expected = last_tkey.as_ref().map(|tkey| tkey.uuid);
            if last_uuid_actual != last_uuid_expected {
                WrongTransactionLink {
                    revision_counter: tkey.revision_counter,
                    expected: last_uuid_expected,
                    actual: last_uuid_actual,
                }
                .fail()?;
            }
        }
        // verify we can parse the timestamp (checking that no error is raised)
        proto_parse::parse_timestamp(&proto.start_timestamp).context(ProtobufParseError)?;
        let encoding_actual =
            proto_parse::parse_encoding(proto.encoding).context(ProtobufParseError)?;
        if encoding_actual != encoding {
            return Err(Error::WrongEncodingError {
                actual: encoding_actual,
                expected: encoding,
            });
        }

        // apply
        for action in proto.actions {
            if let Some(action) = action.action {
                Self::handle_action(state, action, iox_object_store)?;
            }
        }

        Ok(())
    }
}

/// Handle for an open uncommitted transaction.
///
/// Dropping this object w/o calling [`commit`](Self::commit) will issue a warning.
pub struct TransactionHandle<'c> {
    catalog: &'c PreservedCatalog,

    // NOTE: The following two must be an option so we can `take` them during `Self::commit`.
    permit: Option<SemaphorePermit<'c>>,
    transaction: Option<OpenTransaction>,
}

impl<'c> TransactionHandle<'c> {
    async fn new(
        catalog: &'c PreservedCatalog,
        uuid: Uuid,
        start_timestamp: DateTime<Utc>,
    ) -> TransactionHandle<'c> {
        // first acquire semaphore (which is only being used for transactions), then get state lock
        let permit = catalog
            .transaction_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");
        let previous_tkey_guard = catalog.previous_tkey.write();

        let transaction = OpenTransaction::new(&previous_tkey_guard, uuid, start_timestamp);

        // free state for readers again
        drop(previous_tkey_guard);

        let tkey = transaction.tkey();
        info!(?tkey, "transaction started");

        Self {
            catalog,
            transaction: Some(transaction),
            permit: Some(permit),
        }
    }

    /// Get revision counter for this transaction.
    pub fn revision_counter(&self) -> u64 {
        self.transaction
            .as_ref()
            .expect("No transaction in progress?")
            .tkey()
            .revision_counter
    }

    /// Get UUID for this transaction
    pub fn uuid(&self) -> Uuid {
        self.transaction
            .as_ref()
            .expect("No transaction in progress?")
            .tkey()
            .uuid
    }

    /// Write data to object store and commit transaction to underlying catalog.
    ///
    /// # Checkpointing
    /// A [`CheckpointHandle`] will be returned that allows the caller to create a checkpoint. Note
    /// that this handle holds a transaction lock, so it's safe to assume that no other transaction
    /// is in-progress while the caller prepares the checkpoint.
    ///
    /// # Error Handling
    /// When this function returns with an error, it MUST be assumed that the commit has failed and
    /// all actions recorded with this handle are NOT preserved.
    pub async fn commit(mut self) -> Result<CheckpointHandle<'c>> {
        let t = std::mem::take(&mut self.transaction)
            .expect("calling .commit on a closed transaction?!");
        let tkey = t.tkey();

        // write to object store
        match t.store(&self.catalog.iox_object_store).await {
            Ok(()) => {
                // commit to catalog
                let previous_tkey = self.commit_inner(t);
                info!(?tkey, "transaction committed");

                // maybe create a checkpoint
                // IMPORTANT: Create the checkpoint AFTER commiting the transaction to object store
                //            and to the in-memory state. Checkpoints are an optional optimization
                //            and are not required to materialize a transaction.
                Ok(CheckpointHandle {
                    catalog: self.catalog,
                    tkey,
                    previous_tkey,
                    permit: self.permit.take().expect("transaction already dropped?!"),
                })
            }
            Err(e) => {
                warn!(?tkey, "failure while writing transaction, aborting");
                t.abort();
                Err(e)
            }
        }
    }

    /// Commit helper function.
    ///
    /// This function mostly exists for the following reasons:
    ///
    /// - the read-write guard for the inner catalog state should be limited in scope to avoid long
    ///   write-locks
    /// - rustc seems to fold the guard into the async generator state even when we `drop` it
    ///   quickly, making the resulting future `!Send`. However tokio requires our futures to be
    ///   `Send`.
    fn commit_inner(&self, t: OpenTransaction) -> Option<TransactionKey> {
        let mut previous_tkey_guard = self.catalog.previous_tkey.write();
        t.commit(&mut previous_tkey_guard)
    }

    /// Abort transaction w/o commit.
    pub fn abort(mut self) {
        let t = std::mem::take(&mut self.transaction)
            .expect("calling .commit on a closed transaction?!");
        t.abort()
    }

    /// Add a new parquet file to the catalog.
    pub fn add_parquet(&mut self, info: &CatalogParquetInfo) {
        let CatalogParquetInfo {
            path,
            file_size_bytes,
            metadata,
        } = info;

        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .record_action(proto::transaction::action::Action::AddParquet(
                proto::AddParquet {
                    path: Some(proto_parse::unparse_dirs_and_filename(path)),
                    metadata: Bytes::from(metadata.thrift_bytes().to_vec()),
                    file_size_bytes: *file_size_bytes as u64,
                },
            ));
    }

    /// Remove a parquet file from the catalog.
    pub fn remove_parquet(&mut self, path: &ParquetFilePath) {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .record_action(proto::transaction::action::Action::RemoveParquet(
                proto::RemoveParquet {
                    path: Some(proto_parse::unparse_dirs_and_filename(path)),
                },
            ));
    }

    /// Register new delete predicate.
    pub fn delete_predicate(
        &mut self,
        predicate: &DeletePredicate,
        chunks: &[ChunkAddrWithoutDatabase],
    ) {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .record_action(proto::transaction::action::Action::DeletePredicate(
                proto::DeletePredicate {
                    predicate: Some(predicate::serialize::serialize(predicate)),
                    chunks: chunks
                        .iter()
                        .map(|chunk| proto::ChunkAddr {
                            table_name: chunk.table_name.to_string(),
                            partition_key: chunk.partition_key.to_string(),
                            chunk_id: chunk.chunk_id.into(),
                        })
                        .collect(),
                },
            ));
    }
}

impl<'c> Debug for TransactionHandle<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.transaction {
            Some(t) => write!(f, "TransactionHandle(open, {})", t.tkey()),
            None => write!(f, "TransactionHandle(closed)"),
        }
    }
}

impl<'c> Drop for TransactionHandle<'c> {
    fn drop(&mut self) {
        if let Some(t) = self.transaction.take() {
            warn!(?self, "dropped uncommitted transaction, calling abort");
            t.abort();
        }
    }
}

/// Handle that allows to create a checkpoint after a transaction.
///
/// This handle holds a transaction lock.
pub struct CheckpointHandle<'c> {
    catalog: &'c PreservedCatalog,

    // metadata about the just-committed transaction
    tkey: TransactionKey,
    previous_tkey: Option<TransactionKey>,

    // NOTE: The permit is technically used since we use it to reference the semaphore. It
    //       implements `drop` which we rely on.
    #[allow(dead_code)]
    permit: SemaphorePermit<'c>,
}

impl<'c> CheckpointHandle<'c> {
    /// Create a checkpoint for the just-committed transaction.
    ///
    /// Note that `checkpoint_data` must contain the state INCLUDING the just-committed transaction.
    ///
    /// # Error Handling
    /// If the checkpoint creation fails, the commit will still be treated as completed since the
    /// checkpoint is a mere optimization to speed up transaction replay and allow to prune the
    /// history.
    pub async fn create_checkpoint(self, checkpoint_data: CheckpointData) -> Result<()> {
        let iox_object_store = self.catalog.iox_object_store();

        // collect actions for transaction
        let mut actions = Self::create_actions_for_files(checkpoint_data.files);
        actions.append(&mut Self::create_actions_for_delete_predicates(
            checkpoint_data.delete_predicates,
        )?);

        // assemble and store checkpoint protobuf
        let proto = proto::Transaction {
            actions,
            version: TRANSACTION_VERSION,
            uuid: self.tkey.uuid.as_bytes().to_vec().into(),
            revision_counter: self.tkey.revision_counter,
            previous_uuid: self
                .previous_tkey
                .map_or_else(Bytes::new, |tkey| tkey.uuid.as_bytes().to_vec().into()),
            start_timestamp: Some(Utc::now().into()),
            encoding: proto::transaction::Encoding::Full.into(),
        };
        let path = TransactionFilePath::new_checkpoint(self.tkey.revision_counter, self.tkey.uuid);
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .context(ProtobufIOError)?;

        Ok(())
    }

    fn create_actions_for_files(
        files: HashMap<ParquetFilePath, CatalogParquetInfo>,
    ) -> Vec<proto::transaction::Action> {
        // sort by key (= path) for deterministic output
        let files = {
            let mut tmp: Vec<_> = files.into_iter().collect();
            tmp.sort_by_key(|(path, _metadata)| path.clone());
            tmp
        };

        files
            .into_iter()
            .map(|(_, info)| {
                let CatalogParquetInfo {
                    file_size_bytes,
                    metadata,
                    path,
                } = info;

                proto::transaction::Action {
                    action: Some(proto::transaction::action::Action::AddParquet(
                        proto::AddParquet {
                            path: Some(proto_parse::unparse_dirs_and_filename(&path)),
                            file_size_bytes: file_size_bytes as u64,
                            metadata: Bytes::from(metadata.thrift_bytes().to_vec()),
                        },
                    )),
                }
            })
            .collect()
    }

    fn create_actions_for_delete_predicates(
        delete_predicates: HashMap<Arc<DeletePredicate>, HashSet<ChunkAddrWithoutDatabase>>,
    ) -> Result<Vec<proto::transaction::Action>, Error> {
        // sort by key (= path) for deterministic output
        let delete_predicates = {
            let mut tmp: Vec<_> = delete_predicates.into_iter().collect();
            tmp.sort_by_key(|(predicate, _chunks)| Arc::clone(predicate));
            tmp
        };

        delete_predicates
            .into_iter()
            .map(|(predicate, chunks)| {
                // sort chunks for deterministic output
                let chunks = {
                    let mut tmp: Vec<_> = chunks.into_iter().collect();
                    tmp.sort();
                    tmp
                };

                let action = proto::DeletePredicate {
                    predicate: Some(predicate::serialize::serialize(&predicate)),
                    chunks: chunks
                        .iter()
                        .map(|chunk| proto::ChunkAddr {
                            table_name: chunk.table_name.to_string(),
                            partition_key: chunk.partition_key.to_string(),
                            chunk_id: chunk.chunk_id.into(),
                        })
                        .collect(),
                };
                Ok(proto::transaction::Action {
                    action: Some(proto::transaction::action::Action::DeletePredicate(action)),
                })
            })
            .collect()
    }

    /// Get revision counter for this transaction.
    pub fn revision_counter(&self) -> u64 {
        self.tkey.revision_counter
    }

    /// Get UUID for this transaction
    pub fn uuid(&self) -> Uuid {
        self.tkey.uuid
    }
}

impl<'c> Debug for CheckpointHandle<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointHandle")
            .field("tkey", &self.tkey)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use bytes::Bytes;

    use super::*;
    use crate::catalog::test_helpers::{
        break_catalog_with_weird_version, create_delete_predicate, exists, load_err, load_ok,
        new_empty, TestCatalogState, DB_NAME,
    };
    use crate::test_utils::{chunk_addr, make_iox_object_store, make_metadata, TestSize};

    #[tokio::test]
    async fn test_create_empty() {
        let iox_object_store = make_iox_object_store().await;

        assert!(!exists(&iox_object_store).await);
        assert!(load_ok(&iox_object_store).await.is_none());

        new_empty(&iox_object_store).await;

        assert!(exists(&iox_object_store).await);
        assert!(load_ok(&iox_object_store).await.is_some());
    }

    #[tokio::test]
    async fn test_inmem_commit_semantics() {
        let iox_object_store = make_iox_object_store().await;
        assert_single_catalog_inmem_works(&iox_object_store).await;
    }

    #[tokio::test]
    async fn test_store_roundtrip() {
        let iox_object_store = make_iox_object_store().await;
        assert_catalog_roundtrip_works(&iox_object_store).await;
    }

    #[tokio::test]
    async fn test_load_from_empty_store() {
        let iox_object_store = make_iox_object_store().await;
        assert!(load_ok(&iox_object_store).await.is_none());
    }

    #[tokio::test]
    async fn test_missing_transaction() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // remove transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        checked_delete(&iox_object_store, &path).await;

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(err.to_string(), "Missing transaction: 0",);
    }

    #[tokio::test]
    async fn test_transaction_version_mismatch() {
        let iox_object_store = make_iox_object_store().await;
        assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        let (catalog, _state) = load_ok(&iox_object_store).await.unwrap();
        break_catalog_with_weird_version(&catalog).await;

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            format!(
                "Format version of transaction file for revision 2 is 42 but only [{}] are \
                 supported",
                TRANSACTION_VERSION
            )
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_revision() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.revision_counter = 42;
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Wrong revision counter in transaction file: expected 0 but found 42"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_uuid() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        let uuid_expected = Uuid::from_slice(&proto.uuid).unwrap();
        let uuid_actual = Uuid::nil();
        proto.uuid = uuid_actual.as_bytes().to_vec().into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            format!(
                "Wrong UUID for transaction file (revision: 0): expected {} but found {}",
                uuid_expected, uuid_actual
            )
        );
    }

    #[tokio::test]
    async fn test_missing_transaction_uuid() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.uuid = Bytes::new();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: UUID required but not provided"
        );
    }

    #[tokio::test]
    async fn test_broken_transaction_uuid() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.uuid = Bytes::from("foo");
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Cannot parse UUID: invalid bytes length: \
             expected 16, found 3"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_start() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.previous_uuid = Uuid::nil().as_bytes().to_vec().into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Wrong link to previous UUID in revision 0: expected None but found \
             Some(00000000-0000-0000-0000-000000000000)"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_middle() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[1];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.previous_uuid = Uuid::nil().as_bytes().to_vec().into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            format!(
                "Wrong link to previous UUID in revision 1: expected Some({}) but found \
                 Some(00000000-0000-0000-0000-000000000000)",
                trace.tkeys[0].uuid
            )
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_broken() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.previous_uuid = Bytes::from("foo");
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Cannot parse UUID: invalid bytes length: \
             expected 16, found 3"
        );
    }

    #[tokio::test]
    async fn test_broken_protobuf() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let data = Bytes::from("foo");

        iox_object_store
            .put_catalog_transaction_file(&path, data)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Error during protobuf IO: Error during protobuf deserialization: failed to decode \
             Protobuf message: invalid wire type value: 6"
        );
    }

    #[tokio::test]
    async fn test_transaction_handle_debug() {
        let iox_object_store = make_iox_object_store().await;
        let (catalog, _state) = new_empty(&iox_object_store).await;
        let mut t = catalog.open_transaction().await;

        // open transaction
        t.transaction.as_mut().unwrap().proto.uuid = Uuid::nil().as_bytes().to_vec().into();
        assert_eq!(
            format!("{:?}", t),
            "TransactionHandle(open, 1.00000000-0000-0000-0000-000000000000)"
        );

        // "closed" transaction
        t.transaction = None;
        assert_eq!(format!("{:?}", t), "TransactionHandle(closed)");
    }

    #[tokio::test]
    async fn test_fork_transaction() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // re-create transaction file with different UUID
        assert!(trace.tkeys.len() >= 2);
        let mut tkey = trace.tkeys[1];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        let old_uuid = tkey.uuid;

        let new_uuid = Uuid::new_v4();
        tkey.uuid = new_uuid;
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        proto.uuid = new_uuid.as_bytes().to_vec().into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        let (uuid1, uuid2) = if old_uuid < new_uuid {
            (old_uuid, new_uuid)
        } else {
            (new_uuid, old_uuid)
        };
        assert_eq!(
            err.to_string(),
            format!(
                "Fork detected. Revision 1 has two UUIDs {} and {}. Maybe two writer instances \
                with the same server ID were running in parallel?",
                uuid1, uuid2
            )
        );
    }

    #[tokio::test]
    async fn test_fork_checkpoint() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // create checkpoint file with different UUID
        assert!(trace.tkeys.len() >= 2);
        let mut tkey = trace.tkeys[1];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        let old_uuid = tkey.uuid;

        let new_uuid = Uuid::new_v4();
        tkey.uuid = new_uuid;
        let path = TransactionFilePath::new_checkpoint(tkey.revision_counter, tkey.uuid);
        proto.uuid = new_uuid.as_bytes().to_vec().into();
        proto.encoding = proto::transaction::Encoding::Full.into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        let (uuid1, uuid2) = if old_uuid < new_uuid {
            (old_uuid, new_uuid)
        } else {
            (new_uuid, old_uuid)
        };
        assert_eq!(
            err.to_string(),
            format!(
                "Fork detected. Revision 1 has two UUIDs {} and {}. Maybe two writer instances \
                with the same server ID were running in parallel?",
                uuid1, uuid2
            )
        );
    }

    #[tokio::test]
    async fn test_unsupported_upgrade() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.actions.push(proto::transaction::Action {
            action: Some(proto::transaction::action::Action::Upgrade(
                proto::Upgrade {
                    format: "foo".to_string(),
                },
            )),
        });
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Upgrade path not implemented/supported: foo",
        );
    }

    #[tokio::test]
    async fn test_missing_start_timestamp() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.start_timestamp = None;
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Datetime required but missing in serialized \
             catalog"
        );
    }

    #[tokio::test]
    async fn test_broken_start_timestamp() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.start_timestamp = Some(generated_types::google::protobuf::Timestamp {
            seconds: 0,
            nanos: -1,
        });
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Cannot parse datetime in serialized catalog: \
             out of range integral type conversion attempted"
        );
    }

    #[tokio::test]
    async fn test_broken_encoding() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.encoding = -1;
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Cannot parse encoding in serialized catalog: \
             -1 is not a valid, specified variant"
        );
    }

    #[tokio::test]
    async fn test_wrong_encoding_in_transaction_file() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.encoding = proto::transaction::Encoding::Full.into();
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Found wrong encoding in serialized catalog file: Expected Delta but got Full"
        );
    }

    #[tokio::test]
    async fn test_missing_encoding_in_transaction_file() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.encoding = 0;
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Error while parsing protobuf: Cannot parse encoding in serialized catalog: \
            0 is not a valid, specified variant"
        );
    }

    #[tokio::test]
    async fn test_wrong_encoding_in_checkpoint_file() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        let path = TransactionFilePath::new_checkpoint(tkey.revision_counter, tkey.uuid);
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let err = load_err(&iox_object_store).await;
        assert_eq!(
            err.to_string(),
            "Internal: Found wrong encoding in serialized catalog file: Expected Full but got Delta"
        );
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let iox_object_store = make_iox_object_store().await;

        // use common test as baseline
        let mut trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // re-open catalog
        let (catalog, mut state) = load_ok(&iox_object_store).await.unwrap();

        // create empty transaction w/ checkpoint (the delta transaction file is not required for catalog loading)
        {
            let transaction = catalog.open_transaction().await;
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }
        trace.record(&catalog, &state, false);

        // create another transaction on-top that adds a file (this transaction will be required to load the full state)
        {
            let addr = chunk_addr(1337);
            let (path, metadata) =
                make_metadata(&iox_object_store, "foo", addr.clone(), TestSize::Full).await;

            let mut transaction = catalog.open_transaction().await;
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            transaction.add_parquet(&info);
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }
        trace.record(&catalog, &state, false);

        // close catalog again
        drop(catalog);

        // remove first transaction files (that is no longer required)
        for i in 0..(trace.tkeys.len() - 1) {
            if trace.aborted[i] {
                continue;
            }
            let tkey = trace.tkeys[i];
            let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
            checked_delete(&iox_object_store, &path).await;
        }

        // load catalog from store and check replayed state
        let (catalog, state) = load_ok(&iox_object_store).await.unwrap();
        assert_eq!(
            catalog.revision_counter(),
            trace.tkeys.last().unwrap().revision_counter
        );
        assert_catalog_parquet_files(
            &state,
            &get_catalog_parquet_files(trace.states.last().unwrap()),
        );
    }

    #[tokio::test]
    async fn test_delete_predicates() {
        let iox_object_store = make_iox_object_store().await;

        let (catalog, mut state) = new_empty(&iox_object_store).await;

        {
            let mut t = catalog.open_transaction().await;

            // create 3 chunks
            let mut chunk_addrs = vec![];
            for id in 0..3 {
                let chunk_addr = chunk_addr(id);
                let (path, metadata) =
                    make_metadata(&iox_object_store, "foo", chunk_addr.clone(), TestSize::Full)
                        .await;
                let info = CatalogParquetInfo {
                    path,
                    file_size_bytes: 33,
                    metadata: Arc::new(metadata),
                };
                state.insert(info.clone()).unwrap();
                t.add_parquet(&info);

                chunk_addrs.push(chunk_addr);
            }

            // create two predicate
            let predicate_1 = create_delete_predicate(42);
            let chunks_1 = vec![chunk_addrs[0].clone().into()];
            t.delete_predicate(&predicate_1, &chunks_1);
            state.delete_predicate(predicate_1, chunks_1);

            let predicate_2 = create_delete_predicate(1337);
            let chunks_2 = vec![chunk_addrs[0].clone().into(), chunk_addrs[1].clone().into()];
            t.delete_predicate(&predicate_2, &chunks_2);
            state.delete_predicate(predicate_2, chunks_2);

            t.commit().await.unwrap();
        }

        // restoring from the last transaction works
        let (_catalog, state_recovered) = load_ok(&iox_object_store).await.unwrap();
        assert_eq!(
            state.delete_predicates(),
            state_recovered.delete_predicates()
        );

        // add a checkpoint
        {
            let t = catalog.open_transaction().await;

            let ckpt_handle = t.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }

        // restoring from the last checkpoint works
        let (_catalog, state_recovered) = load_ok(&iox_object_store).await.unwrap();
        assert_eq!(
            state.delete_predicates(),
            state_recovered.delete_predicates()
        );
    }

    /// Get sorted list of catalog files from state
    fn get_catalog_parquet_files(
        state: &TestCatalogState,
    ) -> Vec<(ParquetFilePath, IoxParquetMetaData)> {
        let mut files: Vec<(ParquetFilePath, IoxParquetMetaData)> = state
            .files()
            .map(|info| (info.path.clone(), info.metadata.as_ref().clone()))
            .collect();
        files.sort_by_key(|(path, _)| path.clone());
        files
    }

    /// Assert that set of parquet files tracked by a catalog are identical to the given sorted list.
    fn assert_catalog_parquet_files(
        state: &TestCatalogState,
        expected: &[(ParquetFilePath, IoxParquetMetaData)],
    ) {
        let actual = get_catalog_parquet_files(state);
        let mut expected = expected.to_vec();
        expected.sort_by_key(|(path, _)| path.clone());

        for ((actual_path, actual_md), (expected_path, expected_md)) in
            actual.iter().zip(expected.iter())
        {
            assert_eq!(actual_path, expected_path);

            let actual_md = actual_md.decode().unwrap();
            let expected_md = expected_md.decode().unwrap();

            let actual_schema = actual_md.read_schema().unwrap();
            let expected_schema = expected_md.read_schema().unwrap();
            assert_eq!(actual_schema, expected_schema);

            // NOTE: the actual table name is not important here as long as it is the same for both calls, since it is
            // only used to generate out statistics struct (not to read / dispatch anything).
            let actual_stats = actual_md.read_statistics(&actual_schema).unwrap();
            let expected_stats = expected_md.read_statistics(&expected_schema).unwrap();
            assert_eq!(actual_stats, expected_stats);
        }
    }

    async fn checked_delete(iox_object_store: &IoxObjectStore, path: &TransactionFilePath) {
        // issue full GET operation to check if object is preset
        iox_object_store
            .get_catalog_transaction_file(path)
            .await
            .unwrap()
            .map_ok(|bytes| bytes.to_vec())
            .try_concat()
            .await
            .unwrap();

        // delete it
        iox_object_store
            .delete_catalog_transaction_file(path)
            .await
            .unwrap();
    }

    /// Result of [`assert_single_catalog_inmem_works`].
    struct TestTrace {
        /// Traces transaction keys for every (committed and aborted) transaction.
        tkeys: Vec<TransactionKey>,

        /// Traces state after every (committed and aborted) transaction.
        states: Vec<TestCatalogState>,

        /// Traces timestamp after every (committed and aborted) transaction.
        post_timestamps: Vec<DateTime<Utc>>,

        /// Traces if an transaction was aborted.
        aborted: Vec<bool>,
    }

    impl TestTrace {
        fn new() -> Self {
            Self {
                tkeys: vec![],
                states: vec![],
                post_timestamps: vec![],
                aborted: vec![],
            }
        }

        fn record(&mut self, catalog: &PreservedCatalog, state: &TestCatalogState, aborted: bool) {
            self.tkeys.push(catalog.previous_tkey.read().unwrap());
            self.states.push(state.clone());
            self.post_timestamps.push(Utc::now());
            self.aborted.push(aborted);
        }
    }

    async fn assert_single_catalog_inmem_works(
        iox_object_store: &Arc<IoxObjectStore>,
    ) -> TestTrace {
        let (catalog, mut state) = new_empty(iox_object_store).await;

        // track all the intermediate results
        let mut trace = TestTrace::new();

        // empty catalog has no data
        assert_eq!(catalog.revision_counter(), 0);
        assert_catalog_parquet_files(&state, &[]);
        trace.record(&catalog, &state, false);

        let mut expected = vec![];

        // fill catalog with examples
        {
            let mut t = catalog.open_transaction().await;

            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(0), TestSize::Full).await;
            expected.push((path.clone(), metadata.clone()));
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            t.add_parquet(&info);

            let (path, metadata) =
                make_metadata(iox_object_store, "bar", chunk_addr(1), TestSize::Full).await;
            expected.push((path.clone(), metadata.clone()));
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            t.add_parquet(&info);

            let (path, metadata) =
                make_metadata(iox_object_store, "bar", chunk_addr(2), TestSize::Full).await;
            expected.push((path.clone(), metadata.clone()));
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            t.add_parquet(&info);

            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(3), TestSize::Full).await;
            expected.push((path.clone(), metadata.clone()));
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            t.add_parquet(&info);

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 1);
        assert_catalog_parquet_files(&state, &expected);
        trace.record(&catalog, &state, false);

        // modify catalog with examples
        {
            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(4), TestSize::Full).await;
            expected.push((path.clone(), metadata.clone()));

            let mut t = catalog.open_transaction().await;

            // "real" modifications
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };
            state.insert(info.clone()).unwrap();
            t.add_parquet(&info);

            let (path, _) = expected.remove(0);
            state.remove(&path).unwrap();
            t.remove_parquet(&path);

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(&state, &expected);
        trace.record(&catalog, &state, false);

        // uncommitted modifications have no effect
        {
            let mut t = catalog.open_transaction().await;

            let (path, metadata) =
                make_metadata(iox_object_store, "foo", chunk_addr(1), TestSize::Full).await;
            let info = CatalogParquetInfo {
                path,
                file_size_bytes: 33,
                metadata: Arc::new(metadata),
            };

            t.add_parquet(&info);
            t.remove_parquet(&expected[0].0);

            // NO commit here!
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(&state, &expected);
        trace.record(&catalog, &state, true);

        trace
    }

    #[tokio::test]
    async fn test_create_twice() {
        let iox_object_store = make_iox_object_store().await;

        new_empty(&iox_object_store).await;

        let res = PreservedCatalog::new_empty::<TestCatalogState>(
            DB_NAME,
            Arc::clone(&iox_object_store),
            (),
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "Catalog already exists");
    }

    #[tokio::test]
    async fn test_wipe_nothing() {
        let iox_object_store = make_iox_object_store().await;

        PreservedCatalog::wipe(&iox_object_store).await.unwrap();
    }

    #[tokio::test]
    async fn test_wipe_normal() {
        let iox_object_store = make_iox_object_store().await;

        // create a real catalog
        assert_single_catalog_inmem_works(&iox_object_store).await;

        // wipe
        PreservedCatalog::wipe(&iox_object_store).await.unwrap();

        // `exists` and `load` both report "no data"
        assert!(!exists(&iox_object_store).await);
        assert!(load_ok(&iox_object_store).await.is_none());

        // can create new catalog
        new_empty(&iox_object_store).await;
    }

    #[tokio::test]
    async fn test_wipe_broken_catalog() {
        let iox_object_store = make_iox_object_store().await;

        // create a real catalog
        assert_single_catalog_inmem_works(&iox_object_store).await;

        // break
        let (catalog, _state) = load_ok(&iox_object_store).await.unwrap();
        break_catalog_with_weird_version(&catalog).await;

        // wipe
        PreservedCatalog::wipe(&iox_object_store).await.unwrap();

        // `exists` and `load` both report "no data"
        assert!(!exists(&iox_object_store).await);
        assert!(load_ok(&iox_object_store).await.is_none());

        // can create new catalog
        new_empty(&iox_object_store).await;
    }

    #[tokio::test]
    async fn test_transaction_handle_revision_counter() {
        let iox_object_store = make_iox_object_store().await;
        let (catalog, _state) = new_empty(&iox_object_store).await;
        let t = catalog.open_transaction().await;

        assert_eq!(t.revision_counter(), 1);
    }

    #[tokio::test]
    async fn test_transaction_handle_uuid() {
        let iox_object_store = make_iox_object_store().await;
        let (catalog, _state) = new_empty(&iox_object_store).await;
        let mut t = catalog.open_transaction().await;

        t.transaction.as_mut().unwrap().proto.uuid = Uuid::nil().as_bytes().to_vec().into();
        assert_eq!(t.uuid(), Uuid::nil());
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_ok() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        let ts = PreservedCatalog::find_last_transaction_timestamp(&iox_object_store)
            .await
            .unwrap()
            .unwrap();

        // last trace entry is an aborted transaction, so the valid transaction timestamp is the third last
        assert!(trace.aborted[trace.aborted.len() - 1]);
        let second_last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 3];
        assert!(
            ts > second_last_committed_end_ts,
            "failed: last start ts ({}) > second last committed end ts ({})",
            ts,
            second_last_committed_end_ts
        );

        let last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 2];
        assert!(
            ts < last_committed_end_ts,
            "failed: last start ts ({}) < last committed end ts ({})",
            ts,
            last_committed_end_ts
        );
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_empty() {
        let iox_object_store = make_iox_object_store().await;

        assert!(
            PreservedCatalog::find_last_transaction_timestamp(&iox_object_store)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_datetime_broken() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let mut proto = load_transaction_proto(&iox_object_store, &path)
            .await
            .unwrap();
        proto.start_timestamp = None;
        store_transaction_proto(&iox_object_store, &path, &proto)
            .await
            .unwrap();

        let ts = PreservedCatalog::find_last_transaction_timestamp(&iox_object_store)
            .await
            .unwrap()
            .unwrap();

        // last trace entry is an aborted transaction, so the valid transaction timestamp is the third last
        assert!(trace.aborted[trace.aborted.len() - 1]);
        let second_last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 3];
        assert!(
            ts > second_last_committed_end_ts,
            "failed: last start ts ({}) > second last committed end ts ({})",
            ts,
            second_last_committed_end_ts
        );

        let last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 2];
        assert!(
            ts < last_committed_end_ts,
            "failed: last start ts ({}) < last committed end ts ({})",
            ts,
            last_committed_end_ts
        );
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_protobuf_broken() {
        let iox_object_store = make_iox_object_store().await;
        let trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = trace.tkeys[0];
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        let data = Bytes::from("foo");

        iox_object_store
            .put_catalog_transaction_file(&path, data)
            .await
            .unwrap();

        let ts = PreservedCatalog::find_last_transaction_timestamp(&iox_object_store)
            .await
            .unwrap()
            .unwrap();

        // last trace entry is an aborted transaction, so the valid transaction timestamp is the third last
        assert!(trace.aborted[trace.aborted.len() - 1]);
        let second_last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 3];
        assert!(
            ts > second_last_committed_end_ts,
            "failed: last start ts ({}) > second last committed end ts ({})",
            ts,
            second_last_committed_end_ts
        );

        let last_committed_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 2];
        assert!(
            ts < last_committed_end_ts,
            "failed: last start ts ({}) < last committed end ts ({})",
            ts,
            last_committed_end_ts
        );
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_checkpoints_only() {
        let iox_object_store = make_iox_object_store().await;
        let mut trace = assert_single_catalog_inmem_works(&iox_object_store).await;

        let (catalog, state) = load_ok(&iox_object_store).await.unwrap();

        // create empty transaction w/ checkpoint
        {
            let transaction = catalog.open_transaction().await;
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }
        trace.record(&catalog, &state, false);

        // delete transaction files
        for (aborted, tkey) in trace.aborted.iter().zip(trace.tkeys.into_iter()) {
            if *aborted {
                continue;
            }
            let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
            checked_delete(&iox_object_store, &path).await;
        }
        drop(catalog);

        let ts = PreservedCatalog::find_last_transaction_timestamp(&iox_object_store)
            .await
            .unwrap()
            .unwrap();

        // check timestamps
        assert!(!trace.aborted[trace.aborted.len() - 1]);
        let second_last_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 2];
        assert!(
            ts > second_last_end_ts,
            "failed: last start ts ({}) > second last committed end ts ({})",
            ts,
            second_last_end_ts
        );

        let last_end_ts = trace.post_timestamps[trace.post_timestamps.len() - 1];
        assert!(
            ts < last_end_ts,
            "failed: last start ts ({}) < last committed end ts ({})",
            ts,
            last_end_ts
        );
    }

    async fn assert_catalog_roundtrip_works(iox_object_store: &Arc<IoxObjectStore>) {
        // use single-catalog test case as base
        let trace = assert_single_catalog_inmem_works(iox_object_store).await;

        // load catalog from store and check replayed state
        let (catalog, state) = load_ok(iox_object_store).await.unwrap();
        assert_eq!(
            catalog.revision_counter(),
            trace.tkeys.last().unwrap().revision_counter
        );
        assert_catalog_parquet_files(
            &state,
            &get_catalog_parquet_files(trace.states.last().unwrap()),
        );
    }

    #[tokio::test]
    async fn test_exists_considers_checkpoints() {
        let iox_object_store = make_iox_object_store().await;

        assert!(!exists(&iox_object_store).await);

        let (catalog, state) = new_empty(&iox_object_store).await;

        // delete transaction file
        let tkey = catalog.previous_tkey.read().unwrap();
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        checked_delete(&iox_object_store, &path).await;

        // create empty transaction w/ checkpoint
        {
            let transaction = catalog.open_transaction().await;
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }

        // delete transaction file
        let tkey = catalog.previous_tkey.read().unwrap();
        let path = TransactionFilePath::new_transaction(tkey.revision_counter, tkey.uuid);
        checked_delete(&iox_object_store, &path).await;

        drop(catalog);

        assert!(exists(&iox_object_store).await);
        assert!(load_ok(&iox_object_store).await.is_some());
    }
}
