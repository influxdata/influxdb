//! Catalog preservation and transaction handling.
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    fmt::{Debug, Display},
    str::FromStr,
    sync::Arc,
};

use crate::metadata::{parquet_metadata_to_thrift, thrift_to_parquet_metadata};
use bytes::Bytes;
use data_types::server_id::ServerId;
use futures::TryStreamExt;
use generated_types::influxdata::iox::catalog::v1 as proto;
use object_store::{
    path::{parsed::DirsAndFileName, parts::PathPart, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{info, warn};
use parking_lot::RwLock;
use parquet::file::metadata::ParquetMetaData;
use prost::{DecodeError, EncodeError, Message};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

/// Current version for serialized transactions.
///
/// For breaking changes, this will change.
pub const TRANSACTION_VERSION: u32 = 1;

/// File suffix for transaction files in object store.
pub const TRANSACTION_FILE_SUFFIX: &str = "txn";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error during serialization: {}", source))]
    Serialization { source: EncodeError },

    #[snafu(display("Error during deserialization: {}", source))]
    Deserialization { source: DecodeError },

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

    #[snafu(display("Cannot parse UUID: {}", source))]
    UuidParse { source: uuid::Error },

    #[snafu(display("UUID required but not provided"))]
    UuidRequired {},

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

    #[snafu(display("Parquet already exists in catalog: {:?}", path))]
    ParquetFileAlreadyExists { path: DirsAndFileName },

    #[snafu(display("Parquet does not exist in catalog: {:?}", path))]
    ParquetFileDoesNotExist { path: DirsAndFileName },

    #[snafu(display("Cannot encode parquet metadata: {}", source))]
    MetadataEncodingFailed { source: crate::metadata::Error },

    #[snafu(display("Cannot decode parquet metadata: {}", source))]
    MetadataDecodingFailed { source: crate::metadata::Error },

    #[snafu(display("Cannot parse path {:?}: {}", path, source), visibility(pub))]
    PathParseFailed {
        source: crate::storage::Error,
        path: DirsAndFileName,
    },

    #[snafu(
        display("Cannot read schema from {:?}: {}", path, source),
        visibility(pub)
    )]
    SchemaReadFailed {
        source: crate::storage::Error,
        path: DirsAndFileName,
    },

    #[snafu(
        display("Cannot read statistics from {:?}: {}", path, source),
        visibility(pub)
    )]
    StatisticsReadFailed {
        source: crate::metadata::Error,
        path: DirsAndFileName,
    },

    #[snafu(
        display("Catalog state failure when processing {:?}: {}", path, source),
        visibility(pub)
    )]
    CatalogStateFailure {
        source: Box<dyn std::error::Error + Send + Sync>,
        path: DirsAndFileName,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Struct containing all information that a catalog received for a new parquet file.
#[derive(Debug)]
pub struct CatalogParquetInfo {
    /// Full path.
    pub path: DirsAndFileName,

    /// Associated parquet metadata.
    pub metadata: ParquetMetaData,
}

/// Abstraction over how the in-memory state of the catalog works.
pub trait CatalogState {
    /// Input to create a new empty instance.
    ///
    /// See [`new_empty`](Self::new_empty) for details.
    type EmptyInput;

    /// Create empty state w/o any known files.
    fn new_empty(data: Self::EmptyInput) -> Self;

    /// Opens a new state.
    ///
    /// Depending if the state implements full copy-on-write semantics, do one of the following:
    ///
    /// - clone state
    /// - return a pointer to self (e.g. using an [`Arc`](std::sync::Arc))
    fn clone_or_keep(origin: &Arc<Self>) -> Arc<Self>;

    /// Add parquet file to state.
    fn add(
        &self,
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        info: CatalogParquetInfo,
    ) -> Result<()>;

    /// Remove parquet file from state.
    fn remove(&self, path: DirsAndFileName) -> Result<()>;
}

/// Inner mutable part of the preserved catalog.
struct PreservedCatalogInner<S>
where
    S: CatalogState,
{
    previous_tkey: Option<TransactionKey>,
    state: Arc<S>,
}

/// In-memory view of the preserved catalog.
pub struct PreservedCatalog<S>
where
    S: CatalogState,
{
    // We need an RWLock AND a semaphore, so that readers are NOT blocked during an open transactions. Note that this
    // requires a new transaction to:
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
    inner: RwLock<PreservedCatalogInner<S>>,
    transaction_semaphore: Semaphore,

    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    db_name: String,
}

impl<S> PreservedCatalog<S>
where
    S: CatalogState,
{
    /// Create new catalog w/o any data.
    pub fn new_empty(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: String,
        state_data: S::EmptyInput,
    ) -> Self {
        let inner = PreservedCatalogInner {
            previous_tkey: None,
            state: Arc::new(S::new_empty(state_data)),
        };

        Self {
            inner: RwLock::new(inner),
            transaction_semaphore: Semaphore::new(1),
            object_store,
            server_id,
            db_name,
        }
    }

    /// Load existing catalog from store, if it exists.
    pub async fn load(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: String,
        state_data: S::EmptyInput,
    ) -> Result<Option<Self>> {
        // parse all paths into revisions
        let list_path = transactions_path(&object_store, server_id, &db_name);
        let paths = object_store
            .list(Some(&list_path))
            .await
            .context(Read {})?
            .try_concat()
            .await
            .context(Read {})?;
        let mut transactions: HashMap<u64, Uuid> = HashMap::new();
        let mut max_revision = None;
        for path in paths {
            if let Some((revision_counter, uuid)) = parse_transaction_path(path) {
                // keep track of the max
                max_revision = Some(
                    max_revision
                        .map(|m: u64| m.max(revision_counter))
                        .unwrap_or(revision_counter),
                );

                // insert but check for duplicates
                match transactions.entry(revision_counter) {
                    Occupied(o) => {
                        // sort for determinism
                        let (uuid1, uuid2) = if *o.get() < uuid {
                            (*o.get(), uuid)
                        } else {
                            (uuid, *o.get())
                        };

                        Fork {
                            revision_counter,
                            uuid1,
                            uuid2,
                        }
                        .fail()?;
                    }
                    Vacant(v) => {
                        v.insert(uuid);
                    }
                }
            }
        }

        // Check if there is any catalog stored at all
        if transactions.is_empty() {
            return Ok(None);
        }

        // read and replay revisions
        let max_revision = max_revision.expect("transactions list is not empty here");
        let mut state = Arc::new(CatalogState::new_empty(state_data));
        let mut last_tkey = None;
        for rev in 0..=max_revision {
            let uuid = transactions.get(&rev).context(MissingTransaction {
                revision_counter: rev,
            })?;
            let tkey = TransactionKey {
                revision_counter: rev,
                uuid: *uuid,
            };
            let tmp_state = S::clone_or_keep(&state);
            let transaction = OpenTransaction::load_and_apply(
                &object_store,
                server_id,
                &db_name,
                &tkey,
                tmp_state,
                &last_tkey,
            )
            .await?;
            last_tkey = Some(tkey);
            state = transaction.next_state;
        }

        let inner = PreservedCatalogInner {
            previous_tkey: last_tkey,
            state,
        };

        Ok(Some(Self {
            inner: RwLock::new(inner),
            transaction_semaphore: Semaphore::new(1),
            object_store,
            server_id,
            db_name,
        }))
    }

    /// Open a new transaction.
    ///
    /// Note that only a single transaction can be open at any time. This call will `await` until any outstanding
    /// transaction handle is dropped. The newly created transaction will contain the state after `await` (esp.
    /// post-blocking). This system is fair, which means that transactions are given out in the order they were
    /// requested.
    pub async fn open_transaction(&self) -> TransactionHandle<'_, S> {
        TransactionHandle::new(self).await
    }

    /// Return current state.
    pub fn state(&self) -> Arc<S> {
        Arc::clone(&self.inner.read().state)
    }

    /// Get latest revision counter.
    ///
    /// This can be `None` for a newly created catalog.
    pub fn revision_counter(&self) -> Option<u64> {
        self.inner
            .read()
            .previous_tkey
            .clone()
            .map(|tkey| tkey.revision_counter)
    }
}

impl<S> Debug for PreservedCatalog<S>
where
    S: CatalogState,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PreservedCatalog{{..}}")
    }
}

/// Creates object store path where transactions are stored.
///
/// The format is:
///
/// ```text
/// <server_id>/<db_name>/transactions/
/// ```
fn transactions_path(object_store: &ObjectStore, server_id: ServerId, db_name: &str) -> Path {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path.push_dir(db_name.to_string());
    path.push_dir("transactions");

    path
}

/// Creates object store path for given transaction.
///
/// The format is:
///
/// ```text
/// <server_id>/<db_name>/transactions/<revision_counter>/<uuid>.txn
/// ```
fn transaction_path(
    object_store: &ObjectStore,
    server_id: ServerId,
    db_name: &str,
    tkey: &TransactionKey,
) -> Path {
    let mut path = transactions_path(object_store, server_id, db_name);

    // pad number: `u64::MAX.to_string().len()` is 20
    path.push_dir(format!("{:0>20}", tkey.revision_counter));

    let file_name = format!("{}.{}", tkey.uuid, TRANSACTION_FILE_SUFFIX);
    path.set_file_name(file_name);

    path
}

/// Extracts revision counter and UUID from transaction path
fn parse_transaction_path(path: Path) -> Option<(u64, Uuid)> {
    let parsed: DirsAndFileName = path.into();
    if parsed.directories.len() != 4 {
        return None;
    };

    let revision_counter = parsed.directories[3].encoded().parse();

    let name_parts: Vec<_> = parsed
        .file_name
        .as_ref()
        .expect("got file from object store w/o file name (aka only a directory?)")
        .encoded()
        .split('.')
        .collect();
    if (name_parts.len() != 2) || (name_parts[1] != TRANSACTION_FILE_SUFFIX) {
        return None;
    }
    let uuid = Uuid::parse_str(name_parts[0]);

    match (revision_counter, uuid) {
        (Ok(revision_counter), Ok(uuid)) => Some((revision_counter, uuid)),
        _ => None,
    }
}

/// Serialize and store protobuf-encoded transaction.
async fn store_transaction_proto(
    object_store: &ObjectStore,
    path: &Path,
    proto: &proto::Transaction,
) -> Result<()> {
    let mut data = Vec::new();
    proto.encode(&mut data).context(Serialization {})?;
    let data = Bytes::from(data);
    let len = data.len();

    object_store
        .put(
            &path,
            futures::stream::once(async move { Ok(data) }),
            Some(len),
        )
        .await
        .context(Write {})?;

    Ok(())
}

/// Load and deserialize protobuf-encoded transaction from store.
async fn load_transaction_proto(
    object_store: &ObjectStore,
    path: &Path,
) -> Result<proto::Transaction> {
    let data = object_store
        .get(&path)
        .await
        .context(Read {})?
        .map_ok(|bytes| bytes.to_vec())
        .try_concat()
        .await
        .context(Read {})?;
    let proto = proto::Transaction::decode(&data[..]).context(Deserialization {})?;
    Ok(proto)
}

/// Parse UUID from protobuf.
fn parse_uuid(s: &str) -> Result<Option<Uuid>> {
    if s.is_empty() {
        Ok(None)
    } else {
        let uuid = Uuid::from_str(s).context(UuidParse {})?;
        Ok(Some(uuid))
    }
}

/// Parse UUID from protobuf and fail if protobuf did not provide data.
fn parse_uuid_required(s: &str) -> Result<Uuid> {
    parse_uuid(s)?.context(UuidRequired {})
}

/// Parse [`DirsAndFilename`](object_store::path::parsed::DirsAndFileName) from protobuf.
fn parse_dirs_and_filename(proto: &Option<proto::Path>) -> Result<DirsAndFileName> {
    let proto = proto.as_ref().context(PathRequired)?;

    Ok(DirsAndFileName {
        directories: proto
            .directories
            .iter()
            .map(|s| PathPart::from(&s[..]))
            .collect(),
        file_name: Some(PathPart::from(&proto.file_name[..])),
    })
}

/// Store [`DirsAndFilename`](object_store::path::parsed::DirsAndFileName) as protobuf.
fn unparse_dirs_and_filename(path: &DirsAndFileName) -> proto::Path {
    proto::Path {
        directories: path
            .directories
            .iter()
            .map(|part| part.encoded().to_string())
            .collect(),
        file_name: path
            .file_name
            .as_ref()
            .map(|part| part.encoded().to_string())
            .unwrap_or_default(),
    }
}

/// Key to address transactions.
#[derive(Clone, Debug)]
struct TransactionKey {
    revision_counter: u64,
    uuid: Uuid,
}

impl Display for TransactionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.revision_counter, self.uuid)
    }
}

/// Tracker for an open, uncommitted transaction.
struct OpenTransaction<S>
where
    S: CatalogState,
{
    next_state: Arc<S>,
    proto: proto::Transaction,
}

impl<S> OpenTransaction<S>
where
    S: CatalogState,
{
    /// Private API to create new transaction, users should always use [`PreservedCatalog::open_transaction`].
    fn new(catalog_inner: &PreservedCatalogInner<S>) -> Self {
        let (revision_counter, previous_uuid) = match &catalog_inner.previous_tkey {
            Some(tkey) => (tkey.revision_counter + 1, tkey.uuid.to_string()),
            None => (0, String::new()),
        };

        Self {
            next_state: S::clone_or_keep(&catalog_inner.state),
            proto: proto::Transaction {
                actions: vec![],
                version: TRANSACTION_VERSION,
                uuid: Uuid::new_v4().to_string(),
                revision_counter,
                previous_uuid,
            },
        }
    }

    fn tkey(&self) -> TransactionKey {
        TransactionKey {
            revision_counter: self.proto.revision_counter,
            uuid: Uuid::parse_str(&self.proto.uuid).unwrap(),
        }
    }

    /// Handle the given action and populate data to the catalog state.
    ///
    /// The deserializes the action state and passes it to the correct method in [`CatalogState`].
    ///
    /// Note that this method is primarily for replaying transactions and will NOT append the given action to the
    /// current transaction. If you also want to store the given action (e.g. during an in-progress transaction), use
    /// [`handle_action_and_record`](Self::handle_action_and_record).
    fn handle_action(
        state: &S,
        action: &proto::transaction::action::Action,
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<()> {
        match action {
            proto::transaction::action::Action::Upgrade(u) => {
                UnsupportedUpgrade {
                    format: u.format.clone(),
                }
                .fail()?;
            }
            proto::transaction::action::Action::AddParquet(a) => {
                let path = parse_dirs_and_filename(&a.path)?;
                let metadata =
                    thrift_to_parquet_metadata(&a.metadata).context(MetadataDecodingFailed)?;
                state.add(
                    Arc::clone(object_store),
                    server_id,
                    db_name,
                    CatalogParquetInfo { path, metadata },
                )?;
            }
            proto::transaction::action::Action::RemoveParquet(a) => {
                let path = parse_dirs_and_filename(&a.path)?;
                state.remove(path)?;
            }
        };
        Ok(())
    }

    /// Similar to [`handle_action`](Self::handle_action) but this will also append the action to the current
    /// transaction state.
    fn handle_action_and_record(
        &mut self,
        action: proto::transaction::action::Action,
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<()> {
        Self::handle_action(&self.next_state, &action, object_store, server_id, db_name)?;
        self.proto.actions.push(proto::transaction::Action {
            action: Some(action),
        });
        Ok(())
    }

    fn commit(mut self, catalog_inner: &mut PreservedCatalogInner<S>) {
        let tkey = self.tkey();
        std::mem::swap(&mut catalog_inner.state, &mut self.next_state);
        catalog_inner.previous_tkey = Some(tkey);
    }

    async fn store(
        &self,
        object_store: &ObjectStore,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<()> {
        let path = transaction_path(object_store, server_id, db_name, &self.tkey());
        store_transaction_proto(object_store, &path, &self.proto).await?;
        Ok(())
    }

    async fn load_and_apply(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        tkey: &TransactionKey,
        state: Arc<S>,
        last_tkey: &Option<TransactionKey>,
    ) -> Result<Self> {
        // recover state from store
        let path = transaction_path(object_store, server_id, db_name, tkey);
        let proto = load_transaction_proto(object_store, &path).await?;

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
        let uuid_actual = parse_uuid_required(&proto.uuid)?;
        if uuid_actual != tkey.uuid {
            WrongTransactionUuid {
                revision_counter: tkey.revision_counter,
                expected: tkey.uuid,
                actual: uuid_actual,
            }
            .fail()?
        }
        let last_uuid_actual = parse_uuid(&proto.previous_uuid)?;
        let last_uuid_expected = last_tkey.as_ref().map(|tkey| tkey.uuid);
        if last_uuid_actual != last_uuid_expected {
            WrongTransactionLink {
                revision_counter: tkey.revision_counter,
                expected: last_uuid_expected,
                actual: last_uuid_actual,
            }
            .fail()?;
        }

        // apply
        for action in &proto.actions {
            if let Some(action) = action.action.as_ref() {
                Self::handle_action(&state, action, object_store, server_id, db_name)?;
            }
        }

        Ok(Self {
            proto,
            next_state: state,
        })
    }
}

/// Handle for an open uncommitted transaction.
///
/// Dropping this object w/o calling [`commit`](Self::commit) will issue a warning.
pub struct TransactionHandle<'c, S>
where
    S: CatalogState,
{
    catalog: &'c PreservedCatalog<S>,

    // NOTE: The permit is technically used since we use it to reference the semaphore. It implements `drop` which we
    //       rely on.
    #[allow(dead_code)]
    permit: SemaphorePermit<'c>,

    transaction: Option<OpenTransaction<S>>,
}

impl<'c, S> TransactionHandle<'c, S>
where
    S: CatalogState,
{
    async fn new(catalog: &'c PreservedCatalog<S>) -> TransactionHandle<'c, S> {
        // first acquire semaphore (which is only being used for transactions), then get state lock
        let permit = catalog
            .transaction_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");
        let inner_guard = catalog.inner.write();

        let transaction = OpenTransaction::new(&inner_guard);

        // free state for readers again
        drop(inner_guard);

        let tkey = transaction.tkey();
        info!(?tkey, "transaction started");

        Self {
            catalog,
            transaction: Some(transaction),
            permit,
        }
    }

    /// Write data to object store and commit transaction to underlying catalog.
    pub async fn commit(mut self) -> Result<()> {
        // write to object store
        self.transaction
            .as_mut()
            .unwrap()
            .store(
                &self.catalog.object_store,
                self.catalog.server_id,
                &self.catalog.db_name,
            )
            .await?;

        // commit to catalog
        let t = std::mem::take(&mut self.transaction)
            .expect("calling .commit on a closed transaction?!");
        let tkey = t.tkey();
        let mut inner_guard = self.catalog.inner.write();
        t.commit(&mut inner_guard);
        drop(inner_guard);
        info!(?tkey, "transaction committed");

        Ok(())
    }

    /// Add a new parquet file to the catalog.
    ///
    /// If a file with the same path already exists an error will be returned.
    pub fn add_parquet(
        &mut self,
        path: &DirsAndFileName,
        metadata: &ParquetMetaData,
    ) -> Result<()> {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .handle_action_and_record(
                proto::transaction::action::Action::AddParquet(proto::AddParquet {
                    path: Some(unparse_dirs_and_filename(path)),
                    metadata: parquet_metadata_to_thrift(metadata)
                        .context(MetadataEncodingFailed)?,
                }),
                &self.catalog.object_store,
                self.catalog.server_id,
                &self.catalog.db_name,
            )
    }

    /// Remove a parquet file from the catalog.
    ///
    /// Removing files that do not exist or were already removed will result in an error.
    pub fn remove_parquet(&mut self, path: &DirsAndFileName) -> Result<()> {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .handle_action_and_record(
                proto::transaction::action::Action::RemoveParquet(proto::RemoveParquet {
                    path: Some(unparse_dirs_and_filename(path)),
                }),
                &self.catalog.object_store,
                self.catalog.server_id,
                &self.catalog.db_name,
            )
    }
}

impl<'c, S> Debug for TransactionHandle<'c, S>
where
    S: CatalogState,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.transaction {
            Some(t) => write!(f, "TransactionHandle(open, {})", t.tkey()),
            None => write!(f, "TransactionHandle(closed)"),
        }
    }
}

impl<'c, S> Drop for TransactionHandle<'c, S>
where
    S: CatalogState,
{
    fn drop(&mut self) {
        if self.transaction.is_some() {
            warn!(?self, "dropped uncommitted transaction");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, num::NonZeroU32, ops::Deref};

    use crate::{
        metadata::{read_parquet_metadata_from_file, read_statistics_from_parquet_metadata},
        storage::read_schema_from_parquet_metadata,
        utils::{load_parquet_from_store, make_chunk, make_object_store},
    };
    use object_store::parsed_path;

    use super::*;

    #[derive(Clone, Debug)]
    struct TestCatalogStateInner {
        pub parquet_files: HashMap<DirsAndFileName, ParquetMetaData>,
    }

    /// In-memory catalog state, for testing.
    #[derive(Clone, Debug)]
    struct TestCatalogState {
        pub inner: RefCell<TestCatalogStateInner>,
    }

    impl CatalogState for TestCatalogState {
        type EmptyInput = ();

        fn new_empty(_data: Self::EmptyInput) -> Self {
            Self {
                inner: RefCell::new(TestCatalogStateInner {
                    parquet_files: HashMap::new(),
                }),
            }
        }

        fn clone_or_keep(origin: &Arc<Self>) -> Arc<Self> {
            Arc::new(origin.deref().clone())
        }

        fn add(
            &self,
            _object_store: Arc<ObjectStore>,
            _server_id: ServerId,
            _db_name: &str,
            info: CatalogParquetInfo,
        ) -> Result<()> {
            match self.inner.borrow_mut().parquet_files.entry(info.path) {
                Occupied(o) => {
                    return Err(Error::ParquetFileAlreadyExists {
                        path: o.key().clone(),
                    });
                }
                Vacant(v) => {
                    v.insert(info.metadata);
                }
            }

            Ok(())
        }

        fn remove(&self, path: DirsAndFileName) -> Result<()> {
            match self.inner.borrow_mut().parquet_files.entry(path) {
                Occupied(o) => {
                    o.remove();
                }
                Vacant(v) => {
                    return Err(Error::ParquetFileDoesNotExist { path: v.into_key() });
                }
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_inmem_commit_semantics() {
        let object_store = make_object_store();
        assert_single_catalog_inmem_works(&object_store, make_server_id(), "db1").await;
    }

    #[tokio::test]
    async fn test_store_roundtrip() {
        let object_store = make_object_store();
        assert_catalog_roundtrip_works(&object_store, make_server_id(), "db1").await;
    }

    #[tokio::test]
    async fn test_load_from_empty_store() {
        let object_store = make_object_store();
        let option = PreservedCatalog::<TestCatalogState>::load(
            object_store,
            make_server_id(),
            "db1".to_string(),
            (),
        )
        .await
        .unwrap();
        assert!(option.is_none());
    }

    #[tokio::test]
    async fn test_load_from_dirty_store() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1".to_string();

        // wrong file extension
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(format!("{}.foo", Uuid::new_v4()));
        create_empty_file(&object_store, &path).await;

        // no file extension
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(Uuid::new_v4().to_string());
        create_empty_file(&object_store, &path).await;

        // broken UUID
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(format!("foo.{}", TRANSACTION_FILE_SUFFIX));
        create_empty_file(&object_store, &path).await;

        // broken revision counter
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.push_dir("foo");
        path.set_file_name(format!("{}.{}", Uuid::new_v4(), TRANSACTION_FILE_SUFFIX));
        create_empty_file(&object_store, &path).await;

        // file is folder
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.push_dir(format!("{}.{}", Uuid::new_v4(), TRANSACTION_FILE_SUFFIX));
        path.set_file_name("foo");
        create_empty_file(&object_store, &path).await;

        // top-level file
        let mut path = transactions_path(&object_store, server_id, &db_name);
        path.set_file_name(format!("{}.{}", Uuid::new_v4(), TRANSACTION_FILE_SUFFIX));
        create_empty_file(&object_store, &path).await;

        // no data present
        let option = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.clone(),
            (),
        )
        .await
        .unwrap();
        assert!(option.is_none());

        // can still write + read
        assert_catalog_roundtrip_works(&object_store, server_id, &db_name).await;
    }

    #[tokio::test]
    async fn test_missing_transaction() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // remove transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        checked_delete(&object_store, &path).await;

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert!(matches!(
            res,
            Err(Error::MissingTransaction {
                revision_counter: 0
            })
        ));
    }

    #[tokio::test]
    async fn test_transaction_version_mismatch() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.version = 42;
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "Format version of transaction file for revision 0 is 42 but only [1] are supported"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_revision() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.revision_counter = 42;
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "Wrong revision counter in transaction file: expected 0 but found 42"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_uuid() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        let uuid_expected = Uuid::parse_str(&proto.uuid).unwrap();
        let uuid_actual = Uuid::nil();
        proto.uuid = uuid_actual.to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            format!(
                "Wrong UUID for transaction file (revision: 0): expected {} but found {}",
                uuid_expected, uuid_actual
            )
        );
    }

    #[tokio::test]
    async fn test_missing_transaction_uuid() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.uuid = String::new();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "UUID required but not provided"
        );
    }

    #[tokio::test]
    async fn test_broken_transaction_uuid() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.uuid = "foo".to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot parse UUID: invalid length: expected one of [36, 32], found 3"
        );
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_start() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.previous_uuid = Uuid::nil().to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "Wrong link to previous UUID in revision 0: expected None but found Some(00000000-0000-0000-0000-000000000000)");
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_middle() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[1];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.previous_uuid = Uuid::nil().to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), format!("Wrong link to previous UUID in revision 1: expected Some({}) but found Some(00000000-0000-0000-0000-000000000000)", trace.tkeys[0].uuid));
    }

    #[tokio::test]
    async fn test_wrong_transaction_link_broken() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.previous_uuid = "foo".to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "Cannot parse UUID: invalid length: expected one of [36, 32], found 3"
        );
    }

    #[tokio::test]
    async fn test_transaction_handle_debug() {
        let object_store = make_object_store();
        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            object_store,
            make_server_id(),
            "db1".to_string(),
            (),
        );
        let mut t = catalog.open_transaction().await;

        // open transaction
        t.transaction.as_mut().unwrap().proto.uuid = Uuid::nil().to_string();
        assert_eq!(
            format!("{:?}", t),
            "TransactionHandle(open, 0.00000000-0000-0000-0000-000000000000)"
        );

        // "closed" transaction
        t.transaction = None;
        assert_eq!(format!("{:?}", t), "TransactionHandle(closed)");
    }

    #[tokio::test]
    async fn test_fork() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // re-create transaction file with different UUID
        assert!(trace.tkeys.len() >= 2);
        let mut tkey = trace.tkeys[1].clone();
        let path = transaction_path(&object_store, server_id, db_name, &tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        let old_uuid = tkey.uuid;

        let new_uuid = Uuid::new_v4();
        tkey.uuid = new_uuid;
        let path = transaction_path(&object_store, server_id, db_name, &tkey);
        proto.uuid = new_uuid.to_string();
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        let (uuid1, uuid2) = if old_uuid < new_uuid {
            (old_uuid, new_uuid)
        } else {
            (new_uuid, old_uuid)
        };
        assert_eq!(res.unwrap_err().to_string(), format!("Fork detected. Revision 1 has two UUIDs {} and {}. Maybe two writer instances with the same server ID were running in parallel?", uuid1, uuid2));
    }

    #[tokio::test]
    async fn test_unsupported_upgrade() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = transaction_path(&object_store, server_id, db_name, tkey);
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.actions.push(proto::transaction::Action {
            action: Some(proto::transaction::action::Action::Upgrade(
                proto::Upgrade {
                    format: "foo".to_string(),
                },
            )),
        });
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(
            res.unwrap_err().to_string(),
            "Upgrade path not implemented/supported: foo",
        );
    }

    /// Get sorted list of catalog files from state
    fn get_catalog_parquet_files(state: &TestCatalogState) -> Vec<(String, ParquetMetaData)> {
        let mut files: Vec<(String, ParquetMetaData)> = state
            .inner
            .borrow()
            .parquet_files
            .iter()
            .map(|(path, md)| (path.display(), md.clone()))
            .collect();
        files.sort_by_key(|(path, _)| path.clone());
        files
    }

    /// Assert that set of parquet files tracked by a catalog are identical to the given sorted list.
    fn assert_catalog_parquet_files(
        catalog: &PreservedCatalog<TestCatalogState>,
        expected: &[(String, ParquetMetaData)],
    ) {
        let actual = get_catalog_parquet_files(&catalog.state());
        for ((actual_path, actual_md), (expected_path, expected_md)) in
            actual.iter().zip(expected.iter())
        {
            assert_eq!(actual_path, expected_path);

            let actual_schema = read_schema_from_parquet_metadata(actual_md).unwrap();
            let expected_schema = read_schema_from_parquet_metadata(expected_md).unwrap();
            assert_eq!(actual_schema, expected_schema);

            // NOTE: the actual table name is not important here as long as it is the same for both calls, since it is
            // only used to generate out statistics struct (not to read / dispatch anything).
            let actual_stats =
                read_statistics_from_parquet_metadata(actual_md, &actual_schema, "foo").unwrap();
            let expected_stats =
                read_statistics_from_parquet_metadata(expected_md, &expected_schema, "foo")
                    .unwrap();
            assert_eq!(actual_stats, expected_stats);
        }
    }

    /// Creates new test server ID
    fn make_server_id() -> ServerId {
        ServerId::new(NonZeroU32::new(1).unwrap())
    }

    async fn create_empty_file(object_store: &ObjectStore, path: &Path) {
        let data = Bytes::default();
        let len = data.len();

        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }

    async fn checked_delete(object_store: &ObjectStore, path: &Path) {
        // issue full GET operation to check if object is preset
        object_store
            .get(&path)
            .await
            .unwrap()
            .map_ok(|bytes| bytes.to_vec())
            .try_concat()
            .await
            .unwrap();

        // delete it
        object_store.delete(&path).await.unwrap();
    }

    /// Result of [`assert_single_catalog_inmem_works`].
    struct TestTrace {
        tkeys: Vec<TransactionKey>,
        states: Vec<TestCatalogState>,
    }

    impl TestTrace {
        fn new() -> Self {
            Self {
                tkeys: vec![],
                states: vec![],
            }
        }

        fn record(&mut self, catalog: &PreservedCatalog<TestCatalogState>) {
            self.tkeys
                .push(catalog.inner.read().previous_tkey.clone().unwrap());
            self.states.push(catalog.state().deref().clone());
        }
    }

    async fn assert_single_catalog_inmem_works(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
    ) -> TestTrace {
        let catalog = PreservedCatalog::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        );

        // get some test metadata
        let metadata1 = make_metadata(object_store, "foo").await;
        let metadata2 = make_metadata(object_store, "bar").await;

        // track all the intermediate results
        let mut trace = TestTrace::new();

        // empty catalog has no data
        assert!(catalog.revision_counter().is_none());
        assert_catalog_parquet_files(&catalog, &[]);

        // fill catalog with examples
        {
            let mut t = catalog.open_transaction().await;

            t.add_parquet(&parsed_path!("test1"), &metadata1).unwrap();
            t.add_parquet(&parsed_path!(["sub1"], "test1"), &metadata2)
                .unwrap();
            t.add_parquet(&parsed_path!(["sub1"], "test2"), &metadata2)
                .unwrap();
            t.add_parquet(&parsed_path!(["sub2"], "test1"), &metadata1)
                .unwrap();

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter().unwrap(), 0);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test1".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog);

        // modify catalog with examples
        {
            let mut t = catalog.open_transaction().await;

            // "real" modifications
            t.add_parquet(&parsed_path!("test4"), &metadata1).unwrap();
            t.remove_parquet(&parsed_path!("test1")).unwrap();

            // wrong modifications
            t.add_parquet(&parsed_path!(["sub1"], "test2"), &metadata2)
                .expect_err("add file twice should error");
            t.remove_parquet(&parsed_path!("does_not_exist"))
                .expect_err("removing unknown file should error");
            t.remove_parquet(&parsed_path!("test1"))
                .expect_err("removing twice should error");

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter().unwrap(), 1);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog);

        // uncommitted modifications have no effect
        {
            let mut t = catalog.open_transaction().await;

            t.add_parquet(&parsed_path!("test5"), &metadata1).unwrap();
            t.remove_parquet(&parsed_path!(["sub1"], "test2")).unwrap();

            // NO commit here!
        }
        assert_eq!(catalog.revision_counter().unwrap(), 1);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog);

        trace
    }

    async fn assert_catalog_roundtrip_works(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
    ) {
        // use single-catalog test case as base
        let trace = assert_single_catalog_inmem_works(object_store, server_id, db_name).await;

        // load catalog from store and check replayed state
        let catalog =
            PreservedCatalog::load(Arc::clone(object_store), server_id, db_name.to_string(), ())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            catalog.revision_counter().unwrap(),
            trace.tkeys.last().unwrap().revision_counter
        );
        assert_catalog_parquet_files(
            &catalog,
            &get_catalog_parquet_files(trace.states.last().unwrap()),
        );
    }

    /// Create test metadata. See [`make_chunk`] for details.
    async fn make_metadata(
        object_store: &Arc<ObjectStore>,
        column_prefix: &str,
    ) -> ParquetMetaData {
        let chunk = make_chunk(Arc::clone(object_store), column_prefix).await;
        let (_, parquet_data) = load_parquet_from_store(&chunk, Arc::clone(object_store))
            .await
            .unwrap();
        read_parquet_metadata_from_file(parquet_data).unwrap()
    }
}
