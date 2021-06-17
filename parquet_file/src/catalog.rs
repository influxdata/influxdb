//! Catalog preservation and transaction handling.
use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap,
    },
    convert::TryInto,
    fmt::{Debug, Display},
    num::TryFromIntError,
    str::FromStr,
    sync::Arc,
};

use crate::metadata::IoxParquetMetaData;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use data_types::server_id::ServerId;
use futures::TryStreamExt;
use generated_types::influxdata::iox::catalog::v1 as proto;
use object_store::{
    path::{parsed::DirsAndFileName, parts::PathPart, ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use observability_deps::tracing::{info, warn};
use parking_lot::RwLock;
use prost::{DecodeError, EncodeError, Message};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

/// Current version for serialized transactions.
///
/// For breaking changes, this will change.
pub const TRANSACTION_VERSION: u32 = 4;

/// File suffix for transaction files in object store.
pub const TRANSACTION_FILE_SUFFIX: &str = "txn";

/// File suffix for checkpoint files in object store.
pub const CHECKPOINT_FILE_SUFFIX: &str = "ckpt";

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

    #[snafu(
        display("Cannot extract metadata from {:?}: {}", path, source),
        visibility(pub)
    )]
    MetadataExtractFailed {
        source: crate::metadata::Error,
        path: DirsAndFileName,
    },

    #[snafu(
        display("Cannot create parquet chunk from {:?}: {}", path, source),
        visibility(pub)
    )]
    ChunkCreationFailed {
        source: crate::chunk::Error,
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

    #[snafu(display("Catalog already exists"))]
    AlreadyExists {},

    #[snafu(display("Internal: Datetime required but missing in serialized catalog"))]
    DateTimeRequired {},

    #[snafu(display("Internal: Cannot parse datetime in serialized catalog: {}", source))]
    DateTimeParseError { source: TryFromIntError },

    #[snafu(display(
        "Internal: Cannot parse encoding in serialized catalog: {} is not a valid, specified variant",
        data
    ))]
    EncodingParseError { data: i32 },

    #[snafu(display(
        "Internal: Found wrong encoding in serialized catalog file: Expected {:?} but got {:?}",
        expected,
        actual
    ))]
    WrongEncodingError {
        expected: proto::transaction::Encoding,
        actual: proto::transaction::Encoding,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Struct containing all information that a catalog received for a new parquet file.
#[derive(Debug)]
pub struct CatalogParquetInfo {
    /// Full path.
    pub path: DirsAndFileName,

    /// Associated parquet metadata.
    pub metadata: Arc<IoxParquetMetaData>,
}

/// How a transaction ends.
#[derive(Clone, Copy, Debug)]
pub enum TransactionEnd {
    /// Successful commit.
    ///
    /// All buffered/prepared actions must be materialized.
    Commit,

    /// Abort.
    ///
    /// All eagerly applied action must be rolled back.
    Abort,
}

/// Abstraction over how the in-memory state of the catalog works.
pub trait CatalogState {
    /// Input to create a new empty instance.
    ///
    /// See [`new_empty`](Self::new_empty) for details.
    type EmptyInput: Send;

    /// Create empty state w/o any known files.
    fn new_empty(db_name: &str, data: Self::EmptyInput) -> Self;

    /// Helper struct that can be used to remember/cache actions that happened during a transaction.
    ///
    /// This can be useful to implement one of the following systems to handle commits and aborts:
    /// - **copy semantic:** the entire inner state is copied during transaction start and is committed at the end. This
    ///   is esp. useful when state-copies are simple
    /// - **action preperation:** Actions are prepared during the transaction, checked for validity, and are simply
    ///   executed on commit.
    /// - **action rollback:** Actions are eagerly executed during transaction and are rolled back when the transaction
    ///   aborts.
    ///
    /// This type is created by [`transaction_begin`](Self::transaction_begin), will be modified by the actions, and
    /// will be consumed by [`transaction_end`](Self::transaction_end).
    type TransactionState: Send + Sync;

    /// Hook that will be called at transaction start.
    fn transaction_begin(origin: &Arc<Self>) -> Self::TransactionState;

    /// Hook that will be called at transaction end.
    ///
    /// Note that this hook will be called for both successful commits and aborts.
    fn transaction_end(tstate: Self::TransactionState, how: TransactionEnd) -> Arc<Self>;

    /// Add parquet file to state.
    fn add(
        tstate: &mut Self::TransactionState,
        object_store: Arc<ObjectStore>,
        info: CatalogParquetInfo,
    ) -> Result<()>;

    /// Remove parquet file from state.
    fn remove(tstate: &mut Self::TransactionState, path: DirsAndFileName) -> Result<()>;

    /// List all Parquet files that are currently (i.e. by the current version) tracked by the catalog.
    ///
    /// If a file was once [added](Self::add) but later [removed](Self::remove) it MUST NOT appear in the result.
    fn files(&self) -> HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>;
}

/// Find last transaction-start-timestamp.
///
/// This method is designed to read and verify as little as possible and should also work on most broken catalogs.
pub async fn find_last_transaction_timestamp(
    object_store: &ObjectStore,
    server_id: ServerId,
    db_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    let mut res = None;
    for (path, _file_type, _revision_counter, _uuid) in
        list_files(object_store, server_id, db_name).await?
    {
        match load_transaction_proto(object_store, &path).await {
            Ok(proto) => match parse_timestamp(&proto.start_timestamp) {
                Ok(ts) => {
                    res = Some(res.map_or(ts, |res: DateTime<Utc>| res.max(ts)));
                }
                Err(e) => warn!(%e, ?path, "Cannot parse timestamp"),
            },
            Err(e @ Error::Read { .. }) => {
                // bubble up IO error
                return Err(e);
            }
            Err(e) => warn!(%e, ?path, "Cannot read transaction"),
        }
    }
    Ok(res)
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
    S: CatalogState + Send + Sync,
{
    /// Checks if a preserved catalog exists.
    pub async fn exists(
        object_store: &ObjectStore,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<bool> {
        Ok(!list_files(object_store, server_id, db_name)
            .await?
            .is_empty())
    }

    /// Create new catalog w/o any data.
    ///
    /// An empty transaction will be used to mark the catalog start so that concurrent open but still-empty catalogs can
    /// easily be detected.
    pub async fn new_empty(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: impl Into<String> + Send,
        state_data: S::EmptyInput,
    ) -> Result<Self> {
        let db_name = db_name.into();

        if Self::exists(&object_store, server_id, &db_name).await? {
            return Err(Error::AlreadyExists {});
        }

        let inner = PreservedCatalogInner {
            previous_tkey: None,
            state: Arc::new(S::new_empty(&db_name, state_data)),
        };

        let catalog = Self {
            inner: RwLock::new(inner),
            transaction_semaphore: Semaphore::new(1),
            object_store,
            server_id,
            db_name,
        };

        // add empty transaction
        let transaction = catalog.open_transaction().await;
        transaction.commit(false).await?;

        Ok(catalog)
    }

    /// Load existing catalog from store, if it exists.
    ///
    /// Loading starts at the latest checkpoint or -- if none exists -- at transaction `0`. Transactions before that
    /// point are neither verified nor are they required to exist.
    pub async fn load(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: String,
        state_data: S::EmptyInput,
    ) -> Result<Option<Self>> {
        // parse all paths into revisions
        let mut transactions: HashMap<u64, Uuid> = HashMap::new();
        let mut max_revision = None;
        let mut last_checkpoint = None;
        for (_path, file_type, revision_counter, uuid) in
            list_files(&object_store, server_id, &db_name).await?
        {
            // keep track of the max
            max_revision = Some(
                max_revision
                    .map(|m: u64| m.max(revision_counter))
                    .unwrap_or(revision_counter),
            );

            // keep track of latest checkpoint
            if matches!(file_type, FileType::Checkpoint) {
                last_checkpoint = Some(
                    last_checkpoint
                        .map(|m: u64| m.max(revision_counter))
                        .unwrap_or(revision_counter),
                );
            }

            // insert but check for duplicates
            match transactions.entry(revision_counter) {
                Occupied(o) => {
                    // sort for determinism
                    let (uuid1, uuid2) = if *o.get() < uuid {
                        (*o.get(), uuid)
                    } else {
                        (uuid, *o.get())
                    };

                    if uuid1 != uuid2 {
                        Fork {
                            revision_counter,
                            uuid1,
                            uuid2,
                        }
                        .fail()?;
                    }
                }
                Vacant(v) => {
                    v.insert(uuid);
                }
            }
        }

        // Check if there is any catalog stored at all
        if transactions.is_empty() {
            return Ok(None);
        }

        // setup empty state
        let mut state = Arc::new(CatalogState::new_empty(&db_name, state_data));
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
            state = OpenTransaction::load_and_apply(
                &object_store,
                server_id,
                &db_name,
                &tkey,
                &state,
                &last_tkey,
                file_type,
            )
            .await?;
            last_tkey = Some(tkey);
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

    /// Deletes catalog.
    ///
    /// **Always create a backup before wiping your data!**
    ///
    /// This also works for broken catalogs. Also succeeds if no catalog is present.
    ///
    /// Note that wiping the catalog will NOT wipe any referenced parquet files.
    pub async fn wipe(
        object_store: &ObjectStore,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<()> {
        for (path, _file_type, _revision_counter, _uuid) in
            list_files(object_store, server_id, db_name).await?
        {
            object_store.delete(&path).await.context(Write)?;
        }

        Ok(())
    }

    /// Open a new transaction.
    ///
    /// Note that only a single transaction can be open at any time. This call will `await` until any outstanding
    /// transaction handle is dropped. The newly created transaction will contain the state after `await` (esp.
    /// post-blocking). This system is fair, which means that transactions are given out in the order they were
    /// requested.
    pub async fn open_transaction(&self) -> TransactionHandle<'_, S> {
        self.open_transaction_with_uuid(Uuid::new_v4()).await
    }

    /// Crate-private API to open an transaction with a specified UUID. Should only be used for catalog rebuilding or
    /// with a fresh V4-UUID!
    pub(crate) async fn open_transaction_with_uuid(&self, uuid: Uuid) -> TransactionHandle<'_, S> {
        TransactionHandle::new(self, uuid).await
    }

    /// Return current state.
    pub fn state(&self) -> Arc<S> {
        Arc::clone(&self.inner.read().state)
    }

    /// Get latest revision counter.
    pub fn revision_counter(&self) -> u64 {
        self.inner
            .read()
            .previous_tkey
            .clone()
            .map(|tkey| tkey.revision_counter)
            .expect("catalog should have at least an empty transaction")
    }

    /// Object store used by this catalog.
    pub fn object_store(&self) -> Arc<ObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Server ID used by this catalog.
    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    /// Database name used by this catalog.
    pub fn db_name(&self) -> &str {
        &self.db_name
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
fn catalog_path(object_store: &ObjectStore, server_id: ServerId, db_name: &str) -> Path {
    let mut path = object_store.new_path();
    path.push_dir(server_id.to_string());
    path.push_dir(db_name.to_string());
    path.push_dir("transactions");

    path
}

/// Type of catalog file.
#[derive(Debug, Clone, Copy)]
enum FileType {
    /// Ordinary transaction with delta encoding.
    Transaction,

    /// Checkpoints with full state.
    Checkpoint,
}

impl FileType {
    /// Get file suffix for this file type.
    fn suffix(&self) -> &'static str {
        match self {
            Self::Transaction => TRANSACTION_FILE_SUFFIX,
            Self::Checkpoint => CHECKPOINT_FILE_SUFFIX,
        }
    }

    /// Get encoding that should be used for this file.
    fn encoding(&self) -> proto::transaction::Encoding {
        match self {
            Self::Transaction => proto::transaction::Encoding::Delta,
            Self::Checkpoint => proto::transaction::Encoding::Full,
        }
    }
}

/// Creates object store path for given transaction or checkpoint.
///
/// The format is:
///
/// ```text
/// <server_id>/<db_name>/transactions/<revision_counter>/<uuid>.{txn,ckpt}
/// ```
fn file_path(
    object_store: &ObjectStore,
    server_id: ServerId,
    db_name: &str,
    tkey: &TransactionKey,
    file_type: FileType,
) -> Path {
    let mut path = catalog_path(object_store, server_id, db_name);

    // pad number: `u64::MAX.to_string().len()` is 20
    path.push_dir(format!("{:0>20}", tkey.revision_counter));

    let file_name = format!("{}.{}", tkey.uuid, file_type.suffix());
    path.set_file_name(file_name);

    path
}

/// Extracts revision counter, UUID, and file type from transaction or checkpoint path.
fn parse_file_path(path: Path) -> Option<(u64, Uuid, FileType)> {
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
    if name_parts.len() != 2 {
        return None;
    }
    let uuid = Uuid::parse_str(name_parts[0]);

    match (revision_counter, uuid) {
        (Ok(revision_counter), Ok(uuid)) => {
            for file_type in
                std::array::IntoIter::new([FileType::Checkpoint, FileType::Transaction])
            {
                if name_parts[1] == file_type.suffix() {
                    return Some((revision_counter, uuid, file_type));
                }
            }
            None
        }
        _ => None,
    }
}

/// Load a list of all transaction file from object store. Also parse revision counter and transaction UUID.
///
/// The files are in no particular order!
async fn list_files(
    object_store: &ObjectStore,
    server_id: ServerId,
    db_name: &str,
) -> Result<Vec<(Path, FileType, u64, Uuid)>> {
    let list_path = catalog_path(&object_store, server_id, &db_name);
    let paths = object_store
        .list(Some(&list_path))
        .await
        .context(Read {})?
        .map_ok(|paths| {
            paths
                .into_iter()
                .filter_map(|path| {
                    parse_file_path(path.clone()).map(|(revision_counter, uuid, file_type)| {
                        (path.clone(), file_type, revision_counter, uuid)
                    })
                })
                .collect()
        })
        .try_concat()
        .await
        .context(Read {})?;
    Ok(paths)
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

/// Parse timestamp from protobuf.
fn parse_timestamp(
    ts: &Option<generated_types::google::protobuf::Timestamp>,
) -> Result<DateTime<Utc>> {
    let ts: generated_types::google::protobuf::Timestamp =
        ts.as_ref().context(DateTimeRequired)?.clone();
    let ts: DateTime<Utc> = ts.try_into().context(DateTimeParseError)?;
    Ok(ts)
}

/// Parse encoding from protobuf.
fn parse_encoding(encoding: i32) -> Result<proto::transaction::Encoding> {
    let parsed = proto::transaction::Encoding::from_i32(encoding)
        .context(EncodingParseError { data: encoding })?;
    if parsed == proto::transaction::Encoding::Unspecified {
        Err(Error::EncodingParseError { data: encoding })
    } else {
        Ok(parsed)
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
    S: CatalogState + Send + Sync,
{
    tstate: S::TransactionState,
    proto: proto::Transaction,
}

impl<S> OpenTransaction<S>
where
    S: CatalogState + Send + Sync,
{
    /// Private API to create new transaction, users should always use [`PreservedCatalog::open_transaction`].
    fn new(catalog_inner: &PreservedCatalogInner<S>, uuid: Uuid) -> Self {
        let (revision_counter, previous_uuid) = match &catalog_inner.previous_tkey {
            Some(tkey) => (tkey.revision_counter + 1, tkey.uuid.to_string()),
            None => (0, String::new()),
        };

        Self {
            tstate: S::transaction_begin(&catalog_inner.state),
            proto: proto::Transaction {
                actions: vec![],
                version: TRANSACTION_VERSION,
                uuid: uuid.to_string(),
                revision_counter,
                previous_uuid,
                start_timestamp: Some(Utc::now().into()),
                encoding: proto::transaction::Encoding::Delta.into(),
            },
        }
    }

    fn tkey(&self) -> TransactionKey {
        TransactionKey {
            revision_counter: self.proto.revision_counter,
            uuid: Uuid::parse_str(&self.proto.uuid).expect("UUID was checked before"),
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
        tstate: &mut S::TransactionState,
        action: &proto::transaction::action::Action,
        object_store: &Arc<ObjectStore>,
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
                    IoxParquetMetaData::from_thrift(&a.metadata).context(MetadataDecodingFailed)?;
                let metadata = Arc::new(metadata);

                S::add(
                    tstate,
                    Arc::clone(object_store),
                    CatalogParquetInfo { path, metadata },
                )?;
            }
            proto::transaction::action::Action::RemoveParquet(a) => {
                let path = parse_dirs_and_filename(&a.path)?;
                S::remove(tstate, path)?;
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
    ) -> Result<()> {
        Self::handle_action(&mut self.tstate, &action, object_store)?;
        self.proto.actions.push(proto::transaction::Action {
            action: Some(action),
        });
        Ok(())
    }

    /// Commit to mutable catalog and return previous transaction key.
    fn commit(self, catalog_inner: &mut PreservedCatalogInner<S>) -> Option<TransactionKey> {
        let mut tkey = Some(self.tkey());
        catalog_inner.state = S::transaction_end(self.tstate, TransactionEnd::Commit);
        std::mem::swap(&mut catalog_inner.previous_tkey, &mut tkey);
        tkey
    }

    /// Abort transaction
    fn abort(self, catalog_inner: &mut PreservedCatalogInner<S>) {
        catalog_inner.state = S::transaction_end(self.tstate, TransactionEnd::Abort);
    }

    async fn store(
        &self,
        object_store: &ObjectStore,
        server_id: ServerId,
        db_name: &str,
    ) -> Result<()> {
        let path = file_path(
            object_store,
            server_id,
            db_name,
            &self.tkey(),
            FileType::Transaction,
        );
        store_transaction_proto(object_store, &path, &self.proto).await?;
        Ok(())
    }

    async fn load_and_apply(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        tkey: &TransactionKey,
        state: &Arc<S>,
        last_tkey: &Option<TransactionKey>,
        file_type: FileType,
    ) -> Result<Arc<S>> {
        // recover state from store
        let path = file_path(object_store, server_id, db_name, tkey, file_type);
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
        let encoding = file_type.encoding();
        if encoding == proto::transaction::Encoding::Delta {
            // only verify chain for delta encodings
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
        }
        // verify we can parse the timestamp (checking that no error is raised)
        parse_timestamp(&proto.start_timestamp)?;
        let encoding_actual = parse_encoding(proto.encoding)?;
        if encoding_actual != encoding {
            return Err(Error::WrongEncodingError {
                actual: encoding_actual,
                expected: encoding,
            });
        }

        // start transaction
        let mut tstate = S::transaction_begin(state);

        // apply
        for action in &proto.actions {
            if let Some(action) = action.action.as_ref() {
                Self::handle_action(&mut tstate, action, object_store)?;
            }
        }

        // commit
        let state = S::transaction_end(tstate, TransactionEnd::Commit);

        Ok(state)
    }
}

/// Handle for an open uncommitted transaction.
///
/// Dropping this object w/o calling [`commit`](Self::commit) will issue a warning.
pub struct TransactionHandle<'c, S>
where
    S: CatalogState + Send + Sync,
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
    S: CatalogState + Send + Sync,
{
    async fn new(catalog: &'c PreservedCatalog<S>, uuid: Uuid) -> TransactionHandle<'c, S> {
        // first acquire semaphore (which is only being used for transactions), then get state lock
        let permit = catalog
            .transaction_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");
        let inner_guard = catalog.inner.write();

        let transaction = OpenTransaction::new(&inner_guard, uuid);

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
    /// This will first commit to object store and then to the in-memory state.
    ///
    /// If `create_checkpoint` is set this will also create a checkpoint at the end of the commit. Note that if the
    /// checkpoint creation fails, the commit will still be treated as completed since the checkpoint is a mere
    /// optimization to speed up transaction replay and allow to prune the history.
    pub async fn commit(mut self, create_checkpoint: bool) -> Result<()> {
        let t = std::mem::take(&mut self.transaction)
            .expect("calling .commit on a closed transaction?!");
        let tkey = t.tkey();

        // write to object store
        match t
            .store(
                &self.catalog.object_store,
                self.catalog.server_id,
                &self.catalog.db_name,
            )
            .await
        {
            Ok(()) => {
                // commit to catalog
                let (previous_tkey, state) = self.commit_inner(t);
                info!(?tkey, "transaction committed");

                // maybe create a checkpoint
                // IMPORTANT: Create the checkpoint AFTER commiting the transaction to object store and to the in-memory state.
                //            Checkpoints are an optional optimization and are not required to materialize a transaction.
                if create_checkpoint {
                    // NOTE: `inner_guard` will not be re-used here since it is a strong write lock and the checkpoint creation
                    //       only needs a read lock.
                    self.create_checkpoint(tkey, previous_tkey, state).await?;
                }

                Ok(())
            }
            Err(e) => {
                warn!(?tkey, "failure while writing transaction, aborting");
                self.abort_inner(t);
                Err(e)
            }
        }
    }

    /// Commit helper function.
    ///
    /// This function mostly exists for the following reasons:
    ///
    /// - the read-write guard for the inner catalog state should be limited in scope to avoid long write-locks
    /// - rustc seems to fold the guard into the async generator state even when we `drop` it quickly, making the
    ///   resulting future `!Send`. However tokio requires our futures to be `Send`.
    fn commit_inner(&self, t: OpenTransaction<S>) -> (Option<TransactionKey>, Arc<S>) {
        let mut inner_guard = self.catalog.inner.write();
        let previous_tkey = t.commit(&mut inner_guard);
        let state = Arc::clone(&inner_guard.state);

        (previous_tkey, state)
    }

    fn abort_inner(&self, t: OpenTransaction<S>) {
        let mut inner_guard = self.catalog.inner.write();
        t.abort(&mut inner_guard);
    }

    async fn create_checkpoint(
        &self,
        tkey: TransactionKey,
        previous_tkey: Option<TransactionKey>,
        state: Arc<S>,
    ) -> Result<()> {
        let files = state.files();
        let object_store = self.catalog.object_store();
        let server_id = self.catalog.server_id();
        let db_name = self.catalog.db_name();

        // sort by key (= path) for deterministic output
        let files = {
            let mut tmp: Vec<_> = files.into_iter().collect();
            tmp.sort_by_key(|(path, _metadata)| path.clone());
            tmp
        };

        // create transaction to add parquet files
        let actions: Result<Vec<_>, Error> = files
            .into_iter()
            .map(|(path, metadata)| {
                Ok(proto::transaction::Action {
                    action: Some(proto::transaction::action::Action::AddParquet(
                        proto::AddParquet {
                            path: Some(unparse_dirs_and_filename(&path)),
                            metadata: metadata.to_thrift().context(MetadataEncodingFailed)?,
                        },
                    )),
                })
            })
            .collect();
        let actions = actions?;

        // assemble and store checkpoint protobuf
        let proto = proto::Transaction {
            actions,
            version: TRANSACTION_VERSION,
            uuid: tkey.uuid.to_string(),
            revision_counter: tkey.revision_counter,
            previous_uuid: previous_tkey.map_or_else(String::new, |tkey| tkey.uuid.to_string()),
            start_timestamp: Some(Utc::now().into()),
            encoding: proto::transaction::Encoding::Full.into(),
        };
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Checkpoint,
        );
        store_transaction_proto(&object_store, &path, &proto).await?;

        Ok(())
    }

    /// Abort transaction w/o commit.
    pub fn abort(mut self) {
        let t = std::mem::take(&mut self.transaction)
            .expect("calling .commit on a closed transaction?!");
        self.abort_inner(t);
    }

    /// Add a new parquet file to the catalog.
    ///
    /// If a file with the same path already exists an error will be returned.
    pub fn add_parquet(
        &mut self,
        path: &DirsAndFileName,
        metadata: &IoxParquetMetaData,
    ) -> Result<()> {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .handle_action_and_record(
                proto::transaction::action::Action::AddParquet(proto::AddParquet {
                    path: Some(unparse_dirs_and_filename(path)),
                    metadata: metadata.to_thrift().context(MetadataEncodingFailed)?,
                }),
                &self.catalog.object_store,
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
            )
    }
}

impl<'c, S> Debug for TransactionHandle<'c, S>
where
    S: CatalogState + Send + Sync,
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
    S: CatalogState + Send + Sync,
{
    fn drop(&mut self) {
        if let Some(t) = self.transaction.take() {
            warn!(?self, "dropped uncommitted transaction, calling abort");
            self.abort_inner(t);
        }
    }
}

pub mod test_helpers {
    use object_store::parsed_path;

    use crate::test_utils::{chunk_addr, make_metadata, make_object_store};

    use super::*;
    use std::{convert::TryFrom, ops::Deref};

    /// In-memory catalog state, for testing.
    #[derive(Clone, Debug)]
    pub struct TestCatalogState {
        /// Map of all parquet files that are currently registered.
        pub parquet_files: HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
    }

    #[derive(Debug)]
    pub struct TState {
        old: Arc<TestCatalogState>,
        new: TestCatalogState,
    }

    impl CatalogState for TestCatalogState {
        type EmptyInput = ();

        fn new_empty(_db_name: &str, _data: Self::EmptyInput) -> Self {
            Self {
                parquet_files: HashMap::new(),
            }
        }

        type TransactionState = TState;

        fn transaction_begin(origin: &Arc<Self>) -> Self::TransactionState {
            Self::TransactionState {
                old: Arc::clone(origin),
                new: origin.deref().clone(),
            }
        }

        fn transaction_end(tstate: Self::TransactionState, how: TransactionEnd) -> Arc<Self> {
            match how {
                TransactionEnd::Abort => tstate.old,
                TransactionEnd::Commit => Arc::new(tstate.new),
            }
        }

        fn add(
            tstate: &mut Self::TransactionState,
            _object_store: Arc<ObjectStore>,
            info: CatalogParquetInfo,
        ) -> Result<()> {
            match tstate.new.parquet_files.entry(info.path) {
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

        fn remove(tstate: &mut Self::TransactionState, path: DirsAndFileName) -> Result<()> {
            match tstate.new.parquet_files.entry(path) {
                Occupied(o) => {
                    o.remove();
                }
                Vacant(v) => {
                    return Err(Error::ParquetFileDoesNotExist { path: v.into_key() });
                }
            }

            Ok(())
        }

        fn files(&self) -> HashMap<DirsAndFileName, Arc<IoxParquetMetaData>> {
            self.parquet_files.clone()
        }
    }

    /// Break preserved catalog by moving one of the transaction files into a weird unknown version.
    pub async fn break_catalog_with_weird_version<S>(catalog: &PreservedCatalog<S>)
    where
        S: CatalogState + Send + Sync,
    {
        let tkey = get_tkey(catalog);
        let path = file_path(
            &catalog.object_store,
            catalog.server_id,
            &catalog.db_name,
            &tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&catalog.object_store, &path)
            .await
            .unwrap();
        proto.version = 42;
        store_transaction_proto(&catalog.object_store, &path, &proto)
            .await
            .unwrap();
    }

    /// Helper function to ensure that guards don't leak into the future state machine.
    fn get_tkey<S>(catalog: &PreservedCatalog<S>) -> TransactionKey
    where
        S: CatalogState + Send + Sync,
    {
        let guard = catalog.inner.read();
        guard
            .previous_tkey
            .as_ref()
            .expect("should have at least a single transaction")
            .clone()
    }

    /// Torture-test implementations for [`CatalogState`].
    pub async fn assert_catalog_state_implementation<S>(state_data: S::EmptyInput)
    where
        S: CatalogState + Send + Sync,
    {
        // empty state
        let object_store = make_object_store();
        let catalog = PreservedCatalog::<S>::new_empty(
            Arc::clone(&object_store),
            ServerId::try_from(1).unwrap(),
            "db1".to_string(),
            state_data,
        )
        .await
        .unwrap();
        let mut expected = HashMap::new();
        assert_files_eq(&catalog.state().files(), &expected);

        // add files
        let mut chunk_id_watermark = 5;
        {
            let mut transaction = catalog.open_transaction().await;

            for chunk_id in 0..chunk_id_watermark {
                let path = parsed_path!(format!("chunk_{}", chunk_id).as_ref());
                let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(chunk_id)).await;
                transaction.add_parquet(&path, &metadata).unwrap();
                expected.insert(path, Arc::new(metadata));
            }

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // remove files
        {
            let mut transaction = catalog.open_transaction().await;

            let path = parsed_path!("chunk_1");
            transaction.remove_parquet(&path).unwrap();
            expected.remove(&path);

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // add and remove in the same transaction
        {
            let mut transaction = catalog.open_transaction().await;

            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            transaction.add_parquet(&path, &metadata).unwrap();
            transaction.remove_parquet(&path).unwrap();
            chunk_id_watermark += 1;

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // remove and add in the same transaction
        {
            let mut transaction = catalog.open_transaction().await;

            let path = parsed_path!("chunk_2");
            let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(2)).await;
            transaction.remove_parquet(&path).unwrap();
            transaction.add_parquet(&path, &metadata).unwrap();

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // add, remove, add in the same transaction
        {
            let mut transaction = catalog.open_transaction().await;

            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            transaction.add_parquet(&path, &metadata).unwrap();
            transaction.remove_parquet(&path).unwrap();
            transaction.add_parquet(&path, &metadata).unwrap();
            expected.insert(path, Arc::new(metadata));
            chunk_id_watermark += 1;

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // remove, add, remove in same transaction
        {
            let mut transaction = catalog.open_transaction().await;

            let path = parsed_path!("chunk_2");
            let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(2)).await;
            transaction.remove_parquet(&path).unwrap();
            transaction.add_parquet(&path, &metadata).unwrap();
            transaction.remove_parquet(&path).unwrap();
            expected.remove(&path);

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // error handling, no real opt
        {
            let mut transaction = catalog.open_transaction().await;

            // already exists (should also not change the metadata)
            let path = parsed_path!("chunk_0");
            let (_, metadata) = make_metadata(&object_store, "fail", chunk_addr(0)).await;
            let err = transaction.add_parquet(&path, &metadata).unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // does not exist
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let err = transaction.remove_parquet(&path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
            chunk_id_watermark += 1;

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // error handling, still something works
        {
            let mut transaction = catalog.open_transaction().await;

            // already exists (should also not change the metadata)
            let path = parsed_path!("chunk_0");
            let (_, metadata) = make_metadata(&object_store, "fail", chunk_addr(0)).await;
            let err = transaction.add_parquet(&path, &metadata).unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // this transaction will still work
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            transaction.add_parquet(&path, &metadata).unwrap();
            expected.insert(path.clone(), Arc::new(metadata.clone()));
            chunk_id_watermark += 1;

            // recently added
            let err = transaction.add_parquet(&path, &metadata).unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // does not exist
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let err = transaction.remove_parquet(&path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
            chunk_id_watermark += 1;

            // this still works
            let path = parsed_path!("chunk_3");
            transaction.remove_parquet(&path).unwrap();
            expected.remove(&path);

            // recently removed
            let err = transaction.remove_parquet(&path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));

            transaction.commit(true).await.unwrap();
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // transaction aborting
        {
            let mut transaction = catalog.open_transaction().await;

            // add
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            transaction.add_parquet(&path, &metadata).unwrap();
            chunk_id_watermark += 1;

            // remove
            let path = parsed_path!("chunk_4");
            transaction.remove_parquet(&path).unwrap();

            // add and remove
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            transaction.add_parquet(&path, &metadata).unwrap();
            transaction.remove_parquet(&path).unwrap();
            chunk_id_watermark += 1;
        }
        assert_files_eq(&catalog.state().files(), &expected);

        // transaction aborting w/ errors
        {
            let mut transaction = catalog.open_transaction().await;

            // already exists (should also not change the metadata)
            let path = parsed_path!("chunk_0");
            let (_, metadata) = make_metadata(&object_store, "fail", chunk_addr(0)).await;
            let err = transaction.add_parquet(&path, &metadata).unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // does not exist
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let err = transaction.remove_parquet(&path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
            chunk_id_watermark += 1;
        }

        // consume variable so that we can easily add tests w/o re-adding the final modification
        println!("{}", chunk_id_watermark);
    }

    /// Assert that tracked files and their linked metadata are equal.
    fn assert_files_eq(
        actual: &HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
        expected: &HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
    ) {
        let sorted_keys_actual = get_sorted_keys(actual);
        let sorted_keys_expected = get_sorted_keys(expected);
        assert_eq!(sorted_keys_actual, sorted_keys_expected);

        for k in sorted_keys_actual {
            let md_actual = &actual[&k];
            let md_expected = &expected[&k];

            let iox_md_actual = md_actual.read_iox_metadata().unwrap();
            let iox_md_expected = md_expected.read_iox_metadata().unwrap();
            assert_eq!(iox_md_actual, iox_md_expected);

            let schema_actual = md_actual.read_schema().unwrap();
            let schema_expected = md_expected.read_schema().unwrap();
            assert_eq!(schema_actual, schema_expected);

            let stats_actual = md_actual.read_statistics(&schema_actual, "foo").unwrap();
            let stats_expected = md_expected
                .read_statistics(&schema_expected, "foo")
                .unwrap();
            assert_eq!(stats_actual, stats_expected);
        }
    }

    /// Get a sorted list of keys from `HashMap`.
    fn get_sorted_keys<K, V>(map: &HashMap<K, V>) -> Vec<K>
    where
        K: Clone + Ord,
    {
        let mut keys: Vec<K> = map.keys().cloned().collect();
        keys.sort();
        keys
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, ops::Deref};

    use crate::test_utils::{chunk_addr, make_metadata, make_object_store};
    use object_store::parsed_path;

    use super::test_helpers::{
        assert_catalog_state_implementation, break_catalog_with_weird_version, TestCatalogState,
    };
    use super::*;

    #[tokio::test]
    async fn test_create_empty() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        assert!(
            !PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        assert!(
            PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_some());
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
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(format!("{}.foo", Uuid::new_v4()));
        create_empty_file(&object_store, &path).await;

        // no file extension
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(Uuid::new_v4().to_string());
        create_empty_file(&object_store, &path).await;

        // broken UUID
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(format!("foo.{}", TRANSACTION_FILE_SUFFIX));
        create_empty_file(&object_store, &path).await;

        // broken revision counter
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("foo");
        path.set_file_name(format!("{}.{}", Uuid::new_v4(), TRANSACTION_FILE_SUFFIX));
        create_empty_file(&object_store, &path).await;

        // file is folder
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.push_dir(format!("{}.{}", Uuid::new_v4(), TRANSACTION_FILE_SUFFIX));
        path.set_file_name("foo");
        create_empty_file(&object_store, &path).await;

        // top-level file
        let mut path = catalog_path(&object_store, server_id, &db_name);
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        checked_delete(&object_store, &path).await;

        // loading catalog should fail now
        let res = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "Missing transaction: 0",);
    }

    #[tokio::test]
    async fn test_transaction_version_mismatch() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        let catalog = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        break_catalog_with_weird_version(&catalog).await;

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
            format!("Format version of transaction file for revision 2 is 42 but only [{}] are supported", TRANSACTION_VERSION)
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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
    async fn test_broken_protobuf() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let data = Bytes::from("foo");
        let len = data.len();
        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
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
            "Error during deserialization: failed to decode Protobuf message: invalid wire type value: 6"
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
        )
        .await
        .unwrap();
        let mut t = catalog.open_transaction().await;

        // open transaction
        t.transaction.as_mut().unwrap().proto.uuid = Uuid::nil().to_string();
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
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // re-create transaction file with different UUID
        assert!(trace.tkeys.len() >= 2);
        let mut tkey = trace.tkeys[1].clone();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        let old_uuid = tkey.uuid;

        let new_uuid = Uuid::new_v4();
        tkey.uuid = new_uuid;
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
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
    async fn test_fork_checkpoint() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // create checkpoint file with different UUID
        assert!(trace.tkeys.len() >= 2);
        let mut tkey = trace.tkeys[1].clone();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        let old_uuid = tkey.uuid;

        let new_uuid = Uuid::new_v4();
        tkey.uuid = new_uuid;
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Checkpoint,
        );
        proto.uuid = new_uuid.to_string();
        proto.encoding = proto::transaction::Encoding::Full.into();
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
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
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

    #[tokio::test]
    async fn test_missing_start_timestamp() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.start_timestamp = None;
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
            "Internal: Datetime required but missing in serialized catalog"
        );
    }

    #[tokio::test]
    async fn test_broken_start_timestamp() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.start_timestamp = Some(generated_types::google::protobuf::Timestamp {
            seconds: 0,
            nanos: -1,
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
            "Internal: Cannot parse datetime in serialized catalog: out of range integral type conversion attempted"
        );
    }

    #[tokio::test]
    async fn test_broken_encoding() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.encoding = -1;
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
            "Internal: Cannot parse encoding in serialized catalog: -1 is not a valid, specified variant"
        );
    }

    #[tokio::test]
    async fn test_wrong_encoding_in_transaction_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.encoding = proto::transaction::Encoding::Full.into();
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
            "Internal: Found wrong encoding in serialized catalog file: Expected Delta but got Full"
        );
    }

    #[tokio::test]
    async fn test_missing_encoding_in_transaction_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.encoding = 0;
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
            "Internal: Cannot parse encoding in serialized catalog: 0 is not a valid, specified variant"
        );
    }

    #[tokio::test]
    async fn test_wrong_encoding_in_checkpoint_file() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let proto = load_transaction_proto(&object_store, &path).await.unwrap();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Checkpoint,
        );
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
            "Internal: Found wrong encoding in serialized catalog file: Expected Full but got Delta"
        );
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let addr = chunk_addr(1337);
        let db_name = &addr.db_name;
        let (_, metadata) = make_metadata(&object_store, "foo", addr.clone()).await;

        // use common test as baseline
        let mut trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // re-open catalog
        let catalog = PreservedCatalog::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();

        // create empty transaction w/ checkpoint (the delta transaction file is not required for catalog loading)
        {
            let transaction = catalog.open_transaction().await;
            transaction.commit(true).await.unwrap();
        }
        trace.record(&catalog, false);

        // create another transaction on-top that adds a file (this transaction will be required to load the full state)
        {
            let mut transaction = catalog.open_transaction().await;
            transaction
                .add_parquet(&parsed_path!("last_one"), &metadata)
                .unwrap();
            transaction.commit(true).await.unwrap();
        }
        trace.record(&catalog, false);

        // close catalog again
        drop(catalog);

        // remove first transaction files (that is no longer required)
        for i in 0..(trace.tkeys.len() - 1) {
            if trace.aborted[i] {
                continue;
            }
            let tkey = &trace.tkeys[i];
            let path = file_path(
                &object_store,
                server_id,
                db_name,
                tkey,
                FileType::Transaction,
            );
            checked_delete(&object_store, &path).await;
        }

        // load catalog from store and check replayed state
        let catalog = PreservedCatalog::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            catalog.revision_counter(),
            trace.tkeys.last().unwrap().revision_counter
        );
        assert_catalog_parquet_files(
            &catalog,
            &get_catalog_parquet_files(trace.states.last().unwrap()),
        );
    }

    /// Get sorted list of catalog files from state
    fn get_catalog_parquet_files(state: &TestCatalogState) -> Vec<(String, IoxParquetMetaData)> {
        let mut files: Vec<(String, IoxParquetMetaData)> = state
            .parquet_files
            .iter()
            .map(|(path, md)| (path.display(), md.as_ref().clone()))
            .collect();
        files.sort_by_key(|(path, _)| path.clone());
        files
    }

    /// Assert that set of parquet files tracked by a catalog are identical to the given sorted list.
    fn assert_catalog_parquet_files(
        catalog: &PreservedCatalog<TestCatalogState>,
        expected: &[(String, IoxParquetMetaData)],
    ) {
        let actual = get_catalog_parquet_files(&catalog.state());
        for ((actual_path, actual_md), (expected_path, expected_md)) in
            actual.iter().zip(expected.iter())
        {
            assert_eq!(actual_path, expected_path);

            let actual_schema = actual_md.read_schema().unwrap();
            let expected_schema = expected_md.read_schema().unwrap();
            assert_eq!(actual_schema, expected_schema);

            // NOTE: the actual table name is not important here as long as it is the same for both calls, since it is
            // only used to generate out statistics struct (not to read / dispatch anything).
            let actual_stats = actual_md.read_statistics(&actual_schema, "foo").unwrap();
            let expected_stats = expected_md
                .read_statistics(&expected_schema, "foo")
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

        fn record(&mut self, catalog: &PreservedCatalog<TestCatalogState>, aborted: bool) {
            self.tkeys
                .push(catalog.inner.read().previous_tkey.clone().unwrap());
            self.states.push(catalog.state().deref().clone());
            self.post_timestamps.push(Utc::now());
            self.aborted.push(aborted);
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
        )
        .await
        .unwrap();

        // get some test metadata
        let (_, metadata1) = make_metadata(object_store, "foo", chunk_addr(1)).await;
        let (_, metadata2) = make_metadata(object_store, "bar", chunk_addr(1)).await;

        // track all the intermediate results
        let mut trace = TestTrace::new();

        // empty catalog has no data
        assert_eq!(catalog.revision_counter(), 0);
        assert_catalog_parquet_files(&catalog, &[]);
        trace.record(&catalog, false);

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

            t.commit(false).await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 1);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test1".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, false);

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

            t.commit(false).await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, false);

        // uncommitted modifications have no effect
        {
            let mut t = catalog.open_transaction().await;

            t.add_parquet(&parsed_path!("test5"), &metadata1).unwrap();
            t.remove_parquet(&parsed_path!(["sub1"], "test2")).unwrap();

            // NO commit here!
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(
            &catalog,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, true);

        trace
    }

    #[tokio::test]
    async fn test_create_twice() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        let res = PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await;
        assert_eq!(res.unwrap_err().to_string(), "Catalog already exists");
    }

    #[tokio::test]
    async fn test_wipe_nothing() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        PreservedCatalog::<TestCatalogState>::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wipe_normal() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // create a real catalog
        assert_single_catalog_inmem_works(&object_store, make_server_id(), "db1").await;

        // wipe
        PreservedCatalog::<TestCatalogState>::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // `exists` and `load` both report "no data"
        assert!(
            !PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        // can create new catalog
        PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_wipe_broken_catalog() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // create a real catalog
        assert_single_catalog_inmem_works(&object_store, make_server_id(), "db1").await;

        // break
        let catalog = PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        break_catalog_with_weird_version(&catalog).await;

        // wipe
        PreservedCatalog::<TestCatalogState>::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // `exists` and `load` both report "no data"
        assert!(
            !PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        // can create new catalog
        PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_wipe_ignores_unknown_files() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        // create a real catalog
        assert_single_catalog_inmem_works(&object_store, make_server_id(), "db1").await;

        // create junk
        let mut path = catalog_path(&object_store, server_id, &db_name);
        path.push_dir("0");
        path.set_file_name(format!("{}.foo", Uuid::new_v4()));
        create_empty_file(&object_store, &path).await;

        // wipe
        PreservedCatalog::<TestCatalogState>::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // check file is still there
        let prefix = catalog_path(&object_store, server_id, &db_name);
        let files = object_store
            .list(Some(&prefix))
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap();
        let mut files: Vec<_> = files.iter().map(|path| path.display()).collect();
        files.sort();
        assert_eq!(files, vec![path.display()]);
    }

    #[tokio::test]
    async fn test_transaction_handle_revision_counter() {
        let object_store = make_object_store();
        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            object_store,
            make_server_id(),
            "db1".to_string(),
            (),
        )
        .await
        .unwrap();
        let t = catalog.open_transaction().await;

        assert_eq!(t.revision_counter(), 1);
    }

    #[tokio::test]
    async fn test_transaction_handle_uuid() {
        let object_store = make_object_store();
        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            object_store,
            make_server_id(),
            "db1".to_string(),
            (),
        )
        .await
        .unwrap();
        let mut t = catalog.open_transaction().await;

        t.transaction.as_mut().unwrap().proto.uuid = Uuid::nil().to_string();
        assert_eq!(t.uuid(), Uuid::nil());
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_ok() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        let ts = find_last_transaction_timestamp(&object_store, server_id, db_name)
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
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        assert!(
            find_last_transaction_timestamp(&object_store, server_id, db_name)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_find_last_transaction_timestamp_datetime_broken() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let mut proto = load_transaction_proto(&object_store, &path).await.unwrap();
        proto.start_timestamp = None;
        store_transaction_proto(&object_store, &path, &proto)
            .await
            .unwrap();

        let ts = find_last_transaction_timestamp(&object_store, server_id, db_name)
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
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        // break transaction file
        assert!(trace.tkeys.len() >= 2);
        let tkey = &trace.tkeys[0];
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            tkey,
            FileType::Transaction,
        );
        let data = Bytes::from("foo");
        let len = data.len();
        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();

        let ts = find_last_transaction_timestamp(&object_store, server_id, db_name)
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
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";
        let mut trace = assert_single_catalog_inmem_works(&object_store, server_id, db_name).await;

        let catalog = PreservedCatalog::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();

        // create empty transaction w/ checkpoint
        {
            let transaction = catalog.open_transaction().await;
            transaction.commit(true).await.unwrap();
        }
        trace.record(&catalog, false);

        // delete transaction files
        for (aborted, tkey) in trace.aborted.iter().zip(trace.tkeys.iter()) {
            if *aborted {
                continue;
            }
            let path = file_path(
                &object_store,
                server_id,
                db_name,
                tkey,
                FileType::Transaction,
            );
            checked_delete(&object_store, &path).await;
        }
        drop(catalog);

        let ts = find_last_transaction_timestamp(&object_store, server_id, db_name)
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
            catalog.revision_counter(),
            trace.tkeys.last().unwrap().revision_counter
        );
        assert_catalog_parquet_files(
            &catalog,
            &get_catalog_parquet_files(trace.states.last().unwrap()),
        );
    }

    #[tokio::test]
    async fn test_exists_considers_checkpoints() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        assert!(
            !PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );

        let catalog = PreservedCatalog::<TestCatalogState>::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // delete transaction file
        let tkey = catalog.inner.read().previous_tkey.clone().unwrap();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
        checked_delete(&object_store, &path).await;

        // create empty transaction w/ checkpoint
        {
            let transaction = catalog.open_transaction().await;
            transaction.commit(true).await.unwrap();
        }

        // delete transaction file
        let tkey = catalog.inner.read().previous_tkey.clone().unwrap();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
        checked_delete(&object_store, &path).await;

        drop(catalog);

        assert!(
            PreservedCatalog::<TestCatalogState>::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::<TestCatalogState>::load(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_some());
    }

    #[tokio::test]
    async fn test_catalog_state() {
        assert_catalog_state_implementation::<TestCatalogState>(()).await;
    }
}
