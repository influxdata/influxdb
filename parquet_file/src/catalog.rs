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

    #[snafu(display("Cannot commit transaction: {}", source))]
    CommitError { source: Box<Error> },
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

/// Abstraction over how the in-memory state of the catalog works.
pub trait CatalogState {
    /// Input to create a new empty instance.
    ///
    /// See [`new_empty`](Self::new_empty) for details.
    type EmptyInput: Send;

    /// Create empty state w/o any known files.
    fn new_empty(db_name: &str, data: Self::EmptyInput) -> Self;

    /// Add parquet file to state.
    fn add(&mut self, object_store: Arc<ObjectStore>, info: CatalogParquetInfo) -> Result<()>;

    /// Remove parquet file from state.
    fn remove(&mut self, path: DirsAndFileName) -> Result<()>;
}

/// In-memory view of the preserved catalog.
pub struct PreservedCatalog {
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
    previous_tkey: RwLock<Option<TransactionKey>>,
    transaction_semaphore: Semaphore,

    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    db_name: String,
}

impl PreservedCatalog {
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

    /// Create new catalog w/o any data.
    ///
    /// An empty transaction will be used to mark the catalog start so that concurrent open but still-empty catalogs can
    /// easily be detected.
    pub async fn new_empty<S>(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: String,
        state_data: S::EmptyInput,
    ) -> Result<(Self, Arc<S>)>
    where
        S: CatalogState + Send + Sync,
    {
        if Self::exists(&object_store, server_id, &db_name).await? {
            return Err(Error::AlreadyExists {});
        }
        let state = Arc::new(S::new_empty(&db_name, state_data));

        let catalog = Self {
            previous_tkey: RwLock::new(None),
            transaction_semaphore: Semaphore::new(1),
            object_store,
            server_id,
            db_name,
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
    /// Loading starts at the latest checkpoint or -- if none exists -- at transaction `0`. Transactions before that
    /// point are neither verified nor are they required to exist.
    pub async fn load<S>(
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: String,
        state_data: S::EmptyInput,
    ) -> Result<Option<(Self, Arc<S>)>>
    where
        S: CatalogState + Send + Sync,
    {
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
        let mut state = S::new_empty(&db_name, state_data);
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
                &object_store,
                server_id,
                &db_name,
                &tkey,
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
                object_store,
                server_id,
                db_name,
            },
            Arc::new(state),
        )))
    }

    /// Open a new transaction.
    ///
    /// Note that only a single transaction can be open at any time. This call will `await` until any outstanding
    /// transaction handle is dropped. The newly created transaction will contain the state after `await` (esp.
    /// post-blocking). This system is fair, which means that transactions are given out in the order they were
    /// requested.
    pub async fn open_transaction(&self) -> TransactionHandle<'_> {
        self.open_transaction_with_uuid(Uuid::new_v4()).await
    }

    /// Crate-private API to open an transaction with a specified UUID. Should only be used for catalog rebuilding or
    /// with a fresh V4-UUID!
    pub(crate) async fn open_transaction_with_uuid(&self, uuid: Uuid) -> TransactionHandle<'_> {
        TransactionHandle::new(self, uuid).await
    }

    /// Get latest revision counter.
    pub fn revision_counter(&self) -> u64 {
        self.previous_tkey
            .read()
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

impl Debug for PreservedCatalog {
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
struct OpenTransaction {
    proto: proto::Transaction,
}

impl OpenTransaction {
    /// Private API to create new transaction, users should always use [`PreservedCatalog::open_transaction`].
    fn new(previous_tkey: &Option<TransactionKey>, uuid: Uuid) -> Self {
        let (revision_counter, previous_uuid) = match previous_tkey {
            Some(tkey) => (tkey.revision_counter + 1, tkey.uuid.to_string()),
            None => (0, String::new()),
        };

        Self {
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
    fn handle_action<S>(
        state: &mut S,
        action: &proto::transaction::action::Action,
        object_store: &Arc<ObjectStore>,
    ) -> Result<()>
    where
        S: CatalogState,
    {
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

                state.add(
                    Arc::clone(object_store),
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

    async fn load_and_apply<S>(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        tkey: &TransactionKey,
        state: &mut S,
        last_tkey: &Option<TransactionKey>,
        file_type: FileType,
    ) -> Result<()>
    where
        S: CatalogState + Send,
    {
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

        // apply
        for action in &proto.actions {
            if let Some(action) = action.action.as_ref() {
                Self::handle_action(state, action, object_store)?;
            }
        }

        Ok(())
    }
}

/// Structure that holds all information required to create a checkpoint.
///
/// Note that while checkpoint are addressed using the same schema as we use for transaction (revision counter, UUID),
/// they contain the changes at the end (aka including) the transaction they refer.
#[derive(Debug)]
pub struct CheckpointData {
    /// List of all Parquet files that are currently (i.e. by the current version) tracked by the catalog.
    ///
    /// If a file was once added but later removed it MUST NOT appear in the result.
    pub files: HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
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
    async fn new(catalog: &'c PreservedCatalog, uuid: Uuid) -> TransactionHandle<'c> {
        // first acquire semaphore (which is only being used for transactions), then get state lock
        let permit = catalog
            .transaction_semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed");
        let previous_tkey_guard = catalog.previous_tkey.write();

        let transaction = OpenTransaction::new(&previous_tkey_guard, uuid);

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
    /// A [`CheckpointHandle`] will be returned that allows the caller to create a checkpoint. Note that this handle
    /// holds a transaction lock, so it's safe to assume that no other transaction is in-progress while the caller
    /// prepares the checkpoint.
    ///
    /// # Error Handling
    /// When this function returns with an error, it MUST be assumed that the commit has failed and all actions
    /// recorded with this handle are NOT preserved.
    pub async fn commit(mut self) -> Result<CheckpointHandle<'c>> {
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
                let previous_tkey = self.commit_inner(t);
                info!(?tkey, "transaction committed");

                // maybe create a checkpoint
                // IMPORTANT: Create the checkpoint AFTER commiting the transaction to object store and to the in-memory state.
                //            Checkpoints are an optional optimization and are not required to materialize a transaction.
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
    /// - the read-write guard for the inner catalog state should be limited in scope to avoid long write-locks
    /// - rustc seems to fold the guard into the async generator state even when we `drop` it quickly, making the
    ///   resulting future `!Send`. However tokio requires our futures to be `Send`.
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
            .record_action(proto::transaction::action::Action::AddParquet(
                proto::AddParquet {
                    path: Some(unparse_dirs_and_filename(path)),
                    metadata: metadata.to_thrift().context(MetadataEncodingFailed)?,
                },
            ));

        Ok(())
    }

    /// Remove a parquet file from the catalog.
    ///
    /// Removing files that do not exist or were already removed will result in an error.
    pub fn remove_parquet(&mut self, path: &DirsAndFileName) -> Result<()> {
        self.transaction
            .as_mut()
            .expect("transaction handle w/o transaction?!")
            .record_action(proto::transaction::action::Action::RemoveParquet(
                proto::RemoveParquet {
                    path: Some(unparse_dirs_and_filename(path)),
                },
            ));

        Ok(())
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

    // NOTE: The permit is technically used since we use it to reference the semaphore. It implements `drop` which we
    //       rely on.
    #[allow(dead_code)]
    permit: SemaphorePermit<'c>,
}

impl<'c> CheckpointHandle<'c> {
    /// Create a checkpoint for the just-committed transaction.
    ///
    /// Note that `checkpoint_data` must contain the state INCLUDING the just-committed transaction.
    ///
    /// # Error Handling
    /// If the checkpoint creation fails, the commit will still be treated as completed since the checkpoint is a mere
    /// optimization to speed up transaction replay and allow to prune the history.
    pub async fn create_checkpoint(self, checkpoint_data: CheckpointData) -> Result<()> {
        let object_store = self.catalog.object_store();
        let server_id = self.catalog.server_id();
        let db_name = self.catalog.db_name();

        // sort by key (= path) for deterministic output
        let files = {
            let mut tmp: Vec<_> = checkpoint_data.files.into_iter().collect();
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
            uuid: self.tkey.uuid.to_string(),
            revision_counter: self.tkey.revision_counter,
            previous_uuid: self
                .previous_tkey
                .map_or_else(String::new, |tkey| tkey.uuid.to_string()),
            start_timestamp: Some(Utc::now().into()),
            encoding: proto::transaction::Encoding::Full.into(),
        };
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &self.tkey,
            FileType::Checkpoint,
        );
        store_transaction_proto(&object_store, &path, &proto).await?;

        Ok(())
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

pub mod test_helpers {
    use object_store::parsed_path;

    use crate::test_utils::{chunk_addr, make_metadata, make_object_store};

    use super::*;
    use std::convert::TryFrom;

    /// In-memory catalog state, for testing.
    #[derive(Clone, Debug)]
    pub struct TestCatalogState {
        /// Map of all parquet files that are currently registered.
        pub parquet_files: HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
    }

    impl TestCatalogState {
        /// Simple way to create [`CheckpointData`].
        pub fn checkpoint_data(&self) -> CheckpointData {
            CheckpointData {
                files: self.parquet_files.clone(),
            }
        }
    }

    impl CatalogState for TestCatalogState {
        type EmptyInput = ();

        fn new_empty(_db_name: &str, _data: Self::EmptyInput) -> Self {
            Self {
                parquet_files: HashMap::new(),
            }
        }

        fn add(&mut self, _object_store: Arc<ObjectStore>, info: CatalogParquetInfo) -> Result<()> {
            match self.parquet_files.entry(info.path) {
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

        fn remove(&mut self, path: DirsAndFileName) -> Result<()> {
            match self.parquet_files.entry(path) {
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

    /// Break preserved catalog by moving one of the transaction files into a weird unknown version.
    pub async fn break_catalog_with_weird_version(catalog: &PreservedCatalog) {
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
    fn get_tkey(catalog: &PreservedCatalog) -> TransactionKey {
        let guard = catalog.previous_tkey.read();
        guard
            .as_ref()
            .expect("should have at least a single transaction")
            .clone()
    }

    /// Torture-test implementations for [`CatalogState`].
    ///
    /// A function to extract [`CheckpointData`] from the [`CatalogState`] must be provided.
    pub async fn assert_catalog_state_implementation<S, F>(state_data: S::EmptyInput, f: F)
    where
        S: CatalogState + Debug + Send + Sync,
        F: Fn(&S) -> CheckpointData + Send,
    {
        // empty state
        let object_store = make_object_store();
        let (_catalog, state) = PreservedCatalog::new_empty::<S>(
            Arc::clone(&object_store),
            ServerId::try_from(1).unwrap(),
            "db1".to_string(),
            state_data,
        )
        .await
        .unwrap();
        let mut state = Arc::try_unwrap(state).unwrap();
        let mut expected = HashMap::new();
        assert_checkpoint(&state, &f, &expected);

        // add files
        let mut chunk_id_watermark = 5;
        {
            for chunk_id in 0..chunk_id_watermark {
                let path = parsed_path!(format!("chunk_{}", chunk_id).as_ref());
                let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(chunk_id)).await;
                state
                    .add(
                        Arc::clone(&object_store),
                        CatalogParquetInfo {
                            path: path.clone(),
                            metadata: Arc::new(metadata.clone()),
                        },
                    )
                    .unwrap();
                expected.insert(path, Arc::new(metadata));
            }
        }
        assert_checkpoint(&state, &f, &expected);

        // remove files
        {
            let path = parsed_path!("chunk_1");
            state.remove(path.clone()).unwrap();
            expected.remove(&path);
        }
        assert_checkpoint(&state, &f, &expected);

        // add and remove in the same transaction
        {
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata),
                    },
                )
                .unwrap();
            state.remove(path.clone()).unwrap();
            chunk_id_watermark += 1;
        }
        assert_checkpoint(&state, &f, &expected);

        // remove and add in the same transaction
        {
            let path = parsed_path!("chunk_2");
            let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(2)).await;
            state.remove(path.clone()).unwrap();
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata),
                    },
                )
                .unwrap();
        }
        assert_checkpoint(&state, &f, &expected);

        // add, remove, add in the same transaction
        {
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata.clone()),
                    },
                )
                .unwrap();
            state.remove(path.clone()).unwrap();
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata.clone()),
                    },
                )
                .unwrap();
            expected.insert(path, Arc::new(metadata));
            chunk_id_watermark += 1;
        }
        assert_checkpoint(&state, &f, &expected);

        // remove, add, remove in same transaction
        {
            let path = parsed_path!("chunk_2");
            let (_, metadata) = make_metadata(&object_store, "ok", chunk_addr(2)).await;
            state.remove(path.clone()).unwrap();
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata),
                    },
                )
                .unwrap();
            state.remove(path.clone()).unwrap();
            expected.remove(&path);
        }
        assert_checkpoint(&state, &f, &expected);

        // error handling, no real opt
        {
            // already exists (should also not change the metadata)
            let path = parsed_path!("chunk_0");
            let (_, metadata) = make_metadata(&object_store, "fail", chunk_addr(0)).await;
            let err = state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata),
                    },
                )
                .unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // does not exist
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let err = state.remove(path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
            chunk_id_watermark += 1;
        }
        assert_checkpoint(&state, &f, &expected);

        // error handling, still something works
        {
            // already exists (should also not change the metadata)
            let path = parsed_path!("chunk_0");
            let (_, metadata) = make_metadata(&object_store, "fail", chunk_addr(0)).await;
            let err = state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata.clone()),
                    },
                )
                .unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // this transaction will still work
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let (_, metadata) =
                make_metadata(&object_store, "ok", chunk_addr(chunk_id_watermark)).await;
            state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata.clone()),
                    },
                )
                .unwrap();
            expected.insert(path.clone(), Arc::new(metadata.clone()));
            chunk_id_watermark += 1;

            // recently added
            let err = state
                .add(
                    Arc::clone(&object_store),
                    CatalogParquetInfo {
                        path: path.clone(),
                        metadata: Arc::new(metadata),
                    },
                )
                .unwrap_err();
            assert!(matches!(err, Error::ParquetFileAlreadyExists { .. }));

            // does not exist
            let path = parsed_path!(format!("chunk_{}", chunk_id_watermark).as_ref());
            let err = state.remove(path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
            chunk_id_watermark += 1;

            // this still works
            let path = parsed_path!("chunk_3");
            state.remove(path.clone()).unwrap();
            expected.remove(&path);

            // recently removed
            let err = state.remove(path).unwrap_err();
            assert!(matches!(err, Error::ParquetFileDoesNotExist { .. }));
        }
        assert_checkpoint(&state, &f, &expected);

        // consume variable so that we can easily add tests w/o re-adding the final modification
        println!("{}", chunk_id_watermark);
    }

    /// Assert that tracked files and their linked metadata are equal.
    fn assert_checkpoint<S, F>(
        state: &S,
        f: &F,
        expected_files: &HashMap<DirsAndFileName, Arc<IoxParquetMetaData>>,
    ) where
        F: Fn(&S) -> CheckpointData,
    {
        let actual_files = f(state).files;

        let sorted_keys_actual = get_sorted_keys(&actual_files);
        let sorted_keys_expected = get_sorted_keys(expected_files);
        assert_eq!(sorted_keys_actual, sorted_keys_expected);

        for k in sorted_keys_actual {
            let md_actual = &actual_files[&k];
            let md_expected = &expected_files[&k];

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
    use std::num::NonZeroU32;

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
            !PreservedCatalog::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        assert!(PreservedCatalog::exists(&object_store, server_id, db_name,)
            .await
            .unwrap());
        assert!(PreservedCatalog::load::<TestCatalogState>(
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
        let option = PreservedCatalog::load::<TestCatalogState>(
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
        let option = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let (catalog, _state) = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let res = PreservedCatalog::load::<TestCatalogState>(
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
        let (catalog, state) = PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap()
        .unwrap();
        let mut state = Arc::try_unwrap(state).unwrap();

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
            let mut transaction = catalog.open_transaction().await;
            let path = parsed_path!("last_one");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata.clone()));
            transaction.add_parquet(&path, &metadata).unwrap();
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
        let (catalog, state) = PreservedCatalog::load(
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
            &state,
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
        state: &TestCatalogState,
        expected: &[(String, IoxParquetMetaData)],
    ) {
        let actual = get_catalog_parquet_files(state);
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

        fn record(&mut self, catalog: &PreservedCatalog, state: &TestCatalogState, aborted: bool) {
            self.tkeys
                .push(catalog.previous_tkey.read().clone().unwrap());
            self.states.push(state.clone());
            self.post_timestamps.push(Utc::now());
            self.aborted.push(aborted);
        }
    }

    async fn assert_single_catalog_inmem_works(
        object_store: &Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
    ) -> TestTrace {
        let (catalog, state) = PreservedCatalog::new_empty(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();
        let mut state = Arc::try_unwrap(state).unwrap();

        // get some test metadata
        let (_, metadata1) = make_metadata(object_store, "foo", chunk_addr(1)).await;
        let (_, metadata2) = make_metadata(object_store, "bar", chunk_addr(1)).await;

        // track all the intermediate results
        let mut trace = TestTrace::new();

        // empty catalog has no data
        assert_eq!(catalog.revision_counter(), 0);
        assert_catalog_parquet_files(&state, &[]);
        trace.record(&catalog, &state, false);

        // fill catalog with examples
        {
            let mut t = catalog.open_transaction().await;

            let path = parsed_path!("test1");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata1.clone()));
            t.add_parquet(&path, &metadata1).unwrap();

            let path = parsed_path!(["sub1"], "test1");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata2.clone()));
            t.add_parquet(&path, &metadata2).unwrap();

            let path = parsed_path!(["sub1"], "test2");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata2.clone()));
            t.add_parquet(&path, &metadata2).unwrap();

            let path = parsed_path!(["sub2"], "test1");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata1.clone()));
            t.add_parquet(&path, &metadata1).unwrap();

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 1);
        assert_catalog_parquet_files(
            &state,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test1".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, &state, false);

        // modify catalog with examples
        {
            let mut t = catalog.open_transaction().await;

            // "real" modifications
            let path = parsed_path!("test4");
            state
                .parquet_files
                .insert(path.clone(), Arc::new(metadata1.clone()));
            t.add_parquet(&path, &metadata1).unwrap();

            let path = parsed_path!("test1");
            state.parquet_files.remove(&path);
            t.remove_parquet(&path).unwrap();

            t.commit().await.unwrap();
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(
            &state,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, &state, false);

        // uncommitted modifications have no effect
        {
            let mut t = catalog.open_transaction().await;

            t.add_parquet(&parsed_path!("test5"), &metadata1).unwrap();
            t.remove_parquet(&parsed_path!(["sub1"], "test2")).unwrap();

            // NO commit here!
        }
        assert_eq!(catalog.revision_counter(), 2);
        assert_catalog_parquet_files(
            &state,
            &[
                ("sub1/test1".to_string(), metadata2.clone()),
                ("sub1/test2".to_string(), metadata2.clone()),
                ("sub2/test1".to_string(), metadata1.clone()),
                ("test4".to_string(), metadata1.clone()),
            ],
        );
        trace.record(&catalog, &state, true);

        trace
    }

    #[tokio::test]
    async fn test_create_twice() {
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        let res = PreservedCatalog::new_empty::<TestCatalogState>(
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

        PreservedCatalog::wipe(&object_store, server_id, db_name)
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
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // `exists` and `load` both report "no data"
        assert!(
            !PreservedCatalog::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        // can create new catalog
        PreservedCatalog::new_empty::<TestCatalogState>(
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
        let (catalog, _state) = PreservedCatalog::load::<TestCatalogState>(
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
        PreservedCatalog::wipe(&object_store, server_id, db_name)
            .await
            .unwrap();

        // `exists` and `load` both report "no data"
        assert!(
            !PreservedCatalog::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );
        assert!(PreservedCatalog::load::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            ()
        )
        .await
        .unwrap()
        .is_none());

        // can create new catalog
        PreservedCatalog::new_empty::<TestCatalogState>(
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
        PreservedCatalog::wipe(&object_store, server_id, db_name)
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
        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
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
        let (catalog, _state) = PreservedCatalog::new_empty::<TestCatalogState>(
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

        let ts =
            PreservedCatalog::find_last_transaction_timestamp(&object_store, server_id, db_name)
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

        assert!(PreservedCatalog::find_last_transaction_timestamp(
            &object_store,
            server_id,
            db_name
        )
        .await
        .unwrap()
        .is_none());
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

        let ts =
            PreservedCatalog::find_last_transaction_timestamp(&object_store, server_id, db_name)
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

        let ts =
            PreservedCatalog::find_last_transaction_timestamp(&object_store, server_id, db_name)
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

        let (catalog, state) = PreservedCatalog::load::<TestCatalogState>(
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
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }
        trace.record(&catalog, &state, false);

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

        let ts =
            PreservedCatalog::find_last_transaction_timestamp(&object_store, server_id, db_name)
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
        let (catalog, state) =
            PreservedCatalog::load(Arc::clone(object_store), server_id, db_name.to_string(), ())
                .await
                .unwrap()
                .unwrap();
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
        let object_store = make_object_store();
        let server_id = make_server_id();
        let db_name = "db1";

        assert!(
            !PreservedCatalog::exists(&object_store, server_id, db_name,)
                .await
                .unwrap()
        );

        let (catalog, state) = PreservedCatalog::new_empty::<TestCatalogState>(
            Arc::clone(&object_store),
            server_id,
            db_name.to_string(),
            (),
        )
        .await
        .unwrap();

        // delete transaction file
        let tkey = catalog.previous_tkey.read().clone().unwrap();
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
            let ckpt_handle = transaction.commit().await.unwrap();
            ckpt_handle
                .create_checkpoint(state.checkpoint_data())
                .await
                .unwrap();
        }

        // delete transaction file
        let tkey = catalog.previous_tkey.read().clone().unwrap();
        let path = file_path(
            &object_store,
            server_id,
            db_name,
            &tkey,
            FileType::Transaction,
        );
        checked_delete(&object_store, &path).await;

        drop(catalog);

        assert!(PreservedCatalog::exists(&object_store, server_id, db_name,)
            .await
            .unwrap());
        assert!(PreservedCatalog::load::<TestCatalogState>(
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
        assert_catalog_state_implementation::<TestCatalogState, _>(
            (),
            TestCatalogState::checkpoint_data,
        )
        .await;
    }
}
