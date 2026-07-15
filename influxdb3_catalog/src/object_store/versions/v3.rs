#![allow(dead_code)]

use std::io::Cursor;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use influxdb3_wal::{CatalogSnapshotObserver, NoopCatalogSnapshotObserver};
use object_store::ObjectStore;
use object_store::PutOptions;
use object_store::path::Path as ObjPath;
use object_store_utils::RetryableObjectStore;
use observability_deps::tracing::{debug, info, warn};
use uuid::Uuid;

use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::backup::{CatalogCheckpointForBackup, CatalogLogFileForBackup};
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::format::apply::{
    RestorePreload, apply_catalog_file, apply_records, preload_restore_for_file,
};
use crate::format::records::SetStorageMode;
use crate::format::{CatalogFile, MakeRecord};
use crate::object_store::{
    CATALOG_LOG_FILE_EXTENSION, ObjectStoreCatalogError, PersistCatalogResult, Result,
};

const CATALOG_VERSION_PATH: &str = "catalog/v3";

/// An [`InnerCatalog`] reconstructed from the object store, along with
/// metadata about the snapshot file it was loaded from.
#[derive(Debug)]
pub(crate) struct CatalogLoad {
    pub(crate) inner: InnerCatalog,
    /// True when the snapshot file is not in the current single-entry form:
    /// either the legacy multi-group layout (`group_count > 1`) or the
    /// indexless flat layout (`group_count == 0`) that pre-flat readers
    /// reject. Such a file should be rewritten in the current format at
    /// startup.
    pub(crate) snapshot_needs_rewrite: bool,
}

/// Maximum number of in-flight log file fetches during catalog load.
///
/// `load_catalog` may need to replay every log file persisted since the last
/// snapshot. Spawning unbounded concurrency would let a long replay flood
/// the object store; this cap keeps fan-out predictable.
const LOG_FETCH_CONCURRENCY: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct ObjectStoreCatalog {
    pub(crate) prefix: Arc<str>,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) storage_mode: StorageMode,
    /// SLL observer for catalog snapshot persistence events. Defaults to
    /// noop; the enterprise binary swaps in a real adapter via
    /// [`Self::with_catalog_snapshot_observer`] when SLL is enabled.
    pub(crate) catalog_snapshot_observer: Arc<dyn CatalogSnapshotObserver>,
}

impl ObjectStoreCatalog {
    pub(crate) fn new(
        prefix: impl Into<Arc<str>>,
        store: Arc<dyn ObjectStore>,
        storage_mode: StorageMode,
    ) -> Self {
        Self {
            prefix: prefix.into(),
            store,
            storage_mode,
            catalog_snapshot_observer: Arc::new(NoopCatalogSnapshotObserver),
        }
    }

    /// Builder-style override of the catalog snapshot observer.
    pub(crate) fn with_catalog_snapshot_observer(
        mut self,
        observer: Arc<dyn CatalogSnapshotObserver>,
    ) -> Self {
        self.catalog_snapshot_observer = observer;
        self
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Load the log file at `sequence_number`, or `Ok(None)` if absent.
    pub(crate) async fn load_log(
        &self,
        sequence_number: CatalogSequenceNumber,
    ) -> Result<Option<CatalogFile>> {
        let path = CatalogFilePath::log(&self.prefix, sequence_number);
        Ok(self.load_file(&path).await?.map(|(file, _size)| file))
    }

    /// Load the snapshot file, or `Ok(None)` if absent. Returns the
    /// decoded [`CatalogFile`] alongside its on-disk byte size so callers
    /// (notably SLL emission in `load_or_create_catalog`) can report
    /// snapshot size without an extra HEAD request.
    pub(crate) async fn load_snapshot(&self) -> Result<Option<(CatalogFile, u64)>> {
        let path = CatalogFilePath::snapshot(&self.prefix);
        self.load_file(&path).await
    }

    /// Fetch the raw bytes of the object at `path`, returning `Ok(None)` if it
    /// does not exist. Other object-store errors propagate. Shared by
    /// [`Self::load_file`] and [`Self::checkpoint_for_backup`] so the
    /// get/NotFound handling lives in one place.
    async fn read_file_bytes(&self, path: &CatalogFilePath) -> Result<Option<Bytes>> {
        match self.store.get(path).await {
            Ok(get_result) => Ok(Some(get_result.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn load_file(&self, path: &CatalogFilePath) -> Result<Option<(CatalogFile, u64)>> {
        let Some(bytes) = self.read_file_bytes(path).await? else {
            return Ok(None);
        };
        let size_bytes = bytes.len() as u64;
        let mut cursor = Cursor::new(bytes.as_ref());
        Ok(Some((CatalogFile::read_from(&mut cursor)?, size_bytes)))
    }

    /// Reconstruct an [`InnerCatalog`] from the object store: load the
    /// snapshot, then discover and apply every subsequent log file.
    ///
    /// Log discovery uses `list_with_offset` on the logs directory so the
    /// cost scales with the number of log files, not with the range of
    /// sequence numbers. File bodies are fetched in parallel (capped at
    /// [`LOG_FETCH_CONCURRENCY`]) with retries via
    /// [`RetryableObjectStore::get_with_default_retries`]; apply is
    /// sequential because each step mutates `InnerCatalog`.
    ///
    /// Returns `Ok(None)` if no snapshot exists — i.e. no catalog has been
    /// initialized at this prefix.
    pub(crate) async fn load_catalog(&self) -> Result<Option<CatalogLoad>> {
        // `Ok(None)` (no snapshot exists at this prefix) is silent; a real
        // object-store / decode error during startup load fires the SLL
        // error variant so operators see the failure rather than a silent
        // "no catalog observed" gap.
        let Some((snapshot, size_bytes)) = self.load_snapshot().await.inspect_err(|_| {
            self.catalog_snapshot_observer
                .on_catalog_snapshot_error("catalog_snapshot_failed");
        })?
        else {
            return Ok(None);
        };
        let snapshot_sequence = snapshot.sequence_number();
        // Exactly one group is the current writer's form, which every
        // reader vintage accepts. Anything else gets rewritten at startup:
        // the legacy multi-group layout (group_count > 1), and the indexless
        // flat layout (group_count == 0) whose files pre-flat readers
        // reject.
        let snapshot_needs_rewrite = snapshot.header.group_count != 1;
        let catalog_uuid = Uuid::from_u128(snapshot.header.catalog_uuid);
        let mut inner = InnerCatalog::new(Arc::clone(&self.prefix), catalog_uuid);
        // Snapshots normally do not contain restore records, but pre-load
        // defensively in case one slipped in.
        let mut preload =
            preload_restore_for_file(&snapshot, self, inner.committed_feature_level).await?;
        apply_catalog_file(&snapshot, &mut inner, &mut preload)
            .inspect_err(|_| {
                self.catalog_snapshot_observer
                    .on_catalog_snapshot_error("catalog_snapshot_failed");
            })
            .map_err(|e| {
                ObjectStoreCatalogError::unexpected(format!(
                    "failed to apply snapshot for catalog {catalog_uuid} (prefix {}): {e}",
                    self.prefix,
                ))
            })?;
        // SLL: emit on every successful snapshot load — this is the most
        // common observation moment (per-boot, after the snapshot file
        // already exists). The `_persisted` paths in `initialize_snapshot`
        // and `write_checkpoint` cover the cases where THIS node wrote it.
        self.catalog_snapshot_observer
            .on_catalog_snapshot_success(snapshot_sequence, size_bytes);

        // Discover logs at sequences > the snapshot's sequence. `list_with_offset`
        // returns entries strictly after the offset path; zero-padded-20-digit
        // filenames make lexicographic order match sequence order.
        let offset = CatalogFilePath::log(&self.prefix, inner.sequence_number()).into();
        let logs_dir = CatalogFilePath::logs_dir(&self.prefix);
        let mut log_metas = Vec::new();
        let mut list_stream = self.store.list_with_offset(Some(&logs_dir), &offset);
        while let Some(item) = list_stream.next().await {
            log_metas.push(item?);
        }
        log_metas.sort_unstable_by(|a, b| a.location.cmp(&b.location));

        let store = Arc::clone(&self.store);
        let prefix = Arc::clone(&self.prefix);
        let fetched: Vec<CatalogFile> = stream::iter(log_metas)
            .map(|meta| {
                let store = Arc::clone(&store);
                let prefix = Arc::clone(&prefix);
                async move {
                    let context = format!(
                        "loading catalog log {} for catalog {catalog_uuid} (prefix {prefix})",
                        meta.location,
                    );
                    let bytes = store
                        .get_with_default_retries(&meta.location, context)
                        .await?
                        .bytes()
                        .await?;
                    let mut cursor = Cursor::new(bytes.as_ref());
                    CatalogFile::read_from(&mut cursor).map_err(|e| {
                        ObjectStoreCatalogError::unexpected(format!(
                            "failed to parse catalog log at {} for catalog {catalog_uuid} \
                             (prefix {prefix}): {e}",
                            meta.location,
                        ))
                    })
                }
            })
            .buffered(LOG_FETCH_CONCURRENCY)
            .try_collect()
            .await?;

        for file in fetched {
            // Every log replayed on top of the snapshot must carry the same
            // catalog identity. A mismatch means snapshot and logs disagree on
            // identity (e.g. a restore that split the uuid); fail loudly here
            // rather than silently loading a mixed catalog.
            let file_uuid = Uuid::from_u128(file.header.catalog_uuid);
            if file_uuid != catalog_uuid {
                return Err(ObjectStoreCatalogError::unexpected(format!(
                    "catalog uuid mismatch at log sequence {} (prefix {}): snapshot is \
                     {catalog_uuid}, log is {file_uuid}",
                    file.sequence_number(),
                    self.prefix,
                )));
            }
            let mut preload =
                preload_restore_for_file(&file, self, inner.committed_feature_level).await?;
            apply_catalog_file(&file, &mut inner, &mut preload).map_err(|e| {
                ObjectStoreCatalogError::unexpected(format!(
                    "failed to apply log at sequence {} for catalog {catalog_uuid} \
                     (prefix {}): {e}",
                    file.sequence_number(),
                    self.prefix,
                ))
            })?;
        }

        Ok(Some(CatalogLoad {
            inner,
            snapshot_needs_rewrite,
        }))
    }

    /// Load an existing catalog, or initialize a fresh one by writing an
    /// initial snapshot at sequence 0. Two processes racing to initialize
    /// the same prefix at the same time both call [`Self::initialize_snapshot`]
    /// under `PutMode::Create`; the loser reloads whatever the winner wrote.
    ///
    /// The initial snapshot carries a single [`SetStorageMode`] record so
    /// the storage mode configured on this `ObjectStoreCatalog` survives
    /// future reloads.
    pub(crate) async fn load_or_create_catalog(&self) -> Result<CatalogLoad> {
        if let Some(load) = self.load_catalog().await? {
            return Ok(load);
        }

        let catalog_uuid = Uuid::new_v4();
        info!(%catalog_uuid, prefix = %self.prefix, "catalog not found, initializing a new one");
        let mut inner = InnerCatalog::new(Arc::clone(&self.prefix), catalog_uuid);
        let initial_record = SetStorageMode {
            mode: self.storage_mode.into(),
        }
        .make_record(0);
        apply_records(
            &[initial_record],
            &mut inner,
            CatalogSequenceNumber::new(0),
            &mut RestorePreload::empty(),
        )
        .map_err(|e| {
            ObjectStoreCatalogError::unexpected(format!(
                "failed to apply initial storage mode record for catalog {catalog_uuid} \
                 (prefix {}): {e}",
                self.prefix,
            ))
        })?;
        let initial_snapshot = inner.create_snapshot();
        match self.initialize_snapshot(initial_snapshot).await? {
            PersistCatalogResult::Success => Ok(CatalogLoad {
                inner,
                snapshot_needs_rewrite: false,
            }),
            PersistCatalogResult::AlreadyExists => self.load_catalog().await?.ok_or_else(|| {
                ObjectStoreCatalogError::unexpected(
                    "initial snapshot existed but load_catalog returned None",
                )
            }),
        }
    }

    /// Persist a log file at `sequence_number` using create-only semantics.
    pub(crate) async fn persist_log(
        &self,
        sequence_number: CatalogSequenceNumber,
        content: Bytes,
    ) -> Result<PersistCatalogResult> {
        let path = CatalogFilePath::log(&self.prefix, sequence_number);
        let start = Instant::now();
        let result = self.put_if_not_exists(&path, content).await?;
        let elapsed = start.elapsed();
        debug!(
            sequence_number = sequence_number.get(),
            object_path = ?path,
            elapsed_ms = elapsed.as_millis(),
            ?result,
            "persist catalog log file",
        );
        Ok(result)
    }

    /// Create the snapshot file. Create-only so that racing initializers
    /// resolve to a single winner.
    pub(crate) async fn initialize_snapshot(&self, content: Bytes) -> Result<PersistCatalogResult> {
        let path = CatalogFilePath::snapshot(&self.prefix);
        let size_bytes = content.len() as u64;
        let result = self.put_if_not_exists(&path, content).await;
        match &result {
            Ok(PersistCatalogResult::Success) => {
                // Initial snapshot is at sequence 0 by construction.
                self.catalog_snapshot_observer
                    .on_catalog_snapshot_success(0, size_bytes);
            }
            Ok(PersistCatalogResult::AlreadyExists) => {
                // The caller calls `load_catalog` immediately after this on
                // the race-loss path; that load fires the SLL event with the
                // winner's true sequence + size. Skip here to avoid double
                // emission.
            }
            Err(_) => {
                self.catalog_snapshot_observer
                    .on_catalog_snapshot_error("catalog_snapshot_failed");
            }
        }
        result
    }

    /// Update the snapshot file on object store after it has been initialized.
    pub(crate) async fn update_snapshot(&self, content: Bytes) -> Result<()> {
        let path = CatalogFilePath::snapshot(&self.prefix);
        match self.store.put(&path, content.into()).await {
            Ok(put_result) => {
                debug!(?put_result, object_path = ?path, "updated catalog snapshot");
                Ok(())
            }
            Err(error) => {
                warn!(error = ?error, object_path = ?path, "failed to update catalog snapshot");
                Err(error.into())
            }
        }
    }

    /// Overwrite the snapshot file to shorten the log replay chain.
    ///
    /// Old log files are left in place; `load_catalog` ignores logs at
    /// sequences ≤ the snapshot's, so obsolete logs have no effect on
    /// correctness. Storage is reclaimed by a separate garbage collector.
    pub(crate) async fn write_checkpoint(
        &self,
        snapshot_sequence: CatalogSequenceNumber,
        snapshot_bytes: Bytes,
    ) -> Result<()> {
        let path = CatalogFilePath::snapshot(&self.prefix);
        let context = format!(
            "writing catalog checkpoint at sequence {} (prefix {})",
            snapshot_sequence.get(),
            self.prefix,
        );
        let size_bytes = snapshot_bytes.len() as u64;
        self.store
            .put_with_default_retries(&path, snapshot_bytes.into(), context)
            .await
            .inspect_err(|_| {
                self.catalog_snapshot_observer
                    .on_catalog_snapshot_error("catalog_snapshot_failed");
            })?;
        info!(
            sequence = snapshot_sequence.get(),
            "checkpoint snapshot written"
        );
        self.catalog_snapshot_observer
            .on_catalog_snapshot_success(snapshot_sequence.get(), size_bytes);
        Ok(())
    }

    /// Read the live snapshot file and return its sequence number together
    /// with the snapshot path, for use as a backup starting checkpoint.
    ///
    /// In v3 the snapshot file is the durable checkpoint: it holds a complete,
    /// grouped image of the catalog at its sequence number. The v2 concept of a
    /// separate "checkpoint" file maps onto this single snapshot file.
    pub(crate) async fn checkpoint_for_backup(&self) -> Result<CatalogCheckpointForBackup> {
        let path = CatalogFilePath::snapshot(&self.prefix);
        let Some(bytes) = self.read_file_bytes(&path).await? else {
            return Err(ObjectStoreCatalogError::unexpected(format!(
                "no catalog snapshot found for backup at {}",
                path.as_ref(),
            )));
        };
        // The raw bytes are copied verbatim into the backup, so the records do
        // not need decoding here. Still validate the header, payload-length
        // bounds, and payload CRC so a truncated or corrupt snapshot is rejected
        // now rather than surfacing at restore time, then take the sequence
        // from the validated header.
        let mut cursor = Cursor::new(bytes.as_ref());
        let sequence = CatalogFile::read_verified_header(&mut cursor)?.sequence_number;

        Ok(CatalogCheckpointForBackup {
            sequence: CatalogSequenceNumber::new(sequence),
            path: path.into(),
            bytes,
        })
    }

    /// Get the list of log files after the `after` sequence (`after + 1`) and
    /// up to and including the `through` sequence. Used in the backup flow to
    /// determine which log files to include after selecting the checkpoint.
    pub(crate) async fn log_files_for_backup(
        &self,
        after: CatalogSequenceNumber,
        through: CatalogSequenceNumber,
    ) -> Result<Vec<CatalogLogFileForBackup>> {
        if after > through {
            return Err(ObjectStoreCatalogError::unexpected(format!(
                "invalid log file range for backup: after sequence {} is not less than through sequence {}",
                after.get(),
                through.get(),
            )));
        }

        Ok(((after.get() + 1)..=through.get())
            .map(|sequence| {
                let sequence = CatalogSequenceNumber::new(sequence);
                CatalogLogFileForBackup {
                    sequence,
                    path: CatalogFilePath::log(&self.prefix, sequence).into(),
                }
            })
            .collect())
    }

    /// Reconstruct an [`InnerCatalog`] from an explicitly selected snapshot
    /// (checkpoint) file plus a set of log files, rather than the live
    /// well-known paths. Returns `Ok(None)` if the checkpoint snapshot is
    /// absent.
    ///
    /// Log files are sorted and replayed in sequence order; a gap between the
    /// checkpoint sequence and the first log, or between consecutive logs,
    /// surfaces as a replay-sequence-mismatch error so an incomplete restore
    /// source cannot silently produce a partial catalog.
    pub(crate) async fn load_catalog_from_paths(
        &self,
        checkpoint_path: &ObjPath,
        log_paths: &[ObjPath],
    ) -> Result<Option<InnerCatalog>> {
        let Some(snapshot) = self.load_file_at(checkpoint_path).await? else {
            return Ok(None);
        };
        let catalog_uuid = Uuid::from_u128(snapshot.header.catalog_uuid);
        let mut inner = InnerCatalog::new(Arc::clone(&self.prefix), catalog_uuid);
        let mut preload =
            preload_restore_for_file(&snapshot, self, inner.committed_feature_level).await?;
        apply_catalog_file(&snapshot, &mut inner, &mut preload).map_err(|e| {
            ObjectStoreCatalogError::unexpected(format!(
                "failed to apply restore checkpoint at {checkpoint_path} for catalog \
                 {catalog_uuid}: {e}",
            ))
        })?;

        let mut log_paths = log_paths.to_vec();
        log_paths.sort_unstable();

        let mut expected_sequence = inner.sequence_number().next();
        for path in log_paths {
            let log = self.load_file_at(&path).await?.ok_or_else(|| {
                ObjectStoreCatalogError::unexpected(
                    format!("restore log file not found at {path}",),
                )
            })?;
            let actual_sequence = CatalogSequenceNumber::new(log.sequence_number());
            if actual_sequence != expected_sequence {
                return Err(ObjectStoreCatalogError::unexpected(format!(
                    "catalog replay sequence mismatch for {path}: expected {}, got {}",
                    expected_sequence.get(),
                    actual_sequence.get(),
                )));
            }
            let actual_uuid = Uuid::from_u128(log.header.catalog_uuid);
            if actual_uuid != catalog_uuid {
                return Err(ObjectStoreCatalogError::unexpected(format!(
                    "catalog uuid mismatch for {path}: snapshot is {catalog_uuid}, log is \
                     {actual_uuid}",
                )));
            }
            let mut preload =
                preload_restore_for_file(&log, self, inner.committed_feature_level).await?;
            apply_catalog_file(&log, &mut inner, &mut preload).map_err(|e| {
                ObjectStoreCatalogError::unexpected(format!(
                    "failed to apply restore log at {path} for catalog {catalog_uuid}: {e}",
                ))
            })?;
            expected_sequence = expected_sequence.next();
        }

        Ok(Some(inner))
    }

    /// Load and parse a [`CatalogFile`] at an arbitrary object-store path, or
    /// `Ok(None)` if absent. Unlike [`Self::load_file`], the path is supplied
    /// directly rather than derived from the catalog prefix, so backup images
    /// stored under a different prefix can be read.
    async fn load_file_at(&self, path: &ObjPath) -> Result<Option<CatalogFile>> {
        match self.store.get(path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await?;
                let mut cursor = Cursor::new(bytes.as_ref());
                Ok(Some(CatalogFile::read_from(&mut cursor)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn put_if_not_exists(
        &self,
        path: &CatalogFilePath,
        content: Bytes,
    ) -> Result<PersistCatalogResult> {
        match self
            .store
            .put_opts(
                path,
                content.into(),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(PersistCatalogResult::Success),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(PersistCatalogResult::AlreadyExists)
            }
            Err(other) => {
                warn!(error = ?other, object_path = ?path, "failed to persist catalog file");
                Err(other.into())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(ObjPath);

impl CatalogFilePath {
    pub fn log(catalog_prefix: &str, sequence_number: CatalogSequenceNumber) -> Self {
        let num = sequence_number.get();
        Self(ObjPath::from(format!(
            "{catalog_prefix}/{CATALOG_VERSION_PATH}/logs/{num:020}.{CATALOG_LOG_FILE_EXTENSION}",
        )))
    }

    pub fn snapshot(catalog_prefix: &str) -> Self {
        Self(ObjPath::from(format!(
            "{catalog_prefix}/{CATALOG_VERSION_PATH}/snapshot",
        )))
    }

    pub fn logs_dir(catalog_prefix: &str) -> Self {
        Self(ObjPath::from(format!(
            "{catalog_prefix}/{CATALOG_VERSION_PATH}/logs",
        )))
    }

    /// Object-store directory under which a restore stages the backup's
    /// catalog checkpoint and log files.
    ///
    /// Deliberately a sibling of the versioned catalog directories (never
    /// inside a `.../logs` sequence), so staging never collides with — or is
    /// mistaken for — a live, immutable catalog log file. `Catalog::restore`
    /// reads the backup artifacts from here.
    pub fn restore_staging_dir(catalog_prefix: &str, restore_id: &str) -> Self {
        Self(ObjPath::from(format!(
            "{catalog_prefix}/catalog/restores/{restore_id}",
        )))
    }
}

impl Deref for CatalogFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for CatalogFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl From<CatalogFilePath> for ObjPath {
    fn from(path: CatalogFilePath) -> Self {
        path.0
    }
}

#[cfg(test)]
mod tests;
