//! Format-agnostic descriptors for the catalog backup/restore flow.
//!
//! These mirror the v2 backup types but are defined natively for v3 so the
//! active catalog does not depend on v2, which is slated for deletion.

use bytes::Bytes;
use object_store::path::Path;

use crate::catalog::CatalogSequenceNumber;

/// A stable view of the catalog as a starting checkpoint plus the log files
/// that follow it, suitable for copying into a backup image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBackupView {
    /// The starting checkpoint for the backup, which includes the sequence
    /// number and the path to the checkpoint's most recent snapshot. The
    /// checkpoint's sequence number represents a stable view of the catalog at
    /// that point in time.
    pub checkpoint: CatalogCheckpointForBackup,
    /// The sequence number up to which the backup should include log files.
    /// This represents the upper bound of the sequence numbers of the log files
    /// that should be included in the backup.
    pub through_sequence: CatalogSequenceNumber,
    /// The log files that should be included in the backup.
    pub log_files: Vec<CatalogLogFileForBackup>,
}

/// Explicit object-store paths describing a catalog backup image that should
/// be loaded into memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRestoreSource {
    /// The checkpoint file to use as the restore starting point.
    pub checkpoint_path: Path,
    /// The sequenced log files to replay after the checkpoint.
    pub log_paths: Vec<Path>,
}

/// The checkpoint information needed for backup, including the sequence number
/// and the path to the checkpoint's most recent snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogCheckpointForBackup {
    /// The sequence number of the checkpoint, which represents a stable view of the
    /// catalog at the point in time when the checkpoint was taken.
    pub sequence: CatalogSequenceNumber,
    /// The path to the checkpoint's most recent snapshot.
    pub path: Path,
    /// The exact checkpoint payload loaded from `path`.
    ///
    /// The live checkpoint path is overwritten by later checkpoints, so backup
    /// callers must copy these bytes rather than fetch the path again after
    /// selecting the log replay window.
    pub bytes: Bytes,
}

/// The log file information needed for backup, including the sequence number of
/// the log file and the path to the log file. The log files returned are those
/// that have sequence numbers between the checkpoint sequence and the provided
/// through_sequence, inclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogLogFileForBackup {
    /// The sequence number of the log file, which represents the order of the
    /// log file in relation to the checkpoint and other log files. Log files
    /// with sequence numbers greater than the checkpoint sequence and less than
    /// or equal to the through_sequence should be included in the backup.
    pub sequence: CatalogSequenceNumber,
    /// The path to the log file in object store.
    pub path: Path,
}
