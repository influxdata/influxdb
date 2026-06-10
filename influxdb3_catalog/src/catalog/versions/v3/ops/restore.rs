//! Restore op: replace the catalog from an object-store backup image.
//!
//! Unlike every other op, `RestoreOp` does not mutate part of the in-memory
//! catalog — applying its record swaps `*catalog` wholesale with state loaded
//! from a backup snapshot + log files. The op only validates the input source
//! and produces a single [`RestoreCatalog`] record; the actual load + swap
//! happens in the apply driver's restore special case.
//!
//! The op surfaces through
//! [`Catalog::restore`](crate::catalog::versions::v3::catalog::Catalog::restore),
//! which also forces a checkpoint after apply so the restored state becomes
//! the new snapshot baseline (and the backup paths in the persisted restore
//! record stop being load-bearing).

use std::sync::Arc;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::backup::CatalogRestoreSource;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::RecordBatch;
use crate::format::records::RestoreCatalog;

/// Input to [`RestoreOp`]: the backup paths plus an operator-supplied
/// identifier for the restore that surfaces in events and progress logs.
#[derive(Debug, Clone)]
pub(crate) struct RestoreOpArgs {
    pub restore_id: Arc<str>,
    pub source: CatalogRestoreSource,
    pub time_ns: i64,
}

/// Report returned from a successful
/// [`Catalog::restore`](crate::catalog::versions::v3::catalog::Catalog::restore).
#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// The operator-supplied id this restore was tagged with.
    pub restore_id: Arc<str>,
    /// The sequence number the restore record was persisted at — i.e. the
    /// new tip of the catalog log immediately after restore.
    pub restored_sequence: CatalogSequenceNumber,
}

pub(crate) struct RestoreOp {
    restore_id: Arc<str>,
}

impl CatalogOp for RestoreOp {
    type Input = RestoreOpArgs;
    type Output = RestoreReport;

    fn prepare(
        args: &Self::Input,
        _catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if args.source.checkpoint_path.as_ref().is_empty() {
            return Err(CatalogError::InvalidConfiguration {
                message: "restore source has empty checkpoint_path".into(),
            });
        }
        if !records.is_empty() {
            return Err(CatalogError::Internal {
                details: "RestoreOp::prepare requires an empty record batch".to_string(),
            });
        }

        records.push(&RestoreCatalog {
            time_ns: args.time_ns,
            restore_id: args.restore_id.to_string(),
            checkpoint_path: args.source.checkpoint_path.to_string(),
            log_paths: args
                .source
                .log_paths
                .iter()
                .map(|p| p.to_string())
                .collect(),
        });

        Ok(Self {
            restore_id: Arc::clone(&args.restore_id),
        })
    }

    fn output(&self, catalog: &InnerCatalog) -> Self::Output {
        RestoreReport {
            restore_id: Arc::clone(&self.restore_id),
            restored_sequence: catalog.sequence_number(),
        }
    }
}
