//! Config operations: SetGenerationDurationOp, SetStorageModeOp.

use std::time::Duration;

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::error::enterprise::EnterpriseCatalogError;
use crate::format::RecordBatch;
use crate::format::records::{SetGenerationDuration, SetStorageMode};

// ---------------------------------------------------------------------------
// SetGenerationDuration
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SetGenerationDurationArgs {
    pub level: u8,
    pub duration: Duration,
}

pub(crate) struct SetGenerationDurationOp;

impl CatalogOp for SetGenerationDurationOp {
    type Input = SetGenerationDurationArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        // Validate: check for conflicts without mutating
        if let Some(existing) = catalog.generation_config.duration_for_level(args.level) {
            if existing != args.duration {
                return Err(CatalogError::CannotChangeGenerationDuration {
                    level: args.level,
                    existing: existing.into(),
                    attempted: args.duration.into(),
                });
            }
            // Same value — no-op
            return Err(CatalogError::NoCatalogChange {
                details: format!(
                    "generation duration for level {} is already {:?}",
                    args.level, args.duration,
                ),
            });
        }

        records.push(&SetGenerationDuration {
            level: args.level,
            duration_ns: u64::try_from(args.duration.as_nanos())
                .expect("duration exceeds u64 range"),
        });

        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

// ---------------------------------------------------------------------------
// SetAllGenerationDurations — atomic multi-level setter. The slice is
// indexed from 0 = gen1, so durations[i] sets level i + 1. Each adjacent
// pair must be in increasing-multiple alignment (`b % a == 0`).
// Already-set levels with the same duration are tolerated as no-ops; a
// different duration at an existing level surfaces
// `CannotChangeGenerationDuration`.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SetAllGenerationDurationsArgs {
    pub durations: Vec<Duration>,
}

pub(crate) struct SetAllGenerationDurationsOp;

impl CatalogOp for SetAllGenerationDurationsOp {
    type Input = SetAllGenerationDurationsArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if args.durations.is_empty() {
            return Err(CatalogError::NoCatalogChange {
                details: "no generation durations provided".to_string(),
            });
        }

        if args.durations.len() > u8::MAX as usize {
            return Err(EnterpriseCatalogError::TooManyCompactedGenerations {
                requested: args.durations.len() - 1,
            }
            .into());
        }

        // Walk every requested level. New levels mark the batch as
        // having work to do; same-duration existing levels are tolerated;
        // different-duration existing levels are rejected.
        let mut has_new = false;
        for (i, duration) in args.durations.iter().enumerate() {
            let level = (i as u8) + 1;
            match catalog.generation_config.duration_for_level(level) {
                None => has_new = true,
                Some(existing) if existing == *duration => continue,
                Some(existing) => {
                    return Err(CatalogError::CannotChangeGenerationDuration {
                        level,
                        existing: existing.into(),
                        attempted: (*duration).into(),
                    });
                }
            }
        }
        if !has_new {
            return Err(CatalogError::AlreadyExists);
        }

        // Each adjacent pair must satisfy `b % a == 0` so coarser
        // generations align cleanly on finer-generation boundaries.
        let mut iter = args.durations.iter().map(|d| d.as_secs()).peekable();
        while let (Some(a), Some(b)) = (iter.next(), iter.peek().copied()) {
            if b % a != 0 {
                return Err(EnterpriseCatalogError::MisalignedGenerations.into());
            }
        }

        for (i, duration) in args.durations.iter().enumerate() {
            records.push(&SetGenerationDuration {
                level: (i as u8) + 1,
                duration_ns: u64::try_from(duration.as_nanos())
                    .expect("duration exceeds u64 range"),
            });
        }

        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

// ---------------------------------------------------------------------------
// SetStorageMode
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SetStorageModeArgs {
    pub storage_mode: StorageMode,
}

pub(crate) struct SetStorageModeOp;

impl CatalogOp for SetStorageModeOp {
    type Input = SetStorageModeArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        match (catalog.storage_mode, args.storage_mode) {
            (StorageMode::Parquet, StorageMode::ParquetAndPachaTree)
            | (StorageMode::ParquetAndPachaTree, StorageMode::PachaTree) => {}
            (from, to) if from == to => {
                return Err(CatalogError::NoCatalogChange {
                    details: format!("storage mode is already {to:?}"),
                });
            }
            (from, to) => {
                return Err(CatalogError::Internal {
                    details: format!("invalid storage mode transition from {from} to {to}"),
                });
            }
        }

        records.push(&SetStorageMode {
            mode: args.storage_mode.into(),
        });

        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

// ---------------------------------------------------------------------------
// DowngradeStorageMode — direct revert to Parquet, bypassing the normal
// forward-only transition rules. Intended for the operator-driven
// downgrade path; SHOULD NOT be invoked while nodes are running.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DowngradeStorageModeOp;

impl CatalogOp for DowngradeStorageModeOp {
    type Input = ();
    type Output = ();

    fn prepare(
        _args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        if catalog.storage_mode == StorageMode::Parquet {
            return Err(CatalogError::NoCatalogChange {
                details: "storage mode is already Parquet".to_string(),
            });
        }
        records.push(&SetStorageMode {
            mode: StorageMode::Parquet.into(),
        });
        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

#[cfg(test)]
mod tests;
