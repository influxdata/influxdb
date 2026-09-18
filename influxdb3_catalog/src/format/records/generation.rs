//! Generation and storage mode operations (record_ids 23-24).

use influxdb3_catalog_macros::catalog_record;

use super::types::StorageMode;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::storage::StorageMode as SchemaStorageMode;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordApply, record_ids};

/// Set generation duration for a compaction level.
#[catalog_record(id = record_ids::SET_GENERATION_DURATION, shape = 0x8a457297)]
#[derive(Copy)]
pub struct SetGenerationDuration {
    /// Compaction level.
    pub level: u8,
    /// Duration in nanoseconds.
    pub duration_ns: u64,
}

impl RecordApply for SetGenerationDuration {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        catalog
            .generation_config
            .set_duration(
                self.level,
                std::time::Duration::from_nanos(self.duration_ns),
            )
            .map_err(|e| {
                ApplyError(format!(
                    "{}: set generation duration (level={}, duration_ns={}): {e}",
                    Self::NAME,
                    self.level,
                    self.duration_ns,
                ))
            })?;
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::GenerationDurationChanged
    }
}

/// Set the storage mode for the catalog.
#[catalog_record(id = record_ids::SET_STORAGE_MODE, shape = 0xbba17476)]
#[derive(Copy)]
pub struct SetStorageMode {
    /// Storage mode configuration.
    pub mode: StorageMode,
}

impl RecordApply for SetStorageMode {
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        catalog.storage_mode = SchemaStorageMode::from(self.mode);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::StorageModeChanged
    }
}

impl From<StorageMode> for SchemaStorageMode {
    fn from(value: StorageMode) -> Self {
        match value {
            StorageMode::Parquet => Self::Parquet,
            StorageMode::PachaTree => Self::PachaTree,
            StorageMode::ParquetAndPachaTree => Self::ParquetAndPachaTree,
        }
    }
}

impl From<SchemaStorageMode> for StorageMode {
    fn from(value: SchemaStorageMode) -> Self {
        match value {
            SchemaStorageMode::Parquet => Self::Parquet,
            SchemaStorageMode::PachaTree => Self::PachaTree,
            SchemaStorageMode::ParquetAndPachaTree => Self::ParquetAndPachaTree,
        }
    }
}

#[cfg(test)]
mod tests;
