//! Generation and storage mode operations (record_ids 23-24).

use super::impl_bitcode_encoding;
use super::types::StorageMode;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::schema::storage::StorageMode as SchemaStorageMode;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};

/// Set generation duration for a compaction level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SetGenerationDuration {
    /// Compaction level.
    pub level: u8,
    /// Duration in nanoseconds.
    pub duration_ns: u64,
}

impl CatalogRecord for SetGenerationDuration {
    const ID: RecordId = record_ids::SET_GENERATION_DURATION;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetGenerationDuration";

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

inventory::submit! {
    RegisteredRecord::new::<SetGenerationDuration>()
}

/// Set the storage mode for the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct SetStorageMode {
    /// Storage mode configuration.
    pub mode: StorageMode,
}

impl CatalogRecord for SetStorageMode {
    const ID: RecordId = record_ids::SET_STORAGE_MODE;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "SetStorageMode";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        catalog.storage_mode = SchemaStorageMode::from(self.mode);
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::StorageModeChanged
    }
}

inventory::submit! {
    RegisteredRecord::new::<SetStorageMode>()
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

impl_bitcode_encoding!(SetGenerationDuration, SetStorageMode);

#[cfg(test)]
mod tests;
