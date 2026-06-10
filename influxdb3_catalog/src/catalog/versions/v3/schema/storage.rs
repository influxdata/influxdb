use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{CatalogError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    #[default]
    Parquet,
    PachaTree,
    /// Migrating from Parquet to PachaTree storage mode
    ParquetAndPachaTree,
}

impl StorageMode {
    pub fn is_default(mode: &Self) -> bool {
        matches!(mode, Self::Parquet)
    }
}

impl fmt::Display for StorageMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageMode::Parquet => f.write_str("Parquet"),
            StorageMode::PachaTree => f.write_str("PachaTree"),
            StorageMode::ParquetAndPachaTree => f.write_str("Parquet and PachaTree"),
        }
    }
}

// NOTE(tjh): the `GenerationConfig` is an inherited setting on the catalog from the Parquet
// storage engine.
#[derive(Debug, Clone, Default)]
pub struct GenerationConfig {
    /// Map of generation levels to their duration
    pub(crate) generation_durations: BTreeMap<u8, Duration>,
}

impl GenerationConfig {
    pub(crate) fn set_duration(
        &mut self,
        level: impl Into<u8>,
        duration: Duration,
    ) -> Result<bool> {
        let level = level.into();
        match self.generation_durations.entry(level) {
            Entry::Occupied(occupied_entry) => {
                let existing = *occupied_entry.get();
                if existing != duration {
                    Err(CatalogError::CannotChangeGenerationDuration {
                        level,
                        existing: existing.into(),
                        attempted: duration.into(),
                    })
                } else {
                    Ok(false)
                }
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(duration);
                Ok(true)
            }
        }
    }

    pub fn duration_for_level(&self, level: u8) -> Option<Duration> {
        self.generation_durations.get(&level).copied()
    }
}

impl From<StorageMode> for crate::log::StorageMode {
    fn from(value: StorageMode) -> Self {
        match value {
            StorageMode::Parquet => Self::Parquet,
            StorageMode::PachaTree => Self::PachaTree,
            StorageMode::ParquetAndPachaTree => Self::ParquetAndPachaTree,
        }
    }
}

impl From<crate::log::StorageMode> for StorageMode {
    fn from(value: crate::log::StorageMode) -> Self {
        match value {
            crate::log::StorageMode::Parquet => Self::Parquet,
            crate::log::StorageMode::PachaTree => Self::PachaTree,
            crate::log::StorageMode::ParquetAndPachaTree => Self::ParquetAndPachaTree,
        }
    }
}
