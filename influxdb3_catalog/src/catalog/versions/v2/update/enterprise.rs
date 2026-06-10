use std::time::Duration;

use observability_deps::tracing::info;

use crate::{
    CatalogError, Result,
    error::enterprise::EnterpriseCatalogError,
    log::versions::v4::{
        CatalogBatch, GenerationOp, OrderedCatalogBatch, SetGenerationDurationLog,
    },
};

use super::super::Catalog;

impl Catalog {
    /// Set all generation durations (including gen1) in a single atomic operation.
    ///
    /// # Arguments
    /// * `durations` - Array of durations where index 0 is gen1, index 1 is gen2, etc.
    ///
    /// # Returns
    /// * `Ok(Some(batch))` if any new durations were set
    /// * `Ok(None)` if no durations were provided
    /// * `Err` if validation fails or all durations already exist
    pub async fn set_all_generation_durations(
        &self,
        durations: &[Duration],
    ) -> Result<Option<OrderedCatalogBatch>> {
        if durations.is_empty() {
            return Ok(None);
        }

        // Check that the number of durations provided will not lead to overflow a u8
        if durations.len() > u8::MAX as usize {
            return Err(EnterpriseCatalogError::TooManyCompactedGenerations {
                requested: durations.len() - 1, // subtract 1 because gen1 is not a "compacted" generation
            }
            .into());
        }

        info!(?durations, "set all generation durations");
        self.catalog_update_with_retry(|| {
            let time_ns = self.time_provider.now().timestamp_nanos();
            let lock = self.inner.read();

            // Check whether durations are already set and if they would be changed or not:
            let mut has_new = false;
            for (level, duration) in durations
                .iter()
                .enumerate()
                .map(|(i, dur)| (i as u8 + 1, *dur))
            // +1 because generations start at 1
            {
                match lock.check_generation_duration(level, duration) {
                    Ok(_) => {
                        has_new = true;
                    }
                    // Do not break/return on AlreadyExists; this allows users to _add_ generations
                    // to the end of their existing setting
                    Err(CatalogError::AlreadyExists) => continue,
                    Err(error) => return Err(error),
                }
            }
            if !has_new {
                return Err(CatalogError::AlreadyExists);
            }

            // Check the alignment of all durations:
            let mut combined = durations.iter().map(|dur| dur.as_secs()).peekable();
            while let (Some(a), Some(b)) = (combined.next(), combined.peek()) {
                if b % a != 0 {
                    return Err(EnterpriseCatalogError::MisalignedGenerations.into());
                }
            }

            // Compose the batch for the catalog update:
            let ops = durations
                .iter()
                .enumerate()
                .map(|(i, dur)| {
                    GenerationOp::SetGenerationDuration(SetGenerationDurationLog {
                        // safety: this cast is okay because we check that the number of durations
                        // does not exceed 255 and therefore this operation will never overflow
                        // a u8
                        level: i as u8 + 1, // +1 because generations start at 1, not 0
                        duration: *dur,
                    })
                })
                .collect();
            Ok(CatalogBatch::generation(time_ns, ops))
        })
        .await
        .map(Some)
    }
}

#[cfg(test)]
mod tests;
