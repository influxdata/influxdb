use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

use crate::error::DynError;

use super::PartitionFilter;

#[derive(Debug)]
pub struct OrPartitionFilter {
    filters: Vec<Arc<dyn PartitionFilter>>,
}

impl OrPartitionFilter {
    pub fn new(filters: Vec<Arc<dyn PartitionFilter>>) -> Self {
        Self { filters }
    }
}

impl Display for OrPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "or([")?;
        for (i, sub) in self.filters.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{sub}")?;
        }
        write!(f, "])")
    }
}

#[async_trait]
impl PartitionFilter for OrPartitionFilter {
    async fn apply(
        &self,
        partition_id: PartitionId,
        files: &[ParquetFile],
    ) -> Result<bool, DynError> {
        for filter in &self.filters {
            if filter.apply(partition_id, files).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {

    use crate::components::partition_filter::{
        has_files::HasFilesPartitionFilter, max_files::MaxFilesPartitionFilter,
        FalsePartitionFilter, TruePartitionFilter,
    };

    use super::*;

    #[test]
    fn test_display() {
        let has_files = Arc::new(HasFilesPartitionFilter::new());
        let max_num_files = Arc::new(MaxFilesPartitionFilter::new(2));

        let filter = OrPartitionFilter::new(vec![has_files, max_num_files]);

        assert_eq!(format!("{filter}"), "or([has_files, max_files(2)])");
    }

    #[tokio::test]
    async fn test_apply() {
        let p_id = PartitionId::new(1);

        let filter = OrPartitionFilter::new(vec![
            Arc::new(TruePartitionFilter::new()),
            Arc::new(TruePartitionFilter::new()),
        ]);
        assert!(filter.apply(p_id, &[]).await.unwrap());

        let filter = OrPartitionFilter::new(vec![
            Arc::new(TruePartitionFilter::new()),
            Arc::new(FalsePartitionFilter::new()),
        ]);
        assert!(filter.apply(p_id, &[]).await.unwrap());

        let filter = OrPartitionFilter::new(vec![
            Arc::new(FalsePartitionFilter::new()),
            Arc::new(TruePartitionFilter::new()),
        ]);
        assert!(filter.apply(p_id, &[]).await.unwrap());

        let filter = OrPartitionFilter::new(vec![
            Arc::new(FalsePartitionFilter::new()),
            Arc::new(FalsePartitionFilter::new()),
        ]);
        assert!(!filter.apply(p_id, &[]).await.unwrap());
    }
}
