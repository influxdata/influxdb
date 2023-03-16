use std::{
    fmt::{Debug, Display},
    sync::Mutex,
};

use async_trait::async_trait;

use crate::{error::DynError, file_classification::FilesForProgress, PartitionInfo};

use super::PostClassificationPartitionFilter;

pub struct MockPostClassificationPartitionFilter {
    return_values: Mutex<Box<dyn Iterator<Item = Result<bool, DynError>> + Send>>,
}

impl MockPostClassificationPartitionFilter {
    #[cfg(test)]
    pub fn new(return_values: Vec<Result<bool, DynError>>) -> Self {
        Self {
            return_values: Mutex::new(Box::new(return_values.into_iter())),
        }
    }
}

impl Display for MockPostClassificationPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

impl Debug for MockPostClassificationPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PostClassificationPartitionFilter for MockPostClassificationPartitionFilter {
    async fn apply(
        &self,
        _partition_info: &PartitionInfo,
        _files_to_make_progress_on: &FilesForProgress,
    ) -> Result<bool, DynError> {
        self.return_values.lock().unwrap().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(
            MockPostClassificationPartitionFilter::new(vec![Ok(true), Err("problem".into())])
                .to_string(),
            "mock"
        );
    }
}
