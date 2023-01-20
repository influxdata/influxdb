use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{Table, TableId};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait TablesSource: Debug + Display + Send + Sync {
    /// Get Table and TableSchema for a given table
    ///
    /// This method performs retries.
    async fn fetch(&self, table: TableId) -> Option<Table>;
}
