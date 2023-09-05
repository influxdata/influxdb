use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{Column, TableId};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait ColumnsSource: Debug + Display + Send + Sync {
    /// Get Columns for a given table
    ///
    /// This method performs retries.
    async fn fetch(&self, table: TableId) -> Vec<Column>;
}
