//! This package contains definitions for writing data into InfluxDB3. The logical data model is the standard
//! InfluxDB model of Database > Table > Row. As the data arrives into the server it is written into the open
//! segment. The open segment will have a buffer and a persister associated with it.
//!
//! When the segment reaches a certain size, or a certain amount of time has passed, it will be closed and marked
//! to be persisted. A new open segment will be created and new writes will be written to that segment.

pub mod write_buffer;
pub mod catalog;

use std::fmt::Debug;
use std::sync::Arc;
use data_types::NamespaceName;
use iox_query::QueryChunk;
use thiserror::Error;
use async_trait::async_trait;
use datafusion::prelude::Expr;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;



#[derive(Debug, Error)]
pub enum Error {
    #[error("database not found {db_name}")]
    DatabaseNotFound { db_name: String },

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("write buffer error: {0}")]
    WriteBuffer(#[from] write_buffer::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait WriteBuffer: Debug + Send + Sync + 'static {
    async fn write_lp(&self, database: NamespaceName<'static>, lp: &str) -> Result<(), write_buffer::Error>;

    fn get_table_chunks(&self, database_name: &str, table_name: &str, filters: &[Expr], projection: Option<&Vec<usize>>, ctx: &SessionState) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError>;
}
