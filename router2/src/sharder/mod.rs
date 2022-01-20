//! Sharder logic to consistently map operations to a specific sequencer.

mod r#trait;
pub use r#trait::*;

mod table_namespace_sharder;
pub use table_namespace_sharder::*;
