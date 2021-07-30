mod entry;

#[allow(unused_imports, clippy::needless_borrow)]
mod entry_generated;

pub use crate::entry::*;
/// Generated Flatbuffers code for replicating and writing data between IOx
/// servers
pub use entry_generated::influxdata::iox::write::v_1 as entry_fb;
