pub(crate) mod namespace;
pub(crate) mod partition;
pub(crate) mod table;

/// The root node of a [`BufferTree`].
mod root;
#[allow(unused_imports)]
pub(crate) use root::*;

pub(crate) mod post_write;

maybe_pub! {
    pub use super::partition::PartitionData;
}
