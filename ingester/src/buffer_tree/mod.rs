pub(crate) mod namespace;
pub(crate) mod partition;
pub(crate) mod table;

/// The root node of a [`BufferTree`].
mod root;
pub(crate) use root::*;
