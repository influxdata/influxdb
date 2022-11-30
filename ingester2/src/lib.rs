//! IOx Ingester V2 implementation.

#![allow(dead_code)] // Until ingester2 is used.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub
)]

use data_types::{ShardId, ShardIndex};

/// During the testing of ingester2, the catalog will require a ShardId for
/// various operations. This is a const value for these occasions.
const TRANSITION_SHARD_ID: ShardId = ShardId::new(1);
const TRANSITION_SHARD_INDEX: ShardIndex = ShardIndex::new(1);

/// Ingester initialisation methods & types.
///
/// This module defines the public API boundary of the Ingester crate.
mod init;
pub use init::*;

//
// !!! PLEASE DO NOT EXPORT !!!
//
// Please be judicious when deciding if something should be visible outside this
// crate. Overzealous use of `pub` exposes the internals of an Ingester and
// causes tight coupling with other components. I know it's tempting, but it
// causes problems in the long run.
//
// Ideally anything that NEEDS to be shared with other components happens via a
// shared common crate. Any external components should interact with an Ingester
// through its public API only, and not by poking around at the internals.
//

mod arcmap;
mod buffer_tree;
mod deferred_load;
mod dml_sink;
mod query;
mod query_adaptor;
mod sequence_range;
pub(crate) mod server;
mod timestamp_oracle;
mod wal;

#[cfg(test)]
mod test_util;
