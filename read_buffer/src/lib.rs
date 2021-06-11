#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(clippy::clone_on_ref_ptr, clippy::use_self)]
#![allow(dead_code, clippy::too_many_arguments)]
mod chunk;
mod column;
mod row_group;
mod schema;
mod table;
mod value;

// Identifiers that are exported as part of the public API.
pub use chunk::{Chunk as RBChunk, ChunkMetrics, Error};
pub use row_group::{BinaryExpr, Predicate};
pub use schema::*;
pub use table::ReadFilterResults;

/// THIS MODULE SHOULD ONLY BE IMPORTED FOR BENCHMARKS.
///
/// This module lets us expose internal parts of the crate so that we can use
/// libraries like criterion for benchmarking.
///
/// It should not be imported into any non-testing or benchmarking crates.
pub mod benchmarks {
    pub use crate::column::{
        cmp::Operator,
        encoding::scalar::transcoders::*,
        encoding::scalar::{Fixed, FixedNull, ScalarEncoding},
        encoding::string,
        Column, RowIDs,
    };
    pub use crate::row_group::{ColumnType, RowGroup};
    use crate::RBChunk;

    // Allow external benchmarks to use this crate-only test method
    pub fn upsert_table_with_row_group(
        chunk: &mut RBChunk,
        table_name: impl Into<String>,
        row_group: RowGroup,
    ) {
        chunk.upsert_table_with_row_group(table_name, row_group)
    }
}
