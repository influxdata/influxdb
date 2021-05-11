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
pub use chunk::{Chunk, Error};
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
        cmp::Operator, encoding::fixed::Fixed, encoding::fixed_null::FixedNull, encoding::string,
        Column, RowIDs,
    };

    pub use crate::row_group::{ColumnType, RowGroup};
}
