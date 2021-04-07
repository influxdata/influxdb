#![deny(rust_2018_idioms)]
#![warn(clippy::clone_on_ref_ptr, clippy::use_self)]
#![allow(dead_code, clippy::too_many_arguments)]
pub mod chunk;
pub(crate) mod column;
pub(crate) mod row_group;
mod schema;
pub(crate) mod table;
pub(crate) mod value;

// Identifiers that are exported as part of the public API.
pub use chunk::Chunk;
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
        cmp::Operator, encoding::dictionary, encoding::fixed::Fixed,
        encoding::fixed_null::FixedNull, Column, RowIDs,
    };

    pub use crate::row_group::{ColumnType, RowGroup};
}
