//! Ordering machinery shared by IOx's physical plans.
//!
//! This mirrors DataFusion's `physical_plan::sorts`, and holds the pieces of it
//! IOx replaces.

pub mod run_optimized_merge;
