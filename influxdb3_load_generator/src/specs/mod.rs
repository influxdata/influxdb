//! This module contains the built-in specifications for the load generator.

use crate::specification::{DataSpec, QuerierSpec};

mod example;
mod one_mil;

/// Get all built-in specs
pub(crate) fn built_in_specs() -> Vec<BuiltInSpec> {
    // add new built-in specs here to the end of this vec
    vec![example::spec(), one_mil::spec()]
}

pub(crate) fn built_in_spec_names() -> Vec<String> {
    built_in_specs()
        .iter()
        .map(|b| b.name.to_string())
        .collect()
}

/// A built-in specification for the load generator
pub(crate) struct BuiltInSpec {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) write_spec: DataSpec,
    pub(crate) query_spec: QuerierSpec,
}
