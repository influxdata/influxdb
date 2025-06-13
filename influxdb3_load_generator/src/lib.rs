// Workaround for unused crate lint; these are only used in the binary crate.
use dotenvy as _;
use observability_deps as _;

pub mod line_protocol_generator;
pub mod query_generator;
pub mod report;
pub mod specification;
pub mod specs;

pub mod commands {
    pub mod common;
    pub mod full;
    pub mod query;
    pub mod write;
    pub mod write_fixed;
}
