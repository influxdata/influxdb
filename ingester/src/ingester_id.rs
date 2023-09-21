//! A randomised, globally unique identifier of a single ingester instance.
//!
//! The value of this ID is expected to change between restarts of the ingester.

use std::fmt::Display;

use uuid::Uuid;

/// A unique, random, opaque UUID assigned at startup of an ingester.
///
/// This [`IngesterId`] uniquely identifies a single ingester process - it
/// changes each time an ingester starts, reflecting the change of in-memory
/// state between crashes/restarts.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct IngesterId(Uuid);

impl Display for IngesterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl IngesterId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
