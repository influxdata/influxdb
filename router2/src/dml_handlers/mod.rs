//! DML handler layers.
//!
//! The [`DmlHandler`] defines a composable abstraction for building a request
//! processing handler chain:
//!
//! ```text
//!                 ┌──────────────┐    ┌──────────────┐
//!                 │   HTTP API   │    │   gRPC API   │
//!                 └──────────────┘    └──────────────┘
//!                         │                   │
//!                         └─────────┬─────────┘
//!                                   │
//!                                   ▼
//!                      ╔═ DmlHandler Stack ═════╗
//!                      ║                        ║
//!                      ║  ┌──────────────────┐  ║
//!                      ║  │    Namespace     │  ║
//!                      ║  │   Autocreation   │─ ─ ─ ─ ─ ─ ─ ┐
//!                      ║  └──────────────────┘  ║
//!                      ║            │           ║           │
//!                      ║            ▼           ║
//!                      ║  ┌──────────────────┐  ║           │
//!                      ║  │   Partitioner    │  ║
//!                      ║  └──────────────────┘  ║           │
//!                      ║            │           ║  ┌─────────────────┐
//!                      ║            ▼           ║  │ Namespace Cache │
//!                      ║  ┌──────────────────┐  ║  └─────────────────┘
//!                      ║  │      Schema      │  ║           │
//!                      ║  │    Validation    │ ─║─ ─ ─ ─ ─ ─
//!                      ║  └──────────────────┘  ║
//!                      ║            │           ║
//!                      ║            ▼           ║
//!         ┌───────┐    ║  ┌──────────────────┐  ║
//!         │Sharder│◀ ─ ─ ▶│ShardedWriteBuffer│  ║
//!         └───────┘    ║  └──────────────────┘  ║
//!                      ║            │           ║
//!                      ╚════════════│═══════════╝
//!                                   │
//!                                   ▼
//!                           ┌──────────────┐
//!                           │ Write Buffer │
//!                           └──────────────┘
//!                                   │
//!                                   │
//!                          ┌────────▼─────┐
//!                          │    Kafka     ├┐
//!                          └┬─────────────┘├┐
//!                           └┬─────────────┘│
//!                            └──────────────┘
//! ```
//!
//! The HTTP / gRPC APIs decode their respective request format and funnel the
//! resulting operation through the common [`DmlHandler`] composed of the layers
//! described above.
//!
//! The [`NamespaceAutocreation`] handler (for testing only) populates the
//! global catalog with an entry for each namespace it observes, using the
//! [`NamespaceCache`] as an optimisation, allowing the handler to skip sending
//! requests to the catalog for namespaces that are known to exist.
//!
//! Incoming line-protocol writes then pass through the [`Partitioner`], parsing
//! the LP and splitting them into batches per partition, before passing each
//! partitioned batch through the rest of the request pipeline.
//!
//! Writes then pass through the [`SchemaValidator`] applying schema enforcement
//! (a NOP layer for deletes) which pushes additive schema changes to the
//! catalog and populates the [`NamespaceCache`], converging it to match the set
//! of [`NamespaceSchema`] in the global catalog.
//!
//! The [`ShardedWriteBuffer`] uses a sharder implementation to direct the DML
//! operations into a fixed set of sequencers.
//!
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
//! [`NamespaceSchema`]: data_types2::NamespaceSchema

mod r#trait;
pub use r#trait::*;

mod schema_validation;
pub use schema_validation::*;

pub mod nop;

mod sharded_write_buffer;
pub use sharded_write_buffer::*;

mod ns_autocreation;
pub use ns_autocreation::*;

mod partitioner;
pub use partitioner::*;

mod instrumentation;
pub use instrumentation::*;

mod chain;
pub use chain::*;

mod fan_out;
pub use fan_out::*;

mod write_summary;
pub use self::write_summary::*;

#[cfg(test)]
pub mod mock;
