//! DML handler layers.
//!
//! The [`DmlHandler`] defines a composable abstraction for building a request
//! processing handler chain:
//!
//! ```text
//!                                                  ┌─────────────────┐
//!                                                  │ Namespace Cache │
//!                                                  └─────────────────┘
//!                      ╔═ DmlHandler Stack ═════╗           │
//!                      ║                        ║
//!                      ║  ┌──────────────────┐  ║           │
//!                      ║  │   Partitioner    │  ║
//!                      ║  └──────────────────┘  ║           │
//!                      ║            │           ║
//!                      ║            ▼           ║           │
//!                      ║  ┌──────────────────┐  ║
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
//! The HTTP API decodes the request and funnels the resulting operation through
//! the [`DmlHandler`] stack composed of the layers described above.
//!
//! Incoming line-protocol writes pass through the [`Partitioner`], parsing the
//! LP and splitting them into batches per IOx partition, before passing each
//! partitioned batch through the rest of the request pipeline.
//!
//! Writes then pass through the [`SchemaValidator`] applying schema & limit
//! enforcement (a NOP layer for deletes) which pushes additive schema changes
//! to the catalog and populates the [`NamespaceCache`], converging it to match
//! the set of [`NamespaceSchema`] in the global catalog.
//!
//! The [`ShardedWriteBuffer`] uses a sharder implementation to direct the DML
//! operations into a fixed set of shards.
//!
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
//! [`NamespaceSchema`]: data_types::NamespaceSchema

mod r#trait;
pub use r#trait::*;

mod schema_validation;
pub use schema_validation::*;

pub mod nop;

mod sharded_write_buffer;
pub use sharded_write_buffer::*;

mod retention_validation;
pub use retention_validation::*;

mod partitioner;
pub use partitioner::*;

mod instrumentation;
pub use instrumentation::*;

mod chain;
pub use chain::*;

mod fan_out;
pub use fan_out::*;

mod rpc_write;
pub use rpc_write::*;

mod write_summary;
pub use self::write_summary::*;

#[cfg(test)]
pub mod mock;
