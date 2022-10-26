//! IOx router implementation.
//!
//! An IOx router is responsible for:
//!
//! * Creating IOx namespaces & synchronising them within the catalog.
//! * Handling writes:
//!     * Receiving IOx write/delete requests via HTTP.
//!     * Enforcing schema validation & synchronising it within the catalog.
//!     * Deriving the partition key of each DML operation.
//!     * Applying sharding logic.
//!     * Push resulting operations into the appropriate shards (Kafka
//!       partitions if using Kafka).
//!
//! The router is composed of singly-responsible components that each apply a
//! transformation to the request:
//!
//! ```text
//!                           ┌──────────────┐
//!                           │   HTTP API   │
//!                           └──────────────┘
//!                                   │
//!                                   │
//!                                   ▼
//!                      ╔═ NamespaceResolver ════╗
//!                      ║                        ║
//!                      ║  ┌──────────────────┐  ║
//!                      ║  │    Namespace     │  ║
//!                      ║  │   Autocreation   │ ─║─ ─ ─ ─ ─ ─
//!                      ║  └──────────────────┘  ║           │
//!                      ║            │           ║
//!                      ║            ▼           ║           │
//!                      ║  ┌──────────────────┐  ║
//!                      ║  │ NamespaceSchema  │  ║           │
//!                      ║  │     Resolver     │ ─║─ ─ ─ ─ ─ ─
//!                      ║  └──────────────────┘  ║           │
//!                      ║            │           ║
//!                      ╚════════════│═══════════╝           │
//!                                   │              ┌─────────────────┐
//!                                   │              │ Namespace Cache │
//!                                   │              └─────────────────┘
//!                      ╔═ DmlHandler│Stack ═════╗           │
//!                      ║            ▼           ║
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
//! The [`NamespaceAutocreation`] handler (for testing only) populates the
//! global catalog with an entry for each namespace it observes, using the
//! [`NamespaceCache`] as an optimisation, allowing the handler to skip sending
//! requests to the catalog for namespaces that are known to exist.
//!
//! A [`NamespaceResolver`] maps a user-provided namespace string to the
//! catalog ID ([`NamespaceId`]). The [`NamespaceSchemaResolver`] achieves this
//! by inspecting the contents of the [`NamespaceCache`]. If a cache-miss
//! occurs the [`NamespaceSchemaResolver`] queries the catalog and populates the
//! cache for subsequent requests.
//!
//! Once the [`NamespaceId`] has been resolved, the request is passed into the
//! [`DmlHandler`] stack.
//!
//! [`NamespaceAutocreation`]: crate::namespace_resolver::NamespaceAutocreation
//! [`NamespaceSchemaResolver`]: crate::namespace_resolver::NamespaceSchemaResolver
//! [`NamespaceResolver`]: crate::namespace_resolver
//! [`NamespaceId`]: data_types::NamespaceId
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
//! [`NamespaceSchema`]: data_types::NamespaceSchema
//! [`DmlHandler`]: crate::dml_handlers

#![deny(
    rustdoc::broken_intra_doc_links,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]
#![allow(clippy::missing_docs_in_private_items)]

pub mod dml_handlers;
pub mod namespace_cache;
pub mod namespace_resolver;
pub mod server;
pub mod shard;
