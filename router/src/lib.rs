//! IOx router implementation.
//!
//! An IOx router is responsible for:
//!
//! * Creating IOx namespaces & synchronising them within the catalog.
//! * Handling writes:
//!     * Receiving IOx write/delete requests via HTTP
//!     * Creating or validating the write's namespace
//!     * Validating write payloads are within the configured retention period
//!     * Enforcing schema validation & synchronising it within the catalog
//!     * Deriving the partition key of each DML operation.
//!     * Load-balancing & health-checking the pool of downstream ingesters
//!     * Writing payloads to the downstream ingesters
//!
//! The router is composed of singly-responsible components that each apply a
//! transformation to the request:
//!
//! ```text
//!                       ┌──────────────┐
//!                       │   HTTP API   │
//!                       └──────────────┘
//!                               │
//!                               │
//!                               ▼
//!                  ╔═ NamespaceResolver ════╗
//!                  ║                        ║
//!                  ║  ┌──────────────────┐  ║
//!                  ║  │    Namespace     │  ║
//!                  ║  │   Autocreation   │ ─║─ ─ ─ ─ ─ ─ ┐
//!                  ║  └──────────────────┘  ║
//!                  ║            │           ║            │
//!                  ║            ▼           ║
//!                  ║  ┌──────────────────┐  ║            │
//!                  ║  │ NamespaceSchema  │  ║
//!                  ║  │     Resolver     │ ─║─ ─ ─ ─ ─ ─ ┤
//!                  ║  └──────────────────┘  ║
//!                  ║            │           ║            │
//!                  ╚════════════│═══════════╝
//!                               │               ┌─────────────────┐
//!                               │               │ Namespace Cache │
//!                               │               └─────────────────┘
//!                  ╔═ DmlHandler│Stack ═════╗            │
//!                  ║            ▼           ║
//!                  ║                        ║            │
//!                  ║  ┌──────────────────┐  ║
//!                  ║  │RetentionValidator│ ─║─ ─ ─ ─ ─ ─ ┤
//!                  ║  └──────────────────┘  ║
//!                  ║            │           ║            │
//!                  ║            ▼           ║
//!                  ║  ┌──────────────────┐  ║            │
//!                  ║  │      Schema      │  ║
//!                  ║  │    Validation    │ ─║─ ─ ─ ─ ─ ─ ┘
//!                  ║  └──────────────────┘  ║
//!                  ║            │           ║
//!                  ║            ▼           ║
//!                  ║  ┌──────────────────┐  ║
//!                  ║  │   Partitioner    │  ║
//!                  ║  └──────────────────┘  ║
//!                  ╚════════════│═══════════╝
//!                               │
//!                               ▼
//!                       ┌──────────────┐
//!                       │  RPC Writer  │
//!                       └──────────────┘
//!                               │
//!                               │
//!                      ┌ ─ ─ ─ ─▼─ ─ ─
//!                          Ingester   ├
//!                      └ ─ ─ ─ ─ ─ ─ ─ ├
//!                       └ ─ ─ ─ ─ ─ ─ ─ │
//!                        └ ─ ─ ─ ─ ─ ─ ─
//! ```
//!
//! The [`NamespaceAutocreation`] handler (for testing only) populates the
//! global catalog with an entry for each namespace it observes, if enabled. It
//! uses the [`NamespaceCache`] as an optimisation, allowing the handler to skip
//! sending requests to the catalog for namespaces that are known to exist.
//!
//! A [`NamespaceResolver`] maps a user-provided namespace string to the catalog
//! ID ([`NamespaceId`]). The [`NamespaceSchemaResolver`] achieves this by
//! inspecting the contents of the [`NamespaceCache`]. If a cache-miss occurs
//! the [`NamespaceSchemaResolver`] queries the catalog and populates the cache
//! for subsequent requests.
//!
//! Once the [`NamespaceId`] has been resolved, the request is passed into the
//! [`DmlHandler`] stack.
//!
//! ## DML Handlers
//!
//! The handlers are composed together to form a request handling pipeline,
//! optionally transforming the payloads and passing them through to the next
//! handler in the chain.
//!
//! Each DML handler implements the [`DmlHandler`] trait.
//!
//! See the handler types for further documentation:
//!
//! * [`RetentionValidator`]
//! * [`SchemaValidator`]
//! * [`Partitioner`]
//! * [`RpcWrite`]
//!
//! [`NamespaceAutocreation`]: crate::namespace_resolver::NamespaceAutocreation
//! [`NamespaceSchemaResolver`]:
//!     crate::namespace_resolver::NamespaceSchemaResolver
//! [`NamespaceResolver`]: crate::namespace_resolver
//! [`NamespaceId`]: data_types::NamespaceId
//! [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
//! [`NamespaceSchema`]: data_types::NamespaceSchema
//! [`DmlHandler`]: crate::dml_handlers
//! [`RetentionValidator`]: crate::dml_handlers::RetentionValidator
//! [`SchemaValidator`]: crate::dml_handlers::SchemaValidator
//! [`Partitioner`]: crate::dml_handlers::Partitioner
//! [`RpcWrite`]: crate::dml_handlers::RpcWrite

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
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
use workspace_hack as _;

pub mod dml_handlers;
pub mod gossip;
pub mod namespace_cache;
pub mod namespace_resolver;
pub mod server;
