//! DML handler layers.
//!
//! The [`DmlHandler`] defines a composable abstraction for building a request
//! processing handler chain:
//!
//! ```text
//!
//!             ┌──────────────┐    ┌──────────────┐
//!             │   HTTP API   │    │   gRPC API   │
//!             └──────────────┘    └──────────────┘
//!                     │                   │
//!                     └─────────┬─────────┘
//!                               │
//!                               ▼
//!                  ╔═ DmlHandler Stack ═════╗
//!                  ║                        ║
//!                  ║  ┌──────────────────┐  ║
//!                  ║  │      Schema      │  ║
//!                  ║  │    Validation    │  ║
//!                  ║  └──────────────────┘  ║
//!                  ║            │           ║
//!                  ║            ▼           ║
//!                  ║  ┌──────────────────┐  ║  ┌─────────┐
//!                  ║  │ShardedWriteBuffer│◀───▶│ Sharder │
//!                  ║  └──────────────────┘  ║  └─────────┘
//!                  ║            │           ║
//!                  ╚════════════│═══════════╝
//!                               │
//!                               ▼
//!                       ┌──────────────┐
//!                       │ Write Buffer │
//!                       └──────────────┘
//!                               │
//!                               │
//!                      ┌────────▼─────┐
//!                      │    Kafka     ├┐
//!                      └┬─────────────┘├┐
//!                       └┬─────────────┘│
//!                        └──────────────┘
//! ```
//!
//! The HTTP / gRPC APIs decode their respective request format and funnel the
//! resulting operation through the common [`DmlHandler`] composed of the layers
//! described above.

mod r#trait;
pub use r#trait::*;

mod schema_validation;
pub use schema_validation::*;

pub mod nop;

mod sharded_write_buffer;
pub use sharded_write_buffer::*;

#[cfg(test)]
pub mod mock;
