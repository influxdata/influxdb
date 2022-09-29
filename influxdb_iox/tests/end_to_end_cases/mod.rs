mod all_in_one;
// CLI errors when run with heappy (only works via `cargo run`):
// loading shared libraries: libjemalloc.so.2: cannot open shared object file: No such file or directory"
#[cfg(not(feature = "heappy"))]
mod cli;
mod compactor;
mod debug;
mod error;
mod ingester;
mod logging;
mod metrics;
mod namespace;
mod querier;
mod schema;
mod tracing;
