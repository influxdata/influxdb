// The workspace hack crate doesn't actually use any crates.
#![expect(unused_crate_dependencies)]

// A build script is required for cargo to consider build dependencies.
fn main() {}
