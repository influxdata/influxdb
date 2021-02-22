// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![allow(
    unused_imports,
    clippy::redundant_static_lifetimes,
    clippy::redundant_closure,
    clippy::redundant_field_names,
    clippy::clone_on_ref_ptr
)]

mod pb {
    pub mod google {
        pub mod protobuf {
            include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
        }

        pub mod rpc {
            include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
        }
    }
}

pub use pb::google::*;
