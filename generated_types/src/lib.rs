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

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
pub mod influxdata {
    pub mod platform {
        pub mod storage {
            include!(concat!(env!("OUT_DIR"), "/influxdata.platform.storage.rs"));

            // Can't implement `Default` because `prost::Message` implements `Default`
            impl TimestampRange {
                pub fn max() -> Self {
                    TimestampRange {
                        start: std::i64::MIN,
                        end: std::i64::MAX,
                    }
                }
            }
        }
    }

    pub mod iox {
        pub mod management {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.management.v1.rs"));
            }
        }

        pub mod write {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.write.v1.rs"));
            }
        }
    }
}

pub mod com {
    pub mod github {
        pub mod influxdata {
            pub mod idpe {
                pub mod storage {
                    pub mod read {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/com.github.influxdata.idpe.storage.read.rs"
                        ));
                    }
                }
            }
        }
    }
}

// Needed because of https://github.com/hyperium/tonic/issues/471
pub mod grpc {
    pub mod health {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.rs"));
        }
    }
}

include!(concat!(env!("OUT_DIR"), "/wal_generated.rs"));

/// gRPC Storage Service
pub const STORAGE_SERVICE: &str = "influxdata.platform.storage.Storage";
/// gRPC Testing Service
pub const IOX_TESTING_SERVICE: &str = "influxdata.platform.storage.IOxTesting";
/// gRPC Arrow Flight Service
pub const ARROW_SERVICE: &str = "arrow.flight.protocol.FlightService";

pub use com::github::influxdata::idpe::storage::read::*;
pub use influxdata::platform::storage::*;

pub mod google;
