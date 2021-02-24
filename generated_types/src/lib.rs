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
}

include!(concat!(env!("OUT_DIR"), "/wal_generated.rs"));

pub use pb::com::github::influxdata::idpe::storage::read::*;
pub use pb::influxdata::platform::storage::*;

pub use google_types as google;
pub use pb::influxdata;
