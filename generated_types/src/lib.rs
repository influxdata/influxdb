// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
pub mod influxdata {
    pub mod platform {
        pub mod storage {
            include!(concat!(
                env!("OUT_DIR"),
                "/influxdata.platform.storage.read.rs"
            ));
            include!(concat!(
                env!("OUT_DIR"),
                "/influxdata.platform.storage.read.serde.rs"
            ));

            include!(concat!(env!("OUT_DIR"), "/influxdata.platform.storage.rs"));
            include!(concat!(
                env!("OUT_DIR"),
                "/influxdata.platform.storage.serde.rs"
            ));

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
        pub mod delete {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.delete.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.delete.v1.serde.rs"
                ));
            }
        }

        pub mod deployment {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.deployment.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.deployment.v1.serde.rs"
                ));
            }
        }

        pub mod ingester {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.ingester.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.ingester.v1.serde.rs"
                ));
            }
        }

        pub mod management {
            pub mod v1 {
                /// Operation metadata type
                pub const OPERATION_METADATA: &str =
                    "influxdata.iox.management.v1.OperationMetadata";

                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.management.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.management.v1.serde.rs"
                ));
            }
        }

        pub mod namespace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.namespace.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.namespace.v1.serde.rs"
                ));
            }
        }

        pub mod predicate {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.predicate.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.predicate.v1.serde.rs"
                ));
            }
        }

        pub mod preserved_catalog {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.preserved_catalog.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.preserved_catalog.v1.serde.rs"
                ));
            }
        }

        pub mod remote {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.remote.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.remote.v1.serde.rs"
                ));
            }
        }

        pub mod router {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.router.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.router.v1.serde.rs"
                ));
            }
        }

        pub mod schema {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.schema.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.schema.v1.serde.rs"
                ));
            }
        }

        pub mod write_buffer {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.write_buffer.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.write_buffer.v1.serde.rs"
                ));
            }
        }

        pub mod write_summary {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.write_summary.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.write_summary.v1.serde.rs"
                ));
            }
        }
    }

    pub mod pbdata {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/influxdata.pbdata.v1.rs"));
            include!(concat!(env!("OUT_DIR"), "/influxdata.pbdata.v1.serde.rs"));
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

/// gRPC Deployment Service
pub const DEPLOYMENT_SERVICE: &str = "influxdata.iox.deployment.v1.DeploymentService";

/// gRPC Storage Service
pub const STORAGE_SERVICE: &str = "influxdata.platform.storage.Storage";

/// gRPC Testing Service
pub const IOX_TESTING_SERVICE: &str = "influxdata.platform.storage.IOxTesting";

/// gRPC Arrow Flight Service
pub const ARROW_SERVICE: &str = "arrow.flight.protocol.FlightService";

/// The type prefix for any types
pub const ANY_TYPE_PREFIX: &str = "type.googleapis.com";

/// Returns the protobuf URL usable with a google.protobuf.Any message
/// This is the full Protobuf package and message name prefixed by
/// "type.googleapis.com/"
pub fn protobuf_type_url(protobuf_type: &str) -> String {
    format!("{}/{}", ANY_TYPE_PREFIX, protobuf_type)
}

/// Protobuf file descriptor containing all generated types.
/// Useful in gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptor");

/// Compares the protobuf type URL found within a google.protobuf.Any
/// message to an expected Protobuf package and message name
///
/// i.e. strips off the "type.googleapis.com/" prefix from `url`
/// and compares the result with `protobuf_type`
///
/// ```
/// use generated_types::protobuf_type_url_eq;
/// assert!(protobuf_type_url_eq("type.googleapis.com/google.protobuf.Empty", "google.protobuf.Empty"));
/// assert!(!protobuf_type_url_eq("type.googleapis.com/google.protobuf.Empty", "something.else"));
/// ```
pub fn protobuf_type_url_eq(url: &str, protobuf_type: &str) -> bool {
    let mut split = url.splitn(2, '/');
    match (split.next(), split.next()) {
        (Some(ANY_TYPE_PREFIX), Some(t)) => t == protobuf_type,
        _ => false,
    }
}

// TODO: Remove these (#2419)
pub use influxdata::platform::storage::*;

pub mod google;

#[cfg(any(feature = "data_types_conversions", test))]
pub mod chunk;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod database_rules;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod database_state;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod delete_predicate;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod ingester;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod job;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod router;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod server_config;
#[cfg(any(feature = "data_types_conversions", test))]
pub mod write_buffer;

pub use prost::{DecodeError, EncodeError};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_type_url() {
        use influxdata::iox::management::v1::OPERATION_METADATA;

        let t = protobuf_type_url(OPERATION_METADATA);

        assert_eq!(
            &t,
            "type.googleapis.com/influxdata.iox.management.v1.OperationMetadata"
        );

        assert!(protobuf_type_url_eq(&t, OPERATION_METADATA));
        assert!(!protobuf_type_url_eq(&t, "foo"));

        // The URL must start with the type.googleapis.com prefix
        assert!(!protobuf_type_url_eq(
            OPERATION_METADATA,
            OPERATION_METADATA
        ));
    }
}
