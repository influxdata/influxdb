// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![expect(
    clippy::clone_on_ref_ptr,
    clippy::derive_partial_eq_without_eq,
    clippy::large_enum_variant,
    clippy::use_self,
    clippy::allow_attributes,
    clippy::uninlined_format_args,
    // I can't figure out what rustdoc lint triggers in this. It's not any of the individual ones,
    // only rustdoc::all fixes it afaict
    rustdoc::all,
    missing_copy_implementations
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{collections::HashMap, sync::LazyLock};

use prost::Message;
use prost_types::FileDescriptorProto;
// Re-export prost for users of proto types.
pub use prost;
pub use prost_types::FileDescriptorSet;

// Re-export commonly-used Tonic types
pub use tonic::{
    self, Code, IntoRequest, Request, Response, Status, Streaming, async_trait,
    codegen::{StdError, http},
    metadata, transport,
};

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
                        start: i64::MIN,
                        end: i64::MAX,
                    }
                }
            }
        }
        pub mod errors {
            include!(concat!(env!("OUT_DIR"), "/influxdata.platform.errors.rs"));
            include!(concat!(
                env!("OUT_DIR"),
                "/influxdata.platform.errors.serde.rs"
            ));
        }
    }

    pub mod iox {
        pub mod authz {
            pub mod pusher {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/influxdata.iox.authz.pusher.v1.rs"
                    ));
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/influxdata.iox.authz.pusher.v1.serde.rs"
                    ));
                }
            }
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.authz.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.authz.v1.serde.rs"
                ));
            }
        }

        pub mod bulk_ingest {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.bulk_ingest.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.bulk_ingest.v1.serde.rs"
                ));
            }
        }

        pub mod catalog {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.catalog.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog.v1.serde.rs"
                ));
            }
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.catalog.v2.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog.v2.serde.rs"
                ));
            }
        }

        pub mod catalog_cache {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog_cache.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog_cache.v1.serde.rs"
                ));
            }

            impl From<uuid::Uuid> for v1::Uuid {
                fn from(value: uuid::Uuid) -> Self {
                    let (high, low) = value.as_u64_pair();
                    Self { high, low }
                }
            }

            impl From<v1::Uuid> for uuid::Uuid {
                fn from(value: v1::Uuid) -> Self {
                    uuid::Uuid::from_u64_pair(value.high, value.low)
                }
            }
        }

        pub mod catalog_storage {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog_storage.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.catalog_storage.v1.serde.rs"
                ));
            }
        }

        pub mod column_type {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.column_type.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.column_type.v1.serde.rs"
                ));
            }
        }

        pub mod common {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.common.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.common.v1.serde.rs"
                ));
            }
        }

        pub mod compactor {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.compactor.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.compactor.v1.serde.rs"
                ));
            }
        }

        pub mod delete {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.delete.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.delete.v1.serde.rs"
                ));
            }
        }

        pub mod gossip {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.gossip.v1.rs"));
            }

            /// The set of topics used for IOx gossiping.
            ///
            /// NOTE: Don't renumber topics. Don't re-use numbers. Use the range
            /// 0 to 63 for numbers.
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum Topic {
                /// New namespace, table, and column additions observed and
                /// broadcast by the routers.
                SchemaChanges = 1,

                /// Parquet file creation notifications.
                NewParquetFiles = 2,

                /// Compaction round completion notifications.
                CompactionEvents = 3,

                /// Schema cache consistency check / sync / convergence
                /// messages.
                SchemaCacheConsistency = 4,

                /// Partition sort key update notifications.
                PartitionSortKeyUpdates = 5,

                /// Notifications for non-schema changes to namespace state in
                /// the cluster.
                NamespaceEvents = 6,

                /// Metadata after query exec completion (currently only emitted by ingester).
                QueryExecMetadata = 7,
            }

            impl TryFrom<u64> for Topic {
                type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

                fn try_from(v: u64) -> Result<Self, Self::Error> {
                    Ok(match v {
                        v if v == Self::SchemaChanges as u64 => Self::SchemaChanges,
                        v if v == Self::NewParquetFiles as u64 => Self::NewParquetFiles,
                        v if v == Self::CompactionEvents as u64 => Self::CompactionEvents,
                        v if v == Self::SchemaCacheConsistency as u64 => {
                            Self::SchemaCacheConsistency
                        }
                        v if v == Self::PartitionSortKeyUpdates as u64 => {
                            Self::PartitionSortKeyUpdates
                        }
                        v if v == Self::NamespaceEvents as u64 => Self::NamespaceEvents,
                        v if v == Self::QueryExecMetadata as u64 => Self::QueryExecMetadata,
                        _ => return Err(format!("unknown topic id {v}").into()),
                    })
                }
            }

            impl From<Topic> for u64 {
                fn from(v: Topic) -> u64 {
                    v as u64
                }
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

        pub mod namespace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.namespace.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.namespace.v1.serde.rs"
                ));
            }
        }

        pub mod object_store {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.object_store.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.object_store.v1.serde.rs"
                ));
            }
        }

        pub mod partition_template {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.partition_template.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.partition_template.v1.serde.rs"
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

        pub mod querier {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.querier.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.querier.v1.serde.rs"
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

        pub mod skipped_compaction {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.skipped_compaction.v1.rs"
                ));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.skipped_compaction.v1.serde.rs"
                ));
            }
        }

        pub mod table {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.table.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.table.v1.serde.rs"
                ));
            }
        }

        pub mod wal {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.wal.v1.rs"));
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.wal.v1.serde.rs"));
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq)]
        /// A selector enum for querying a router or catalog entity (currently either a namespace or table)
        /// via some searchable property.
        pub enum Target {
            /// Selects a namespace or table by its name.
            ///
            /// For active (non-deleted) entities, names are guaranteed to be unique within a
            /// given scope (ex. all active tables within a namespace have a unique name)
            Name(String),
            /// Selects a namespace or table by its unique ID.
            ///
            /// This ID is unique across all active and deleted entities of the same type.
            Id(i64),
        }

        impl core::fmt::Display for Target {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    Self::Id(id) => {
                        id.fmt(f)?;
                        write!(f, " (ID)")
                    }
                    Self::Name(name) => {
                        name.fmt(f)?;
                        write!(f, " (name)")
                    }
                }
            }
        }

        impl From<i64> for Target {
            fn from(value: i64) -> Self {
                Self::Id(value)
            }
        }

        impl From<String> for Target {
            fn from(value: String) -> Self {
                Self::Name(value)
            }
        }

        impl<'a> From<&'a String> for Target {
            fn from(value: &'a String) -> Self {
                Self::Name(value.into())
            }
        }

        impl<'a> From<&'a str> for Target {
            fn from(value: &'a str) -> Self {
                Self::Name(value.into())
            }
        }

        macro_rules! target_from_impls {
            ($proto_type:ty) => {
                target_from_impls!($proto_type, Name, Id);
            };
            ($proto_type:ty, $name_variant:ident, $id_variant:ident) => {
                impl From<$proto_type> for Target {
                    fn from(value: $proto_type) -> Self {
                        use $proto_type::*;
                        match value {
                            $name_variant(name) => Self::Name(name.into()),
                            $id_variant(id) => Self::Id(id),
                        }
                    }
                }
                impl From<Target> for $proto_type {
                    fn from(value: Target) -> Self {
                        match value {
                            Target::Name(name) => Self::$name_variant(name.into()),
                            Target::Id(id) => Self::$id_variant(id),
                        }
                    }
                }
                impl From<Target> for Option<$proto_type> {
                    fn from(value: Target) -> Self {
                        Some(value.into())
                    }
                }
            };
        }

        target_from_impls!(
            table::v1::get_tables_request::Target,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            table::v1::get_table_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            table::v1::get_table_request::TableTarget,
            TableName,
            TableId
        );
        target_from_impls!(
            table::v1::create_table_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(namespace::v1::get_namespace_request::Target);
        target_from_impls!(namespace::v1::delete_namespace_request::Target);
        target_from_impls!(namespace::v1::update_namespace_retention_request::Target);
        target_from_impls!(
            namespace::v1::update_namespace_service_protection_limit_request::Target
        );
        target_from_impls!(catalog::v2::namespace_update_retention_period_request::Target);
        target_from_impls!(catalog::v2::namespace_soft_delete_request::Target);
        target_from_impls!(catalog::v2::namespace_update_table_limit_request::Target);
        target_from_impls!(catalog::v2::namespace_update_column_limit_request::Target);
        target_from_impls!(
            bulk_ingest::v1::upsert_sort_key_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            bulk_ingest::v1::upsert_sort_key_request::TableTarget,
            TableName,
            TableId
        );
        target_from_impls!(
            bulk_ingest::v1::new_parquet_metadata_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            bulk_ingest::v1::new_parquet_metadata_request::TableTarget,
            TableName,
            TableId
        );
        target_from_impls!(
            schema::v1::get_schema_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            schema::v1::get_schema_request::TableTarget,
            TableName,
            TableId
        );
        target_from_impls!(
            schema::v1::upsert_schema_request::NamespaceTarget,
            NamespaceName,
            NamespaceId
        );
        target_from_impls!(
            schema::v1::upsert_schema_request::TableTarget,
            TableName,
            TableId
        );
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
    format!("{ANY_TYPE_PREFIX}/{protobuf_type}")
}

/// Protobuf file descriptor containing all generated types.
/// Useful in gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("proto_descriptor");

/// A map of [`tonic::server::NamedService::NAME`] strings to
/// [`prost_types::FileDescriptorProto`]s. This is intended to be used to selectively populating
/// the gRPC reflection service only with service protos being registered in a given IOx component.
pub static FILE_DESCRIPTOR_MAP: LazyLock<HashMap<String, FileDescriptorProto>> =
    LazyLock::new(|| {
        FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
            .expect("decoding FILE_DESCRIPTOR_SET")
            .file
            .into_iter()
            .flat_map(|proto| {
                proto
                    .service
                    .iter()
                    .map(|svc| {
                        // Figure out the fully-qualified name of this service.
                        //
                        // This string will be in the form of "<package>.<service>"
                        // where "package" may itself contain multiple parts
                        // delimited by "."
                        //
                        // If no package exists, use the service name only.
                        let qualified_name = proto
                            .package
                            .iter()
                            .chain(svc.name.iter())
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(".");

                        (qualified_name, proto.clone())
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    });

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

pub use prost::{DecodeError, EncodeError};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::influxdata::iox::gossip::Topic;

    use super::*;

    #[test]
    fn test_protobuf_type_url() {
        let t = protobuf_type_url(STORAGE_SERVICE);

        assert_eq!(
            &t,
            "type.googleapis.com/influxdata.platform.storage.Storage"
        );

        assert!(protobuf_type_url_eq(&t, STORAGE_SERVICE));
        assert!(!protobuf_type_url_eq(&t, "foo"));

        // The URL must start with the type.googleapis.com prefix
        assert!(!protobuf_type_url_eq(STORAGE_SERVICE, STORAGE_SERVICE,));
    }

    #[test]
    fn test_gossip_topics() {
        let topics = [
            Topic::SchemaChanges,
            Topic::NewParquetFiles,
            Topic::CompactionEvents,
            Topic::SchemaCacheConsistency,
            Topic::PartitionSortKeyUpdates,
            Topic::NamespaceEvents,
            Topic::QueryExecMetadata,
        ];

        for topic in topics {
            let v = u64::from(topic);
            let got = Topic::try_from(v).expect("failed to round-trip topic");
            assert_eq!(got, topic);
        }

        // Adding a new topic? Add it to the test cases too and then add it to
        // this match (that forces a compile-time error and makes you read this
        // message).
        match topics[0] {
            Topic::SchemaChanges => {}
            Topic::NewParquetFiles => {}
            Topic::CompactionEvents => {}
            Topic::SchemaCacheConsistency => {}
            Topic::PartitionSortKeyUpdates => {}
            Topic::NamespaceEvents => {}
            Topic::QueryExecMetadata => {}
        }
    }

    #[test]
    fn test_file_descriptor_map() {
        // use the following command if the list of expected keys needs to be updated:
        // grep -E -rn 'const NAME' target/debug/build/generated_types-*/out/*rs | awk -F" = " '{print $2}' | sort | uniq | tr -d \;
        let expected_keys = vec![
            "influxdata.platform.storage.Storage",
            "influxdata.platform.storage.IOxTesting",
            "google.longrunning.Operations",
            "influxdata.iox.catalog.v1.CatalogService",
            "influxdata.iox.catalog.v2.CatalogService",
            "influxdata.iox.catalog_storage.v1.CatalogStorageService",
            "influxdata.iox.schema.v1.SchemaService",
            "influxdata.iox.gossip.v1.AntiEntropyService",
            "influxdata.iox.authz.v1.IoxAuthorizerService",
            "influxdata.iox.delete.v1.DeleteService",
            "influxdata.iox.namespace.v1.NamespaceService",
            "influxdata.iox.ingester.v1.PersistService",
            "influxdata.iox.ingester.v1.WriteService",
            "influxdata.iox.querier.v1.QueryLogService",
            "grpc.health.v1.Health",
            "influxdata.iox.compactor.v1.CompactionService",
            "influxdata.iox.object_store.v1.ObjectStoreService",
            "influxdata.iox.table.v1.TableService",
            "influxdata.iox.bulk_ingest.v1.BulkIngestService",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let fd_map_actual_keys: HashSet<String> = FILE_DESCRIPTOR_MAP.keys().cloned().collect();

        assert_eq!(fd_map_actual_keys, expected_keys);
    }
}
