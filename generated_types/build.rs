//! Compiles Protocol Buffers into native Rust types.

use std::env;
use std::path::{Path, PathBuf};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    generate_grpc_types(&root)?;

    Ok(())
}

/// Schema used with IOx specific gRPC requests
///
/// Creates:
///
/// - `influxdata.iox.delete.v1.rs`
/// - `influxdata.iox.deployment.v1.rs`
/// - `influxdata.iox.ingester.v1.rs`
/// - `influxdata.iox.management.v1.rs`
/// - `influxdata.iox.namespace.v1.rs`
/// - `influxdata.iox.preserved_catalog.v1.rs`
/// - `influxdata.iox.remote.v1.rs`
/// - `influxdata.iox.router.v1.rs`
/// - `influxdata.iox.schema.v1.rs`
/// - `influxdata.iox.write.v1.rs`
/// - `influxdata.iox.write_buffer.v1.rs`
/// - `influxdata.platform.storage.rs`
fn generate_grpc_types(root: &Path) -> Result<()> {
    let delete_path = root.join("influxdata/iox/delete/v1");
    let deployment_path = root.join("influxdata/iox/deployment/v1");
    let ingester_path = root.join("influxdata/iox/ingester/v1");
    let management_path = root.join("influxdata/iox/management/v1");
    let namespace_path = root.join("influxdata/iox/namespace/v1");
    let predicate_path = root.join("influxdata/iox/predicate/v1");
    let preserved_catalog_path = root.join("influxdata/iox/preserved_catalog/v1");
    let remote_path = root.join("influxdata/iox/remote/v1");
    let router_path = root.join("influxdata/iox/router/v1");
    let schema_path = root.join("influxdata/iox/schema/v1");
    let storage_path = root.join("influxdata/platform/storage");
    let write_buffer_path = root.join("influxdata/iox/write_buffer/v1");
    let write_summary_path = root.join("influxdata/iox/write_summary/v1");

    let proto_files = vec![
        delete_path.join("service.proto"),
        deployment_path.join("service.proto"),
        ingester_path.join("parquet_metadata.proto"),
        ingester_path.join("query.proto"),
        management_path.join("chunk.proto"),
        management_path.join("database_rules.proto"),
        management_path.join("jobs.proto"),
        management_path.join("partition.proto"),
        management_path.join("partition_template.proto"),
        management_path.join("server_config.proto"),
        management_path.join("service.proto"),
        namespace_path.join("service.proto"),
        predicate_path.join("predicate.proto"),
        preserved_catalog_path.join("catalog.proto"),
        preserved_catalog_path.join("parquet_metadata.proto"),
        root.join("google/longrunning/operations.proto"),
        root.join("google/rpc/error_details.proto"),
        root.join("google/rpc/status.proto"),
        root.join("grpc/health/v1/service.proto"),
        root.join("influxdata/pbdata/v1/influxdb_pb_data_protocol.proto"),
        remote_path.join("remote.proto"),
        remote_path.join("service.proto"),
        router_path.join("router.proto"),
        router_path.join("service.proto"),
        router_path.join("shard.proto"),
        schema_path.join("service.proto"),
        storage_path.join("predicate.proto"),
        storage_path.join("service.proto"),
        storage_path.join("source.proto"),
        storage_path.join("storage_common.proto"),
        storage_path.join("test.proto"),
        write_buffer_path.join("write_buffer.proto"),
        write_summary_path.join("write_summary.proto"),
    ];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let mut config = prost_build::Config::new();

    config
        .compile_well_known_types()
        .disable_comments(&[".google"])
        .extern_path(".google.protobuf", "::pbjson_types")
        .bytes(&[
            ".influxdata.iox.management.v1.Chunk.id",
            ".influxdata.iox.management.v1.ClosePartitionChunkRequest.chunk_id",
            ".influxdata.iox.management.v1.CompactChunks.chunks",
            ".influxdata.iox.management.v1.CompactObjectStoreChunks.chunks",
            ".influxdata.iox.management.v1.CompactObjectStoreChunksRequest.chunk_ids",
            ".influxdata.iox.management.v1.DropChunk.chunk_id",
            ".influxdata.iox.management.v1.PersistChunks.chunks",
            ".influxdata.iox.management.v1.WriteChunk.chunk_id",
            ".influxdata.iox.management.v1.LoadReadBufferChunk.chunk_id",
            ".influxdata.iox.management.v1.LoadPartitionChunkRequest.chunk_id",
            ".influxdata.iox.management.v1.UnloadPartitionChunkRequest.chunk_id",
            ".influxdata.iox.preserved_catalog.v1.AddParquet.metadata",
            ".influxdata.iox.preserved_catalog.v1.ChunkAddr.chunk_id",
            ".influxdata.iox.preserved_catalog.v1.IoxMetadata.chunk_id",
            ".influxdata.iox.preserved_catalog.v1.Transaction.previous_uuid",
            ".influxdata.iox.preserved_catalog.v1.Transaction.uuid",
            ".influxdata.iox.write.v1.WriteEntryRequest.entry",
        ])
        .btree_map(&[
            ".influxdata.iox.preserved_catalog.v1.DatabaseCheckpoint.sequencer_numbers",
            ".influxdata.iox.preserved_catalog.v1.PartitionCheckpoint.sequencer_numbers",
        ]);

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .format(true)
        .compile_with_config(config, &proto_files, &[root])?;

    let descriptor_set = std::fs::read(descriptor_path)?;

    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[
            ".influxdata.iox",
            ".influxdata.pbdata",
            ".influxdata.platform.storage",
            ".google.longrunning",
            ".google.rpc",
        ])?;

    Ok(())
}
