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
/// - `com.github.influxdata.idpe.storage.read.rs`
/// - `influxdata.iox.catalog.v1.rs`
/// - `influxdata.iox.management.v1.rs`
/// - `influxdata.iox.write.v1.rs`
/// - `influxdata.platform.storage.rs`
fn generate_grpc_types(root: &Path) -> Result<()> {
    let catalog_path = root.join("influxdata/iox/catalog/v1");
    let idpe_path = root.join("com/github/influxdata/idpe/storage/read");
    let management_path = root.join("influxdata/iox/management/v1");
    let storage_path = root.join("influxdata/platform/storage");
    let write_path = root.join("influxdata/iox/write/v1");

    let proto_files = vec![
        catalog_path.join("catalog.proto"),
        catalog_path.join("parquet_metadata.proto"),
        catalog_path.join("predicate.proto"),
        idpe_path.join("source.proto"),
        management_path.join("chunk.proto"),
        management_path.join("database_rules.proto"),
        management_path.join("jobs.proto"),
        management_path.join("partition.proto"),
        management_path.join("partition_template.proto"),
        management_path.join("server_config.proto"),
        management_path.join("service.proto"),
        management_path.join("shard.proto"),
        management_path.join("write_buffer.proto"),
        root.join("google/longrunning/operations.proto"),
        root.join("google/rpc/error_details.proto"),
        root.join("google/rpc/status.proto"),
        root.join("grpc/health/v1/service.proto"),
        root.join("influxdata/pbdata/v1/influxdb_pb_data_protocol.proto"),
        storage_path.join("predicate.proto"),
        storage_path.join("service.proto"),
        storage_path.join("storage_common.proto"),
        storage_path.join("storage_common_idpe.proto"),
        storage_path.join("test.proto"),
        write_path.join("service.proto"),
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
            ".influxdata.iox.catalog.v1.AddParquet.metadata",
            ".influxdata.iox.catalog.v1.ChunkAddr.chunk_id",
            ".influxdata.iox.catalog.v1.IoxMetadata.chunk_id",
            ".influxdata.iox.catalog.v1.Transaction.previous_uuid",
            ".influxdata.iox.catalog.v1.Transaction.uuid",
            ".influxdata.iox.management.v1.Chunk.id",
            ".influxdata.iox.management.v1.ClosePartitionChunkRequest.chunk_id",
            ".influxdata.iox.management.v1.CompactChunks.chunks",
            ".influxdata.iox.management.v1.DropChunk.chunk_id",
            ".influxdata.iox.management.v1.PersistChunks.chunks",
            ".influxdata.iox.management.v1.WriteChunk.chunk_id",
            ".influxdata.iox.management.v1.UnloadPartitionChunkRequest.chunk_id",
            ".influxdata.iox.write.v1.WriteEntryRequest.entry",
        ])
        .btree_map(&[
            ".influxdata.iox.catalog.v1.DatabaseCheckpoint.sequencer_numbers",
            ".influxdata.iox.catalog.v1.PartitionCheckpoint.sequencer_numbers",
        ]);

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .format(true)
        .compile_with_config(config, &proto_files, &[root.into()])?;

    let descriptor_set = std::fs::read(descriptor_path)?;

    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".influxdata", ".google.longrunning", ".google.rpc"])?;

    Ok(())
}
