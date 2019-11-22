use tower_grpc_build;

fn main() {
    // Build kv
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/delorean/delorean.proto"], &["proto/delorean"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
    println!("cargo:rerun-if-changed=proto/delorean/delorean.proto");
 }