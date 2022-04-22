//! Tests for influxrpc / Storage gRPC endpoints

mod data;
mod dump;
mod exprs;
mod metadata;
mod read_filter;

use std::sync::Arc;

use generated_types::ReadSource;
use prost::Message;
use test_helpers_end_to_end_ng::FCustom;
use test_helpers_end_to_end_ng::{maybe_skip_integration, MiniCluster, Step, StepTest};

use self::data::DataGenerator;

/// Runs the specified custom function on a cluster with no data
pub(crate) async fn run_no_data_test(custom: FCustom) {
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_standard(database_url).await;

    StepTest::new(&mut cluster, vec![Step::Custom(custom)])
        .run()
        .await
}

/// Run the custom test function with a cluster that has had the data from `generator` loaded
pub(crate) async fn run_data_test(generator: Arc<DataGenerator>, custom: FCustom) {
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_standard(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(generator.line_protocol().to_string()),
            Step::WaitForReadable,
            Step::Custom(custom),
        ],
    )
    .run()
    .await
}

/// Creates the appropriate `Any` protobuf magic for a read source
/// with a specified org and bucket name
pub(crate) fn make_read_source(
    cluster: &MiniCluster,
) -> Option<generated_types::google::protobuf::Any> {
    let org_id = cluster.org_id();
    let bucket_id = cluster.bucket_id();
    let org_id = u64::from_str_radix(org_id, 16).unwrap();
    let bucket_id = u64::from_str_radix(bucket_id, 16).unwrap();

    let partition_id = u64::from(u32::MAX);
    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };

    // Do the magic to-any conversion
    let mut d = bytes::BytesMut::new();
    read_source.encode(&mut d).unwrap();
    let read_source = generated_types::google::protobuf::Any {
        type_url: "/TODO".to_string(),
        value: d.freeze(),
    };

    Some(read_source)
}
