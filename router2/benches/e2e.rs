use std::collections::BTreeSet;
use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use data_types::database_rules::{PartitionTemplate, TemplatePart};
use hyper::{Body, Request};
use router2::dml_handlers::{Partitioner, ShardedWriteBuffer};
use router2::sequencer::Sequencer;
use router2::server::http::HttpDelegate;
use router2::sharder::JumpHash;
use tokio::runtime::Runtime;
use write_buffer::core::WriteBufferWriting;
use write_buffer::mock::{MockBufferForWriting, MockBufferSharedState};

// Init a mock write buffer with the given number of sequencers.
fn init_write_buffer(n_sequencers: u32) -> ShardedWriteBuffer<JumpHash<Arc<Sequencer>>> {
    let time = time::MockProvider::new(time::Time::from_timestamp_millis(668563200000));
    let write_buffer: Arc<dyn WriteBufferWriting> = Arc::new(
        MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_sequencers(
                n_sequencers.try_into().expect("cannot have 0 sequencers"),
            ),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer"),
    );

    let shards: BTreeSet<_> = write_buffer.sequencer_ids();
    ShardedWriteBuffer::new(
        shards
            .into_iter()
            .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer)))
            .map(Arc::new)
            .collect::<JumpHash<_>>(),
    )
}

fn setup_server() -> HttpDelegate<Partitioner<ShardedWriteBuffer<JumpHash<Arc<Sequencer>>>>> {
    let handler_stack = init_write_buffer(1);
    let handler_stack = Partitioner::new(
        handler_stack,
        PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        },
    );
    HttpDelegate::new(1024, handler_stack)
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

fn e2e_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e");

    let delegate = setup_server();
    benchmark_e2e(
        &mut group,
        &delegate,
        "https://bananas.example/api/v2/write?org=bananas&bucket=test",
        "platanos,tag1=A,tag2=B val=42i 123456",
    );

    group.finish();
}

fn benchmark_e2e(
    group: &mut BenchmarkGroup<WallTime>,
    http_delegate: &HttpDelegate<Partitioner<ShardedWriteBuffer<JumpHash<Arc<Sequencer>>>>>,
    uri: &'static str,
    body_str: &'static str,
) {
    group.throughput(Throughput::Bytes(body_str.len() as u64));
    group.bench_function("e2e", |b| {
        b.to_async(runtime()).iter(|| {
            let request = Request::builder()
                .uri(uri)
                .method("POST")
                .body(Body::from(body_str.as_bytes()))
                .unwrap();
            http_delegate.route(request)
        })
    });
}

criterion_group!(benches, e2e_benchmarks);
criterion_main!(benches);
