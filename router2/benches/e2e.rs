use std::{collections::BTreeSet, iter, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use data_types2::{PartitionTemplate, TemplatePart};
use hyper::{Body, Request};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use router2::{
    dml_handlers::{
        DmlHandlerChainExt, FanOutAdaptor, Partitioner, SchemaValidator, ShardedWriteBuffer,
        WriteSummaryAdapter,
    },
    namespace_cache::{MemoryNamespaceCache, ShardedCache},
    sequencer::Sequencer,
    server::http::HttpDelegate,
    sharder::JumpHash,
};
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
            .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer), &Default::default()))
            .map(Arc::new)
            .collect::<JumpHash<_>>(),
    )
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

    let delegate = {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ns_cache = Arc::new(ShardedCache::new(
            iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        ));

        let write_buffer = init_write_buffer(1);
        let schema_validator =
            SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &*metrics);
        let partitioner = Partitioner::new(PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        });

        let handler_stack = schema_validator.and_then(
            partitioner.and_then(WriteSummaryAdapter::new(FanOutAdaptor::new(write_buffer))),
        );

        HttpDelegate::new(1024, Arc::new(handler_stack), &metrics)
    };

    let body_str = "platanos,tag1=A,tag2=B val=42i 123456";

    group.throughput(Throughput::Bytes(body_str.len() as u64));
    group.bench_function("e2e", |b| {
        b.to_async(runtime()).iter(|| {
            let request = Request::builder()
                .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
                .method("POST")
                .body(Body::from(body_str.as_bytes()))
                .unwrap();
            delegate.route(request)
        })
    });

    group.finish();
}

criterion_group!(benches, e2e_benchmarks);
criterion_main!(benches);
