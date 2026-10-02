//! Benchmarks comparing `RunOptimizedStreamingMergeBuilder` with DataFusion's
//! `StreamingMergeBuilder`.
//!
//! Both merge the same sorted streams, so the only thing that varies is how
//! the merge decides what to emit. The axis that matters is how finely the
//! streams interleave: one globally sorted table is cut into chunks of `rows`
//! and dealt round-robin to the streams, so `rows` is exactly the block length
//! the run-optimized merge should find. At `rows = 1` the streams are perfectly
//! striped and there is no block structure at all; at `rows = BATCH_SIZE` a
//! block is a whole batch.
//!
//! [`Case::verify`] asserts the merge really sees the block length its case
//! names.

// Tests and benchmarks don't use all the crate dependencies and that's all
// right.
#![expect(unused_crate_dependencies)]

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, AsArray, DictionaryArray, Int64Array, RecordBatch, StringArray,
        StringDictionaryBuilder, TimestampNanosecondArray, UInt32Array,
    },
    compute::{SortOptions, take},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit},
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::{
    execution::{
        SendableRecordBatchStream,
        memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool},
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col},
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        sorts::streaming_merge::StreamingMergeBuilder,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, stream};
use iox_query::sorts::run_optimized_merge::{
    RunOptimizedMergeMetrics, RunOptimizedStreamingMergeBuilder,
};
use tokio::runtime::Runtime;

/// The number of rows in a batch, matching DataFusion's default.
const BATCH_SIZE: usize = 8192;

/// The total number of input batches in every case.
const BATCH_COUNT: usize = 64;

/// The number of distinct tag values, i.e. the number of series.
const TAG_VALUES: usize = 16;

/// Set to print each case's metrics before the timing loop.
const METRICS_ENV: &str = "MERGE_BENCH_METRICS";

/// Never hand over to DataFusion, so that the block merge's own curve is what
/// is measured.
const NEVER_HAND_OVER: usize = 0;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "tag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn ordering(schema: &SchemaRef) -> LexOrdering {
    let sort = |name: &str| {
        PhysicalSortExpr::new(
            col(name, schema).unwrap_or_else(|_| panic!("{name} column")),
            SortOptions::default(),
        )
    };
    LexOrdering::new([sort("tag"), sort("time")]).expect("non-empty ordering")
}

/// Every row of the benchmark, in `(tag, time)` order.
///
/// The tag is held undictionaried so that each input batch can be given its own
/// dictionary, as batches read from separate chunks have.
struct SortedTable {
    tags: StringArray,
    times: TimestampNanosecondArray,
    values: Int64Array,
}

impl SortedTable {
    fn new(total: usize) -> Self {
        let per_tag = total / TAG_VALUES;
        Self {
            tags: StringArray::from_iter_values(
                (0..total).map(|p| format!("server{:03}", p / per_tag)),
            ),
            times: TimestampNanosecondArray::from_iter_values(
                (0..total).map(|p| (p % per_tag) as i64),
            ),
            values: Int64Array::from_iter_values((0..total).map(|p| p as i64)),
        }
    }

    /// The rows at `positions`, with a dictionary holding only the tag values
    /// those rows use.
    fn batch(&self, schema: &SchemaRef, positions: &[u32]) -> RecordBatch {
        let indices = UInt32Array::from(positions.to_vec());
        let tags = take(&self.tags, &indices, None).expect("taken tags");
        let tags = tags.as_string::<i32>();
        let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
        for row in 0..tags.len() {
            dictionary.append_value(tags.value(row));
        }
        let tags: DictionaryArray<Int32Type> = dictionary.finish();

        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(tags) as ArrayRef,
                take(&self.times, &indices, None).expect("taken times"),
                take(&self.values, &indices, None).expect("taken values"),
            ],
        )
        .expect("valid batch")
    }
}

/// One set of sorted streams, and the two merges over them.
struct Case {
    runtime: Runtime,
    schema: SchemaRef,
    expr: LexOrdering,
    pool: Arc<dyn MemoryPool>,

    /// The batches of each input stream, held rather than the streams
    /// themselves because a stream can only be consumed once.
    streams: Vec<Vec<RecordBatch>>,

    rows: usize,
}

impl Case {
    /// `stream_count` sorted streams, holding one sorted table dealt out in
    /// chunks of `chunk` rows.
    fn new(stream_count: usize, chunk: usize) -> Self {
        let total = BATCH_COUNT * BATCH_SIZE;
        assert_eq!(0, total % (stream_count * chunk), "chunks must deal evenly");
        assert!(chunk <= BATCH_SIZE, "a chunk must fit in a batch");
        assert_eq!(0, BATCH_SIZE % chunk, "chunks must tile a batch");

        let schema = schema();
        let table = SortedTable::new(total);

        // Chunk `c` belongs to stream `c % stream_count`; each stream's rows,
        // read in order, are sorted.
        let mut positions = vec![Vec::new(); stream_count];
        for c in 0..total / chunk {
            let start = c * chunk;
            positions[c % stream_count].extend((start..start + chunk).map(|p| p as u32));
        }
        let streams = positions
            .iter()
            .map(|p| {
                p.chunks(BATCH_SIZE)
                    .map(|rows| table.batch(&schema, rows))
                    .collect::<Vec<_>>()
            })
            .collect();

        let case = Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("tokio runtime"),
            expr: ordering(&schema),
            schema,
            pool: Arc::new(UnboundedMemoryPool::default()),
            streams,
            rows: total,
        };
        case.verify(chunk);
        case
    }

    fn input(&self) -> Vec<SendableRecordBatchStream> {
        self.streams
            .iter()
            .map(|batches| {
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    stream::iter(batches.clone().into_iter().map(Ok)),
                )) as SendableRecordBatchStream
            })
            .collect()
    }

    fn reservation(&self) -> datafusion::execution::memory_pool::MemoryReservation {
        MemoryConsumer::new("merge benchmark").register(&self.pool)
    }

    /// The run-optimized merge, handing over below `threshold` mean block rows.
    fn run_optimized(&self, threshold: usize, metrics: Option<&ExecutionPlanMetricsSet>) {
        let owned = ExecutionPlanMetricsSet::new();
        let metrics = metrics.unwrap_or(&owned);
        let stream = RunOptimizedStreamingMergeBuilder::new()
            .with_streams(self.input())
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr)
            .with_metrics(BaselineMetrics::new(metrics, 0))
            .with_merge_metrics(RunOptimizedMergeMetrics::new(metrics, 0))
            .with_batch_size(BATCH_SIZE)
            .with_reservation(self.reservation())
            .with_degradation_threshold(threshold)
            .build()
            .expect("valid merge");
        self.drain(stream);
    }

    /// DataFusion's row-at-a-time merge over the same streams.
    fn streaming_merge(&self) {
        let metrics = ExecutionPlanMetricsSet::new();
        let stream = StreamingMergeBuilder::new()
            .with_streams(self.input())
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr)
            .with_metrics(BaselineMetrics::new(&metrics, 0))
            .with_batch_size(BATCH_SIZE)
            .with_reservation(self.reservation())
            .build()
            .expect("valid merge");
        self.drain(stream);
    }

    fn drain(&self, mut stream: SendableRecordBatchStream) {
        self.runtime.block_on(async {
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.expect("merged batch").num_rows();
            }
            assert_eq!(self.rows, rows);
        });
    }

    /// Check the merge really finds blocks of the length this case names, and
    /// report its metrics when asked.
    fn verify(&self, chunk: usize) {
        let metrics = ExecutionPlanMetricsSet::new();
        self.run_optimized(NEVER_HAND_OVER, Some(&metrics));
        let metrics = metrics.clone_inner().aggregate_by_name();

        assert_eq!(
            chunk.min(BATCH_SIZE),
            metric(&metrics, "mean_block_rows"),
            "the merge does not see the block length this case names"
        );
        if std::env::var_os(METRICS_ENV).is_some() {
            eprintln!(
                "\n=== {} streams / {chunk} rows ===\n{}",
                self.streams.len(),
                metrics.sorted_for_display().timestamps_removed(),
            );
        }
    }
}

/// The value of a named metric.
///
/// `MetricsSet::sum_by_name` ignores the custom values the merge reports, so
/// the set is scanned by name instead.
fn metric(metrics: &MetricsSet, name: &str) -> usize {
    metrics
        .iter()
        .find(|m| m.value().name() == name)
        .unwrap_or_else(|| panic!("{name} reported"))
        .value()
        .as_usize()
}

/// Sweep the block length to find where block-at-a-time merging stops paying.
///
/// `RunOptimized` never hands over, so its curve is the block merge alone;
/// `RunOptimized/guarded` is the same merge with the shipped threshold, and
/// should track whichever of the other two is cheaper.
fn crossover(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossover/4_streams");
    group.throughput(Throughput::Elements((BATCH_COUNT * BATCH_SIZE) as u64));

    for rows in [1usize, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 8192] {
        let case = Case::new(4, rows);
        group.bench_with_input(BenchmarkId::new("RunOptimized", rows), &case, |b, case| {
            b.iter(|| case.run_optimized(NEVER_HAND_OVER, None));
        });
        group.bench_with_input(
            BenchmarkId::new("RunOptimized/guarded", rows),
            &case,
            |b, case| {
                b.iter(|| {
                    case.run_optimized(
                        iox_query::sorts::run_optimized_merge::DEFAULT_DEGRADATION_THRESHOLD_ROWS,
                        None,
                    );
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("StreamingMerge", rows),
            &case,
            |b, case| {
                b.iter(|| case.streaming_merge());
            },
        );
    }
    group.finish();
}

/// How both merges scale with the number of streams, at the two extremes of
/// block structure.
fn stream_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("streams");
    group.throughput(Throughput::Elements((BATCH_COUNT * BATCH_SIZE) as u64));

    for streams in [2usize, 4, 8, 16, 32] {
        for rows in [1usize, BATCH_SIZE] {
            let case = Case::new(streams, rows);
            let name = format!("{streams}/{rows}_row_blocks");
            group.bench_with_input(BenchmarkId::new("RunOptimized", &name), &case, |b, case| {
                b.iter(|| case.run_optimized(NEVER_HAND_OVER, None))
            });
            group.bench_with_input(
                BenchmarkId::new("StreamingMerge", &name),
                &case,
                |b, case| b.iter(|| case.streaming_merge()),
            );
        }
    }
    group.finish();
}

/// How the merge scales with the length of the winner's leaf-to-root path,
/// at block lengths where clipping actually has to search.
///
/// The path is `log2(streams)` nodes long and each node may clip the block, so
/// this is the shape that decides how much the per-node work costs.
fn path_length(c: &mut Criterion) {
    let mut group = c.benchmark_group("path_length");
    group.throughput(Throughput::Elements((BATCH_COUNT * BATCH_SIZE) as u64));

    for streams in [4usize, 16, 64] {
        for rows in [16usize, 64, 256] {
            let case = Case::new(streams, rows);
            group.bench_with_input(
                BenchmarkId::new("RunOptimized", format!("{streams}/{rows}")),
                &case,
                |b, case| b.iter(|| case.run_optimized(NEVER_HAND_OVER, None)),
            );
        }
    }
    group.finish();
}

/// How the merge copes with a large number of inputs.
///
/// Each input holds one contiguous span of the table, so blocks are as long as
/// they can be and the tournament does little work. What is left is the cost
/// of the bookkeeping that grows with the input count, such as dropping an
/// input's comparators when its batch is released.
fn many_inputs(c: &mut Criterion) {
    let mut group = c.benchmark_group("many_inputs");
    group.throughput(Throughput::Elements((BATCH_COUNT * BATCH_SIZE) as u64));

    for inputs in [64usize, 256, 1024, 4096] {
        let rows = (BATCH_COUNT * BATCH_SIZE / inputs).min(BATCH_SIZE);
        let case = Case::new(inputs, rows);
        group.bench_with_input(
            BenchmarkId::new("RunOptimized", inputs),
            &case,
            |b, case| b.iter(|| case.run_optimized(NEVER_HAND_OVER, None)),
        );
        group.bench_with_input(
            BenchmarkId::new("StreamingMerge", inputs),
            &case,
            |b, case| b.iter(|| case.streaming_merge()),
        );
    }
    group.finish();
}

criterion_group!(benches, crossover, stream_count, path_length, many_inputs);
criterion_main!(benches);
