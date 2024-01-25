use std::path::Path;

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId,
    Criterion, Throughput,
};
use data_types::partition_template::TablePartitionTemplateOverride;
use generated_types::influxdata::iox::partition_template::v1::{self as proto, Bucket};
use partition::partition_batch;
use schema::Projection;

fn partitioner_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("partitioner");

    ////////////////////////////////////////////////////////////////////////////
    // A medium batch.
    bench(
        &mut group,
        "tag_hit",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("env".to_string())),
        }],
        "test_fixtures/lineproto/prometheus.lp",
    );

    bench(
        &mut group,
        "tag_miss",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("bananas".to_string())),
        }],
        "test_fixtures/lineproto/prometheus.lp",
    );

    bench(
        &mut group,
        "YYYY-MM-DD strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_string(),
            )),
        }],
        "test_fixtures/lineproto/prometheus.lp",
    );

    bench(
        &mut group,
        "long strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat("%Y-%C-%y-%m-%b-%B-%h-%d-%e-%a-%A-%w-%u-%U-%W-%G-%g-%V-%j-%D-%x-%F-%v-%H-%k-%I-%l-%P-%p-%M-%S-%f-%.f-%.3f-%.6f-%.9f-%3f-%6f-%9f-%R-%T-%X-%r-%Z-%z-%:z-%::z-%:::z-%c-%+-%s-%t-%n-%%".to_string())),
        }],
        "test_fixtures/lineproto/prometheus.lp",
    );

    bench(
        &mut group,
        "hash bucket on tag",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::Bucket(Bucket {
                tag_name: "env".to_string(),
                num_buckets: 100,
            })),
        }],
        "test_fixtures/lineproto/prometheus.lp",
    );

    ////////////////////////////////////////////////////////////////////////////
    // A large batch.
    bench(
        &mut group,
        "tag_hit",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("host".to_string())),
        }],
        "test_fixtures/lineproto/metrics.lp",
    );

    bench(
        &mut group,
        "tag_miss",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("bananas".to_string())),
        }],
        "test_fixtures/lineproto/metrics.lp",
    );

    bench(
        &mut group,
        "YYYY-MM-DD strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_string(),
            )),
        }],
        "test_fixtures/lineproto/metrics.lp",
    );

    bench(
        &mut group,
        "long strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat("%Y-%C-%y-%m-%b-%B-%h-%d-%e-%a-%A-%w-%u-%U-%W-%G-%g-%V-%j-%D-%x-%F-%v-%H-%k-%I-%l-%P-%p-%M-%S-%f-%.f-%.3f-%.6f-%.9f-%3f-%6f-%9f-%R-%T-%X-%r-%Z-%z-%:z-%::z-%:::z-%c-%+-%s-%t-%n-%%".to_string())),
        }],
        "test_fixtures/lineproto/metrics.lp",
    );

    bench(
        &mut group,
        "hash bucket on tag",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::Bucket(Bucket {
                tag_name: "host".to_string(),
                num_buckets: 100,
            })),
        }],
        "test_fixtures/lineproto/metrics.lp",
    );

    ////////////////////////////////////////////////////////////////////////////
    // A small batch.
    bench(
        &mut group,
        "tag_hit",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("location".to_string())),
        }],
        "test_fixtures/lineproto/temperature.lp",
    );

    bench(
        &mut group,
        "tag_miss",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TagValue("bananas".to_string())),
        }],
        "test_fixtures/lineproto/temperature.lp",
    );

    bench(
        &mut group,
        "YYYY-MM-DD strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_string(),
            )),
        }],
        "test_fixtures/lineproto/temperature.lp",
    );

    bench(
        &mut group,
        "long strftime",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat("%Y-%C-%y-%m-%b-%B-%h-%d-%e-%a-%A-%w-%u-%U-%W-%G-%g-%V-%j-%D-%x-%F-%v-%H-%k-%I-%l-%P-%p-%M-%S-%f-%.f-%.3f-%.6f-%.9f-%3f-%6f-%9f-%R-%T-%X-%r-%Z-%z-%:z-%::z-%:::z-%c-%+-%s-%t-%n-%%".to_string())),
        }],
        "test_fixtures/lineproto/temperature.lp",
    );

    bench(
        &mut group,
        "hash bucket on tag",
        vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::Bucket(Bucket {
                tag_name: "location".to_string(),
                num_buckets: 100,
            })),
        }],
        "test_fixtures/lineproto/temperature.lp",
    );

    group.finish();
}

fn bench(
    group: &mut BenchmarkGroup<'_, WallTime>,
    template_name: &str,
    partition_template: Vec<proto::TemplatePart>,
    file_path: &str, // Relative to the crate root
) {
    // Un-normalise the path, adjusting back to the crate root.
    let file_path = format!("{}/../{}", env!("CARGO_MANIFEST_DIR"), file_path);
    let path = Path::new(&file_path);
    let partition_template = TablePartitionTemplateOverride::try_new(
        Some(proto::PartitionTemplate {
            parts: partition_template,
        }),
        &Default::default(),
    )
    .unwrap();

    // Read the benchmark data
    let data = std::fs::read_to_string(path).unwrap();
    let row_count = data.chars().filter(|&v| v == '\n').count();

    // Generate the mutable batch partitioner input
    let mutable_batch_input: Vec<_> = mutable_batch_lp::lines_to_batches(&data, 42)
        .unwrap()
        .into_iter()
        .map(|(_table_name, batch)| batch)
        .collect();

    // Generate the record batch partitioner input
    let record_batch_input: Vec<_> = mutable_batch_input
        .iter()
        .map(|batch| batch.to_arrow(Projection::All).unwrap())
        .collect();

    group.throughput(Throughput::Elements(row_count as _));
    group.bench_function(
        BenchmarkId::new(
            format!("{template_name} (mutable batch)"),
            path.file_name().unwrap().to_str().unwrap(),
        ),
        |b| {
            b.iter_batched(
                || mutable_batch_input.clone(),
                |input| {
                    for batch in input {
                        partition_batch(&batch, &partition_template).for_each(drop);
                    }
                },
                BatchSize::NumIterations(1),
            )
        },
    );
    group.bench_function(
        BenchmarkId::new(
            format!("{template_name} (record batch)"),
            path.file_name().unwrap().to_str().unwrap(),
        ),
        |b| {
            b.iter_batched(
                || record_batch_input.clone(),
                |input| {
                    for batch in input {
                        partition_batch(&batch, &partition_template).for_each(drop);
                    }
                },
                BatchSize::NumIterations(1),
            )
        },
    );
}

criterion_group!(benches, partitioner_benchmarks);
criterion_main!(benches);
