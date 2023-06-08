use std::{path::Path, sync::Arc};

use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId,
    Criterion, Throughput,
};
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    NamespaceId, NamespaceName, NamespaceSchema, TableId,
};
use generated_types::influxdata::iox::partition_template::v1 as proto;
use hashbrown::HashMap;
use once_cell::sync::Lazy;
use router::dml_handlers::{DmlHandler, Partitioner};
use tokio::runtime::Runtime;

static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

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

    group.finish();
}

fn bench(
    group: &mut BenchmarkGroup<WallTime>,
    template_name: &str,
    partition_template: Vec<proto::TemplatePart>,
    file_path: &str, // Relative to the crate root
) {
    // Un-normalise the path, adjusting back to the crate root.
    let file_path = format!("{}/../{}", env!("CARGO_MANIFEST_DIR"), file_path);
    let path = Path::new(&file_path);
    let partition_template =
        NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
            parts: partition_template,
        })
        .unwrap();

    let schema = Arc::new(NamespaceSchema {
        id: NamespaceId::new(42),
        tables: Default::default(),
        max_columns_per_table: 1000,
        max_tables: 1000,
        retention_period_ns: None,
        partition_template: partition_template.clone(),
    });

    // Read the benchmark data
    let data = std::fs::read_to_string(path).unwrap();
    let row_count = data.chars().filter(|&v| v == '\n').count();

    // Initialise the partitioner
    let partitioner = Partitioner::default();

    // Generate the partitioner input
    let input = mutable_batch_lp::lines_to_batches(&data, 42)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, (name, payload))| {
            (
                TableId::new(i as _),
                (
                    name,
                    TablePartitionTemplateOverride::try_new(None, &partition_template).unwrap(),
                    payload,
                ),
            )
        })
        .collect::<HashMap<_, _>>();

    group.throughput(Throughput::Elements(row_count as _));
    group.bench_function(
        BenchmarkId::new(template_name, path.file_name().unwrap().to_str().unwrap()),
        |b| {
            b.to_async(runtime()).iter_batched(
                || input.clone(),
                |input| partitioner.write(&NAMESPACE, Arc::clone(&schema), input, None),
                BatchSize::NumIterations(1),
            )
        },
    );
}

criterion_group!(benches, partitioner_benchmarks);
criterion_main!(benches);
