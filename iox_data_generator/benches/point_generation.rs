use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use iox_data_generator::{
    specification::{AgentSpec, DataSpec, FieldSpec, FieldValueSpec, MeasurementSpec},
    write::PointsWriterBuilder,
};

pub fn single_agent(c: &mut Criterion) {
    let spec = DataSpec {
        base_seed: Some("faster faster faster".into()),
        name: "benchmark".into(),
        values: vec![],
        tag_sets: vec![],
        agents: vec![AgentSpec {
            name: "agent-1".into(),
            count: None,
            sampling_interval: Some("1s".to_string()),
            name_tag_key: None,
            tags: vec![],
            measurements: vec![MeasurementSpec {
                name: "measurement-1".into(),
                count: None,
                tags: vec![],
                fields: vec![FieldSpec {
                    name: "field-1".into(),
                    field_value_spec: FieldValueSpec::Bool(true),
                    count: None,
                }],
            }],
        }],
    };

    let mut points_writer = PointsWriterBuilder::new_no_op(true);

    let start_datetime = Some(0);
    let one_hour_s = 60 * 60;
    let ns_per_second = 1_000_000_000;
    let end_datetime = Some(one_hour_s * ns_per_second);

    let expected_points = 3601;

    let mut group = c.benchmark_group("single_agent");
    group.throughput(Throughput::Elements(expected_points));

    group.bench_function("single agent with basic configuration", |b| {
        b.iter(|| {
            let r = block_on({
                iox_data_generator::generate::<rand::rngs::SmallRng>(
                    &spec,
                    &mut points_writer,
                    start_datetime,
                    end_datetime,
                    0,
                    false,
                    1,
                )
            });
            let n_points = r.expect("Could not generate data");
            assert_eq!(n_points, expected_points as usize);
        })
    });
}

#[tokio::main]
async fn block_on<F: std::future::Future>(f: F) -> F::Output {
    f.await
}

criterion_group!(benches, single_agent);
criterion_main!(benches);
