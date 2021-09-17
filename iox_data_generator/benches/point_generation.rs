use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use iox_data_generator::{
    specification::{AgentSpec, DataSpec, FieldSpec, FieldValueSpec, MeasurementSpec},
    write::PointsWriterBuilder,
    tag_set::GeneratedTagSets,
};
use iox_data_generator::agent::Agent;
use rand::Rng;

pub fn single_agent(c: &mut Criterion) {
    let spec = DataSpec {
        base_seed: Some("faster faster faster".into()),
        include_spec_tag: Some(true),
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
                tag_set: None,
                tag_pairs: vec![],
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
                    false,
                )
            });
            let n_points = r.expect("Could not generate data");
            assert_eq!(n_points, expected_points as usize);
        })
    });
}

pub fn agent_pre_generated(c: &mut Criterion) {
    let spec: DataSpec = toml::from_str(r#"
    name = "benchmark"

    [[values]]
    name = "parent"
    template = "parent{{id}}"
    cardinality = 10
    has_one = ["has_one"]

    [[values]]
    name = "child"
    template = "child{{id}}"
    belongs_to = "parent"
    cardinality = 5

    [[values]]
    name = "has_one"
    template = "has_one{{id}}"
    cardinality = 3

    [[tag_sets]]
    name = "the-set"
    for_each = [
      "parent",
      "parent.has_one",
      "parent.child",
    ]

    [[agents]]
    name = "agent-1"
    sampling_interval = "1s"

    [[agents.measurements]]
    name = "measurement-1"
    tag_set = "the-set"
    tag_pairs = [
        {key = "foo", template = "bar{{measurement.id}}"},
        {key = "hello", template = "world"},
    ]

    [[agents.measurements.fields]]
    name = "field-1"
    bool = true
    "#).unwrap();

    let seed = spec.base_seed.to_owned().unwrap_or_else(|| {
        let mut rng = rand::thread_rng();
        format!("{:04}", rng.gen_range(0..10000))
    });

    let generated_tag_sets = GeneratedTagSets::from_spec(&spec).unwrap();

    let mut points_writer = PointsWriterBuilder::new_no_op(true);

    let start_datetime = Some(0);
    let one_hour_s = 60 * 60;
    let ns_per_second = 1_000_000_000;
    let end_datetime = Some(one_hour_s * ns_per_second);

    let mut agent = Agent::<rand::rngs::SmallRng>::new(
        &spec.agents[0],
        "foo",
        1,
        seed,
        vec![],
        start_datetime,
        end_datetime,
        0,
        false,
        &generated_tag_sets,
    ).unwrap();
    let expected_points = 180050;

    let mut group = c.benchmark_group("agent_pre_generated");
    group.measurement_time(std::time::Duration::from_secs(50));
    group.throughput(Throughput::Elements(expected_points));

    group.bench_function("single agent with basic configuration", |b| {
        b.iter(|| {
            agent.reset_current_date_time(0);
            let points_writer = points_writer.build_for_agent("foo").unwrap();
            let r = block_on({
              agent.generate_all(points_writer, 1)
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

criterion_group!(benches, single_agent, agent_pre_generated);
criterion_main!(benches);
