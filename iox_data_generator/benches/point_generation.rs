use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use iox_data_generator::{
    agent::Agent,
    specification::{
        AgentAssignmentSpec, AgentSpec, DataSpec, DatabaseWriterSpec, FieldSpec, FieldValueSpec,
        MeasurementSpec,
    },
    tag_set::GeneratedTagSets,
    write::PointsWriterBuilder,
};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

pub fn single_agent(c: &mut Criterion) {
    let spec = DataSpec {
        name: "benchmark".into(),
        values: vec![],
        tag_sets: vec![],
        agents: vec![AgentSpec {
            name: "foo".to_string(),
            measurements: vec![MeasurementSpec {
                name: "measurement-1".into(),
                count: None,
                fields: vec![FieldSpec {
                    name: "field-1".into(),
                    field_value_spec: FieldValueSpec::Bool(true),
                    count: None,
                }],
                tag_set: None,
                tag_pairs: vec![],
            }],
            has_one: vec![],
            tag_pairs: vec![],
        }],
        database_writers: vec![DatabaseWriterSpec {
            database_ratio: Some(1.0),
            database_regex: None,
            agents: vec![AgentAssignmentSpec {
                name: "foo".to_string(),
                count: None,
                sampling_interval: "1s".to_string(),
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
            let r = block_on(iox_data_generator::generate(
                &spec,
                vec!["foo_bar".to_string()],
                &mut points_writer,
                start_datetime,
                end_datetime,
                0,
                false,
                1,
                false,
            ));
            let n_points = r.expect("Could not generate data");
            assert_eq!(n_points, expected_points as usize);
        })
    });
}

pub fn agent_pre_generated(c: &mut Criterion) {
    let spec: DataSpec = toml::from_str(
        r#"
name = "storage_cardinality_example"

# Values are automatically generated before the agents are initialized. They generate tag key/value
# pairs with the name of the value as the tag key and the evaluated template as the value. These
# pairs are Arc wrapped so they can be shared across tagsets and used in the agents as
# pre-generated data.
[[values]]
# the name must not have a . in it, which is used to access children later. Otherwise it's open.
name = "role"
# the template can use a number of helpers to get an id, a random string and the name, see below
# for examples
template = "storage"
# this number of tag pairs will be generated. If this is > 1, the id or a random character string
# should be used in the template to ensure that the tag key/value pairs are unique.
cardinality = 1

[[values]]
name = "url"
template = "http://127.0.0.1:6060/metrics/usage"
cardinality = 1

[[values]]
name = "org_id"
# Fill in the value with the cardinality counter and 15 random alphanumeric characters
template = "{{id}}_{{random 15}}"
cardinality = 1000
has_one = ["env"]

[[values]]
name = "env"
template = "whatever-environment-{{id}}"
cardinality = 10

[[values]]
name = "bucket_id"
# a bucket belongs to an org. With this, you would be able to access the org.id or org.value in the
# template
belongs_to = "org_id"
# each bucket will have a unique id, which is used here to guarantee uniqueness even across orgs.
# We also have a random 15 character alphanumeric sequence to pad out the value length.
template = "{{id}}_{{random 15}}"
# For each org, 3 buckets will be generated
cardinality = 3

[[values]]
name = "partition_id"
template = "{{id}}"
cardinality = 10

# makes a tagset so every bucket appears in every partition. The other tags are descriptive and
# don't increase the cardinality beyond count(bucket) * count(partition). Later this example will
# use the agent and measurement generation to take this base tagset and increase cardinality on a
# per-agent basis.
[[tag_sets]]
name = "bucket_set"
for_each = [
    "role",
    "url",
    "org_id",
    "org_id.env",
    "org_id.bucket_id",
    "partition_id",
]

[[agents]]
name = "foo"

[[agents.measurements]]
name = "storage_usage_bucket_cardinality"
# each sampling will have all the tag sets from this collection in addition to the tags and
# tag_pairs specified
tag_set = "bucket_set"
# for each agent, this specific measurement will be decorated with these additional tags.
tag_pairs = [
    {key = "node_id", template = "{{agent.id}}"},
    {key = "hostname", template = "{{agent.id}}"},
    {key = "host", template = "storage-{{agent.id}}"},
]

[[agents.measurements.fields]]
name = "gauge"
i64_range = [1, 8147240]

[[database_writers]]
agents = [{name = "foo", sampling_interval = "1s", count = 3}]
"#,
    )
    .unwrap();

    let generated_tag_sets = GeneratedTagSets::from_spec(&spec).unwrap();

    let mut points_writer = PointsWriterBuilder::new_no_op(true);

    let start_datetime = Some(0);
    let one_hour_s = 60 * 60;
    let ns_per_second = 1_000_000_000;
    let end_datetime = Some(one_hour_s * ns_per_second);

    let mut agents = Agent::from_spec(
        &spec.agents[0],
        3,
        Duration::from_millis(10),
        start_datetime,
        end_datetime,
        0,
        false,
        &generated_tag_sets,
    )
    .unwrap();
    let agent = agents.first_mut().unwrap();
    let expected_points = 30000;

    let counter = Arc::new(AtomicU64::new(0));
    let request_counter = Arc::new(AtomicU64::new(0));
    let mut group = c.benchmark_group("agent_pre_generated");
    group.measurement_time(std::time::Duration::from_secs(50));
    group.throughput(Throughput::Elements(expected_points));

    group.bench_function("single agent with basic configuration", |b| {
        b.iter(|| {
            agent.reset_current_date_time(0);
            let points_writer =
                Arc::new(points_writer.build_for_agent("foo", "foo", "foo").unwrap());
            let r = block_on(agent.generate_all(
                points_writer,
                1,
                Arc::clone(&counter),
                Arc::clone(&request_counter),
            ));
            let n_points = r.expect("Could not generate data");
            assert_eq!(n_points.row_count, expected_points as usize);
        })
    });
}

#[tokio::main]
async fn block_on<F: std::future::Future>(f: F) -> F::Output {
    f.await
}

criterion_group!(benches, single_agent, agent_pre_generated);
criterion_main!(benches);
