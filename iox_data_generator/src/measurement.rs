//! Generating a set of points for one measurement configuration

#![allow(clippy::result_large_err)]

use crate::{
    field::FieldGeneratorImpl,
    specification, substitution,
    tag_pair::TagPair,
    tag_set::{GeneratedTagSets, TagSet},
};
use influxdb2_client::models::WriteDataPoint;
use serde_json::json;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

/// Measurement-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating measurements
#[derive(Snafu, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display(
        "Could not build data point for measurement `{}` with Influx Client, caused by:\n{}",
        name,
        source
    ))]
    InfluxDataPointError {
        name: String,
        source: influxdb2_client::models::data_point::DataPointError,
    },

    #[snafu(display("Could not create measurement name, caused by:\n{}", source))]
    CouldNotCreateMeasurementName { source: crate::substitution::Error },

    #[snafu(display(
        "Could not create field generator sets for measurement `{}`, caused by:\n{}",
        name,
        source
    ))]
    CouldNotCreateFieldGeneratorSets {
        name: String,
        source: crate::field::Error,
    },

    #[snafu(display(
        "Tag set {} referenced not found for measurement {}",
        tag_set,
        measurement
    ))]
    GeneratedTagSetNotFound {
        tag_set: String,
        measurement: String,
    },

    #[snafu(display("Could not compile template `{}`, caused by:\n{}", template, source))]
    CantCompileTemplate {
        source: handlebars::TemplateError,
        template: String,
    },

    #[snafu(display("Could not render template `{}`, caused by:\n{}", template, source))]
    CantRenderTemplate {
        source: handlebars::RenderError,
        template: String,
    },

    #[snafu(display("Error creating measurement tag pairs: {}", source))]
    CouldNotCreateMeasurementTagPairs { source: crate::tag_pair::Error },
}

/// Generate measurements
#[derive(Debug)]
pub struct MeasurementGenerator {
    measurement: Arc<Mutex<Measurement>>,
}

impl MeasurementGenerator {
    /// Create the count specified number of measurement generators from
    /// the passed `MeasurementSpec`
    pub fn from_spec(
        agent_id: usize,
        spec: &specification::MeasurementSpec,
        execution_start_time: i64,
        generated_tag_sets: &GeneratedTagSets,
        agent_tag_pairs: &[Arc<TagPair>],
    ) -> Result<Vec<Self>> {
        let count = spec.count.unwrap_or(1) + 1;

        (1..count)
            .map(|measurement_id| {
                Self::new(
                    agent_id,
                    measurement_id,
                    spec,
                    execution_start_time,
                    generated_tag_sets,
                    agent_tag_pairs,
                )
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Create a new way to generate measurements from a specification
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        agent_id: usize,
        measurement_id: usize,
        spec: &specification::MeasurementSpec,
        execution_start_time: i64,
        generated_tag_sets: &GeneratedTagSets,
        agent_tag_pairs: &[Arc<TagPair>],
    ) -> Result<Self> {
        let measurement_name = substitution::render_once(
            "measurement",
            &spec.name,
            &json!({
                "agent": {"id": agent_id},
                "measurement": {"id": measurement_id},
            }),
        )
        .context(CouldNotCreateMeasurementNameSnafu)?;

        let fields = spec
            .fields
            .iter()
            .map(|field_spec| {
                let data = json!({
                    "agent": {"id": agent_id},
                    "measurement": {"id": measurement_id, "name": &measurement_name},
                });

                FieldGeneratorImpl::from_spec(field_spec, data, execution_start_time)
            })
            .collect::<crate::field::Result<Vec<_>>>()
            .context(CouldNotCreateFieldGeneratorSetsSnafu {
                name: &measurement_name,
            })?
            .into_iter()
            .flatten()
            .collect();

        // generate the tag pairs
        let template_data = json!({
            "agent": {"id": agent_id},
            "measurement": {"id": measurement_id, "name": &measurement_name},
        });

        let mut tag_pairs = TagPair::pairs_from_specs(&spec.tag_pairs, template_data)
            .context(CouldNotCreateMeasurementTagPairsSnafu)?;
        for t in agent_tag_pairs {
            tag_pairs.push(Arc::clone(t));
        }

        let generated_tag_sets = match &spec.tag_set {
            Some(t) => Arc::clone(generated_tag_sets.sets_for(t).context(
                GeneratedTagSetNotFoundSnafu {
                    tag_set: t,
                    measurement: &measurement_name,
                },
            )?),
            // if there's no generated tag set, just have an empty set as a single row so
            // it can be used to generate the single line that will come out of each generation
            // for this measurement.
            None => Arc::new(vec![TagSet { tags: vec![] }]),
        };

        // I have this gnarly tag ordering construction so that I can keep the pre-generated
        // tag sets in their existing vecs without moving them around so that I can have
        // many thousands of agents and measurements that use the same tagset without blowing
        // up the number of vectors and memory I consume.
        let mut tag_ordering: Vec<_> = tag_pairs
            .iter()
            .enumerate()
            .map(|(i, p)| (p.key(), TagOrdering::Pair(i)))
            .chain(
                generated_tag_sets[0]
                    .tags
                    .iter()
                    .enumerate()
                    .map(|(i, p)| (p.key.to_string(), TagOrdering::Generated(i))),
            )
            .collect();
        tag_ordering.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let tag_ordering: Vec<_> = tag_ordering.into_iter().map(|(_, o)| o).collect();

        Ok(Self {
            measurement: Arc::new(Mutex::new(Measurement {
                name: measurement_name,
                tag_pairs,
                generated_tag_sets,
                tag_ordering,
                fields,
            })),
        })
    }

    /// Create a line iterator to generate lines for a single sampling
    pub fn generate(&mut self, timestamp: i64) -> Result<MeasurementLineIterator> {
        Ok(MeasurementLineIterator {
            measurement: Arc::clone(&self.measurement),
            index: 0,
            timestamp,
        })
    }
}

/// Details for the measurement to be generated. Can generate many lines
/// for each sampling.
#[derive(Debug)]
pub struct Measurement {
    name: String,
    tag_pairs: Vec<Arc<TagPair>>,
    generated_tag_sets: Arc<Vec<TagSet>>,
    tag_ordering: Vec<TagOrdering>,
    fields: Vec<FieldGeneratorImpl>,
}

impl Measurement {
    /// The number of lines that will be generated for each sampling of this measurement.
    pub fn line_count(&self) -> usize {
        self.generated_tag_sets.len()
    }

    /// Write the specified line as line protocol to the passed in writer.
    pub fn write_index_to<W: std::io::Write>(
        &mut self,
        index: usize,
        timestamp: i64,
        mut w: W,
    ) -> std::io::Result<()> {
        write!(w, "{}", self.name)?;
        let row_tags = &self.generated_tag_sets[index].tags;
        for t in &self.tag_ordering {
            match t {
                TagOrdering::Generated(index) => {
                    let t = &row_tags[*index];
                    write!(w, ",{}={}", t.key, t.value)?;
                }
                TagOrdering::Pair(index) => {
                    let t = &self.tag_pairs[*index].as_ref();
                    match t {
                        TagPair::Static(t) => write!(w, ",{}={}", t.key, t.value)?,
                        TagPair::Regenerating(t) => {
                            let mut t = t.lock().expect("mutex poisoned");
                            let p = t.tag_pair();
                            write!(w, ",{}={}", p.key, p.value)?
                        }
                    }
                }
            }
        }

        for (i, field) in self.fields.iter_mut().enumerate() {
            let d = if i == 0 { b" " } else { b"," };
            w.write_all(d)?;

            match field {
                FieldGeneratorImpl::Bool(f) => {
                    let v = f.generate_value();
                    write!(w, "{}={}", f.name, if v { "t" } else { "f" })?;
                }
                FieldGeneratorImpl::I64(f) => {
                    let v = f.generate_value();
                    write!(w, "{}={}i", f.name, v)?;
                }
                FieldGeneratorImpl::F64(f) => {
                    let v = f.generate_value();
                    write!(w, "{}={}", f.name, v)?;
                }
                FieldGeneratorImpl::String(f) => {
                    let v = f.generate_value(timestamp);
                    write!(w, "{}=\"{}\"", f.name, v)?;
                }
                FieldGeneratorImpl::Uptime(f) => match f.kind {
                    specification::UptimeKind::I64 => {
                        let v = f.generate_value();
                        write!(w, "{}={}i", f.name, v)?;
                    }
                    specification::UptimeKind::Telegraf => {
                        let v = f.generate_value_as_string();
                        write!(w, "{}=\"{}\"", f.name, v)?;
                    }
                },
            }
        }

        writeln!(w, " {timestamp}")
    }
}

#[derive(Debug)]
enum TagOrdering {
    Pair(usize),
    Generated(usize),
}

/// Iterator to generate the lines for a given measurement
#[derive(Debug)]
pub struct MeasurementLineIterator {
    measurement: Arc<Mutex<Measurement>>,
    index: usize,
    timestamp: i64,
}

impl MeasurementLineIterator {
    /// Number of lines that will be generated for this measurement
    pub fn line_count(&self) -> usize {
        let m = self.measurement.lock().expect("mutex poinsoned");
        m.line_count()
    }
}

impl Iterator for MeasurementLineIterator {
    type Item = LineToGenerate;

    /// Get the details for the next `LineToGenerate`
    fn next(&mut self) -> Option<Self::Item> {
        let m = self.measurement.lock().expect("mutex poinsoned");

        if self.index >= m.line_count() {
            None
        } else {
            let n = Some(LineToGenerate {
                measurement: Arc::clone(&self.measurement),
                index: self.index,
                timestamp: self.timestamp,
            });
            self.index += 1;
            n
        }
    }
}

/// A pointer to the line to be generated. Will be evaluated when asked to write.
#[derive(Debug)]
pub struct LineToGenerate {
    /// The measurement state to be used to generate the line
    pub measurement: Arc<Mutex<Measurement>>,
    /// The index into the generated tag pairs of the line we're generating
    pub index: usize,
    /// The timestamp of the line that we're generating
    pub timestamp: i64,
}

impl WriteDataPoint for LineToGenerate {
    /// Generate the data and write the line to the passed in writer.
    fn write_data_point_to<W>(&self, w: W) -> std::io::Result<()>
    where
        W: std::io::Write,
    {
        let mut m = self.measurement.lock().expect("mutex poisoned");
        m.write_index_to(self.index, self.timestamp, w)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::specification::*;
    use influxdb2_client::models::WriteDataPoint;
    use std::str;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl MeasurementGenerator {
        fn generate_string(&mut self, timestamp: i64) -> Result<String> {
            self.generate_strings(timestamp)
                .map(|mut strings| strings.swap_remove(0))
        }

        fn generate_strings(&mut self, timestamp: i64) -> Result<Vec<String>> {
            let points = self.generate(timestamp)?;
            points
                .into_iter()
                .map(|point| {
                    let mut v = Vec::new();
                    point.write_data_point_to(&mut v)?;
                    Ok(String::from_utf8(v)?)
                })
                .collect()
        }
    }

    #[test]
    fn generate_measurement() -> Result {
        let fake_now = 5678;

        // This is the same as the previous test but with an additional field.
        let measurement_spec = MeasurementSpec {
            name: "cpu".into(),
            count: Some(2),
            fields: vec![
                FieldSpec {
                    name: "load".into(),
                    field_value_spec: FieldValueSpec::F64 { range: 0.0..100.0 },
                    count: None,
                },
                FieldSpec {
                    name: "response_time".into(),
                    field_value_spec: FieldValueSpec::I64 {
                        range: 0..60_000,
                        increment: false,
                        reset_after: None,
                    },
                    count: None,
                },
            ],
            tag_set: None,
            tag_pairs: vec![],
        };

        let generated_tag_sets = GeneratedTagSets::default();

        let mut measurement_generator =
            MeasurementGenerator::new(0, 0, &measurement_spec, fake_now, &generated_tag_sets, &[])
                .unwrap();

        let line_protocol = vec![measurement_generator.generate_string(fake_now)?];
        let response_times = extract_field_values("response_time", &line_protocol);

        let next_line_protocol = vec![measurement_generator.generate_string(fake_now + 1)?];
        let next_response_times = extract_field_values("response_time", &next_line_protocol);

        // Each line should have a different response time unless we get really, really unlucky
        assert_ne!(response_times, next_response_times);

        Ok(())
    }

    #[test]
    fn generate_measurement_with_basic_tags() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".to_string(),
            count: None,
            tag_set: None,
            tag_pairs: vec![
                TagPairSpec {
                    key: "some_name".to_string(),
                    template: "some_value".to_string(),
                    count: None,
                    regenerate_after_lines: None,
                },
                TagPairSpec {
                    key: "tag_name".to_string(),
                    template: "tag_value".to_string(),
                    count: None,
                    regenerate_after_lines: None,
                },
            ],
            fields: vec![FieldSpec {
                name: "field_name".to_string(),
                field_value_spec: FieldValueSpec::I64 {
                    range: 1..1,
                    increment: false,
                    reset_after: None,
                },
                count: None,
            }],
        };
        let generated_tag_sets = GeneratedTagSets::default();

        let mut measurement_generator =
            MeasurementGenerator::new(0, 0, &measurement_spec, fake_now, &generated_tag_sets, &[])
                .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol,
            format!(
                "measurement,some_name=some_value,tag_name=tag_value field_name=1i {fake_now}\n"
            )
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_tags_with_count() {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".to_string(),
            count: None,
            tag_set: None,
            tag_pairs: vec![TagPairSpec {
                key: "some_name".to_string(),
                template: "some_value {{id}}".to_string(),
                count: Some(2),
                regenerate_after_lines: None,
            }],
            fields: vec![FieldSpec {
                name: "field_name".to_string(),
                field_value_spec: FieldValueSpec::I64 {
                    range: 1..1,
                    increment: false,
                    reset_after: None,
                },
                count: None,
            }],
        };
        let generated_tag_sets = GeneratedTagSets::default();

        let mut measurement_generator =
            MeasurementGenerator::new(0, 0, &measurement_spec, fake_now, &generated_tag_sets, &[])
                .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now).unwrap();

        assert_eq!(
            line_protocol,
            format!(
                "measurement,some_name=some_value 1,some_name2=some_value 2 field_name=1i {fake_now}\n"
            )
        );
    }

    #[test]
    fn regenerating_after_lines() {
        let data_spec: specification::DataSpec = toml::from_str(
            r#"
            name = "ex"

            [[values]]
            name = "foo"
            template = "{{id}}"
            cardinality = 3

            [[tag_sets]]
            name = "foo_set"
            for_each = ["foo"]

            [[agents]]
            name = "foo"

            [[agents.measurements]]
            name = "m1"
            tag_set = "foo_set"
            tag_pairs = [{key = "reg", template = "data-{{line_number}}", regenerate_after_lines = 2}]

            [[agents.measurements.fields]]
            name = "val"
            i64_range = [3, 3]

            [[database_writers]]
            agents = [{name = "foo", sampling_interval = "10s"}]"#,
        )
            .unwrap();

        let fake_now = 678;

        let generated_tag_sets = GeneratedTagSets::from_spec(&data_spec).unwrap();

        let mut measurement_generator = MeasurementGenerator::new(
            42,
            1,
            &data_spec.agents[0].measurements[0],
            fake_now,
            &generated_tag_sets,
            &[],
        )
        .unwrap();

        let points = measurement_generator.generate(fake_now).unwrap();
        let mut v = Vec::new();
        for point in points {
            point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = str::from_utf8(&v).unwrap();

        assert_eq!(
            line_protocol,
            format!(
                "m1,foo=1,reg=data-1 val=3i {fake_now}\nm1,foo=2,reg=data-1 val=3i {fake_now}\nm1,foo=3,reg=data-3 val=3i {fake_now}\n"
            )
        );
    }

    #[test]
    fn tag_set_and_tag_pairs() {
        let data_spec: specification::DataSpec = toml::from_str(
            r#"
            name = "ex"

            [[values]]
            name = "foo"
            template = "foo-{{id}}"
            cardinality = 2

            [[tag_sets]]
            name = "foo_set"
            for_each = ["foo"]

            [[agents]]
            name = "foo"

            [[agents.measurements]]
            name = "m1"
            tag_set = "foo_set"
            tag_pairs = [{key = "hello", template = "world{{measurement.id}}"}]

            [[agents.measurements.fields]]
            name = "val"
            i64_range = [3, 3]

            [[database_writers]]
            database_ratio = 1.0
            agents = [{name = "foo", sampling_interval = "10s"}]"#,
        )
        .unwrap();

        let fake_now = 678;

        let generated_tag_sets = GeneratedTagSets::from_spec(&data_spec).unwrap();

        let mut measurement_generator = MeasurementGenerator::new(
            42,
            1,
            &data_spec.agents[0].measurements[0],
            fake_now,
            &generated_tag_sets,
            &[],
        )
        .unwrap();

        let points = measurement_generator.generate(fake_now).unwrap();
        let mut v = Vec::new();
        for point in points {
            point.write_data_point_to(&mut v).unwrap();
        }
        let line_protocol = str::from_utf8(&v).unwrap();

        assert_eq!(
            line_protocol,
            format!(
                "m1,foo=foo-1,hello=world1 val=3i {fake_now}\nm1,foo=foo-2,hello=world1 val=3i {fake_now}\n"
            )
        );
    }

    fn extract_field_values<'a>(field_name: &str, lines: &'a [String]) -> Vec<&'a str> {
        lines
            .iter()
            .map(|line| {
                let mut split = line.splitn(2, ' ');
                split.next();
                let after_space = split.next().unwrap();
                let prefix = format!(",{field_name}=");
                let after = after_space.rsplit_once(&prefix).unwrap().1;
                after.split_once(',').map_or(after, |x| x.0)
            })
            .collect()
    }
}
