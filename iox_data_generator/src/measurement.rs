//! Generating a set of points for one measurement configuration

use crate::{
    field::FieldGeneratorSet,
    specification,
    substitution::Substitute,
    tag::{Tag, TagGeneratorSet},
    DataGenRng, RandomNumberGenerator,
};

use influxdb2_client::models::DataPoint;
use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use std::fmt;

/// Measurement-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating measurements
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when building a data point with the Influx DB
    /// client
    #[snafu(display(
        "Could not build data point for measurement `{}` with Influx Client, caused by:\n{}",
        name,
        source
    ))]
    InfluxDataPointError {
        /// The name of the relevant measurement
        name: String,
        /// Underlying Influx Client error that caused this problem
        source: influxdb2_client::models::data_point::DataPointError,
    },

    /// Error that may happen when substituting placeholder values
    #[snafu(display("Could not create measurement name, caused by:\n{}", source))]
    CouldNotCreateMeasurementName {
        /// Underlying `substitution` module error that caused this problem
        source: crate::substitution::Error,
    },

    /// Error that may happen when creating tag generator sets
    #[snafu(display(
        "Could not create tag generator sets for measurement `{}`, caused by:\n{}",
        name,
        source
    ))]
    CouldNotCreateTagGeneratorSets {
        /// The name of the relevant measurement
        name: String,
        /// Underlying `tag` module error that caused this problem
        source: crate::tag::Error,
    },

    /// Error that may happen when creating field generator sets
    #[snafu(display(
        "Could not create field generator sets for measurement `{}`, caused by:\n{}",
        name,
        source
    ))]
    CouldNotCreateFieldGeneratorSets {
        /// The name of the relevant measurement
        name: String,
        /// Underlying `field` module error that caused this problem
        source: crate::field::Error,
    },

    /// Error that may happen when generating a particular set of tags
    #[snafu(display(
        "Could not generate tags for measurement `{}`, caused by:\n{}",
        name,
        source
    ))]
    CouldNotGenerateTags {
        /// The name of the relevant measurement
        name: String,
        /// Underlying `tag` module error that caused this problem
        source: crate::tag::Error,
    },
}

/// A set of `count` measurements that have the same configuration but different
/// `measurement_id`s. The `generate` method on a `MeasurementGeneratorSet` will
/// always return `count` points.
#[derive(Debug)]
pub struct MeasurementGeneratorSet<T: DataGenRng> {
    measurement_generators: Vec<MeasurementGenerator<T>>,
}

impl<T: DataGenRng> MeasurementGeneratorSet<T> {
    /// Create a new set of measurement generators for a particular agent and
    /// measurement specification.
    pub fn new(
        agent_name: &str,
        agent_id: usize,
        spec: &specification::MeasurementSpec,
        parent_seed: impl fmt::Display,
        static_tags: &[Tag],
        execution_start_time: i64,
    ) -> Result<Self> {
        let count = spec.count.unwrap_or(1);

        let measurement_generators = (0..count)
            .map(|measurement_id| {
                MeasurementGenerator::new(
                    agent_name,
                    agent_id,
                    measurement_id,
                    spec,
                    &parent_seed,
                    static_tags,
                    execution_start_time,
                )
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            measurement_generators,
        })
    }

    /// Create one set of points
    pub fn generate(&mut self, timestamp: i64) -> Result<Vec<DataPoint>> {
        let generate_results = self
            .measurement_generators
            .iter_mut()
            .map(|mg| mg.generate(timestamp));

        itertools::process_results(generate_results, |points| points.flatten().collect())
    }
}

/// Generate measurements
#[derive(Debug)]
pub struct MeasurementGenerator<T: DataGenRng> {
    #[allow(dead_code)]
    rng: RandomNumberGenerator<T>,
    name: String,
    static_tags: Vec<Tag>,
    tag_generator_sets: Vec<TagGeneratorSet<T>>,
    total_tag_cardinality: usize,
    field_generator_sets: Vec<FieldGeneratorSet>,
    count: usize,
}

impl<T: DataGenRng> MeasurementGenerator<T> {
    /// Create a new way to generate measurements from a specification
    pub fn new(
        agent_name: impl Into<String>,
        agent_id: usize,
        measurement_id: usize,
        spec: &specification::MeasurementSpec,
        parent_seed: impl fmt::Display,
        static_tags: &[Tag],
        execution_start_time: i64,
    ) -> Result<Self> {
        let agent_name = agent_name.into();
        let spec_name = Substitute::once(
            &spec.name,
            &[
                ("agent_id", &agent_id.to_string()),
                ("agent_name", &agent_name),
                ("measurement_id", &measurement_id.to_string()),
            ],
        )
        .context(CouldNotCreateMeasurementName)?;

        let seed = format!("{}-{}", parent_seed, spec_name);
        let rng = RandomNumberGenerator::<T>::new(seed);

        let tag_generator_sets: Vec<TagGeneratorSet<T>> = spec
            .tags
            .iter()
            .map(|tag_spec| TagGeneratorSet::new(agent_id, measurement_id, tag_spec, &rng.seed))
            .collect::<crate::tag::Result<_>>()
            .context(CouldNotCreateTagGeneratorSets { name: &spec_name })?;

        let total_tag_cardinality = tag_generator_sets
            .iter()
            .map(|tgs| tgs.tag_cardinality())
            .product();

        let field_generator_sets = spec
            .fields
            .iter()
            .map(|field_spec| {
                FieldGeneratorSet::new::<T>(
                    &agent_name,
                    agent_id,
                    measurement_id,
                    field_spec,
                    &rng.seed,
                    execution_start_time,
                )
            })
            .collect::<crate::field::Result<_>>()
            .context(CouldNotCreateFieldGeneratorSets { name: &spec_name })?;

        Ok(Self {
            rng,
            name: spec_name,
            static_tags: static_tags.to_vec(),
            tag_generator_sets,
            total_tag_cardinality,
            field_generator_sets,
            count: spec.count.unwrap_or(1),
        })
    }
}

impl<T: DataGenRng> MeasurementGenerator<T> {
    fn generate(&mut self, timestamp: i64) -> Result<Vec<DataPoint>> {
        // Split out the tags that we want all combinations of. Perhaps these should be
        // a different type?
        let mut tags_with_cardinality: Vec<_> = itertools::process_results(
            self.tag_generator_sets
                .iter_mut()
                .filter(|tgs| tgs.tag_cardinality() > 1)
                .map(TagGeneratorSet::generate),
            |tags| {
                tags.multi_cartesian_product()
                    .map(|tag_set| tag_set.into_iter().flatten().collect())
                    .collect()
            },
        )
        .context(CouldNotGenerateTags { name: &self.name })?;

        // Ensure we generate something even when there are no tags.
        if tags_with_cardinality.is_empty() {
            tags_with_cardinality.push(Vec::new());
        }

        let total_tag_cardinality = self.total_tag_cardinality;
        assert_eq!(tags_with_cardinality.len(), total_tag_cardinality);

        // Split out the tags that we don't want to include when we're generating all
        // possible combinations above. Perhaps these should be a different
        // type? Leaving the type annotation here because it's terrible and
        // confusing otherwise.
        //
        // This type is made up of:
        //
        // - `Vec<Tag>` comes from one call to `TagGenerator::generate`. Tag
        //   configurations with a `count` value > 1 generate multiple tags with
        //   different keys but the same value for each generation. The length of this
        //   vector is the tag configuration's `count`.
        // - `Vec<Vec<Tag>>` comes from one call to `TagGenerator::generate_to_zip` and
        //   is a list of either cloned or resampled tags from this TagGenerator. The
        //   length of this vector is `total_tag_cardinality`.
        // - `Vec<Vec<Vec<Tag>>>` comes from collecting all these lists from each
        //   `TagGeneratorSet` that has a cardinality of 1 (the default). Each
        //   `TagGeneratorSet` corresponds to one tag configuration.
        let tags_without_cardinality_columns = self
            .tag_generator_sets
            .iter_mut()
            .filter(|tgs| tgs.tag_cardinality() == 1)
            .map(|tgs| tgs.generate_to_zip(total_tag_cardinality).unwrap());

        // This is doing a zip over an arbitrary number of iterators... itertools has
        // something that produces tuples but I want it to produce Vectors
        let mut tags_without_cardinality_column_iters: Vec<_> = tags_without_cardinality_columns
            .map(|column| column.into_iter())
            .collect();

        // For each group of tags that will become one row,
        for v in &mut tags_with_cardinality {
            // Get the rest of the tags that belong with this row that were either cloned or
            // resampled according to their configuration
            let tag_row: Vec<Vec<Tag>> = tags_without_cardinality_column_iters
                .iter_mut()
                .map(|column_iter| {
                    column_iter.next().expect(
                        "Should have generated `total_tag_cardinality` items, \
                         which should match the length of `tags_with_cardinality`",
                    )
                })
                .collect();
            // If count can't be combined with replacements, this `for` loop wouldn't be
            // needed
            for mut tags in tag_row {
                v.append(&mut tags);
            }
        }

        tags_with_cardinality
            .iter()
            .map(|tags| self.one(&tags[..], timestamp))
            .collect()
    }

    fn one(&mut self, tags: &[Tag], timestamp: i64) -> Result<DataPoint> {
        let mut point = DataPoint::builder(&self.name);

        point = self
            .static_tags
            .iter()
            .fold(point, |point, tag| point.tag(&tag.key, &tag.value));

        point = tags
            .iter()
            .fold(point, |point, tag| point.tag(&tag.key, &tag.value));

        for fgs in &mut self.field_generator_sets {
            for field in fgs.generate(timestamp) {
                point = point.field(&field.key, field.value);
            }
        }

        point = point.timestamp(timestamp);

        point
            .build()
            .context(InfluxDataPointError { name: &self.name })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{specification::*, DynamicRng, ZeroRng, TEST_SEED};
    use influxdb2_client::models::WriteDataPoint;
    use std::str;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl<T: DataGenRng> MeasurementGenerator<T> {
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
        let fake_now = 1234;

        let measurement_spec = MeasurementSpec {
            name: "cpu".into(),
            count: None,
            tags: vec![],
            fields: vec![FieldSpec {
                name: "response_time".into(),
                field_value_spec: FieldValueSpec::I64 {
                    range: 0..60,
                    increment: false,
                    reset_after: None,
                },
                count: None,
            }],
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol,
            format!("cpu response_time=0i {}\n", fake_now)
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_stable_rngs() -> Result {
        let fake_now = 5678;

        // This is the same as the previous test but with an additional field.
        let measurement_spec = MeasurementSpec {
            name: "cpu".into(),
            count: Some(2),
            tags: vec![],
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
        };

        let mut measurement_generator = MeasurementGenerator::<DynamicRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
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
    fn generate_measurement_always_including_some_tags() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "cpu".into(),
            count: None,
            tags: vec![],
            fields: vec![FieldSpec {
                name: "response_time".into(),
                field_value_spec: FieldValueSpec::I64 {
                    range: 0..60,
                    increment: false,
                    reset_after: None,
                },
                count: None,
            }],
        };

        let always_tags = vec![Tag::new("my_tag", "my_val")];

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &always_tags,
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol,
            format!("cpu,my_tag=my_val response_time=0i {}\n", fake_now),
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_basic_tags() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".into(),
            tags: vec![
                TagSpec {
                    name: "tag_name".into(),
                    value: "tag_value".into(),
                    ..Default::default()
                },
                TagSpec {
                    name: "some_name".into(),
                    value: "some_value".into(),
                    ..Default::default()
                },
            ],
            fields: vec![FieldSpec {
                name: "field_name".into(),
                ..FieldSpec::default()
            }],
            ..Default::default()
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol,
            format!(
                "measurement,some_name=some_value,tag_name=tag_value field_name=f {}\n",
                fake_now
            )
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_tags_with_count() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".into(),
            tags: vec![TagSpec {
                name: "{{agent_id}}--{{measurement_id}}--tag_name--{{tag_id}}".into(),
                value: "tag_value".into(),
                count: Some(2),
                ..Default::default()
            }],
            fields: vec![FieldSpec {
                name: "field_name".into(),
                ..FieldSpec::default()
            }],
            ..Default::default()
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            42,
            99,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol,
            format!("measurement,42--99--tag_name--0=tag_value,42--99--tag_name--1=tag_value field_name=f {}\n", fake_now),
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_tags_with_cardinality() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".into(),
            tags: vec![TagSpec {
                name: "tag_name".into(),
                value: "tag_value--{{cardinality}}".into(),
                cardinality: Some(2),
                ..Default::default()
            }],
            fields: vec![FieldSpec {
                name: "field_name".into(),
                ..FieldSpec::default()
            }],
            ..Default::default()
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_strings(fake_now)?;

        assert_eq!(
            line_protocol[0],
            format!(
                "measurement,tag_name=tag_value--0 field_name=f {}\n",
                fake_now
            )
        );
        assert_eq!(
            line_protocol[1],
            format!(
                "measurement,tag_name=tag_value--1 field_name=f {}\n",
                fake_now
            )
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_tags_with_multiple_cardinality() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".into(),
            tags: vec![
                TagSpec {
                    name: "alpha".into(),
                    value: "alpha--{{cardinality}}".into(),
                    cardinality: Some(2),
                    ..Default::default()
                },
                TagSpec {
                    name: "beta".into(),
                    value: "beta--{{cardinality}}".into(),
                    cardinality: Some(2),
                    ..Default::default()
                },
            ],
            fields: vec![FieldSpec {
                name: "field_name".into(),
                ..FieldSpec::default()
            }],
            ..Default::default()
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol = measurement_generator.generate_strings(fake_now)?;

        assert_eq!(
            line_protocol[0],
            format!(
                "measurement,alpha=alpha--0,beta=beta--0 field_name=f {}\n",
                fake_now
            )
        );
        assert_eq!(
            line_protocol[1],
            format!(
                "measurement,alpha=alpha--0,beta=beta--1 field_name=f {}\n",
                fake_now
            )
        );
        assert_eq!(
            line_protocol[2],
            format!(
                "measurement,alpha=alpha--1,beta=beta--0 field_name=f {}\n",
                fake_now
            )
        );
        assert_eq!(
            line_protocol[3],
            format!(
                "measurement,alpha=alpha--1,beta=beta--1 field_name=f {}\n",
                fake_now
            )
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_tags_with_increment_every() -> Result {
        let fake_now = 678;

        let measurement_spec = MeasurementSpec {
            name: "measurement".into(),
            tags: vec![TagSpec {
                name: "tag_name".into(),
                value: "tag_value--{{counter}}".into(),
                increment_every: Some(2),
                ..Default::default()
            }],
            fields: vec![FieldSpec {
                name: "field_name".into(),
                ..FieldSpec::default()
            }],
            ..Default::default()
        };

        let mut measurement_generator = MeasurementGenerator::<ZeroRng>::new(
            "agent_name",
            0,
            0,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let line_protocol_1 = measurement_generator.generate_string(fake_now)?;
        let line_protocol_2 = measurement_generator.generate_string(fake_now)?;
        let line_protocol_3 = measurement_generator.generate_string(fake_now)?;

        assert_eq!(
            line_protocol_1,
            format!(
                "measurement,tag_name=tag_value--0 field_name=f {}\n",
                fake_now,
            ),
        );
        assert_eq!(
            line_protocol_2,
            format!(
                "measurement,tag_name=tag_value--0 field_name=f {}\n",
                fake_now,
            ),
        );
        assert_eq!(
            line_protocol_3,
            format!(
                "measurement,tag_name=tag_value--1 field_name=f {}\n",
                fake_now,
            ),
        );

        Ok(())
    }

    #[test]
    fn generate_measurement_with_replacement() -> Result {
        let fake_now = 91011;

        let measurement_spec = MeasurementSpec {
            name: "measurement-{{agent_id}}-{{measurement_id}}".into(),
            count: Some(2),
            tags: vec![],
            fields: vec![FieldSpec {
                name: "field-{{agent_id}}-{{measurement_id}}-{{field_id}}".into(),
                field_value_spec: FieldValueSpec::I64 {
                    range: 0..60,
                    increment: false,
                    reset_after: None,
                },
                count: Some(2),
            }],
        };

        let mut measurement_generator_set = MeasurementGeneratorSet::<ZeroRng>::new(
            "agent_name",
            42,
            &measurement_spec,
            TEST_SEED,
            &[],
            fake_now,
        )
        .unwrap();

        let points = measurement_generator_set.generate(fake_now).unwrap();
        let mut v = Vec::new();
        for point in points {
            point.write_data_point_to(&mut v)?;
        }
        let line_protocol = str::from_utf8(&v)?;

        assert_eq!(
            line_protocol,
            format!(
                "measurement-42-0 field-42-0-0=0i,field-42-0-1=0i {}
measurement-42-1 field-42-1-0=0i,field-42-1-1=0i {}
",
                fake_now, fake_now
            )
        );

        Ok(())
    }

    #[test]
    fn guid_and_guid_with_cardinality() -> Result<()> {
        let fake_now = 678;

        let spec: specification::MeasurementSpec = toml::from_str(
            r#"
            name = "traces"

            [[tags]]
            name = "trace_id"
            value = "value-{{guid}}"

            [[tags]]
            name = "span_id"
            value = "value-{{guid}}"
            cardinality = 2

            [[fields]]
            name = "timing"
            i64_range = [5, 100]"#,
        )
        .unwrap();

        let mut measurement_generator = MeasurementGenerator::<DynamicRng>::new(
            "agent_name",
            0,
            0,
            &spec,
            TEST_SEED,
            &[],
            fake_now,
        )?;

        let line_protocol = measurement_generator.generate_strings(fake_now)?;

        let mut trace_ids = extract_tag_values("trace_id", &line_protocol);
        trace_ids.sort_unstable();
        trace_ids.dedup();
        // Both lines should have the same trace ID
        assert_eq!(trace_ids.len(), 1);

        let mut span_ids = extract_tag_values("span_id", &line_protocol);
        span_ids.sort_unstable();
        span_ids.dedup();
        // Each line should have a different span ID
        assert_eq!(span_ids.len(), 2);

        let next_line_protocol = measurement_generator.generate_strings(fake_now)?;

        let mut next_trace_ids = extract_tag_values("trace_id", &next_line_protocol);
        next_trace_ids.sort_unstable();
        next_trace_ids.dedup();
        // Both lines should have the same trace ID
        assert_eq!(next_trace_ids.len(), 1);

        // On each generation, there should be a new trace id
        assert_ne!(trace_ids, next_trace_ids);

        let mut next_span_ids = extract_tag_values("span_id", &next_line_protocol);
        next_span_ids.sort_unstable();
        next_span_ids.dedup();
        // Each line should have a different span ID
        assert_eq!(next_span_ids.len(), 2);

        // On each generation, there should be new span IDs too
        assert_ne!(span_ids, next_span_ids);

        Ok(())
    }

    #[test]
    fn tag_replacements_with_resampling_true() -> Result<()> {
        resampling_test("resample_every_line = true", true)
    }

    #[test]
    fn tag_replacements_with_resampling_false() -> Result<()> {
        resampling_test("resample_every_line = false", false)
    }

    #[test]
    fn tag_replacements_with_default_resampling_false() -> Result<()> {
        resampling_test("", false)
    }

    fn resampling_test(resampling_toml: &str, expect_different: bool) -> Result<()> {
        let fake_now = 678;

        let spec: specification::MeasurementSpec = toml::from_str(&format!(
            r#"
            name = "resampling"

            [[tags]]
            name = "tag-1"
            value = "value-{{{{cardinality}}}}"
            cardinality = 10

            [[tags]]
            name = "host"
            value = "{{{{host}}}}"
            replacements = [
              {{replace = "host", with = ["serverA", "serverB", "serverC", "serverD"]}},
            ]
            {}

            [[fields]]
            name = "timing"
            i64_range = [5, 100]"#,
            resampling_toml
        ))
        .unwrap();

        let mut measurement_generator = MeasurementGenerator::<DynamicRng>::new(
            "agent_name",
            0,
            0,
            &spec,
            TEST_SEED,
            &[],
            fake_now,
        )?;

        let lines = measurement_generator.generate_strings(fake_now)?;
        let mut host_values = extract_tag_values("host", &lines);
        host_values.sort_unstable();
        host_values.dedup();

        if expect_different {
            assert!(host_values.len() > 1);
        } else {
            assert_eq!(host_values.len(), 1);
        }

        Ok(())
    }

    // Hacktacular extracting of values from line protocol without pulling in another crate
    fn extract_tag_values<'a>(tag_name: &str, lines: &'a [String]) -> Vec<&'a str> {
        lines
            .iter()
            .map(|line| {
                let before_space = line.splitn(2, ' ').next().unwrap();
                let prefix = format!(",{}=", tag_name);
                let after = before_space.rsplitn(2, &prefix).next().unwrap();
                after.splitn(2, ',').next().unwrap()
            })
            .collect()
    }

    fn extract_field_values<'a>(field_name: &str, lines: &'a [String]) -> Vec<&'a str> {
        lines
            .iter()
            .map(|line| {
                let mut split = line.splitn(2, ' ');
                split.next();
                let after_space = split.next().unwrap();
                let prefix = format!(",{}=", field_name);
                let after = after_space.rsplitn(2, &prefix).next().unwrap();
                after.splitn(2, ',').next().unwrap()
            })
            .collect()
    }
}
