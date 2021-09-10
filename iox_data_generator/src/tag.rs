//! Generating a set of tag keys and values given a specification

use crate::{
    specification,
    substitution::{pick_from_replacements, Substitute},
    DataGenRng, RandomNumberGenerator,
};
use snafu::{ResultExt, Snafu};
use std::fmt;

/// Tag-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating tags
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when substituting placeholder values in tag keys
    #[snafu(display("Could not create tag key, caused by:\n{}", source))]
    CouldNotCreateTagKey {
        /// Underlying `substitution` module error that caused this problem
        source: crate::substitution::Error,
    },

    /// Error that may happen when substituting placeholder values in tag values
    #[snafu(display(
        "Could not generate tag value for tag `{}`, caused by:\n{}",
        key,
        source
    ))]
    CouldNotGenerateTagValue {
        /// The key of the tag we couldn't create a value for
        key: String,
        /// Underlying `substitution` module error that caused this problem
        source: crate::substitution::Error,
    },
}

/// A generated tag value that will be used in a generated data point.
#[derive(Debug, Clone, PartialEq)]
pub struct Tag {
    /// The key for the tag
    pub key: String,
    /// The value for the tag
    pub value: String,
}

impl Tag {
    /// Create a new tag with the given key and value.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// A set of `count` tags that have the same configuration but different
/// `tag_id`s.
#[derive(Debug)]
pub struct TagGeneratorSet<T: DataGenRng> {
    tags: Vec<TagGenerator<T>>,
}

impl<T: DataGenRng> TagGeneratorSet<T> {
    /// Create a new set of tag generators for a particular agent, measurement,
    /// and tag specification.
    pub fn new(
        agent_id: usize,
        measurement_id: usize,
        spec: &specification::TagSpec,
        parent_seed: impl fmt::Display,
    ) -> Result<Self> {
        let cardinality = spec.cardinality.unwrap_or(1);

        let seed = format!("{}-{}", parent_seed, spec.name);

        let tags = (0..cardinality)
            .map(|cardinality| {
                TagGenerator::new(agent_id, measurement_id, spec, cardinality, &seed)
            })
            .collect::<Result<_>>()?;

        Ok(Self { tags })
    }

    /// Generate one set of tags
    pub fn generate(&mut self) -> Result<Vec<Vec<Tag>>> {
        self.tags.iter_mut().map(TagGenerator::generate).collect()
    }

    /// For tags that shouldn't be included in the multi cartesian product
    /// because they have cardinality 1, this method takes the number of
    /// lines needed, looks at whether this tag should be resampled or not,
    /// and generates the number of lines worth of tags requested.
    pub fn generate_to_zip(&mut self, num_lines: usize) -> Result<Vec<Vec<Tag>>> {
        // This is a hack. A better way would be to have a different type for tags with
        // cardinality = 1, and only that type has this method.
        if self.tags.len() != 1 {
            panic!("generate_to_zip is only for use with cardinality 1")
        }
        (&mut self.tags[0]).generate_to_zip(num_lines)
    }

    /// The cardinality of this tag configuration, used to figure out how many
    /// rows each generation will create in total.
    pub fn tag_cardinality(&self) -> usize {
        self.tags.len()
    }
}

#[derive(Debug)]
struct TagGenerator<T: DataGenRng> {
    agent_id: String,
    measurement_id: String,
    tags: Vec<Tag>,
    cardinality: u32,
    counter: usize,
    current_tick: usize,
    increment_every: Option<usize>,
    rng: RandomNumberGenerator<T>,
    replacements: Vec<specification::Replacement>,
    resample_every_line: bool,
}

impl<T: DataGenRng> TagGenerator<T> {
    fn new(
        agent_id: usize,
        measurement_id: usize,
        spec: &specification::TagSpec,
        cardinality: u32,
        parent_seed: impl fmt::Display,
    ) -> Result<Self> {
        let count = spec.count.unwrap_or(1);
        let increment_every = spec.increment_every;
        let agent_id = agent_id.to_string();
        let measurement_id = measurement_id.to_string();

        let seed = format!("{}-{}-{}", parent_seed, spec.name, cardinality);
        let rng = RandomNumberGenerator::<T>::new(seed);

        let tags = (0..count)
            .map(|tag_id| {
                let key = Substitute::once(
                    &spec.name,
                    &[
                        ("agent_id", &agent_id),
                        ("measurement_id", &measurement_id),
                        ("tag_id", &tag_id.to_string()),
                    ],
                )
                .context(CouldNotCreateTagKey)?;

                Ok(Tag {
                    key,
                    value: spec.value.clone(),
                })
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            agent_id,
            measurement_id,
            tags,
            cardinality,
            counter: 0,
            current_tick: 0,
            increment_every,
            rng,
            replacements: spec.replacements.clone(),
            resample_every_line: spec.resample_every_line,
        })
    }

    fn generate(&mut self) -> Result<Vec<Tag>> {
        let counter = self.increment().to_string();
        let cardinality_string = self.cardinality.to_string();
        let guid = self.rng.guid().to_string();

        let mut substitutions = pick_from_replacements(&mut self.rng, &self.replacements);
        substitutions.insert("agent_id", &self.agent_id);
        substitutions.insert("measurement_id", &self.measurement_id);
        substitutions.insert("counter", &counter);
        substitutions.insert("cardinality", &cardinality_string);
        substitutions.insert("guid", &guid);
        let substitutions: Vec<_> = substitutions.into_iter().collect();

        self.tags
            .iter()
            .map(|tag| {
                let key = tag.key.clone();
                let value = Substitute::once(&tag.value, &substitutions)
                    .context(CouldNotGenerateTagValue { key: &key })?;

                Ok(Tag { key, value })
            })
            .collect()
    }

    // if count and replacements/resampling could never be used on the same tag
    // configuration, then this could return `Result<Vec<Tag>>` I think. This
    // could also possibly return an iterator rather than a Vec; the measurement
    // immediately iterates over it
    fn generate_to_zip(&mut self, num_lines: usize) -> Result<Vec<Vec<Tag>>> {
        if self.resample_every_line {
            Ok((0..num_lines)
                .map(|_| self.generate())
                .collect::<Result<_>>()?)
        } else {
            let tags = self.generate()?;
            Ok(std::iter::repeat(tags).take(num_lines).collect())
        }
    }

    /// Returns the current value and potentially increments the counter for
    /// next time.
    fn increment(&mut self) -> usize {
        let counter = self.counter;

        if let Some(increment) = self.increment_every {
            self.current_tick += 1;
            if self.current_tick >= increment {
                self.counter += 1;
                self.current_tick = 0;
            }
        }

        counter
    }
}

/// Cycles through each value for each agent tag
pub struct AgentTagIterator {
    iters: Vec<Box<dyn Iterator<Item = Tag>>>,
}

impl fmt::Debug for AgentTagIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTagIterator")
            .field("iters", &"(dynamic)")
            .finish()
    }
}

impl AgentTagIterator {
    /// Create a new iterator to manage the cycling
    pub fn new(agent_tags: &[specification::AgentTag]) -> Self {
        Self {
            iters: agent_tags
                .iter()
                .map(|agent_tag| {
                    boxed_cycling_iter(agent_tag.key.clone(), agent_tag.values.clone())
                })
                .collect(),
        }
    }
}

fn boxed_cycling_iter(key: String, values: Vec<String>) -> Box<dyn Iterator<Item = Tag>> {
    Box::new(values.into_iter().cycle().map(move |v| Tag::new(&key, &v)))
}

impl Iterator for AgentTagIterator {
    type Item = Vec<Tag>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iters.iter_mut().flat_map(|i| i.next()).collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{specification::*, ZeroRng, TEST_SEED};

    #[test]
    fn empty_agent_spec_tag_set_always_returns_empty_vec() {
        let agent = AgentSpec {
            tags: vec![],
            ..AgentSpec::default()
        };

        let mut iter = AgentTagIterator::new(&agent.tags);

        assert_eq!(iter.next().unwrap(), vec![]);
    }

    #[test]
    fn agent_spec_tag_set() {
        let tag_alpha = toml::from_str(
            r#"key = "alpha"
               values = ["1", "2", "3"]"#,
        )
        .unwrap();
        let tag_omega = toml::from_str(
            r#"key = "omega"
               values = ["apple", "grape"]"#,
        )
        .unwrap();

        let agent = AgentSpec {
            tags: vec![tag_alpha, tag_omega],
            ..AgentSpec::default()
        };

        let mut iter = AgentTagIterator::new(&agent.tags);

        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "1"), Tag::new("omega", "apple"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "2"), Tag::new("omega", "grape"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "3"), Tag::new("omega", "apple"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "1"), Tag::new("omega", "grape"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "2"), Tag::new("omega", "apple"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "3"), Tag::new("omega", "grape"),]
        );
        assert_eq!(
            iter.next().unwrap(),
            vec![Tag::new("alpha", "1"), Tag::new("omega", "apple"),]
        );
    }

    #[test]
    fn all_the_tag_substitutions_everywhere() -> Result<()> {
        let spec = TagSpec {
            name: "{{agent_id}}x{{measurement_id}}x{{tag_id}}".into(),
            value: "{{agent_id}}v{{measurement_id}}v{{cardinality}}v{{counter}}".into(),
            count: Some(2),
            cardinality: Some(3),
            increment_every: Some(1),
            ..Default::default()
        };

        let mut tg = TagGeneratorSet::<ZeroRng>::new(22, 33, &spec, TEST_SEED)?;

        let tags = tg.generate()?;
        assert_eq!(
            vec![
                vec![
                    Tag::new("22x33x0", "22v33v0v0"),
                    Tag::new("22x33x1", "22v33v0v0"),
                ],
                vec![
                    Tag::new("22x33x0", "22v33v1v0"),
                    Tag::new("22x33x1", "22v33v1v0"),
                ],
                vec![
                    Tag::new("22x33x0", "22v33v2v0"),
                    Tag::new("22x33x1", "22v33v2v0"),
                ],
            ],
            tags
        );

        let tags = tg.generate()?;
        assert_eq!(
            vec![
                vec![
                    Tag::new("22x33x0", "22v33v0v1"),
                    Tag::new("22x33x1", "22v33v0v1"),
                ],
                vec![
                    Tag::new("22x33x0", "22v33v1v1"),
                    Tag::new("22x33x1", "22v33v1v1"),
                ],
                vec![
                    Tag::new("22x33x0", "22v33v2v1"),
                    Tag::new("22x33x1", "22v33v2v1"),
                ],
            ],
            tags
        );

        Ok(())
    }

    #[test]
    fn string_replacements() -> Result<()> {
        let host_tag_spec: specification::TagSpec = toml::from_str(
            r#"name = "host"
               value = "{{host}}"
               replacements = [
                 {replace = "host", with = ["serverA", "serverB", "serverC", "serverD"]},
               ]"#,
        )
        .unwrap();

        let mut tg = TagGeneratorSet::<ZeroRng>::new(22, 33, &host_tag_spec, TEST_SEED)?;

        let tags = tg.generate()?;

        assert_eq!(vec![vec![Tag::new("host", "serverA")]], tags);

        Ok(())
    }

    #[test]
    fn generate_to_zip_with_resample() -> Result<()> {
        let host_tag_spec: specification::TagSpec = toml::from_str(
            r#"name = "host"
               value = "{{host}}"
               replacements = [
                 {replace = "host", with = ["serverA", "serverB", "serverC", "serverD"]},
               ]
               resample_every_line = true
               "#,
        )
        .unwrap();

        let mut tg = TagGeneratorSet::<ZeroRng>::new(22, 33, &host_tag_spec, TEST_SEED)?;

        let tags = tg.generate_to_zip(3)?;

        assert_eq!(
            vec![
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")],
            ],
            tags
        );

        Ok(())
    }

    #[test]
    fn generate_to_zip_without_resample() -> Result<()> {
        let host_tag_spec: specification::TagSpec = toml::from_str(
            r#"name = "host"
               value = "{{host}}"
               replacements = [
                 {replace = "host", with = ["serverA", "serverB", "serverC", "serverD"]},
               ]
               resample_every_line = false
               "#,
        )
        .unwrap();

        let mut tg = TagGeneratorSet::<ZeroRng>::new(22, 33, &host_tag_spec, TEST_SEED)?;

        let tags = tg.generate_to_zip(3)?;

        assert_eq!(
            vec![
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")],
            ],
            tags
        );

        Ok(())
    }

    #[test]
    fn generate_to_zip_with_default_no_resample() -> Result<()> {
        let host_tag_spec: specification::TagSpec = toml::from_str(
            r#"name = "host"
               value = "{{host}}"
               replacements = [
                 {replace = "host", with = ["serverA", "serverB", "serverC", "serverD"]},
               ]"#,
        )
        .unwrap();

        let mut tg = TagGeneratorSet::<ZeroRng>::new(22, 33, &host_tag_spec, TEST_SEED)?;

        let tags = tg.generate_to_zip(3)?;

        assert_eq!(
            vec![
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")],
                vec![Tag::new("host", "serverA")]
            ],
            tags
        );

        Ok(())
    }
}
