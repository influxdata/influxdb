//! Reading and interpreting data generation specifications.

use humantime::parse_duration;
use regex::Regex;
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{fs, ops::Range, str::FromStr, sync::Arc, time::Duration};
use tracing::warn;

/// Errors that may happen while reading a TOML specification.
#[derive(Snafu, Debug)]
#[allow(missing_docs)]
pub enum Error {
    /// File-related error that may happen while reading a specification
    #[snafu(display(
        r#"Error reading data spec from TOML file at {}: {}"#,
        file_name,
        source
    ))]
    ReadFile {
        file_name: String,
        /// Underlying I/O error that caused this problem
        source: std::io::Error,
    },

    /// TOML parsing error that may happen while interpreting a specification
    #[snafu(display(r#"Error parsing data spec from TOML: {}"#, source))]
    Parse {
        /// Underlying TOML error that caused this problem
        source: toml::de::Error,
    },

    #[snafu(display("Sampling interval must be valid string: {}", source))]
    InvalidSamplingInterval { source: humantime::DurationError },

    #[snafu(display(
        "Agent {} referenced in database_writers, but not present in spec",
        agent
    ))]
    AgentNotFound { agent: String },

    #[snafu(display("database_writers can only use database_ratio or database_regex, not both"))]
    DatabaseWritersConfig,

    #[snafu(display(
        "database_writer missing database_regex. If one uses a regex, all others must also use it"
    ))]
    RegexMissing,

    #[snafu(display("database_writers regex {} failed with error: {}", regex, source))]
    RegexCompile { regex: String, source: regex::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The full specification for the generation of a data set.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DataSpec {
    /// This name can be referenced in handlebars templates as `{{spec_name}}`
    pub name: String,
    /// Specifies values that are generated before agents are created. These values
    /// can be used in tag set specs, which will pre-create tag sets that can then be
    /// used by the agent specs.
    #[serde(default)]
    pub values: Vec<ValuesSpec>,
    /// Specifies collections of tag sets that can be referenced by agents. These
    /// pre-generated tag sets are an efficient way to have many tags without
    /// re-rendering their values on every agent generation. They can also have
    /// dependent values, making it easy to create high cardinality data sets
    /// without running through many handlebar renders while having a well defined
    /// set of tags that appear.
    #[serde(default)]
    pub tag_sets: Vec<TagSetsSpec>,
    /// The specification for the agents that can be used to write data to databases.
    pub agents: Vec<AgentSpec>,
    /// The specification for writing to the provided list of databases.
    pub database_writers: Vec<DatabaseWriterSpec>,
}

impl DataSpec {
    /// Given a filename, read the file and parse the specification.
    pub fn from_file(file_name: &str) -> Result<Self> {
        let spec_toml = fs::read_to_string(file_name).context(ReadFileSnafu { file_name })?;
        Self::from_str(&spec_toml)
    }

    /// Given a collection of database names, assign each a set of agents based on the spec
    pub fn database_split_to_agents<'a>(
        &'a self,
        databases: &'a [String],
    ) -> Result<Vec<DatabaseAgents<'a>>> {
        let mut database_agents = Vec::with_capacity(databases.len());

        let mut start = 0;

        // either all database writers must use regex or none of them can. It's either ratio or
        // regex for assignment
        let use_ratio = self.database_writers[0].database_regex.is_none();
        for b in &self.database_writers {
            if use_ratio && b.database_regex.is_some() {
                return DatabaseWritersConfigSnafu.fail();
            }
        }

        for w in &self.database_writers {
            let agents: Vec<_> = w
                .agents
                .iter()
                .map(|a| {
                    let count = a.count.unwrap_or(1);
                    let sampling_interval = parse_duration(&a.sampling_interval)
                        .context(InvalidSamplingIntervalSnafu)?;
                    let spec = self
                        .agent_by_name(&a.name)
                        .context(AgentNotFoundSnafu { agent: &a.name })?;

                    Ok(AgentAssignment {
                        spec,
                        count,
                        sampling_interval,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let agents = Arc::new(agents);

            let selected_databases = if use_ratio {
                if start >= databases.len() {
                    warn!(
                        "database_writers percentages > 1.0. Writer {:?} and later skipped.",
                        w
                    );
                    break;
                }

                let mut end = (databases.len() as f64 * w.database_ratio.unwrap_or(1.0)).ceil()
                    as usize
                    + start;
                if end > databases.len() {
                    end = databases.len();
                }

                let selected_databases = databases[start..end].iter().collect::<Vec<_>>();
                start = end;
                selected_databases
            } else {
                let p = w.database_regex.as_ref().context(RegexMissingSnafu)?;
                let re = Regex::new(p).context(RegexCompileSnafu { regex: p })?;
                databases
                    .iter()
                    .filter(|name| re.is_match(name))
                    .collect::<Vec<_>>()
            };

            for database in selected_databases {
                database_agents.push(DatabaseAgents {
                    database,
                    agent_assignments: Arc::clone(&agents),
                })
            }
        }

        Ok(database_agents)
    }

    /// Get the agent spec by its name
    pub fn agent_by_name(&self, name: &str) -> Option<&AgentSpec> {
        self.agents.iter().find(|&a| a.name == name)
    }
}

#[derive(Debug)]
/// Assignment info for an agent to a database
pub struct AgentAssignment<'a> {
    /// The agent specification for writing to the assigned database
    pub spec: &'a AgentSpec,
    /// The number of these agents that should be writing to the database
    pub count: usize,
    /// The sampling interval agents will generate data on
    pub sampling_interval: Duration,
}

#[derive(Debug)]
/// Agent assignments mapped to a database
pub struct DatabaseAgents<'a> {
    /// The database data will get written to
    pub database: &'a str,
    /// The agents specifications that will be writing to the database
    pub agent_assignments: Arc<Vec<AgentAssignment<'a>>>,
}

impl FromStr for DataSpec {
    type Err = Error;

    fn from_str(spec_toml: &str) -> std::result::Result<Self, <Self as FromStr>::Err> {
        let spec: Self = toml::from_str(spec_toml).context(ParseSnafu)?;
        Ok(spec)
    }
}

/// The specification of values that can be used to generate tag sets
#[derive(Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct ValuesSpec {
    /// The name of the collection of values
    pub name: String,
    /// If values not specified this handlebars template will be used to create each value in the
    /// collection
    pub template: String,
    /// How many of these values should be generated. If belongs_to is
    /// specified, each parent will have this many of this value. So
    /// the total number of these values generated would be parent.len() * self.cardinality
    pub cardinality: usize,
    /// A collection of strings to other values. Each one of these values will have one
    /// of the referenced has_one. Further, when generating this, the has_one collection
    /// will cycle through so that each successive value will use the next has_one value
    /// for association
    pub has_one: Option<Vec<String>>,
    /// A collection of values that each of these values belongs to. These relationships
    /// can be referenced in the value generation and in the generation of tag sets.
    pub belongs_to: Option<String>,
}

impl ValuesSpec {
    /// returns true if there are other value collections that this values spec must use to
    /// be generated
    pub fn has_dependent_values(&self) -> bool {
        self.has_one.is_some() || self.belongs_to.is_some()
    }
}

/// The specification of tag sets that can be referenced in measurements to pull a pre-generated
/// set of tags in.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct TagSetsSpec {
    /// The name of the tag set spec
    pub name: String,
    /// An array of the `ValuesSpec` to loop through. To reference parent belongs_to or has_one
    /// values, the parent should come first and then the has_one or child next. Each successive
    /// entry in this array is a nested loop. Multiple has_one and a belongs_to on a parent can
    /// be traversed.
    pub for_each: Vec<String>,
}

/// The specification for what should be written to the list of provided databases.
/// Databases will be written to by one or more agents with the given sampling interval and
/// agent count.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DatabaseWriterSpec {
    /// The ratio of databases from the provided list that should use these agents. The
    /// ratios of the collection of database_writer specs should add up to 1.0. If ratio
    /// is not provided it will default to 1.0 (useful for when you specify only a single
    /// database_writer.
    ///
    /// The interval over the provided list of databases is the cumulative sum of the
    /// previous ratios to this ratio. So if you have 10 input databases and 3 database_writers
    /// with ratios (in order) of `[0.2, 0.4, and 0.6]` you would have the input list of
    /// 10 databases split into these three based on their index in the list: `[0, 1]`,
    /// `[2, 5]`, and `[6, 9]`. The first 2 databases, then the next 4, then the remaining 6.
    ///
    /// The list isn't shuffled as ratios are applied.
    pub database_ratio: Option<f64>,
    /// Regex to select databases from the provided list. If regex is used in any one
    /// of the database_writers, it must be used for all of them.
    pub database_regex: Option<String>,
    /// The agents that should be used to write to these databases.
    pub agents: Vec<AgentAssignmentSpec>,
}

/// The specification for the specific configuration of how an agent should write to a database.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AgentAssignmentSpec {
    /// The name of the `AgentSpec` to use
    pub name: String,
    /// The number of these agents that should write to the database
    pub count: Option<usize>,
    /// How frequently each agent will write to the database. This is applicable when using the
    /// --continue flag. Otherwise, if doing historical backfill, timestamps of generated data
    /// will be this far apart and data will be written in as quickly as possible.
    pub sampling_interval: String,
}

/// The specification of the behavior of an agent, the entity responsible for
/// generating a number of data points according to its configuration.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    /// The name of the agent, which can be referenced in templates with `agent.name`.
    pub name: String,
    /// The specifications for the measurements for the agent to generate.
    pub measurements: Vec<MeasurementSpec>,
    /// A collection of strings that reference other `Values` collections. Each agent will have one
    /// of the referenced has_one. Further, when generating this, the has_one collection
    /// will cycle through so that each successive agent will use the next has_one value
    /// for association
    #[serde(default)]
    pub has_one: Vec<String>,
    /// Specification of tag key/value pairs that get generated once and reused for
    /// every sampling. Every measurement (and thus line) will have these tag pairs added onto it.
    /// The template can use `{{agent.id}}` to reference the agent's id and `{{guid}}` or
    /// `{{random N}}` to generate random strings.
    #[serde(default)]
    pub tag_pairs: Vec<TagPairSpec>,
}

/// The specification of how to generate data points for a particular
/// measurement.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct MeasurementSpec {
    /// Name of the measurement. Can be a plain string or a string with
    /// placeholders for:
    ///
    /// - `{{agent.id}}` - the agent ID
    /// - `{{measurement.id}}` - the measurement's ID, which must be used if
    ///   `count` > 1 so that unique measurement names are created
    pub name: String,
    /// The number of measurements with this configuration that should be
    /// created. Default value is 1. If specified, use `{{id}}`
    /// in this measurement's `name` to create unique measurements.
    pub count: Option<usize>,
    /// Specifies a tag set to include in every sampling in addition to tags specified
    pub tag_set: Option<String>,
    /// Specification of tag key/value pairs that get generated once and reused for
    /// every sampling.
    #[serde(default)]
    pub tag_pairs: Vec<TagPairSpec>,
    /// Specification of the fields for this measurement. At least one field is
    /// required.
    pub fields: Vec<FieldSpec>,
}

/// Specification of a tag key/value pair whose template will be evaluated once and
/// the value will be reused across every sampling.
#[derive(Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct TagPairSpec {
    /// The tag key. If `count` is specified, the id of the tag will be automatically
    /// appended to the end of the key to ensure it is unique.
    pub key: String,
    /// The template to generate the tag value
    pub template: String,
    /// If specified, this number of tags will be generated with this template. Each will
    /// have a key of `key#` where # is the number. Useful for creating a degenerate case
    /// of having dozens or hundreds of tags
    pub count: Option<usize>,
    /// If specified, the tag template will be re-evaluated after this many lines have been
    /// generated. This will go across samplings. For example, if you have this set to 3 and
    /// each sample generates two lines, it will get regenerated after the first line in the
    /// second sample. This is useful for simulating things like tracing use cases or ephemeral
    /// identifiers like process or container IDs. The template has access to the normal data
    /// accessible as well as `line_number`.
    pub regenerate_after_lines: Option<usize>,
}

/// The specification of how to generate field keys and values for a particular
/// measurement.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(from = "FieldSpecIntermediate")]
pub struct FieldSpec {
    /// Key/name for this field. Can be a plain string or a string with
    /// placeholders for:
    ///
    /// - `{{agent.id}}` - the agent ID
    /// - `{{measurement.id}}` - the measurement ID
    /// - `{{field.id}}` - the field ID, which must be used if `count` > 1 so
    ///   that unique field names are created
    pub name: String,
    /// Specification for the value for this field.
    pub field_value_spec: FieldValueSpec,
    /// How many fields with this configuration should be created
    pub count: Option<usize>,
}

impl From<FieldSpecIntermediate> for FieldSpec {
    fn from(value: FieldSpecIntermediate) -> Self {
        let field_value_spec = if let Some(b) = value.bool {
            FieldValueSpec::Bool(b)
        } else if let Some((start, end)) = value.i64_range {
            FieldValueSpec::I64 {
                range: (start..end),
                increment: value.increment.unwrap_or(false),
                reset_after: value.reset_after,
            }
        } else if let Some((start, end)) = value.f64_range {
            FieldValueSpec::F64 {
                range: (start..end),
            }
        } else if let Some(pattern) = value.template {
            FieldValueSpec::String {
                pattern,
                replacements: value.replacements,
            }
        } else if let Some(kind) = value.uptime {
            FieldValueSpec::Uptime { kind }
        } else {
            panic!(
                "Can't tell what type of field value you're trying to specify with this \
                configuration: `{value:?}"
            );
        };

        Self {
            name: value.name,
            field_value_spec,
            count: value.count,
        }
    }
}

/// The specification of a field value of a particular type. Instances should be
/// created by converting a `FieldSpecIntermediate`, which more closely matches
/// the TOML structure.
#[derive(Debug, PartialEq)]
pub enum FieldValueSpec {
    /// Configuration of a boolean field.
    Bool(bool),
    /// Configuration of an integer field.
    I64 {
        /// The `Range` in which random integer values will be generated. If the
        /// range only contains one value, all instances of this field
        /// will have the same value.
        range: Range<i64>,
        /// When set to true, after an initial random value in the range is
        /// generated, a random increment in the range will be generated
        /// and added to the initial value. That means the
        /// value for this field will always be increasing. When the value
        /// reaches the max value of i64, the value will wrap around to
        /// the min value of i64 and increment again.
        increment: bool,
        /// If `increment` is true, after this many samples, reset the value to
        /// start the increasing value over. If this is `None`, the
        /// value won't restart until reaching the max value of i64. If
        /// `increment` is false, this has no effect.
        reset_after: Option<usize>,
    },
    /// Configuration of a floating point field.
    F64 {
        /// The `Range` in which random floating point values will be generated.
        /// If start == end, all instances of this field will have the
        /// same value.
        range: Range<f64>,
    },
    /// Configuration of a string field.
    String {
        /// Pattern containing placeholders that specifies how to generate the
        /// string values.
        ///
        /// Valid placeholders include:
        ///
        /// - `{{agent_name}}` - the agent spec's name, with any replacements
        ///   done
        /// - `{{time}}` - the current time in nanoseconds since the epoch.
        ///   TODO: support specifying a strftime
        /// - any other placeholders as specified in `replacements`. If a
        ///   placeholder has no value specified in `replacements`, it will end
        ///   up as-is in the field value.
        pattern: String,
        /// A list of replacement placeholders and the values to replace them
        /// with. The values can optionally have weights associated with
        /// them to change the probabilities that its value
        /// will be used.
        replacements: Vec<Replacement>,
    },
    /// Configuration of a field with the value of the number of seconds the
    /// data generation tool has been running.
    Uptime {
        /// Format of the uptime value in this field
        kind: UptimeKind,
    },
}

/// The kind of field value to create using the data generation tool's uptime
#[derive(Debug, PartialEq, Eq, Copy, Clone, Deserialize)]
pub enum UptimeKind {
    /// Number of seconds since the tool started running as an i64 field
    #[serde(rename = "i64")]
    I64,
    /// Number of seconds since the tool started running, formatted as a string
    /// field containing the value in the format "x day(s), HH:MM"
    #[serde(rename = "telegraf")]
    Telegraf,
}

#[cfg(test)]
impl Default for FieldValueSpec {
    fn default() -> Self {
        Self::Bool(true)
    }
}

/// An intermediate representation of the field specification that more directly
/// corresponds to the way field configurations are expressed in TOML. This
/// structure is transformed into the `FieldValueSpec` enum that ensures the
/// options for the different field value types are mutually exclusive.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct FieldSpecIntermediate {
    /// Key/name for this field. Can be a plain string or a string with
    /// placeholders for:
    ///
    /// - `{{agent_id}}` - the agent ID
    /// - `{{measurement_id}}` - the measurement ID
    /// - `{{field_id}}` - the field ID, which must be used if `count` > 1 so
    ///   that unique field names are created
    name: String,
    /// The number of fields with this configuration that should be created.
    /// Default value is 1. If specified, use `{{field_id}}` in this field's
    /// `name` to create unique fields.
    count: Option<usize>,
    /// Specify `bool` to make a field that has the Boolean type. `true` means
    /// to generate the boolean randomly with equal probability. `false`
    /// means...? Specifying any other optional fields along with this one
    /// is invalid.
    bool: Option<bool>,
    /// Specify `i64_range` to make an integer field. The values will be
    /// randomly generated within the specified range with equal
    /// probability. If the range only contains one element, all occurrences
    /// of this field will have the same value. Can be combined with
    /// `increment`; specifying any other optional fields is invalid.
    i64_range: Option<(i64, i64)>,
    /// Specify `f64_range` to make a floating point field. The values will be
    /// randomly generated within the specified range. If start == end, all
    /// occurrences of this field will have that value.
    /// Can this be combined with `increment`?
    f64_range: Option<(f64, f64)>,
    /// When set to true with an `i64_range` (is this valid with any other
    /// type?), after an initial random value is generated, a random
    /// increment will be generated and added to the initial value. That
    /// means the value for this field will always be increasing. When the value
    /// reaches the end of the range...? The end of the range will be repeated
    /// forever? The series will restart at the start of the range?
    /// Something else? Setting this to `Some(false)` has the same effect as
    /// `None`.
    increment: Option<bool>,
    /// If `increment` is true, after this many samples, reset the value to
    /// start the increasing value over. If this is `None`, the value won't
    /// restart until reaching the max value of i64. If `increment` is
    /// false, this has no effect.
    reset_after: Option<usize>,
    /// Set `pattern` to make a field with the string type. If this doesn't
    /// include any placeholders, all occurrences of this field will have
    /// this value.
    ///
    /// Valid placeholders include:
    ///
    /// - `{{agent.id}}` - the agent spec's name, with any replacements done
    /// - any other placeholders as specified in `replacements`. If a
    ///   placeholder has no value specified in `replacements`, it will end up
    ///   as-is in the field value.
    template: Option<String>,
    /// A list of replacement placeholders and the values to replace them with.
    /// If a placeholder specified here is not used in `pattern`, it will
    /// have no effect. The values may optionally have a probability weight
    /// specified with them; if not specified, the value will have weight 1.
    /// If no weights are specified, the values will be generated with equal
    /// probability.
    #[serde(default)]
    replacements: Vec<Replacement>,
    /// The kind of uptime that should be used for this field. If specified, no
    /// other options are valid. If not specified, this is not an uptime
    /// field.
    uptime: Option<UptimeKind>,
}

/// The specification of what values to substitute in for placeholders specified
/// in `String` field values.
#[derive(Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Replacement {
    /// A placeholder key that can be used in field `pattern`s.
    pub replace: String,
    /// The possible values to use instead of the placeholder key in `pattern`.
    /// Values may optionally have a weight specified. If no weights are
    /// specified, the values will be randomly generated with equal
    /// probability. The weights are passed to [`rand`'s `choose_weighted`
    /// method][choose_weighted] and are a relative likelihood such that the
    /// probability of each item being selected is its weight divided by the sum
    /// of all weights in this group.
    ///
    /// [choose_weighted]: https://docs.rs/rand/0.7.3/rand/seq/trait.SliceRandom.html#tymethod.choose_weighted
    pub with: Vec<ReplacementValue>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged, deny_unknown_fields)]
/// A possible value to use instead of a placeholder key, optionally with an
/// associated weight. If no weight is specified, the weight used will be 1.
pub enum ReplacementValue {
    /// Just a value without a weight
    String(String),
    /// A value with a specified relative likelihood weight that gets passed on
    /// to [`rand`'s `choose_weighted` method][choose_weighted]. The
    /// probability of each item being selected is its weight divided by the
    /// sum of all weights in the `Replacement` group.
    ///
    /// [choose_weighted]: https://docs.rs/rand/0.7.3/rand/seq/trait.SliceRandom.html#tymethod.choose_weighted
    Weighted(String, u32),
}

impl ReplacementValue {
    /// The associated replacement value
    pub fn value(&self) -> &str {
        use ReplacementValue::*;
        match self {
            String(s) => s,
            Weighted(s, ..) => s,
        }
    }

    /// The associated weight value specified; defaults to 1.
    pub fn weight(&self) -> u32 {
        use ReplacementValue::*;
        match self {
            String(..) => 1,
            Weighted(.., w) => *w,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sample_schemas_parse() {
        let schemas: Vec<&str> = vec![
            include_str!("../schemas/storage_cardinality_example.toml"),
            include_str!("../schemas/cap-write.toml"),
            include_str!("../schemas/tracing-spec.toml"),
            include_str!("../schemas/full_example.toml"),
        ];

        for s in schemas {
            if let Err(e) = DataSpec::from_str(s) {
                panic!("error {e:?} on\n{s}")
            }
        }
    }

    #[test]
    fn not_specifying_vectors_gets_default_empty_vector() {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "host"
template = "server"

[[database_writers]]
database_ratio = 1.0
agents = [{name = "foo", sampling_interval = "10s"}]
"#;
        let spec = DataSpec::from_str(toml).unwrap();

        let agent0 = &spec.agents[0];
        assert!(agent0.tag_pairs.is_empty());

        let agent0_measurements = &agent0.measurements;
        let a0m0 = &agent0_measurements[0];
        assert!(a0m0.tag_pairs.is_empty());

        let a0m0_fields = &a0m0.fields;
        let a0m0f0 = &a0m0_fields[0];
        let field_spec = &a0m0f0.field_value_spec;

        assert!(
            matches!(
                field_spec,
                FieldValueSpec::String { replacements, .. } if replacements.is_empty()
            ),
            "expected a String field with empty replacements; was {field_spec:?}"
        );
    }

    #[test]
    fn split_databases_by_writer_spec_ratio() {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"
[[agents.measurements]]
name = "cpu"
[[agents.measurements.fields]]
name = "host"
template = "server"

[[agents]]
name = "bar"
[[agents.measurements]]
name = "whatevs"
[[agents.measurements.fields]]
name = "val"
i64_range = [0, 10]

[[database_writers]]
database_ratio = 0.6
agents = [{name = "foo", sampling_interval = "10s"}]

[[database_writers]]
database_ratio = 0.4
agents = [{name = "bar", sampling_interval = "1m", count = 3}]
"#;
        let spec = DataSpec::from_str(toml).unwrap();
        let databases = vec!["a_1".to_string(), "a_2".to_string(), "b_1".to_string()];

        let database_agents = spec.database_split_to_agents(&databases).unwrap();

        let b = &database_agents[0];
        assert_eq!(b.database, &databases[0]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");

        let b = &database_agents[1];
        assert_eq!(b.database, &databases[1]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");

        let b = &database_agents[2];
        assert_eq!(b.database, &databases[2]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(60)
        );
        assert_eq!(b.agent_assignments[0].count, 3);
        assert_eq!(b.agent_assignments[0].spec.name, "bar");
    }

    #[test]
    fn split_databases_by_writer_spec_regex() {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"
[[agents.measurements]]
name = "cpu"
[[agents.measurements.fields]]
name = "host"
template = "server"

[[agents]]
name = "bar"
[[agents.measurements]]
name = "whatevs"
[[agents.measurements.fields]]
name = "val"
i64_range = [0, 10]

[[database_writers]]
database_regex = "foo.*"
agents = [{name = "foo", sampling_interval = "10s"}]

[[database_writers]]
database_regex = ".*_bar"
agents = [{name = "bar", sampling_interval = "1m", count = 3}]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let databases = vec![
            "foo_1".to_string(),
            "foo_2".to_string(),
            "asdf_bar".to_string(),
        ];

        let database_agents = spec.database_split_to_agents(&databases).unwrap();

        let b = &database_agents[0];
        assert_eq!(b.database, &databases[0]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");

        let b = &database_agents[1];
        assert_eq!(b.database, &databases[1]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");

        let b = &database_agents[2];
        assert_eq!(b.database, &databases[2]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(60)
        );
        assert_eq!(b.agent_assignments[0].count, 3);
        assert_eq!(b.agent_assignments[0].spec.name, "bar");
    }

    #[test]
    fn split_databases_by_writer_regex_and_ratio_error() {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"
[[agents.measurements]]
name = "cpu"
[[agents.measurements.fields]]
name = "host"
template = "server"

[[agents]]
name = "bar"
[[agents.measurements]]
name = "whatevs"
[[agents.measurements.fields]]
name = "val"
i64_range = [0, 10]

[[database_writers]]
database_ratio = 0.8
agents = [{name = "foo", sampling_interval = "10s"}]

[[database_writers]]
database_regex = "foo.*"
agents = [{name = "bar", sampling_interval = "1m", count = 3}]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let databases = vec!["a_1".to_string(), "a_2".to_string(), "b_1".to_string()];

        let database_agents = spec.database_split_to_agents(&databases);
        assert!(matches!(
            database_agents.unwrap_err(),
            Error::DatabaseWritersConfig
        ));
    }

    #[test]
    fn split_databases_by_writer_ratio_defaults() {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"
[[agents.measurements]]
name = "cpu"
[[agents.measurements.fields]]
name = "host"
template = "server"

[[database_writers]]
agents = [{name = "foo", sampling_interval = "10s"}]
"#;

        let spec = DataSpec::from_str(toml).unwrap();
        let databases = vec!["a_1".to_string(), "a_2".to_string()];

        let database_agents = spec.database_split_to_agents(&databases).unwrap();

        let b = &database_agents[0];
        assert_eq!(b.database, &databases[0]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");

        let b = &database_agents[1];
        assert_eq!(b.database, &databases[1]);
        assert_eq!(
            b.agent_assignments[0].sampling_interval,
            Duration::from_secs(10)
        );
        assert_eq!(b.agent_assignments[0].count, 1);
        assert_eq!(b.agent_assignments[0].spec.name, "foo");
    }
}
