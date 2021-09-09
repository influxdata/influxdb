//! Reading and interpreting data generation specifications.

use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::{fs, ops::Range, str::FromStr};

/// Errors that may happen while reading a TOML specification.
#[derive(Snafu, Debug)]
pub enum Error {
    /// File-related error that may happen while reading a specification
    #[snafu(display(r#"Error reading data spec from TOML file: {}"#, source))]
    ReadFile {
        /// Underlying I/O error that caused this problem
        source: std::io::Error,
    },

    /// TOML parsing error that may happen while interpreting a specification
    #[snafu(display(r#"Error parsing data spec from TOML: {}"#, source))]
    Parse {
        /// Underlying TOML error that caused this problem
        source: toml::de::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The full specification for the generation of a data set.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DataSpec {
    /// Every point generated from this configuration will contain a tag
    /// `data_spec=[this value]` to identify what generated that data. This
    /// name can also be used in string replacements by using the
    /// placeholder `{{data_spec}}`.
    pub name: String,
    /// A string to be used as the seed to the random number generators.
    ///
    /// When specified, this is used as a base seed propagated through all
    /// measurements, tags, and fields, which will each have their own
    /// random number generator seeded by this seed plus their name. This
    /// has the effect of keeping each value sequence generated per measurement,
    /// tag, or field stable even if the configurations in other parts of the
    /// schema are changed. That is, if you have a field named `temp` and on
    /// the first run with base seed `foo` generates the values `[10, 50,
    /// 72, 3]`, and then you add another field named `weight` to the schema
    /// and run with base seed `foo` again, the values generated for `temp`
    /// should again be `[10, 50, 72, 3]`. This enables incremental
    /// development of a schema without churn, if that is undesired.
    ///
    /// When this is not specified, the base seed will be randomly generated. It
    /// will be printed to stdout so that the value used can be specified in
    /// future configurations if reproducing a particular set of sequences
    /// is desired.
    pub base_seed: Option<String>,
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
    /// The specification for the data-generating agents in this data set.
    pub agents: Vec<AgentSpec>,
}

impl DataSpec {
    /// Given a filename, read the file and parse the specification.
    pub fn from_file(file_name: &str) -> Result<Self> {
        let spec_toml = fs::read_to_string(file_name).context(ReadFile)?;
        Self::from_str(&spec_toml)
    }
}

impl FromStr for DataSpec {
    type Err = Error;

    fn from_str(spec_toml: &str) -> std::result::Result<Self, <Self as FromStr>::Err> {
        let spec: Self = toml::from_str(spec_toml).context(Parse)?;
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
    /// The handlebars template to create each value in the collection
    pub value: String,
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
    /// An array of the values to loop through. To reference parent belongs_to or has_one
    /// values, the parent should come first and then the has_one or child next. See the
    /// doc for `ForEachValueTag` and its `value` for more detail.
    pub for_each: Vec<String>,
}

/// The specification of the behavior of an agent, the entity responsible for
/// generating a number of data points according to its configuration.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    /// Used as the value for the `name` tag if `name_tag_key` is `Some`; has no
    /// effect if `name_tag_key` is not specified.
    ///
    /// Can be a plain string or a string with placeholders for:
    ///
    /// - `{{agent_id}}` - the agent ID
    pub name: String,
    /// Specifies the number of agents that should be created with this spec.
    /// Default value is 1.
    pub count: Option<usize>,
    /// How often this agent should generate samples, in a duration string. If
    /// not specified, this agent will only generate one sample.
    pub sampling_interval: Option<String>,
    /// If specified, every measurement generated by this agent will include a
    /// tag with this `String` as its key, and with the `AgentSpec`'s `name`
    /// as the value (with any substitutions in the `name` performed)
    pub name_tag_key: Option<String>,
    /// If specified, the values of the tags will be cycled through per `Agent`
    /// instance such that all measurements generated by that agent will
    /// contain tags with the specified name and that agent's `name` field
    /// (with replacements made) as the value.
    #[serde(default)]
    pub tags: Vec<AgentTag>,
    /// The specifications for the measurements for the agent to generate.
    pub measurements: Vec<MeasurementSpec>,
}

/// Tags that are associated to all measurements that a particular agent
/// generates. The values are rotated through so that each agent gets one of the
/// specified values for this key.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct AgentTag {
    /// The tag key to use when adding this tag to all measurements for an agent
    pub key: String,
    /// The values to cycle through for each agent for this tag key
    pub values: Vec<String>,
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
    /// - `{{agent_id}}` - the agent ID
    /// - `{{measurement_id}}` - the measurement's ID, which must be used if
    ///   `count` > 1 so that unique measurement names are created
    pub name: String,
    /// The number of measurements with this configuration that should be
    /// created. Default value is 1. If specified, use `{{measurement_id}}`
    /// in this measurement's `name` to create unique measurements.
    pub count: Option<usize>,
    /// Specification of the tags for this measurement
    #[serde(default)]
    pub tags: Vec<TagSpec>,
    /// Specification of the fields for this measurement. At least one field is
    /// required.
    pub fields: Vec<FieldSpec>,
}

/// The specification of how to generate tag keys and values for a particular
/// measurement.
#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(Default))]
#[serde(deny_unknown_fields)]
pub struct TagSpec {
    /// Key/name for this tag. Can be a plain string or a string with
    /// placeholders for:
    ///
    /// - `{{agent_id}}` - the agent ID
    /// - `{{measurement_id}}` - the measurement ID
    /// - `{{tag_id}}` - the tag ID, which must be used if `count` > 1 so that
    ///   unique tag names are created
    pub name: String,
    /// Value for this tag. Can be a plain string or a string with placeholders
    /// for:
    ///
    /// - `{{agent_id}}` - the agent ID
    /// - `{{measurement_id}}` - the measurement ID
    /// - `{{cardinality}}` - the cardinality counter value. Must use this or
    ///   `{{guid}}` if `cardinality` > 1 so that unique tag values are created
    /// - `{{counter}}` - the increment counter value. Only useful if
    ///   `increment_every` is set.
    /// - `{{guid}}` - a randomly generated unique string. If `cardinality` > 1,
    ///   each tag will have a different GUID.
    pub value: String,
    /// The number of tags with this configuration that should be created.
    /// Default value is 1. If specified, use `{{tag_id}}` in this tag's
    /// `name` to create unique tags.
    pub count: Option<usize>,
    /// A number that controls how many values are generated, which impacts how
    /// many rows are created for each agent generation. Default value is 1.
    /// If specified, use `{{cardinality}}` or `{{guid}}` in this tag's
    /// `value` to create unique values.
    pub cardinality: Option<u32>,
    /// How often to increment the `{{counter}}` value. For example, if
    /// `increment_every` is set to 10, `{{counter}}` will increase by 1
    /// after every 10 agent generations. This simulates temporal tag values
    /// like process IDs or container IDs in tags. If not specified, the value
    /// of `{{counter}}` will always be 0.
    pub increment_every: Option<usize>,
    /// A list of replacement placeholders and the values to replace them with.
    /// The values can optionally have weights associated with them to
    /// change the probabilities that its value will be used.
    #[serde(default)]
    pub replacements: Vec<Replacement>,
    /// When there are replacements specified and other tags in this measurement
    /// with cardinality greater than 1, this option controls whether this
    /// tag will get a new replacement value on every line in a generation
    /// (`true`) or whether it will be sampled once and have the same value
    /// on every line in a generation (`false`). If there are no replacements on
    /// this tag or any other tags with a cardinality greater than one, this
    /// has no effect.
    #[serde(default)]
    pub resample_every_line: bool,
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
    /// - `{{agent_id}}` - the agent ID
    /// - `{{measurement_id}}` - the measurement ID
    /// - `{{field_id}}` - the field ID, which must be used if `count` > 1 so
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
        } else if let Some(pattern) = value.pattern {
            FieldValueSpec::String {
                pattern,
                replacements: value.replacements,
            }
        } else if let Some(kind) = value.uptime {
            FieldValueSpec::Uptime { kind }
        } else {
            panic!(
                "Can't tell what type of field value you're trying to specify with this \
                configuration: `{:?}",
                value
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
#[derive(Debug, PartialEq, Copy, Clone, Deserialize)]
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
    /// - `{{agent_name}}` - the agent spec's name, with any replacements done
    /// - `{{time}}` - the current time in nanoseconds since the epoch. TODO:
    ///   support specifying a strftime
    /// - any other placeholders as specified in `replacements`. If a
    ///   placeholder has no value specified in `replacements`, it will end up
    ///   as-is in the field value.
    pattern: Option<String>,
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
#[derive(Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Debug, Deserialize, PartialEq, Clone)]
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

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    static TELEGRAF_TOML: &str = include_str!("../schemas/telegraf.toml");

    #[test]
    fn parse_spec() -> Result {
        let spec = DataSpec::from_str(TELEGRAF_TOML)?;

        assert_eq!(spec.name, "demo_schema");
        assert_eq!(spec.agents.len(), 2);

        let agent0 = &spec.agents[0];
        assert_eq!(agent0.name, "demo");

        let agent0_measurements = &agent0.measurements;
        assert_eq!(agent0_measurements.len(), 1);

        let a0m0 = &agent0_measurements[0];
        assert_eq!(a0m0.name, "some_measurement");

        let a0m0_fields = &a0m0.fields;
        assert_eq!(a0m0_fields.len(), 5);

        let a0m0f0 = &a0m0_fields[0];
        assert_eq!(a0m0f0.name, "field1");
        assert_eq!(a0m0f0.field_value_spec, FieldValueSpec::Bool(true));

        let a0m0f1 = &a0m0_fields[1];
        assert_eq!(a0m0f1.name, "field2");
        assert_eq!(
            a0m0f1.field_value_spec,
            FieldValueSpec::I64 {
                range: 3..200,
                increment: false,
                reset_after: None,
            }
        );

        let a0m0f2 = &a0m0_fields[2];
        assert_eq!(a0m0f2.name, "field3");
        assert_eq!(
            a0m0f2.field_value_spec,
            FieldValueSpec::I64 {
                range: 1000..5000,
                increment: true,
                reset_after: None,
            }
        );

        let a0m0f3 = &a0m0_fields[3];
        assert_eq!(a0m0f3.name, "field4");
        assert_eq!(
            a0m0f3.field_value_spec,
            FieldValueSpec::F64 { range: 0.0..100.0 }
        );

        let a0m0f4 = &a0m0_fields[4];
        assert_eq!(a0m0f4.name, "field5");
        assert_eq!(
            a0m0f4.field_value_spec,
            FieldValueSpec::String {
                pattern:
                    "{{agent_name}} foo {{level}} {{format-time \"%Y-%m-%d %H:%M\"}} {{random 200}}"
                        .into(),
                replacements: vec![
                    Replacement {
                        replace: "color".into(),
                        with: vec![
                            ReplacementValue::String("red".into()),
                            ReplacementValue::String("blue".into()),
                            ReplacementValue::String("green".into())
                        ],
                    },
                    Replacement {
                        replace: "level".into(),
                        with: vec![
                            ReplacementValue::Weighted("info".into(), 800),
                            ReplacementValue::Weighted("warn".into(), 195),
                            ReplacementValue::Weighted("error".into(), 5)
                        ],
                    }
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn parse_fully_supported_spec() -> Result<()> {
        // The fully supported spec is mostly for manual testing, but we should make
        // sure while developing that it's valid as well so that when we go to
        // do manual testing it isn't broken

        // Also read it from the file to test `DataSpec::from_file` rather than
        // include_str

        let data_spec = DataSpec::from_file("schemas/fully-supported.toml")?;

        assert_eq!(data_spec.name, "demo_schema");

        Ok(())
    }

    #[test]
    fn not_specifying_vectors_gets_default_empty_vector() {
        let toml = r#"
name = "demo_schema"
base_seed = "this is a demo"

[[agents]]
name = "basic"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "host"
pattern = "server"
"#;
        let spec = DataSpec::from_str(toml).unwrap();

        let agent0 = &spec.agents[0];
        assert!(agent0.tags.is_empty());

        let agent0_measurements = &agent0.measurements;
        let a0m0 = &agent0_measurements[0];
        assert!(a0m0.tags.is_empty());

        let a0m0_fields = &a0m0.fields;
        let a0m0f0 = &a0m0_fields[0];
        let field_spec = &a0m0f0.field_value_spec;

        assert!(
            matches!(field_spec, FieldValueSpec::String { replacements, .. } if replacements.is_empty()),
            "expected a String field with empty replacements; was {:?}",
            field_spec
        );
    }
}
