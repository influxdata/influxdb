//! Generating a set of field keys and values given a specification

use crate::{
    now_ns, specification,
    substitution::{pick_from_replacements, Substitute},
    DataGenRng, RandomNumberGenerator,
};

use influxdb2_client::models::FieldValue;
use rand::Rng;
use serde::Serialize;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeMap, fmt, ops::Range, time::Duration};

/// Field-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating fields
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when substituting placeholder values
    #[snafu(display("Could not create field name, caused by:\n{}", source))]
    CouldNotCreateFieldName {
        /// Underlying `substitution` module error that caused this problem
        source: crate::substitution::Error,
    },

    /// Error that may happen when substituting placeholder values
    #[snafu(display("Could not compile field name template, caused by:\n{}", source))]
    CouldNotCompileStringTemplate {
        /// Underlying `substitution` module error that caused this problem
        source: crate::substitution::Error,
    },
}

/// A generated field value that will be used in a generated data point.
#[derive(Debug, PartialEq)]
pub struct Field {
    /// The key for the field
    pub key: String,
    /// The value for the field
    pub value: FieldValue,
}

impl Field {
    /// Create a new field with the given key and value.
    pub fn new(key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// A set of `count` fields that have the same configuration but different
/// `field_id`s.
pub struct FieldGeneratorSet {
    field_generators: Vec<Box<dyn FieldGenerator + Send>>,
}

// field_generators doesn't implement Debug
impl fmt::Debug for FieldGeneratorSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldGeneratorSet")
            .field("field_generators", &"(dynamic)")
            .finish()
    }
}

impl FieldGeneratorSet {
    /// Create a new set of field generators for a particular agent,
    /// measurement, and field specification.
    pub fn new<T: DataGenRng>(
        agent_name: &str,
        agent_id: usize,
        measurement_id: usize,
        spec: &specification::FieldSpec,
        parent_seed: &str,
        execution_start_time: i64,
    ) -> Result<Self> {
        let count = spec.count.unwrap_or(1);

        let field_generators = (0..count)
            .map(|field_id| {
                field_spec_to_generator::<T>(
                    agent_name,
                    agent_id,
                    measurement_id,
                    field_id,
                    spec,
                    parent_seed,
                    execution_start_time,
                )
            })
            .collect::<Result<_>>()?;

        Ok(Self { field_generators })
    }

    /// Create one set of fields
    pub fn generate(&mut self, timestamp: i64) -> Vec<Field> {
        self.field_generators
            .iter_mut()
            .map(|fg| fg.generate(timestamp))
            .collect()
    }
}

trait FieldGenerator {
    fn generate(&mut self, timestamp: i64) -> Field;
}

/// Generate boolean field names and values.
#[derive(Debug)]
pub struct BooleanFieldGenerator<T: DataGenRng> {
    name: String,
    rng: RandomNumberGenerator<T>,
}

impl<T: DataGenRng> BooleanFieldGenerator<T> {
    /// Create a new boolean field generator that will always use the specified
    /// name.
    pub fn new(name: &str, parent_seed: &str) -> Self {
        let name = name.into();
        let seed = format!("{}-{}", parent_seed, name);
        let rng = RandomNumberGenerator::<T>::new(seed);

        Self { name, rng }
    }
}

impl<T: DataGenRng> FieldGenerator for BooleanFieldGenerator<T> {
    fn generate(&mut self, _timestamp: i64) -> Field {
        let b: bool = self.rng.gen();
        Field::new(&self.name, b)
    }
}

/// Generate integer field names and values.
#[derive(Debug)]
pub struct I64FieldGenerator<T: DataGenRng> {
    name: String,
    range: Range<i64>,
    increment: bool,
    rng: RandomNumberGenerator<T>,
    previous_value: i64,
    reset_after: Option<usize>,
    current_tick: usize,
}

impl<T: DataGenRng> I64FieldGenerator<T> {
    /// Create a new integer field generator that will always use the specified
    /// name.
    pub fn new(
        name: impl Into<String>,
        range: &Range<i64>,
        increment: bool,
        reset_after: Option<usize>,
        parent_seed: impl fmt::Display,
    ) -> Self {
        let name = name.into();
        let seed = format!("{}-{}", parent_seed, name);
        let rng = RandomNumberGenerator::<T>::new(seed);

        Self {
            name,
            range: range.to_owned(),
            increment,
            rng,
            previous_value: 0,
            reset_after,
            current_tick: 0,
        }
    }
}

impl<T: DataGenRng> FieldGenerator for I64FieldGenerator<T> {
    fn generate(&mut self, _timestamp: i64) -> Field {
        let mut value = if self.range.start == self.range.end {
            self.range.start
        } else {
            self.rng.gen_range(self.range.clone())
        };

        if self.increment {
            self.previous_value = self.previous_value.wrapping_add(value);
            value = self.previous_value;

            if let Some(reset) = self.reset_after {
                self.current_tick += 1;
                if self.current_tick >= reset {
                    self.previous_value = 0;
                    self.current_tick = 0;
                }
            }
        }

        Field::new(&self.name, value)
    }
}

/// Generate floating point field names and values.
#[derive(Debug)]
pub struct F64FieldGenerator<T: DataGenRng> {
    name: String,
    range: Range<f64>,
    rng: RandomNumberGenerator<T>,
}

impl<T: DataGenRng> F64FieldGenerator<T> {
    /// Create a new floating point field generator that will always use the
    /// specified name.
    pub fn new(
        name: impl Into<String>,
        range: &Range<f64>,
        parent_seed: impl fmt::Display,
    ) -> Self {
        let name = name.into();
        let seed = format!("{}-{}", parent_seed, name);
        let rng = RandomNumberGenerator::<T>::new(seed);

        Self {
            name,
            range: range.to_owned(),
            rng,
        }
    }
}

impl<T: DataGenRng> FieldGenerator for F64FieldGenerator<T> {
    fn generate(&mut self, _timestamp: i64) -> Field {
        let value = if (self.range.start - self.range.end).abs() < f64::EPSILON {
            self.range.start
        } else {
            self.rng.gen_range(self.range.clone())
        };

        Field::new(&self.name, value)
    }
}

/// Generate string field names and values.
#[derive(Debug)]
pub struct StringFieldGenerator<T: DataGenRng> {
    agent_name: String,
    name: String,
    substitute: Substitute,
    rng: RandomNumberGenerator<T>,
    replacements: Vec<specification::Replacement>,
}

impl<T: DataGenRng> StringFieldGenerator<T> {
    /// Create a new string field generator
    pub fn new(
        agent_name: impl Into<String>,
        name: impl Into<String>,
        pattern: impl Into<String>,
        parent_seed: impl fmt::Display,
        replacements: Vec<specification::Replacement>,
    ) -> Result<Self> {
        let name = name.into();
        let seed = format!("{}-{}", parent_seed, name);
        let rng = RandomNumberGenerator::<T>::new(seed);
        let substitute = Substitute::new(pattern, RandomNumberGenerator::<T>::new(&rng.seed))
            .context(CouldNotCompileStringTemplate {})?;

        Ok(Self {
            agent_name: agent_name.into(),
            name,
            substitute,
            rng,
            replacements,
        })
    }
}

impl<T: DataGenRng> FieldGenerator for StringFieldGenerator<T> {
    fn generate(&mut self, timestamp: i64) -> Field {
        #[derive(Serialize)]
        struct Values<'a> {
            #[serde(flatten)]
            replacements: BTreeMap<&'a str, &'a str>,
            agent_name: &'a str,
            timestamp: i64,
        }

        let values = Values {
            replacements: pick_from_replacements(&mut self.rng, &self.replacements),
            agent_name: &self.agent_name,
            timestamp,
        };

        let value = self
            .substitute
            .evaluate(&values)
            .expect("Unable to substitute string field value");

        Field::new(&self.name, value)
    }
}

/// Generate an i64 field that has the name `uptime` and the value of the number
/// of seconds since the data_generator started running
#[derive(Debug)]
pub struct UptimeFieldGenerator {
    name: String,
    execution_start_time: i64,
    kind: specification::UptimeKind,
}

impl UptimeFieldGenerator {
    fn new(
        name: impl Into<String>,
        kind: &specification::UptimeKind,
        execution_start_time: i64,
    ) -> Self {
        Self {
            name: name.into(),
            kind: *kind,
            execution_start_time,
        }
    }
}

impl FieldGenerator for UptimeFieldGenerator {
    fn generate(&mut self, _timestamp: i64) -> Field {
        use specification::UptimeKind::*;

        let elapsed = Duration::from_nanos((now_ns() - self.execution_start_time) as u64);
        let elapsed_seconds = elapsed.as_secs();

        match self.kind {
            I64 => Field::new(&self.name, elapsed_seconds as i64),
            Telegraf => {
                let days = elapsed_seconds / (60 * 60 * 24);
                let days_plural = if days == 1 { "" } else { "s" };

                let mut minutes = elapsed_seconds / 60;
                let mut hours = minutes / 60;
                hours %= 24;
                minutes %= 60;

                let duration_string =
                    format!("{} day{}, {:02}:{:02}", days, days_plural, hours, minutes);
                Field::new(&self.name, duration_string)
            }
        }
    }
}

fn field_spec_to_generator<T: DataGenRng>(
    agent_name: &str,
    agent_id: usize,
    measurement_id: usize,
    field_id: usize,
    spec: &specification::FieldSpec,
    parent_seed: &str,
    execution_start_time: i64,
) -> Result<Box<dyn FieldGenerator + Send>> {
    use specification::FieldValueSpec::*;

    let spec_name = Substitute::once(
        &spec.name,
        &[
            ("agent_id", &agent_id.to_string()),
            ("measurement_id", &measurement_id.to_string()),
            ("field_id", &field_id.to_string()),
        ],
    )
    .context(CouldNotCreateFieldName)?;

    Ok(match &spec.field_value_spec {
        Bool(true) => Box::new(BooleanFieldGenerator::<T>::new(&spec_name, parent_seed)),
        Bool(false) => unimplemented!("Not sure what false means for bool fields yet"),
        I64 {
            range,
            increment,
            reset_after,
        } => Box::new(I64FieldGenerator::<T>::new(
            &spec_name,
            range,
            *increment,
            *reset_after,
            parent_seed,
        )),
        F64 { range } => Box::new(F64FieldGenerator::<T>::new(&spec_name, range, parent_seed)),
        String {
            pattern,
            replacements,
        } => Box::new(StringFieldGenerator::<T>::new(
            agent_name,
            &spec_name,
            pattern,
            parent_seed,
            replacements.to_vec(),
        )?),
        Uptime { kind } => Box::new(UptimeFieldGenerator::new(
            &spec_name,
            kind,
            execution_start_time,
        )),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{DynamicRng, ZeroRng, TEST_SEED};
    use test_helpers::approximately_equal;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    // Shortcut functions that panic for getting values out of fields for test convenience
    impl Field {
        fn i64(&self) -> i64 {
            match self.value {
                FieldValue::I64(v) => v,
                ref other => panic!("expected i64, got {:?}", other),
            }
        }

        fn f64(&self) -> f64 {
            match self.value {
                FieldValue::F64(v) => v,
                ref other => panic!("expected f64, got {:?}", other),
            }
        }

        fn bool(&self) -> bool {
            match self.value {
                FieldValue::Bool(v) => v,
                ref other => panic!("expected bool, got {:?}", other),
            }
        }

        fn string(&self) -> String {
            match &self.value {
                FieldValue::String(v) => v.clone(),
                ref other => panic!("expected String, got {:?}", other),
            }
        }
    }

    #[test]
    fn generate_boolean_field() {
        let mut bfg = BooleanFieldGenerator::<ZeroRng>::new("bfg", TEST_SEED);

        assert!(!bfg.generate(1234).bool());
    }

    #[test]
    fn generate_i64_field_always_the_same() {
        // If the specification has the same number for the start and end of the
        // range...
        let mut i64fg =
            I64FieldGenerator::<DynamicRng>::new("i64fg", &(3..3), false, None, TEST_SEED);

        let i64_fields: Vec<_> = (0..10).map(|_| i64fg.generate(1234).i64()).collect();
        let expected = i64_fields[0];

        // All the values generated will always be the same.
        assert!(
            i64_fields.iter().all(|f| *f == expected),
            "{:?}",
            i64_fields
        );

        // If the specification has n for the start and n+1 for the end of the range...
        let mut i64fg =
            I64FieldGenerator::<DynamicRng>::new("i64fg", &(4..5), false, None, TEST_SEED);

        let i64_fields: Vec<_> = (0..10).map(|_| i64fg.generate(1234).i64()).collect();
        // We know what the value will be even though we're using a real random number generator
        let expected = 4;

        // All the values generated will also always be the same, because the end of the
        // range is exclusive.
        assert!(
            i64_fields.iter().all(|f| *f == expected),
            "{:?}",
            i64_fields
        );
    }

    #[test]
    fn generate_i64_field_within_a_range() {
        let range = 3..1000;

        let mut i64fg =
            I64FieldGenerator::<DynamicRng>::new("i64fg", &range, false, None, TEST_SEED);

        let val = i64fg.generate(1234).i64();

        assert!(range.contains(&val), "`{}` was not in the range", val);
    }

    #[test]
    fn generate_incrementing_i64_field() {
        let mut i64fg =
            I64FieldGenerator::<DynamicRng>::new("i64fg", &(3..10), true, None, TEST_SEED);

        let val1 = i64fg.generate(1234).i64();
        let val2 = i64fg.generate(1234).i64();
        let val3 = i64fg.generate(1234).i64();
        let val4 = i64fg.generate(1234).i64();

        assert!(val1 < val2, "`{}` < `{}` was false", val1, val2);
        assert!(val2 < val3, "`{}` < `{}` was false", val2, val3);
        assert!(val3 < val4, "`{}` < `{}` was false", val3, val4);
    }

    #[test]
    fn incrementing_i64_wraps() {
        let rng = RandomNumberGenerator::<DynamicRng>::new(TEST_SEED);
        let range = 3..10;
        let previous_value = i64::MAX;

        // Construct by hand to set the previous value at the end of i64's range
        let mut i64fg = I64FieldGenerator {
            name: "i64fg".into(),
            range: range.clone(),
            increment: true,
            reset_after: None,
            rng,
            previous_value,
            current_tick: 0,
        };

        let resulting_range =
            range.start.wrapping_add(previous_value)..range.end.wrapping_add(previous_value);

        let val = i64fg.generate(1234).i64();

        assert!(
            resulting_range.contains(&val),
            "`{}` was not in the range",
            val
        );
    }

    #[test]
    fn incrementing_i64_that_resets() {
        let reset_after = Some(3);
        let mut i64fg =
            I64FieldGenerator::<DynamicRng>::new("i64fg", &(3..10), true, reset_after, TEST_SEED);

        let val1 = i64fg.generate(1234).i64();
        let val2 = i64fg.generate(1234).i64();
        let val3 = i64fg.generate(1234).i64();
        let val4 = i64fg.generate(1234).i64();

        assert!(val1 < val2, "`{}` < `{}` was false", val1, val2);
        assert!(val2 < val3, "`{}` < `{}` was false", val2, val3);
        assert!(val4 < val3, "`{}` < `{}` was false", val4, val3);
    }

    #[test]
    fn generate_f64_field_always_the_same() {
        // If the specification has the same number for the start and end of the
        // range...
        let start_and_end = 3.0;
        let range = start_and_end..start_and_end;
        let mut f64fg = F64FieldGenerator::<DynamicRng>::new("f64fg", &range, TEST_SEED);

        let f64_fields: Vec<_> = (0..10).map(|_| f64fg.generate(1234).f64()).collect();

        // All the values generated will always be the same known value.
        assert!(
            f64_fields.iter().all(|f| approximately_equal(*f, start_and_end)),
            "{:?}",
            f64_fields
        );
    }

    #[test]
    fn generate_f64_field_within_a_range() {
        let range = 3.0..1000.0;
        let mut f64fg = F64FieldGenerator::<DynamicRng>::new("f64fg", &range, TEST_SEED);

        let val = f64fg.generate(1234).f64();
        assert!(range.contains(&val), "`{}` was not in the range", val);
    }

    #[test]
    fn generate_string_field_without_replacements() {
        let fake_now = 11111;

        let mut stringfg = StringFieldGenerator::<DynamicRng>::new(
            "agent_name",
            "stringfg",
            "my value",
            TEST_SEED,
            vec![],
        )
        .unwrap();

        assert_eq!("my value", stringfg.generate(fake_now).string());
    }

    #[test]
    fn generate_string_field_with_provided_replacements() {
        let fake_now = 5555555555;

        let mut stringfg = StringFieldGenerator::<DynamicRng>::new(
            "double-oh-seven",
            "stringfg",
            r#"{{agent_name}}---{{random 16}}---{{format-time "%s%f"}}"#,
            TEST_SEED,
            vec![],
        )
        .unwrap();

        let string_val1 = stringfg.generate(fake_now).string();
        let string_val2 = stringfg.generate(fake_now).string();

        assert!(
            string_val1.starts_with("double-oh-seven---"),
            "`{}` did not start with `double-oh-seven---`",
            string_val1
        );
        assert!(
            string_val1.ends_with("---5555555555"),
            "`{}` did not end with `---5555555555`",
            string_val1
        );
        assert!(
            string_val2.starts_with("double-oh-seven---"),
            "`{}` did not start with `double-oh-seven---`",
            string_val2
        );
        assert!(
            string_val2.ends_with("---5555555555"),
            "`{}` did not end with `---5555555555`",
            string_val2
        );

        assert_ne!(string_val1, string_val2, "random value should change");
    }

    #[test]
    #[should_panic(expected = "Unable to substitute string field value")]
    fn unknown_replacement_errors() {
        let fake_now = 55555;

        let mut stringfg = StringFieldGenerator::<DynamicRng>::new(
            "arbitrary",
            "stringfg",
            "static-{{unknown}}",
            TEST_SEED,
            vec![],
        )
        .unwrap();

        stringfg.generate(fake_now);
    }

    #[test]
    fn replacements_no_weights() -> Result<()> {
        let fake_now = 55555;

        let toml: specification::FieldSpec = toml::from_str(
            r#"
            name = "sf"
            pattern = "foo {{level}}"
            replacements = [
              {replace = "level", with = ["info", "warn", "error"]}
            ]"#,
        )
        .unwrap();
        let mut stringfg =
            field_spec_to_generator::<ZeroRng>("agent_name", 0, 0, 0, &toml, TEST_SEED, fake_now)?;

        assert_eq!("foo info", stringfg.generate(fake_now).string());
        Ok(())
    }

    #[test]
    fn replacements_with_weights() -> Result<()> {
        let fake_now = 55555;

        let toml: specification::FieldSpec = toml::from_str(
            r#"
            name = "sf"
            pattern = "foo {{level}}"
            replacements = [
              {replace = "level", with = [["info", 1000000], ["warn", 1], ["error", 0]]}
            ]"#,
        )
        .unwrap();
        let mut stringfg =
            field_spec_to_generator::<ZeroRng>("agent_name", 0, 0, 0, &toml, TEST_SEED, fake_now)?;

        assert_eq!("foo info", stringfg.generate(fake_now).string());
        Ok(())
    }

    #[test]
    fn uptime_i64() -> Result<()> {
        let fake_now = 55555;

        // Pretend data generator started running 10 seconds ago
        let seconds_ago = 10;
        let fake_start_execution_time = now_ns() - seconds_ago * 1_000_000_000;

        let toml: specification::FieldSpec = toml::from_str(
            r#"
            name = "arbitrary" # field name doesn't have to be uptime
            uptime = "i64""#,
        )
        .unwrap();
        let mut uptimefg = field_spec_to_generator::<DynamicRng>(
            "agent_name",
            0,
            0,
            0,
            &toml,
            TEST_SEED,
            fake_start_execution_time,
        )?;

        assert_eq!(seconds_ago, uptimefg.generate(fake_now).i64());
        Ok(())
    }

    #[test]
    fn uptime_telegraf() -> Result<()> {
        let fake_now = 55555;

        // Pretend data generator started running 10 days, 2 hours, and 33 minutes ago
        let seconds_ago = 10 * 24 * 60 * 60 + 2 * 60 * 60 + 33 * 60;
        let fake_start_execution_time = now_ns() - seconds_ago * 1_000_000_000;

        let toml: specification::FieldSpec = toml::from_str(
            r#"
            name = "arbitrary" # field name doesn't have to be uptime
            uptime = "telegraf""#,
        )
        .unwrap();
        let mut uptimefg = field_spec_to_generator::<DynamicRng>(
            "agent_name",
            0,
            0,
            0,
            &toml,
            TEST_SEED,
            fake_start_execution_time,
        )?;

        assert_eq!("10 days, 02:33", uptimefg.generate(fake_now).string());

        // Pretend data generator started running 1 day, 14 hours, and 5 minutes ago
        // to exercise different formatting
        let seconds_in_1_day = 24 * 60 * 60;
        let seconds_in_14_hours = 14 * 60 * 60;
        let seconds_in_5_minutes = 5 * 60;

        let seconds_ago = seconds_in_1_day + seconds_in_14_hours + seconds_in_5_minutes;
        let fake_start_execution_time = now_ns() - seconds_ago * 1_000_000_000;

        let mut uptimefg = field_spec_to_generator::<DynamicRng>(
            "agent_name",
            0,
            0,
            0,
            &toml,
            TEST_SEED,
            fake_start_execution_time,
        )?;

        assert_eq!("1 day, 14:05", uptimefg.generate(fake_now).string());

        Ok(())
    }
}
