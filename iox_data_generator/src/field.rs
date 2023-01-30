//! Generating a set of field keys and values given a specification

use crate::{
    now_ns, specification,
    substitution::{self, pick_from_replacements},
};

use handlebars::Handlebars;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use serde_json::json;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use std::{ops::Range, time::Duration};

/// Field-specific Results
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that may happen while creating fields
#[derive(Snafu, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Could not create field name, caused by:\n{}", source))]
    CouldNotCreateFieldName { source: crate::substitution::Error },

    #[snafu(display("Could not compile string field template: {}", source))]
    CouldNotCompileStringTemplate {
        #[snafu(source(from(handlebars::TemplateError, Box::new)))]
        source: Box<handlebars::TemplateError>,
    },

    #[snafu(display("Could not render string field template: {}", source))]
    CouldNotRenderStringTemplate {
        #[snafu(source(from(handlebars::RenderError, Box::new)))]
        source: Box<handlebars::RenderError>,
    },
}

/// Different field type generators
#[derive(Debug)]
pub enum FieldGeneratorImpl {
    /// Boolean field generator
    Bool(BooleanFieldGenerator),
    /// Integer field generator
    I64(I64FieldGenerator),
    /// Float field generator
    F64(F64FieldGenerator),
    /// String field generator
    String(Box<StringFieldGenerator>),
    /// Uptime field generator
    Uptime(UptimeFieldGenerator),
}

impl FieldGeneratorImpl {
    /// Create fields that will generate according to the spec
    pub fn from_spec(
        spec: &specification::FieldSpec,
        data: Value,
        execution_start_time: i64,
    ) -> Result<Vec<Self>> {
        use specification::FieldValueSpec::*;

        let field_count = spec.count.unwrap_or(1);

        let mut fields = Vec::with_capacity(field_count);

        for field_id in 1..field_count + 1 {
            let mut data = data.clone();
            let d = data.as_object_mut().expect("data must be object");
            d.insert("field".to_string(), json!({ "id": field_id }));

            let field_name = substitution::render_once("field", &spec.name, &data)
                .context(CouldNotCreateFieldNameSnafu)?;

            let rng =
                SmallRng::from_rng(&mut rand::thread_rng()).expect("SmallRng should always create");

            let field = match &spec.field_value_spec {
                Bool(true) => Self::Bool(BooleanFieldGenerator::new(&field_name, rng)),
                Bool(false) => unimplemented!("Not sure what false means for bool fields yet"),
                I64 {
                    range,
                    increment,
                    reset_after,
                } => Self::I64(I64FieldGenerator::new(
                    &field_name,
                    range,
                    *increment,
                    *reset_after,
                    rng,
                )),
                F64 { range } => Self::F64(F64FieldGenerator::new(&field_name, range, rng)),
                String {
                    pattern,
                    replacements,
                } => Self::String(Box::new(StringFieldGenerator::new(
                    &field_name,
                    pattern,
                    data,
                    replacements.to_vec(),
                    rng,
                )?)),
                Uptime { kind } => Self::Uptime(UptimeFieldGenerator::new(
                    &field_name,
                    kind,
                    execution_start_time,
                )),
            };

            fields.push(field);
        }

        Ok(fields)
    }

    /// Writes the field in line protocol to the passed writer
    pub fn write_to<W: std::io::Write>(&mut self, mut w: W, timestamp: i64) -> std::io::Result<()> {
        match self {
            Self::Bool(f) => {
                let v: bool = f.rng.gen();
                write!(w, "{}={}", f.name, v)
            }
            Self::I64(f) => {
                let v = f.generate_value();
                write!(w, "{}={}", f.name, v)
            }
            Self::F64(f) => {
                let v = f.generate_value();
                write!(w, "{}={}", f.name, v)
            }
            Self::String(f) => {
                let v = f.generate_value(timestamp);
                write!(w, "{}=\"{}\"", f.name, v)
            }
            Self::Uptime(f) => match f.kind {
                specification::UptimeKind::I64 => {
                    let v = f.generate_value();
                    write!(w, "{}={}", f.name, v)
                }
                specification::UptimeKind::Telegraf => {
                    let v = f.generate_value_as_string();
                    write!(w, "{}=\"{}\"", f.name, v)
                }
            },
        }
    }
}

/// Generate boolean field names and values.
#[derive(Debug)]
pub struct BooleanFieldGenerator {
    /// The name (key) of the field
    pub name: String,
    rng: SmallRng,
}

impl BooleanFieldGenerator {
    /// Create a new boolean field generator that will always use the specified
    /// name.
    pub fn new(name: &str, rng: SmallRng) -> Self {
        let name = name.into();

        Self { name, rng }
    }

    /// Generate a random value
    pub fn generate_value(&mut self) -> bool {
        self.rng.gen()
    }
}

/// Generate integer field names and values.
#[derive(Debug)]
pub struct I64FieldGenerator {
    /// The name (key) of the field
    pub name: String,
    range: Range<i64>,
    increment: bool,
    rng: SmallRng,
    previous_value: i64,
    reset_after: Option<usize>,
    current_tick: usize,
}

impl I64FieldGenerator {
    /// Create a new integer field generator that will always use the specified
    /// name.
    pub fn new(
        name: impl Into<String>,
        range: &Range<i64>,
        increment: bool,
        reset_after: Option<usize>,
        rng: SmallRng,
    ) -> Self {
        Self {
            name: name.into(),
            range: range.to_owned(),
            increment,
            rng,
            previous_value: 0,
            reset_after,
            current_tick: 0,
        }
    }

    /// Generate a random value
    pub fn generate_value(&mut self) -> i64 {
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

        value
    }
}

/// Generate floating point field names and values.
#[derive(Debug)]
pub struct F64FieldGenerator {
    /// The name (key) of the field
    pub name: String,
    range: Range<f64>,
    rng: SmallRng,
}

impl F64FieldGenerator {
    /// Create a new floating point field generator that will always use the
    /// specified name.
    pub fn new(name: impl Into<String>, range: &Range<f64>, rng: SmallRng) -> Self {
        Self {
            name: name.into(),
            range: range.to_owned(),
            rng,
        }
    }

    /// Generate a random value
    pub fn generate_value(&mut self) -> f64 {
        if (self.range.start - self.range.end).abs() < f64::EPSILON {
            self.range.start
        } else {
            self.rng.gen_range(self.range.clone())
        }
    }
}

/// Generate string field names and values.
#[derive(Debug)]
pub struct StringFieldGenerator {
    /// The name (key) of the field
    pub name: String,
    rng: SmallRng,
    replacements: Vec<specification::Replacement>,
    handlebars: Handlebars<'static>,
    data: Value,
}

impl StringFieldGenerator {
    /// Create a new string field generator
    pub fn new(
        name: impl Into<String>,
        template: impl Into<String>,
        data: Value,
        replacements: Vec<specification::Replacement>,
        rng: SmallRng,
    ) -> Result<Self> {
        let name = name.into();
        let mut registry = substitution::new_handlebars_registry();
        registry
            .register_template_string(&name, template.into())
            .context(CouldNotCompileStringTemplateSnafu)?;

        Ok(Self {
            name,
            rng,
            replacements,
            handlebars: registry,
            data,
        })
    }

    /// Generate a random value
    pub fn generate_value(&mut self, timestamp: i64) -> String {
        let replacements = pick_from_replacements(&mut self.rng, &self.replacements);
        let d = self.data.as_object_mut().expect("data must be object");

        if replacements.is_empty() {
            d.remove("replacements");
        } else {
            d.insert("replacements".to_string(), json!(replacements));
        }

        d.insert("timestamp".to_string(), json!(timestamp));

        self.handlebars
            .render(&self.name, &self.data)
            .expect("Unable to substitute string field value")
    }
}

/// Generate an i64 field that has the name `uptime` and the value of the number
/// of seconds since the data generator started running
#[derive(Debug)]
pub struct UptimeFieldGenerator {
    /// The name (key) of the field
    pub name: String,
    execution_start_time: i64,
    /// The specification type of the uptime field. Either an int64 or a string
    pub kind: specification::UptimeKind,
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

    /// Generates the uptime as an i64
    pub fn generate_value(&mut self) -> i64 {
        let elapsed = Duration::from_nanos((now_ns() - self.execution_start_time) as u64);
        elapsed.as_secs() as i64
    }

    /// Generates the uptime as a string, which is what should be used if `self.kind == specification::UptimeKind::Telegraf`
    pub fn generate_value_as_string(&mut self) -> String {
        let elapsed_seconds = self.generate_value();
        let days = elapsed_seconds / (60 * 60 * 24);
        let days_plural = if days == 1 { "" } else { "s" };

        let mut minutes = elapsed_seconds / 60;
        let mut hours = minutes / 60;
        hours %= 24;
        minutes %= 60;

        format!("{days} day{days_plural}, {hours:02}:{minutes:02}")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::specification::UptimeKind;
    use rand::SeedableRng;
    use test_helpers::approximately_equal;

    #[test]
    fn generate_i64_field_always_the_same() {
        // If the specification has the same number for the start and end of the
        // range...
        let mut i64fg =
            I64FieldGenerator::new("i64fg", &(3..3), false, None, SmallRng::from_entropy());

        let i64_fields: Vec<_> = (0..10).map(|_| i64fg.generate_value()).collect();
        let expected = i64_fields[0];

        // All the values generated will always be the same.
        assert!(i64_fields.iter().all(|f| *f == expected), "{i64_fields:?}");

        // If the specification has n for the start and n+1 for the end of the range...
        let mut i64fg =
            I64FieldGenerator::new("i64fg", &(4..5), false, None, SmallRng::from_entropy());

        let i64_fields: Vec<_> = (0..10).map(|_| i64fg.generate_value()).collect();
        // We know what the value will be even though we're using a real random number generator
        let expected = 4;

        // All the values generated will also always be the same, because the end of the
        // range is exclusive.
        assert!(i64_fields.iter().all(|f| *f == expected), "{i64_fields:?}");
    }

    #[test]
    fn generate_i64_field_within_a_range() {
        let range = 3..1000;

        let mut i64fg =
            I64FieldGenerator::new("i64fg", &range, false, None, SmallRng::from_entropy());

        let val = i64fg.generate_value();

        assert!(range.contains(&val), "`{val}` was not in the range");
    }

    #[test]
    fn generate_incrementing_i64_field() {
        let mut i64fg =
            I64FieldGenerator::new("i64fg", &(3..10), true, None, SmallRng::from_entropy());

        let val1 = i64fg.generate_value();
        let val2 = i64fg.generate_value();
        let val3 = i64fg.generate_value();
        let val4 = i64fg.generate_value();

        assert!(val1 < val2, "`{val1}` < `{val2}` was false");
        assert!(val2 < val3, "`{val2}` < `{val3}` was false");
        assert!(val3 < val4, "`{val3}` < `{val4}` was false");
    }

    #[test]
    fn incrementing_i64_wraps() {
        let rng = SmallRng::from_entropy();
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

        let val = i64fg.generate_value();

        assert!(
            resulting_range.contains(&val),
            "`{val}` was not in the range"
        );
    }

    #[test]
    fn incrementing_i64_that_resets() {
        let reset_after = Some(3);
        let mut i64fg = I64FieldGenerator::new(
            "i64fg",
            &(3..8),
            true,
            reset_after,
            SmallRng::from_entropy(),
        );

        let val1 = i64fg.generate_value();
        let val2 = i64fg.generate_value();
        let val3 = i64fg.generate_value();
        let val4 = i64fg.generate_value();

        assert!(val1 < val2, "`{val1}` < `{val2}` was false");
        assert!(val2 < val3, "`{val2}` < `{val3}` was false");
        assert!(val4 < val3, "`{val4}` < `{val3}` was false");
    }

    #[test]
    fn generate_f64_field_always_the_same() {
        // If the specification has the same number for the start and end of the
        // range...
        let start_and_end = 3.0;
        let range = start_and_end..start_and_end;
        let mut f64fg = F64FieldGenerator::new("f64fg", &range, SmallRng::from_entropy());

        let f64_fields: Vec<_> = (0..10).map(|_| f64fg.generate_value()).collect();

        // All the values generated will always be the same known value.
        assert!(
            f64_fields
                .iter()
                .all(|f| approximately_equal(*f, start_and_end)),
            "{f64_fields:?}"
        );
    }

    #[test]
    fn generate_f64_field_within_a_range() {
        let range = 3.0..1000.0;
        let mut f64fg = F64FieldGenerator::new("f64fg", &range, SmallRng::from_entropy());

        let val = f64fg.generate_value();
        assert!(range.contains(&val), "`{val}` was not in the range");
    }

    #[test]
    fn generate_string_field_with_data() {
        let fake_now = 1633595510000000000;

        let mut stringfg = StringFieldGenerator::new(
            "str",
            r#"my value {{measurement.name}} {{format-time "%Y-%m-%d"}}"#,
            json!({"measurement": {"name": "foo"}}),
            vec![],
            SmallRng::from_entropy(),
        )
        .unwrap();

        assert_eq!("my value foo 2021-10-07", stringfg.generate_value(fake_now));
    }

    #[test]
    fn uptime_i64() {
        // Pretend data generator started running 10 seconds ago
        let seconds_ago = 10;
        let execution_start_time = now_ns() - seconds_ago * 1_000_000_000;
        let mut uptimefg = UptimeFieldGenerator::new("foo", &UptimeKind::I64, execution_start_time);

        assert_eq!(seconds_ago, uptimefg.generate_value());
    }

    #[test]
    fn uptime_telegraf() {
        // Pretend data generator started running 10 days, 2 hours, and 33 minutes ago
        let seconds_ago = 10 * 24 * 60 * 60 + 2 * 60 * 60 + 33 * 60;
        let execution_start_time = now_ns() - seconds_ago * 1_000_000_000;
        let mut uptimefg = UptimeFieldGenerator::new("foo", &UptimeKind::I64, execution_start_time);

        assert_eq!("10 days, 02:33", uptimefg.generate_value_as_string());

        // Pretend data generator started running 1 day, 14 hours, and 5 minutes ago
        // to exercise different formatting
        let seconds_in_1_day = 24 * 60 * 60;
        let seconds_in_14_hours = 14 * 60 * 60;
        let seconds_in_5_minutes = 5 * 60;

        let seconds_ago = seconds_in_1_day + seconds_in_14_hours + seconds_in_5_minutes;
        let execution_start_time = now_ns() - seconds_ago * 1_000_000_000;

        let mut uptimefg = UptimeFieldGenerator::new("foo", &UptimeKind::I64, execution_start_time);

        assert_eq!("1 day, 14:05", uptimefg.generate_value_as_string());
    }
}
