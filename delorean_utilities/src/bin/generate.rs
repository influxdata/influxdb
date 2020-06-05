#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

//! Utility to generate data to ingest for development and testing purposes.
//!
//! Similar to `storectl generate`.
//!
//! # Usage
//!
//! ```
//! cargo run --bin generate > line-protocol.txt
//! ```

use rand::prelude::*;
use std::{
    convert::TryFrom,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() {
    // TODO: turn these into command line arguments
    let num_points = 100;
    let max_tags_per_point = 5;
    let max_fields_per_point = 5;

    // TODO: Optionally use a seed provided by a command line argument and output the seed used to enable
    // reproducibility.
    let mut rng = rand::thread_rng();

    // Generate fields such that each field always has the same type throughout the batch
    let field_definitions: Vec<_> = (0..max_fields_per_point)
        .map(|num| Field::generate(&mut rng, num))
        .collect();

    for _ in 0..num_points {
        println!(
            "{}",
            Point::generate(&mut rng, max_tags_per_point, &field_definitions)
        );
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Point {
    measurement_name: String,
    tags: Vec<Tag>,
    fields: Vec<Field>,
    timestamp: Option<i64>,
}

impl Point {
    fn generate(
        rng: &mut impl Rng,
        max_tags_per_point: usize,
        field_definitions: &[Field],
    ) -> Point {
        let num_tags = rng.gen_range(0, max_tags_per_point);
        let tags = (0..num_tags).map(|num| Tag::generate(rng, num)).collect();

        // Must have at least one field, so start the range at 1
        let num_fields = rng.gen_range(1, field_definitions.len());
        let fields = field_definitions[..num_fields]
            .iter()
            .map(|field| field.generate_similar(rng))
            .collect();

        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let now_ns = i64::try_from(since_the_epoch.as_nanos()).expect("Time does not fit");

        Point {
            measurement_name: "m0".into(),
            tags,
            fields,
            timestamp: Some(now_ns),
        }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.measurement_name)?;

        let mut tags = self.tags.iter();

        if let Some(tag) = tags.next() {
            write!(f, ",{}", tag)?;

            for tag in tags {
                write!(f, ",{}", tag)?;
            }
        }

        write!(f, " ")?;

        // TODO: Error if there are no fields?
        let mut fields = self.fields.iter();

        if let Some(field) = fields.next() {
            write!(f, "{}", field)?;

            for field in fields {
                write!(f, ",{}", field)?;
            }
        }

        if let Some(time) = self.timestamp {
            write!(f, " {}", time)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Tag {
    key: String,
    value: String,
}

impl Tag {
    fn generate(rng: &mut impl Rng, num: usize) -> Tag {
        Tag {
            key: format!("tag{}", num),
            value: format!("value{}", rng.gen_range(0, 10)),
        }
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Field {
    key: String,
    value: FieldValue,
}

impl Field {
    fn generate(rng: &mut impl Rng, num: usize) -> Field {
        Field {
            key: format!("field{}", num),
            value: FieldValue::generate(rng),
        }
    }

    fn generate_similar(&self, rng: &mut impl Rng) -> Field {
        Field {
            key: self.key.clone(),
            value: self.value.generate_similar(rng),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FieldValue {
    Float(f64),
    Integer(i64),
    // Change `number_of_variants` and the `match` below in `generate` when more variants get added!
    // String(String),
    // Boolean(bool),
}

impl FieldValue {
    fn generate(rng: &mut impl Rng) -> FieldValue {
        // Randomly select a variant
        let number_of_variants = 2;
        let which_variant = rng.gen_range(0, number_of_variants);

        match which_variant {
            0 => FieldValue::Float(rng.gen()),
            1 => FieldValue::Integer(rng.gen()),
            other => unreachable!("Not sure which FieldValue variant to build from {}", other),
        }
    }

    fn generate_similar(&self, rng: &mut impl Rng) -> FieldValue {
        match self {
            FieldValue::Float(_) => FieldValue::Float(rng.gen()),
            FieldValue::Integer(_) => FieldValue::Integer(rng.gen()),
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Float(value) => write!(f, "{}", value),
            FieldValue::Integer(value) => write!(f, "{}i", value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_points_without_tags_or_timestamp() {
        let point = Point {
            measurement_name: "m0".into(),
            tags: vec![],
            fields: vec![
                Field {
                    key: "f0".into(),
                    value: FieldValue::Float(1.0),
                },
                Field {
                    key: "f1".into(),
                    value: FieldValue::Integer(2),
                },
            ],
            timestamp: None,
        };
        assert_eq!(point.to_string(), "m0 f0=1,f1=2i");
    }

    #[test]
    fn print_points_without_timestamp() {
        let point = Point {
            measurement_name: "m0".into(),
            tags: vec![
                Tag {
                    key: "t0".into(),
                    value: "v0".into(),
                },
                Tag {
                    key: "t1".into(),
                    value: "v1".into(),
                },
            ],
            fields: vec![Field {
                key: "f1".into(),
                value: FieldValue::Integer(2),
            }],
            timestamp: None,
        };
        assert_eq!(point.to_string(), "m0,t0=v0,t1=v1 f1=2i");
    }

    #[test]
    fn print_points_with_everything() {
        let point = Point {
            measurement_name: "m0".into(),
            tags: vec![
                Tag {
                    key: "t0".into(),
                    value: "v0".into(),
                },
                Tag {
                    key: "t1".into(),
                    value: "v1".into(),
                },
            ],
            fields: vec![
                Field {
                    key: "f0".into(),
                    value: FieldValue::Float(1.0),
                },
                Field {
                    key: "f1".into(),
                    value: FieldValue::Integer(2),
                },
            ],
            timestamp: Some(1_583_443_428_970_606_000),
        };
        assert_eq!(
            point.to_string(),
            "m0,t0=v0,t1=v1 f0=1,f1=2i 1583443428970606000"
        );
    }
}
