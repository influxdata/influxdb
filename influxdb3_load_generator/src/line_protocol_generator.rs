//! This contains the logic for creating generators for a given spec for the number of workers.

use crate::specification::{DataSpec, FieldKind, MeasurementSpec};
use rand::distributions::Alphanumeric;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use tokio::io;

pub type WriterId = usize;

pub fn create_generators(
    spec: &DataSpec,
    writer_count: usize,
) -> Result<Vec<Generator>, anyhow::Error> {
    let mut generators = vec![];
    let mut arc_strings = HashMap::new();

    for writer_id in 1..writer_count + 1 {
        let mut measurements = vec![];

        for m in &spec.measurements {
            let copies = m.copies.unwrap_or(1);

            for measurement_id in 1..copies + 1 {
                measurements.push(create_measurement(
                    m,
                    writer_id,
                    writer_count,
                    measurement_id,
                    &mut arc_strings,
                ));
            }
        }

        generators.push(Generator {
            writer_id,
            measurements,
        });
    }

    Ok(generators)
}

fn create_measurement<'a>(
    spec: &'a MeasurementSpec,
    writer_id: WriterId,
    writer_count: usize,
    measurement_id: usize,
    arc_strings: &mut HashMap<&'a str, Arc<str>>,
) -> Measurement {
    let name = Arc::clone(arc_strings.entry(spec.name.as_str()).or_insert_with(|| {
        let m = spec.name.replace(' ', "\\ ").replace(',', "\\,");
        Arc::from(m.as_str())
    }));

    let max_cardinality = spec.max_cardinality();
    let max_cardinality = usize::div_ceil(max_cardinality, writer_count);
    let lines_per_sample = spec.lines_per_sample.unwrap_or(max_cardinality);

    let mut tags = vec![];

    for t in &spec.tags {
        let key = Arc::clone(arc_strings.entry(t.key.as_str()).or_insert_with(|| {
            let k = t
                .key
                .replace(' ', "\\ ")
                .replace(',', "\\,")
                .replace('=', "\\=");
            Arc::from(k.as_str())
        }));

        let value = t.value.as_ref().map(|v| {
            Arc::clone(arc_strings.entry(v.as_str()).or_insert_with(|| {
                let v = v
                    .replace(' ', "\\ ")
                    .replace(',', "\\,")
                    .replace('=', "\\=");
                Arc::from(v.as_str())
            }))
        });

        let (cardinality_id_min, cardinality_id_max) = t
            .cardinality_min_max(writer_id, writer_count)
            .unwrap_or((0, 0));

        let append_writer_id = t.append_writer_id.unwrap_or(false);
        let append_copy_id = t.append_copy_id.unwrap_or(false);
        let copies = t.copies.unwrap_or(1);

        for copy_id in 1..copies + 1 {
            tags.push(Tag {
                key: Arc::clone(&key),
                value: value.clone(),
                copy_id,
                cardinality_id_min,
                cardinality_id_max,
                cardinality_id_current: cardinality_id_min,
                append_writer_id,
                append_copy_id,
            });
        }
    }

    let mut fields = vec![];

    for f in &spec.fields {
        let key = Arc::clone(arc_strings.entry(f.key.as_str()).or_insert_with(|| {
            let k = f
                .key
                .replace(' ', "\\ ")
                .replace(',', "\\,")
                .replace('=', "\\=");
            Arc::from(k.as_str())
        }));

        let copies = f.copies.unwrap_or(1);

        for copy_id in 1..copies + 1 {
            let random_null = f.null_probability.map(|p| (p, SmallRng::from_entropy()));

            match &f.field {
                FieldKind::Bool(_) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::Boolean(BooleanValue::Random(
                            SmallRng::from_entropy(),
                        )),
                    });
                }
                FieldKind::String(s) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::String(StringValue::Fixed(Arc::clone(
                            arc_strings
                                .entry(s.as_str())
                                .or_insert_with(|| Arc::from(s.as_str())),
                        ))),
                    });
                }
                FieldKind::StringRandom(size) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::String(StringValue::Random(
                            *size,
                            SmallRng::from_entropy(),
                        )),
                    });
                }
                FieldKind::Integer(i) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::Integer(IntegerValue::Fixed(*i)),
                    });
                }
                FieldKind::IntegerRange(min, max) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::Integer(IntegerValue::Random(
                            Range {
                                start: *min,
                                end: *max,
                            },
                            SmallRng::from_entropy(),
                        )),
                    });
                }
                FieldKind::Float(f) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::Float(FloatValue::Fixed(*f)),
                    });
                }
                FieldKind::FloatRange(min, max) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        random_null,
                        field_value: FieldValue::Float(FloatValue::Random(
                            Range {
                                start: *min,
                                end: *max,
                            },
                            SmallRng::from_entropy(),
                        )),
                    });
                }
            }
        }
    }

    Measurement {
        name,
        copy_id: measurement_id,
        tags,
        fields,
        lines_per_sample,
    }
}

/// This struct holds the generator for each writer.
#[derive(Debug)]
pub struct Generator {
    pub writer_id: WriterId,
    measurements: Vec<Measurement>,
}

impl Generator {
    pub fn new(writer_id: WriterId) -> Self {
        Self {
            writer_id,
            measurements: Vec::new(),
        }
    }

    /// Return a single sample run from the generator as a string.
    pub fn dry_run(&mut self, timestamp: i64) -> String {
        // create a buffer and write a single sample to it
        let mut buffer = Vec::new();
        self.write_sample_to(timestamp, &mut buffer)
            .expect("writing to buffer should succeed");

        // convert the buffer to a string and return it
        String::from_utf8(buffer).expect("buffer should be valid utf8")
    }

    pub fn write_sample_to<W: Write>(
        &mut self,
        timestamp: i64,
        mut w: W,
    ) -> io::Result<WriteSummary> {
        let mut write_summary = WriteSummary {
            bytes_written: 0,
            lines_written: 0,
            tags_written: 0,
            fields_written: 0,
        };

        let mut w = ByteCounter::new(&mut w);

        for measurement in &mut self.measurements {
            for _ in 0..measurement.lines_per_sample {
                if measurement.copy_id > 1 {
                    write!(w, "{}_{}", measurement.name, measurement.copy_id)?;
                } else {
                    write!(w, "{}", measurement.name)?;
                }

                for tag in &mut measurement.tags {
                    tag.write_to(self.writer_id, &mut w)?;
                }
                write_summary.tags_written += measurement.tags.len();

                for (i, field) in measurement.fields.iter_mut().enumerate() {
                    let separator = if i == 0 { " " } else { "," };
                    write!(w, "{}", separator)?;
                    field.write_to(&mut w)?;
                }
                write_summary.fields_written += measurement.fields.len();

                writeln!(w, " {}", timestamp)?;

                write_summary.lines_written += 1;
            }
        }

        write_summary.bytes_written = w.bytes_written();

        Ok(write_summary)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WriteSummary {
    pub bytes_written: usize,
    pub lines_written: usize,
    pub tags_written: usize,
    pub fields_written: usize,
}

#[derive(Debug)]
struct Measurement {
    name: Arc<str>,
    copy_id: usize,
    tags: Vec<Tag>,
    fields: Vec<Field>,
    lines_per_sample: usize,
}

#[derive(Debug)]
struct Tag {
    key: Arc<str>,
    value: Option<Arc<str>>,
    copy_id: usize,
    cardinality_id_min: usize,
    cardinality_id_max: usize,
    cardinality_id_current: usize,
    append_writer_id: bool,
    append_copy_id: bool,
}

impl Tag {
    fn write_to<W: Write>(
        &mut self,
        writer_id: WriterId,
        w: &mut ByteCounter<W>,
    ) -> io::Result<()> {
        if self.copy_id > 1 {
            write!(w, ",{}_{}=", self.key, self.copy_id)?;
        } else {
            write!(w, ",{}=", self.key)?;
        }

        if let Some(v) = &self.value {
            write!(w, "{}", v)?;
        }

        // append the writer id with a preceding w if we're supposed to
        if self.append_writer_id {
            write!(w, "{}", writer_id)?;
        }

        // append the copy id with a preceding c if we're supposed to
        if self.append_copy_id {
            write!(w, "{}", self.copy_id)?;
        }

        // keep track of the cardinality id if min and max are different
        if self.cardinality_id_min != 0 && self.cardinality_id_max != 0 {
            // reset the id back to min if we've cycled through them all
            if self.cardinality_id_current > self.cardinality_id_max {
                self.cardinality_id_current = self.cardinality_id_min;
            }

            // write the cardinality counter value to the tag value
            write!(w, "{}", self.cardinality_id_current)?;

            self.cardinality_id_current += 1;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Field {
    key: Arc<str>,
    copy_id: usize,
    random_null: Option<(f64, SmallRng)>,
    field_value: FieldValue,
}

#[derive(Debug)]
enum FieldValue {
    Integer(IntegerValue),
    Float(FloatValue),
    String(StringValue),
    Boolean(BooleanValue),
}

impl Field {
    fn write_to<W: Write>(&mut self, w: &mut ByteCounter<W>) -> io::Result<()> {
        // if there are random nulls, check and return without writing the field if it hits the
        // probability
        if let Some((probability, rng)) = &mut self.random_null {
            let val: f64 = rng.gen();
            if val <= *probability {
                return Ok(());
            }
        }

        if self.copy_id > 1 {
            write!(w, "{}_{}=", self.key, self.copy_id)?;
        } else {
            write!(w, "{}=", self.key)?;
        }

        match &mut self.field_value {
            FieldValue::Integer(f) => match f {
                IntegerValue::Fixed(v) => write!(w, "{}i", v)?,
                IntegerValue::Random(range, rng) => {
                    let v: i64 = rng.gen_range(range.clone());
                    write!(w, "{}i", v)?;
                }
            },
            FieldValue::Float(f) => match f {
                FloatValue::Fixed(v) => write!(w, "{}", v)?,
                FloatValue::Random(range, rng) => {
                    let v: f64 = rng.gen_range(range.clone());
                    write!(w, "{:.3}", v)?;
                }
            },
            FieldValue::String(s) => match s {
                StringValue::Fixed(v) => write!(w, "\"{}\"", v)?,
                StringValue::Random(size, rng) => {
                    let random: String = rng
                        .sample_iter(&Alphanumeric)
                        .take(*size)
                        .map(char::from)
                        .collect();

                    write!(w, "\"{}\"", random)?;
                }
            },
            FieldValue::Boolean(f) => match f {
                BooleanValue::Random(rng) => {
                    let v: bool = rng.gen();
                    write!(w, "{}", v)?;
                }
            },
        }

        Ok(())
    }
}

#[derive(Debug)]
enum IntegerValue {
    Fixed(i64),
    Random(Range<i64>, SmallRng),
}

#[derive(Debug)]
enum FloatValue {
    Fixed(f64),
    Random(Range<f64>, SmallRng),
}

#[derive(Debug)]
enum StringValue {
    Fixed(Arc<str>),
    Random(usize, SmallRng),
}

#[derive(Debug)]
enum BooleanValue {
    Random(SmallRng),
}

struct ByteCounter<W> {
    inner: W,
    count: usize,
}

impl<W> ByteCounter<W>
where
    W: Write,
{
    fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    fn bytes_written(&self) -> usize {
        self.count
    }
}

impl<W> Write for ByteCounter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf);
        if let Ok(size) = res {
            self.count += size
        }
        res
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::specification::{FieldSpec, TagSpec};
    #[test]
    fn example_spec_lp() {
        let spec = DataSpec {
            name: "foo".to_string(),
            measurements: vec![MeasurementSpec {
                name: "m".to_string(),
                tags: vec![TagSpec {
                    key: "t".to_string(),
                    copies: Some(2),
                    append_copy_id: None,
                    value: Some("w".to_string()),
                    append_writer_id: None,
                    cardinality: Some(10),
                }],
                fields: vec![
                    FieldSpec {
                        key: "i".to_string(),
                        copies: Some(2),
                        null_probability: None,
                        field: FieldKind::Integer(42),
                    },
                    FieldSpec {
                        key: "f".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::Float(6.8),
                    },
                    FieldSpec {
                        key: "s".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::String("hello".to_string()),
                    },
                ],
                copies: Some(1),
                lines_per_sample: Some(2),
            }],
        };
        let mut generators = create_generators(&spec, 2).unwrap();

        let lp = generators.get_mut(0).unwrap().dry_run(123);
        let actual: Vec<&str> = lp.split('\n').collect();
        let expected: Vec<&str> = vec![
            "m,t=w1,t_2=w1 i=42i,i_2=42i,f=6.8,s=\"hello\" 123",
            "m,t=w2,t_2=w2 i=42i,i_2=42i,f=6.8,s=\"hello\" 123",
            "",
        ];
        assert_eq!(actual, expected);

        let lp = generators.get_mut(1).unwrap().dry_run(567);
        let actual: Vec<&str> = lp.split('\n').collect();
        let expected: Vec<&str> = vec![
            "m,t=w6,t_2=w6 i=42i,i_2=42i,f=6.8,s=\"hello\" 567",
            "m,t=w7,t_2=w7 i=42i,i_2=42i,f=6.8,s=\"hello\" 567",
            "",
        ];
        assert_eq!(actual, expected);
    }
}
