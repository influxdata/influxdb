//! This contains the logic for creating generators for a given spec for the number of workers.

use crate::report::WriteReporter;
use crate::specification::{DataSpec, FieldKind, MeasurementSpec};
use chrono::{DateTime, Local};
use influxdb3_client::{Client, Precision};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use std::collections::HashMap;
use std::io::Write;
use std::ops::Add;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;
use tokio::time::Instant;

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
            let null_probability = f.null_probability;
            match &f.field {
                FieldKind::Bool(_) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Boolean(BooleanValue::Random),
                    });
                }
                FieldKind::BoolToggle => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Boolean(BooleanValue::Toggle(false)),
                    });
                }
                FieldKind::String(s) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
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
                        null_probability,
                        field_value: FieldValue::String(StringValue::Random(*size)),
                    });
                }
                FieldKind::StringSeq(prefix) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::String(StringValue::Sequential(
                            Arc::from(prefix.clone()),
                            0,
                        )),
                    });
                }
                FieldKind::Integer(i) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Integer(IntegerValue::Fixed(*i)),
                    });
                }
                FieldKind::IntegerRange(min, max) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Integer(IntegerValue::Random(Range {
                            start: *min,
                            end: *max,
                        })),
                    });
                }
                FieldKind::IntegerSeq => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Integer(IntegerValue::Sequential(0)),
                    });
                }
                FieldKind::Float(f) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Float(FloatValue::Fixed(*f)),
                    });
                }
                FieldKind::FloatRange(min, max) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Float(FloatValue::Random(Range {
                            start: *min,
                            end: *max,
                        })),
                    });
                }
                FieldKind::FloatSeqWithInc(inc) => {
                    fields.push(Field {
                        key: Arc::clone(&key),
                        copy_id,
                        null_probability,
                        field_value: FieldValue::Float(FloatValue::SequentialWithInc {
                            next: 0f64,
                            inc: *inc,
                        }),
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
    pub fn dry_run(&mut self, sample_time: DateTime<Local>, rng: &mut impl RngCore) -> String {
        // create a buffer and write a single sample to it
        let mut buffer = Vec::new();
        self.write_sample_to(sample_time, &mut buffer, rng)
            .expect("writing to buffer should succeed");

        // convert the buffer to a string and return it
        String::from_utf8(buffer).expect("buffer should be valid utf8")
    }

    pub fn write_sample_to<W: Write>(
        &mut self,
        sample_time: DateTime<Local>,
        mut w: W,
        rng: &mut impl RngCore,
    ) -> io::Result<WriteSummary> {
        let mut write_summary = WriteSummary {
            bytes_written: 0,
            lines_written: 0,
            tags_written: 0,
            fields_written: 0,
        };

        let mut w = ByteCounter::new(&mut w);
        let timestamp_micros = sample_time.timestamp_micros();
        let timestamp_nanos = timestamp_micros * 1000;

        for measurement in &mut self.measurements {
            for i in 0..measurement.lines_per_sample {
                // NOTE: if we don't generate a new time stamp for each lines_per_sample AND if we
                // have a cardinality < lines_per_sample then we will only get an actual number of
                // samples per batch equal to the cardinality since uniqueness in line protocol is
                // based on unique values of the tuple (table, tag set, timestamp).
                //
                // to avoid that outcome, we ensure here that our batch values are distinct from
                // one another even in low-cardinality situations by forcing uniqueness via
                // nanosecond increments in the sample's timestamp
                //
                // note that this means each millisecond timestamp supports up to 1_000_000 unique
                // entries (ie lines per sample); beyond that there is a risk of overlap if the
                // given sample_time instances happen in back-to-back milliseconds.
                let timestamp = timestamp_nanos + i as i64;
                if measurement.copy_id > 1 {
                    write!(w, "{}_{}", measurement.name, measurement.copy_id)?;
                } else {
                    write!(w, "{}", measurement.name)?;
                }

                // TODO: support non-overlapping simultaneous writers by introducing an optional
                // UUID generated when initializing the runner that gets set as the value for a
                // "writer-id" tag here
                for tag in &mut measurement.tags {
                    tag.write_to(self.writer_id, &mut w)?;
                }
                write_summary.tags_written += measurement.tags.len();

                for (i, field) in measurement.fields.iter_mut().enumerate() {
                    let separator = if i == 0 { " " } else { "," };
                    write!(w, "{separator}")?;
                    field.write_to(&mut w, rng)?;
                }
                write_summary.fields_written += measurement.fields.len();

                writeln!(w, " {timestamp}")?;

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
pub struct Measurement {
    pub name: Arc<str>,
    copy_id: usize,
    pub tags: Vec<Tag>,
    pub fields: Vec<Field>,
    lines_per_sample: usize,
}

impl From<Generator> for Vec<Measurement> {
    fn from(g: Generator) -> Vec<Measurement> {
        g.measurements
    }
}

#[derive(Debug)]
pub struct Tag {
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
            write!(w, "{v}")?;
        }

        // append the writer id with a preceding w if we're supposed to
        if self.append_writer_id {
            write!(w, "{writer_id}")?;
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
pub struct Field {
    pub key: Arc<str>,
    copy_id: usize,
    null_probability: Option<f64>,
    pub field_value: FieldValue,
}

impl Field {
    pub fn keyvalue(&self) -> (Arc<str>, FieldValue) {
        (Arc::clone(&self.key), self.field_value.clone())
    }
}

#[derive(Clone, Debug)]
pub enum FieldValue {
    Integer(IntegerValue),
    Float(FloatValue),
    String(StringValue),
    Boolean(BooleanValue),
}

impl Field {
    fn write_to<W: Write>(
        &mut self,
        w: &mut ByteCounter<W>,
        rng: &mut impl RngCore,
    ) -> io::Result<()> {
        // if there are random nulls, check and return without writing the field if it hits the
        // probability
        if let Some(probability) = &mut self.null_probability {
            let val: f64 = rng.r#gen();
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
                IntegerValue::Fixed(v) => write!(w, "{v}i")?,
                IntegerValue::Random(range) => {
                    let v: i64 = rng.gen_range(range.clone());
                    write!(w, "{v}i")?;
                }
                IntegerValue::Sequential(v) => {
                    write!(w, "{v}u")?;
                    *v += 1;
                }
            },
            FieldValue::Float(f) => match f {
                FloatValue::Fixed(v) => write!(w, "{v}")?,
                FloatValue::Random(range) => {
                    let v: f64 = rng.gen_range(range.clone());
                    write!(w, "{v:.3}")?;
                }
                FloatValue::SequentialWithInc { next, inc } => {
                    write!(w, "{next:.10}")?;
                    *next += *inc;
                }
            },
            FieldValue::String(s) => match s {
                StringValue::Fixed(v) => write!(w, "\"{v}\"")?,
                StringValue::Random(size) => {
                    let random: String = rng
                        .sample_iter(&Alphanumeric)
                        .take(*size)
                        .map(char::from)
                        .collect();

                    write!(w, "\"{random}\"")?;
                }
                StringValue::Sequential(prefix, inc) => {
                    write!(w, "\"{prefix}{inc}\"")?;
                    *inc += 1;
                }
            },
            FieldValue::Boolean(f) => match f {
                BooleanValue::Random => {
                    let v: bool = rng.r#gen();
                    write!(w, "{v}")?;
                }
                BooleanValue::Toggle(current) => {
                    write!(w, "{current}")?;
                    *current = !*current;
                }
            },
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum IntegerValue {
    Fixed(i64),
    Random(Range<i64>),
    Sequential(u64),
}

#[derive(Clone, Debug)]
pub enum FloatValue {
    Fixed(f64),
    Random(Range<f64>),
    SequentialWithInc { next: f64, inc: f64 },
}

#[derive(Clone, Debug)]
pub enum StringValue {
    Fixed(Arc<str>),
    Random(usize),
    Sequential(Arc<str>, u64),
}

#[derive(Clone, Copy, Debug)]
pub enum BooleanValue {
    Random,
    Toggle(bool),
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

#[derive(Debug)]
pub struct GeneratorRunner {
    pub generator: Generator,
    pub client: Client,
    pub database_name: String,
    pub sampling_interval: Duration,

    reporter: Option<Arc<WriteReporter>>,
    start_time: Option<DateTime<Local>>,
    end_time: Option<DateTime<Local>>,
}

impl GeneratorRunner {
    pub fn new(
        generator: Generator,
        client: Client,
        database_name: String,
        sampling_interval: Duration,
    ) -> Self {
        Self {
            generator,
            client,
            database_name,
            sampling_interval,
            reporter: None,
            start_time: None,
            end_time: None,
        }
    }

    pub fn with_reporter(mut self, reporter: Arc<WriteReporter>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    pub fn with_start_time(mut self, start_time: DateTime<Local>) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn with_end_time(mut self, end_time: DateTime<Local>) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub async fn run(mut self, mut rng: impl RngCore) -> Output {
        // if not generator 1, pause for 100ms to let it start the run to create the schema
        if self.generator.writer_id != 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut sample_buffer = vec![];

        // if the start time is set, load the historical samples as quickly as possible
        if let Some(mut start_time) = self.start_time {
            let mut sample_len = self
                .write_sample(&mut rng, sample_buffer, start_time, true)
                .await;

            loop {
                start_time = start_time.add(self.sampling_interval);
                if start_time > Local::now()
                    || self
                        .end_time
                        .map(|end_time| start_time > end_time)
                        .unwrap_or(false)
                {
                    println!(
                        "writer {} finished historical replay at: {:?}",
                        self.generator.writer_id, start_time
                    );
                    break;
                }

                sample_buffer = Vec::with_capacity(sample_len);
                sample_len = self
                    .write_sample(&mut rng, sample_buffer, start_time, false)
                    .await;
            }
        }

        // write data until end time or forever
        let mut interval = tokio::time::interval(self.sampling_interval);
        let mut sample_len = 1024 * 1024 * 1024;

        // we only want to print the error the very first time it happens
        let mut print_err = true;

        loop {
            interval.tick().await;
            let now = Local::now();
            if let Some(end_time) = self.end_time
                && now > end_time
            {
                println!(
                    "writer {} completed at {}",
                    self.generator.writer_id, end_time
                );
                return Output {
                    measurements: self.generator.into(),
                };
            }

            sample_buffer = Vec::with_capacity(sample_len);
            sample_len = self
                .write_sample(&mut rng, sample_buffer, now, print_err)
                .await;
            print_err = false;
        }
    }

    async fn write_sample(
        &mut self,
        rng: &mut impl RngCore,
        mut buffer: Vec<u8>,
        sample_time: DateTime<Local>,
        print_err: bool,
    ) -> usize {
        // generate the sample, and keep track of the length to set the buffer size for the next loop
        let summary = self
            .generator
            .write_sample_to(sample_time, &mut buffer, rng)
            .expect("failed to write sample");
        let sample_len = buffer.len();

        // time and send the write request
        let start_request = Instant::now();
        let res = self
            .client
            .api_v3_write_lp(self.database_name.clone())
            .precision(Precision::Nanosecond)
            .accept_partial(false)
            .body(buffer)
            .send()
            .await;
        let response_time = start_request.elapsed().as_millis() as u64;

        // log the report
        match res {
            Ok(_) => {
                self.reporter.as_ref().inspect(|r| {
                    r.report_write(
                        self.generator.writer_id,
                        summary,
                        response_time,
                        Local::now(),
                    )
                });
            }
            Err(e) => {
                // if it's the first error, print the details
                if print_err {
                    eprintln!(
                        "Error on writer {} writing to server: {:?}",
                        self.generator.writer_id, e
                    );
                }
                self.reporter.as_ref().inspect(|r| {
                    r.report_failure(self.generator.writer_id, response_time, Local::now())
                });
            }
        }

        sample_len
    }
}

#[derive(Debug)]
pub struct Output {
    pub measurements: Vec<Measurement>,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use rand::{SeedableRng, rngs::SmallRng};

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
        let mut rng = SmallRng::from_entropy();

        let t = Local.timestamp_millis_opt(123).unwrap();
        let lp = generators.get_mut(0).unwrap().dry_run(t, &mut rng);
        let actual: Vec<&str> = lp.split('\n').collect();
        let expected: Vec<&str> = vec![
            "m,t=w1,t_2=w1 i=42i,i_2=42i,f=6.8,s=\"hello\" 123000000",
            "m,t=w2,t_2=w2 i=42i,i_2=42i,f=6.8,s=\"hello\" 123000001",
            "",
        ];
        assert_eq!(actual, expected);

        let t = Local.timestamp_millis_opt(567).unwrap();
        let lp = generators.get_mut(1).unwrap().dry_run(t, &mut rng);
        let actual: Vec<&str> = lp.split('\n').collect();
        let expected: Vec<&str> = vec![
            "m,t=w6,t_2=w6 i=42i,i_2=42i,f=6.8,s=\"hello\" 567000000",
            "m,t=w7,t_2=w7 i=42i,i_2=42i,f=6.8,s=\"hello\" 567000001",
            "",
        ];
        assert_eq!(actual, expected);
    }
}
