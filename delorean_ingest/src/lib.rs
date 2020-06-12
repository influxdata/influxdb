#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

//! Library with code for (aspirationally) ingesting various data formats into Delorean
//! Currently supports converting LineProtocol
//! TODO move this to delorean/src/ingest/line_protocol.rs?
use log::debug;
use snafu::{ResultExt, Snafu};

use std::collections::BTreeMap;

use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError, Packer};
use delorean_table_schema::{DataType, Schema, SchemaBuilder};

#[derive(Debug, Clone)]
pub struct ConversionSettings {
    /// How many `ParsedLine` structures to buffer before determining the schema
    sample_size: usize,
    // Buffer up tp this many ParsedLines per measurement before writing them
    measurement_write_buffer_size: usize,
}

impl ConversionSettings {}

impl Default for ConversionSettings {
    /// Reasonable defult settings
    fn default() -> Self {
        ConversionSettings {
            sample_size: 5,
            measurement_write_buffer_size: 8000,
        }
    }
}

/// Converts `ParsedLines` into the delorean_table internal columnar
/// data format and then passes that converted data to a
/// `DeloreanTableWriter`
pub struct LineProtocolConverter<'a> {
    settings: ConversionSettings,

    // The converters for each measurement.
    // Key: measurement_name
    converters: BTreeMap<delorean_line_parser::EscapedStr<'a>, MeasurementConverter<'a>>,

    table_writer_source: Box<dyn DeloreanTableWriterSource>,
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Conversion needs at least one line of data"#))]
    NeedsAtLeastOneLine,

    // Only a single line protocol measurement field is currently supported
    #[snafu(display(r#"More than one measurement not yet supported: {}"#, message))]
    OnlyOneMeasurementSupported { message: String },

    #[snafu(display(r#"Error writing to TableWriter: {}"#, source))]
    Writing { source: TableError },

    #[snafu(display(r#"Error creating TableWriter: {}"#, source))]
    WriterCreation { source: TableError },
}

/// Handles buffering `ParsedLine` objects and deducing a schema from that sample
#[derive(Debug)]
struct MeasurementSampler<'a> {
    settings: ConversionSettings,

    /// The buffered lines to use as a sample
    schema_sample: Vec<ParsedLine<'a>>,
}

/// Handles actually packing (copy/reformat) of ParsedLines and
/// writing them to a table writer.
struct MeasurementWriter<'a> {
    settings: ConversionSettings,

    /// Schema which describes the lines being written
    schema: Schema,

    /// The sink to which tables are being written
    table_writer: Box<dyn DeloreanTableWriter>,

    /// lines buffered
    write_buffer: Vec<ParsedLine<'a>>,
}

/// Tracks the conversation state for each measurement: either in
/// "UnknownSchema" mode when the schema is still unknown or "KnownSchema" mode once
/// the schema is known.
#[derive(Debug)]
enum MeasurementConverter<'a> {
    UnknownSchema(MeasurementSampler<'a>),
    KnownSchema(MeasurementWriter<'a>),
}

impl std::fmt::Debug for MeasurementWriter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeasurementWriter")
            .field("settings", &self.settings)
            .field("schema", &self.schema)
            .field("table_writer", &"DYNAMIC")
            .field("write_buffer.size", &self.write_buffer.len())
            .finish()
    }
}

impl std::fmt::Debug for LineProtocolConverter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineProtocolConverter")
            .field("settings", &self.settings)
            .field("converters", &self.converters)
            .field("table_writer_source", &"DYNAMIC")
            .finish()
    }
}

impl<'a> MeasurementConverter<'a> {
    /// Changes from `MeasurementSampler` -> `MeasurementWriter` if
    /// not yet in writing mode and the schema sample is full. The
    /// conversion can be forced even if there are not enough samples
    /// by specifing `force=true` (e.g. at the end of the input stream).
    fn prepare_for_writing(
        &mut self,
        table_writer_source: &mut dyn DeloreanTableWriterSource,
        force: bool,
    ) -> Result<(), Error> {
        match self {
            MeasurementConverter::UnknownSchema(sampler) => {
                if force || sampler.sample_full() {
                    debug!(
                        "Preparing for write, deducing schema (sample_full={}, force={})",
                        sampler.sample_full(),
                        force
                    );

                    let schema = sampler.deduce_schema_from_sample()?;
                    debug!("Deduced line protocol schema: {:#?}", schema);
                    let table_writer = table_writer_source
                        .next_writer(&schema)
                        .context(WriterCreation)?;

                    let mut writer =
                        MeasurementWriter::new(sampler.settings.clone(), schema, table_writer);

                    debug!("Completed change to writing mode");
                    for line in sampler.schema_sample.drain(..) {
                        writer.buffer_line(line)?;
                    }
                    *self = MeasurementConverter::KnownSchema(writer);
                } else {
                    debug!("Schema sample not yet full, waiting for more lines");
                }
            }
            // Already in writing mode
            MeasurementConverter::KnownSchema(_) => {}
        };
        Ok(())
    }
}

impl<'a> LineProtocolConverter<'a> {
    /// Construct a converter. All converted data will be written to
    /// the respective DeloreanTableWriter returned by
    /// `table_writer_source`.
    pub fn new(
        settings: ConversionSettings,
        table_writer_source: Box<dyn DeloreanTableWriterSource>,
    ) -> Self {
        LineProtocolConverter {
            settings,
            table_writer_source,
            converters: BTreeMap::new(),
        }
    }

    /// Converts `ParesdLine`s from any number of measurements and
    /// writes them out to `DeloreanTableWriters`. Note that data is
    /// internally buffered and may not be written until a call to
    /// `finalize`.
    pub fn convert(
        &mut self,
        lines: impl IntoIterator<Item = ParsedLine<'a>>,
    ) -> Result<&mut Self, Error> {
        for line in lines {
            let series = &line.series;

            // TODO remove the to_string conversion
            //let measurement_string = series.measurement.to_string();
            let settings = &self.settings;
            let series_measurement = series.measurement.clone();
            let mut converter = self
                .converters
                .entry(series_measurement)
                .or_insert_with(|| {
                    MeasurementConverter::UnknownSchema(MeasurementSampler::new(settings.clone()))
                });

            // This currently dispatches row by row. It might help
            // group `ParsedLines` by measurement first.
            match &mut converter {
                MeasurementConverter::UnknownSchema(sampler) => {
                    sampler.add_sample(line);
                }
                MeasurementConverter::KnownSchema(writer) => {
                    writer.buffer_line(line)?;
                }
            }
            converter.prepare_for_writing(&mut self.table_writer_source, false)?;
        }
        Ok(self)
    }

    /// Finalizes all work of this converter and calls `close()` on the
    /// underlying writer.
    pub fn finalize(&mut self) -> Result<&mut Self, Error> {
        // If we haven't yet switched to writing mode, do so now
        for converter in self.converters.values_mut() {
            converter.prepare_for_writing(&mut self.table_writer_source, true)?;

            match converter {
                MeasurementConverter::UnknownSchema(_) => {
                    unreachable!("Should be prepared for writing");
                }
                MeasurementConverter::KnownSchema(writer) => writer.finalize()?,
            }
        }
        Ok(self)
    }
}

impl<'a> MeasurementSampler<'a> {
    fn new(settings: ConversionSettings) -> Self {
        let schema_sample = Vec::with_capacity(settings.sample_size);
        MeasurementSampler {
            settings,
            schema_sample,
        }
    }

    fn sample_full(&self) -> bool {
        self.schema_sample.len() >= self.settings.sample_size
    }

    fn add_sample(&mut self, line: ParsedLine<'a>) {
        self.schema_sample.push(line);
    }

    /// Use the contents of self.schema_sample to deduce the Schema of
    /// `ParsedLine`s and return the deduced schema
    fn deduce_schema_from_sample(&mut self) -> Result<Schema, Error> {
        if self.schema_sample.is_empty() {
            return Err(Error::NeedsAtLeastOneLine {});
        }

        let mut builder = SchemaBuilder::new(&self.schema_sample[0].series.measurement);

        for line in &self.schema_sample {
            let series = &line.series;
            if &series.measurement != builder.get_measurement_name() {
                return Err(Error::OnlyOneMeasurementSupported {
                    message: format!(
                        "Saw new measurement {}, had been using measurement {}",
                        builder.get_measurement_name(),
                        series.measurement
                    ),
                });
            }
            if let Some(tag_set) = &series.tag_set {
                for (tag_name, _) in tag_set {
                    // FIXME avoid the copy / creation of a string!
                    builder = builder.tag(&tag_name.to_string());
                }
            }
            for (field_name, field_value) in &line.field_set {
                let field_type = match field_value {
                    FieldValue::F64(_) => DataType::Float,
                    FieldValue::I64(_) => DataType::Integer,
                };
                // FIXME: avoid the copy!
                builder = builder.field(&field_name.to_string(), field_type);
            }
        }

        Ok(builder.build())
    }
}

impl<'a> MeasurementWriter<'a> {
    /// Create a new measurement writer which will buffer up to
    /// the number of samples specified in settings before writing to the output
    pub fn new(
        settings: ConversionSettings,
        schema: Schema,
        table_writer: Box<dyn DeloreanTableWriter>,
    ) -> Self {
        let write_buffer = Vec::with_capacity(settings.measurement_write_buffer_size);

        MeasurementWriter {
            settings,
            schema,
            table_writer,
            write_buffer,
        }
    }

    fn buffer_full(&self) -> bool {
        self.write_buffer.len() >= self.settings.measurement_write_buffer_size
    }

    /// Buffers a `ParsedLine`s (which are row-based) in preparation for column packing and writing
    pub fn buffer_line(&mut self, line: ParsedLine<'a>) -> Result<(), Error> {
        if self.buffer_full() {
            self.flush_buffer()?;
        }
        self.write_buffer.push(line);
        Ok(())
    }

    /// Flushes all ParsedLines and writes them to the underlying
    /// table writer in a single chunk
    fn flush_buffer(&mut self) -> Result<(), Error> {
        debug!("Flushing buffer {} rows", self.write_buffer.len());
        let packers = pack_lines(&self.schema, &self.write_buffer);
        self.table_writer.write_batch(&packers).context(Writing)?;
        self.write_buffer.clear();
        Ok(())
    }

    /// Finalizes all work of this converter and closes the underlying writer.
    pub fn finalize(&mut self) -> Result<(), Error> {
        self.flush_buffer()?;
        self.table_writer.close().context(Writing)
    }
}

/// Internal implementation: packs the `ParsedLine` structures for a
/// single measurement into a format suitable for writing
///
/// # Panics
///
/// The caller is responsible for ensuring that all `ParsedLines` come
/// from the same measurement.  This function will panic if that is
/// not true.
///
///
/// TODO: improve performance by reusing the the Vec<Packer> rather
/// than always making new ones
fn pack_lines<'a>(schema: &Schema, lines: &[ParsedLine<'a>]) -> Vec<Packer> {
    let col_defs = schema.get_col_defs();
    let mut packers: Vec<_> = col_defs
        .iter()
        .enumerate()
        .map(|(idx, col_def)| {
            debug!("  Column definition [{}] = {:?}", idx, col_def);
            Packer::with_capacity(col_def.data_type, lines.len())
        })
        .collect();

    // map col_name -> Packer;
    let mut packer_map: BTreeMap<_, _> = col_defs
        .iter()
        .map(|x| &x.name)
        .zip(packers.iter_mut())
        .collect();

    for line in lines {
        let timestamp_col_name = schema.timestamp();

        // all packers should be the same size
        let starting_len = packer_map
            .get(timestamp_col_name)
            .expect("should always have timestamp column")
            .len();
        assert!(
            packer_map.values().all(|x| x.len() == starting_len),
            "All packers should have started at the same size"
        );

        let series = &line.series;

        assert_eq!(
            series.measurement,
            schema.measurement(),
            "Different measurements detected. Expected {} found {}",
            schema.measurement(),
            series.measurement
        );

        if let Some(tag_set) = &series.tag_set {
            for (tag_name, tag_value) in tag_set {
                let tag_name_str = tag_name.to_string();
                if let Some(packer) = packer_map.get_mut(&tag_name_str) {
                    packer.pack_str(Some(&tag_value.to_string()));
                } else {
                    panic!(
                        "tag {} seen in input that has no matching column in schema",
                        tag_name
                    )
                }
            }
        }

        for (field_name, field_value) in &line.field_set {
            let field_name_str = field_name.to_string();
            if let Some(packer) = packer_map.get_mut(&field_name_str) {
                match *field_value {
                    FieldValue::F64(f) => packer.pack_f64(Some(f)),
                    FieldValue::I64(i) => packer.pack_i64(Some(i)),
                }
            } else {
                panic!(
                    "field {} seen in input that has no matching column in schema",
                    field_name
                )
            }
        }

        if let Some(packer) = packer_map.get_mut(timestamp_col_name) {
            packer.pack_i64(line.timestamp);
        } else {
            panic!("No {} field present in schema...", timestamp_col_name);
        }

        // Now, go over all packers and add missing values if needed
        for packer in packer_map.values_mut() {
            if packer.len() < starting_len + 1 {
                assert_eq!(packer.len(), starting_len, "packer should be unchanged");
                packer.pack_none();
            } else {
                assert_eq!(
                    starting_len + 1,
                    packer.len(),
                    "packer should have only one value packed for a total of {}, instead had {}",
                    starting_len + 1,
                    packer.len(),
                )
            }
        }

        // Should have added one value to all packers. Asser that invariant here
        assert!(
            packer_map.values().all(|x| x.len() == starting_len + 1),
            "Should have added 1 row to all packers"
        );
    }
    packers
}

#[cfg(test)]
mod delorean_ingest_tests {
    use super::*;
    use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError};
    use delorean_table_schema::ColumnDefinition;
    use delorean_test_helpers::approximately_equal;

    use std::sync::{Arc, Mutex};

    /// Record what happens when the writer is created so we can
    /// inspect it as part of the tests. It uses string manipulation
    /// for quick test writing and easy debugging
    struct WriterLog {
        events: Vec<String>,
    }
    impl WriterLog {
        fn new() -> Self {
            WriterLog { events: Vec::new() }
        }
    }

    /// copies the events out of the shared log
    fn get_events(log: &Arc<Mutex<WriterLog>>) -> Vec<String> {
        let log_mut = log.lock().expect("got the lock for log");
        log_mut.events.to_vec()
    }

    /// Adds a new event to the log
    fn log_event(log: &Arc<Mutex<WriterLog>>, event: String) {
        let mut mut_log = log.lock().expect("get the log for writing");
        mut_log.events.push(event);
    }

    struct NoOpWriter {
        /// use a ptr and mutex so we can inspect the shared value of the
        /// log during tests. Could probably use an Rc instead, but Arc may
        /// be useful when implementing this multi threaded
        log: Arc<Mutex<WriterLog>>,
        measurement_name: String,
    }
    impl NoOpWriter {
        fn new(log: Arc<Mutex<WriterLog>>, measurement_name: String) -> Self {
            NoOpWriter {
                log,
                measurement_name,
            }
        }
    }

    impl DeloreanTableWriter for NoOpWriter {
        fn write_batch(&mut self, packers: &[Packer]) -> Result<(), TableError> {
            if packers.is_empty() {
                log_event(
                    &self.log,
                    format!(
                        "[{}] Wrote no data; no packers passed",
                        self.measurement_name
                    ),
                );
            }

            let rows_written = packers.iter().fold(packers[0].len(), |cur_len, packer| {
                assert_eq!(
                    packer.len(),
                    cur_len,
                    "Some packer had a different number of rows"
                );
                cur_len
            });

            log_event(
                &self.log,
                format!(
                    "[{}] Wrote batch of {} cols, {} rows",
                    self.measurement_name,
                    packers.len(),
                    rows_written
                ),
            );
            Ok(())
        }

        fn close(&mut self) -> Result<(), TableError> {
            log_event(&self.log, format!("[{}] Closed", self.measurement_name));
            Ok(())
        }
    }

    /// Constructs NoOpWriters
    struct NoOpWriterSource {
        log: Arc<Mutex<WriterLog>>,
    }

    impl NoOpWriterSource {
        fn new(log: Arc<Mutex<WriterLog>>) -> Box<Self> {
            Box::new(NoOpWriterSource { log })
        }
    }

    impl DeloreanTableWriterSource for NoOpWriterSource {
        fn next_writer(
            &mut self,
            schema: &Schema,
        ) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
            let measurement_name = schema.measurement().to_string();
            log_event(
                &self.log,
                format!("Created writer for measurement {}", measurement_name),
            );
            Ok(Box::new(NoOpWriter::new(
                self.log.clone(),
                measurement_name,
            )))
        }
    }

    fn only_good_lines(data: &str) -> Vec<ParsedLine<'_>> {
        delorean_line_parser::parse_lines(data)
            .filter_map(|r| {
                assert!(r.is_ok());
                r.ok()
            })
            .collect()
    }

    fn get_sampler_settings() -> ConversionSettings {
        ConversionSettings {
            sample_size: 2,
            ..Default::default()
        }
    }

    #[test]
    fn measurement_sampler_add_sample() {
        let mut parsed_lines = only_good_lines(
            r#"
            cpu usage_system=64i 1590488773254420000
            cpu usage_system=67i 1590488773254430000
            cpu usage_system=68i 1590488773254440000"#,
        )
        .into_iter();

        let mut sampler = MeasurementSampler::new(get_sampler_settings());
        assert_eq!(sampler.sample_full(), false);

        sampler.add_sample(parsed_lines.next().unwrap());
        assert_eq!(sampler.sample_full(), false);

        sampler.add_sample(parsed_lines.next().unwrap());
        assert_eq!(sampler.sample_full(), true);

        // note it is ok to put more lines in than sample
        sampler.add_sample(parsed_lines.next().unwrap());
        assert_eq!(sampler.sample_full(), true);

        assert_eq!(sampler.schema_sample.len(), 3);
    }

    #[test]
    fn measurement_sampler_deduce_schema_no_lines() {
        let mut sampler = MeasurementSampler::new(get_sampler_settings());
        let schema_result = sampler.deduce_schema_from_sample();
        assert!(matches!(schema_result, Err(Error::NeedsAtLeastOneLine)));
    }

    /// Creates a sampler and feeds all the lines found in data into it
    fn make_sampler_from_data(data: &str) -> MeasurementSampler<'_> {
        let parsed_lines = only_good_lines(data);
        let mut sampler = MeasurementSampler::new(get_sampler_settings());
        for line in parsed_lines {
            sampler.add_sample(line)
        }
        sampler
    }

    #[test]
    fn measurement_sampler_deduce_schema_one_line() {
        let mut sampler =
            make_sampler_from_data("cpu,host=A,region=west usage_system=64i 1590488773254420000");

        let schema = sampler
            .deduce_schema_from_sample()
            .expect("Successful schema conversion");

        assert_eq!(schema.measurement(), "cpu");

        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn measurement_sampler_deduce_schema_multi_line_same_schema() {
        let mut sampler = make_sampler_from_data(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_system=67i 1590488773254430000"#,
        );

        let schema = sampler
            .deduce_schema_from_sample()
            .expect("Successful schema conversion");
        assert_eq!(schema.measurement(), "cpu");

        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn measurement_sampler_deduce_schema_multi_line_new_field() {
        // given two lines of protocol data that have different field names
        let mut sampler = make_sampler_from_data(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_user=61.32 1590488773254430000"#,
        );

        // when we extract the schema
        let schema = sampler
            .deduce_schema_from_sample()
            .expect("Successful schema conversion");
        assert_eq!(schema.measurement(), "cpu");

        // then both field names appear in the resulting schema
        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("usage_user", 3, DataType::Float)
        );
        assert_eq!(
            cols[4],
            ColumnDefinition::new("timestamp", 4, DataType::Timestamp)
        );
    }

    #[test]
    fn measurement_sampler_deduce_schema_multi_line_new_tags() {
        // given two lines of protocol data that have different tags
        let mut sampler = make_sampler_from_data(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A,fail_group=Z usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let schema = sampler
            .deduce_schema_from_sample()
            .expect("Successful schema conversion");
        assert_eq!(schema.measurement(), "cpu");

        // Then both tag names appear in the resulting schema
        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("fail_group", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn measurement_sampler_deduce_schema_multi_line_field_changed() {
        // given two lines of protocol data that have apparently different data types for the field:
        let mut sampler = make_sampler_from_data(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A usage_system=61.1 1590488773254430000"#,
        );

        // when we extract the schema
        let schema = sampler
            .deduce_schema_from_sample()
            .expect("Successful schema conversion");
        assert_eq!(schema.measurement(), "cpu");

        // Then the first field type appears in the resulting schema (TBD is this what we want??)
        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("usage_system", 1, DataType::Integer)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("timestamp", 2, DataType::Timestamp)
        );
    }

    #[test]
    fn measurement_sampler_deduce_schema_multi_line_measurement_changed() {
        // given two lines of protocol data for two different measurements
        let mut sampler = make_sampler_from_data(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            vcpu,host=A usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let schema_result = sampler.deduce_schema_from_sample();

        // Then the converter does not support it
        assert!(matches!(
            schema_result,
            Err(Error::OnlyOneMeasurementSupported { message: _ })
        ));
    }

    // --- Tests for MeasurementWriter
    fn get_writer_settings() -> ConversionSettings {
        ConversionSettings {
            measurement_write_buffer_size: 2,
            ..Default::default()
        }
    }

    #[test]
    fn measurement_writer_buffering() -> Result<(), Error> {
        let log = Arc::new(Mutex::new(WriterLog::new()));
        let table_writer = Box::new(NoOpWriter::new(log.clone(), String::from("cpu")));

        let schema = SchemaBuilder::new("cpu")
            .field("usage_system", DataType::Integer)
            .build();

        let mut writer = MeasurementWriter::new(get_writer_settings(), schema, table_writer);
        assert_eq!(writer.write_buffer.capacity(), 2);

        let mut parsed_lines = only_good_lines(
            r#"
            cpu usage_system=64i 1590488773254420000
            cpu usage_system=67i 1590488773254430000
            cpu usage_system=68i 1590488773254440000"#,
        )
        .into_iter();

        // no rows should have been written
        assert_eq!(get_events(&log).len(), 0);

        // buffer size is 2 we don't expect any writes until three rows are pushed
        writer.buffer_line(parsed_lines.next().expect("parse success"))?;
        assert_eq!(get_events(&log).len(), 0);
        writer.buffer_line(parsed_lines.next().expect("parse success"))?;
        assert_eq!(get_events(&log).len(), 0);

        // this should cause a flush and write
        writer.buffer_line(parsed_lines.next().expect("parse success"))?;
        assert_eq!(
            get_events(&log),
            vec!["[cpu] Wrote batch of 2 cols, 2 rows"]
        );

        // finalize should write out the last line
        writer.finalize()?;
        assert_eq!(
            get_events(&log),
            vec![
                "[cpu] Wrote batch of 2 cols, 2 rows",
                "[cpu] Wrote batch of 2 cols, 1 rows",
                "[cpu] Closed",
            ]
        );

        Ok(())
    }

    // ----- Tests for pack_data -----

    // given protocol data for each datatype, ensure it is packed
    // as expected.  NOTE the line protocol parser only handles
    // Float and Int field values at the time of this writing so I
    // can't test bool and string here.
    //
    // TODO: add test coverage for string and bool fields when that is available
    static LP_DATA: &str = r#"
               cpu,tag1=A int_field=64i,float_field=100.0 1590488773254420000
               cpu,tag1=B int_field=65i,float_field=101.0 1590488773254430000
               cpu        int_field=66i,float_field=102.0 1590488773254440000
               cpu,tag1=C               float_field=103.0 1590488773254450000
               cpu,tag1=D int_field=67i                   1590488773254460000
               cpu,tag1=E int_field=68i,float_field=104.0
               cpu,tag1=F int_field=69i,float_field=105.0 1590488773254470000
             "#;
    static EXPECTED_NUM_LINES: usize = 7;

    fn parse_data_into_sampler() -> Result<MeasurementSampler<'static>, Error> {
        let mut sampler = MeasurementSampler::new(get_sampler_settings());

        for line in only_good_lines(LP_DATA) {
            sampler.add_sample(line);
        }
        Ok(sampler)
    }

    #[test]
    fn pack_data_schema() -> Result<(), Error> {
        let schema = parse_data_into_sampler()?.deduce_schema_from_sample()?;

        // Then the correct schema is extracted
        let cols = schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("int_field", 1, DataType::Integer)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("float_field", 2, DataType::Float)
        );

        Ok(())
    }

    // gets the packer's value as a string.
    fn get_string_val(packer: &Packer, idx: usize) -> &str {
        packer.as_string_packer().values[idx].as_utf8().unwrap()
    }

    // gets the packer's value as an int
    fn get_int_val(packer: &Packer, idx: usize) -> i64 {
        packer.as_int_packer().values[idx]
    }

    // gets the packer's value as an int
    fn get_float_val(packer: &Packer, idx: usize) -> f64 {
        packer.as_float_packer().values[idx]
    }

    #[test]
    fn pack_data_value() -> Result<(), Error> {
        let mut sampler = parse_data_into_sampler()?;
        let schema = sampler.deduce_schema_from_sample()?;

        let packers = pack_lines(&schema, &sampler.schema_sample);

        // 4 columns so 4 packers
        assert_eq!(packers.len(), 4);

        // all packers should have packed all lines
        for p in &packers {
            assert_eq!(p.len(), EXPECTED_NUM_LINES);
        }

        // Tag values
        let tag_packer = &packers[0];
        assert_eq!(get_string_val(tag_packer, 0), "A");
        assert_eq!(get_string_val(tag_packer, 1), "B");
        assert!(packers[0].is_null(2));
        assert_eq!(get_string_val(tag_packer, 3), "C");
        assert_eq!(get_string_val(tag_packer, 4), "D");
        assert_eq!(get_string_val(tag_packer, 5), "E");
        assert_eq!(get_string_val(tag_packer, 6), "F");

        // int_field values
        let int_field_packer = &packers[1];
        assert_eq!(get_int_val(int_field_packer, 0), 64);
        assert_eq!(get_int_val(int_field_packer, 1), 65);
        assert_eq!(get_int_val(int_field_packer, 2), 66);
        assert!(int_field_packer.is_null(3));
        assert_eq!(get_int_val(int_field_packer, 4), 67);
        assert_eq!(get_int_val(int_field_packer, 5), 68);
        assert_eq!(get_int_val(int_field_packer, 6), 69);

        // float_field values
        let float_field_packer = &packers[2];
        assert!(approximately_equal(
            get_float_val(float_field_packer, 0),
            100.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 1),
            101.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 2),
            102.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 3),
            103.0
        ));
        assert!(float_field_packer.is_null(4));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 5),
            104.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 6),
            105.0
        ));

        // timestamp values
        let timestamp_packer = &packers[3];
        assert_eq!(get_int_val(timestamp_packer, 0), 1_590_488_773_254_420_000);
        assert_eq!(get_int_val(timestamp_packer, 1), 1_590_488_773_254_430_000);
        assert_eq!(get_int_val(timestamp_packer, 2), 1_590_488_773_254_440_000);
        assert_eq!(get_int_val(timestamp_packer, 3), 1_590_488_773_254_450_000);
        assert_eq!(get_int_val(timestamp_packer, 4), 1_590_488_773_254_460_000);
        assert!(timestamp_packer.is_null(5));
        assert_eq!(get_int_val(timestamp_packer, 6), 1_590_488_773_254_470_000);

        Ok(())
    }

    // ----- Tests for LineProtocolConverter -----

    #[test]
    fn conversion_of_no_lines() {
        let parsed_lines = only_good_lines("");
        let log = Arc::new(Mutex::new(WriterLog::new()));

        let settings = ConversionSettings::default();
        let mut converter =
            LineProtocolConverter::new(settings, NoOpWriterSource::new(log.clone()));
        converter
            .convert(parsed_lines)
            .expect("conversion ok")
            .finalize()
            .expect("finalize");

        // no rows should have been written
        assert_eq!(get_events(&log).len(), 0);
    }

    #[test]
    fn conversion_with_multiple_measurements() -> Result<(), Error> {
        // These lines have interleaved measurements to force the
        // state machine in LineProtocolConverter::convert through all
        // the branches
        let parsed_lines = only_good_lines(
            r#"h2o_temperature,location=santa_monica surface_degrees=65.2,bottom_degrees=50.4 1568756160
               air_temperature,location=santa_monica sea_level_degrees=77.3,tenk_feet_feet_degrees=40.0 1568756160
               h2o_temperature,location=santa_monica surface_degrees=63.6,bottom_degrees=49.2 1600756160
               air_temperature,location=santa_monica sea_level_degrees=77.6,tenk_feet_feet_degrees=40.9 1600756160
               h2o_temperature,location=coyote_creek surface_degrees=55.1,bottom_degrees=51.3 1568756160
               air_temperature,location=coyote_creek sea_level_degrees=77.2,tenk_feet_feet_degrees=40.8 1568756160
               air_temperature,location=puget_sound sea_level_degrees=77.5,tenk_feet_feet_degrees=41.1 1568756160
               h2o_temperature,location=coyote_creek surface_degrees=50.2,bottom_degrees=50.9 1600756160
               h2o_temperature,location=puget_sound surface_degrees=55.8,bottom_degrees=40.2 1568756160
"#,
        );
        let log = Arc::new(Mutex::new(WriterLog::new()));

        let settings = ConversionSettings {
            sample_size: 2,
            measurement_write_buffer_size: 3,
        };

        let mut converter =
            LineProtocolConverter::new(settings, NoOpWriterSource::new(log.clone()));

        converter
            .convert(parsed_lines)
            .expect("conversion ok")
            .finalize()
            .expect("finalize");

        assert_eq!(
            get_events(&log),
            vec![
                "Created writer for measurement h2o_temperature",
                "Created writer for measurement air_temperature",
                "[air_temperature] Wrote batch of 4 cols, 3 rows",
                "[h2o_temperature] Wrote batch of 4 cols, 3 rows",
                "[air_temperature] Wrote batch of 4 cols, 1 rows",
                "[air_temperature] Closed",
                "[h2o_temperature] Wrote batch of 4 cols, 2 rows",
                "[h2o_temperature] Closed",
            ]
        );

        Ok(())
    }
}
