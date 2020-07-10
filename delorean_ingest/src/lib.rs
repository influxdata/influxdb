//! Library with code for (aspirationally) ingesting various data formats into Delorean
//! Currently supports converting LineProtocol
//! TODO move this to delorean/src/ingest/line_protocol.rs?
#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_table::{
    packers::{Packer, Packers},
    ByteArray, DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError,
};
use delorean_table_schema::{DataType, Schema, SchemaBuilder};
use delorean_tsm::{
    mapper::{ColumnData, MeasurementTable, TSMMeasurementMapper},
    reader::{BlockDecoder, TSMBlockReader, TSMIndexReader},
    BlockType, TSMError,
};
use log::debug;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Seek},
};

#[derive(Debug, Clone, Copy)]
pub struct ConversionSettings {
    /// How many `ParsedLine` structures to buffer before determining the schema
    sample_size: usize,
    // Buffer up tp this many ParsedLines per measurement before writing them
    measurement_write_buffer_size: usize,
}

impl ConversionSettings {}

impl Default for ConversionSettings {
    /// Reasonable default settings
    fn default() -> Self {
        Self {
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
    //
    // NB Use owned strings as key so we can look up by str
    converters: BTreeMap<String, MeasurementConverter<'a>>,

    table_writer_source: Box<dyn DeloreanTableWriterSource>,
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Conversion needs at least one line of data"#))]
    NeedsAtLeastOneLine,

    // Only a single line protocol measurement field is currently supported
    #[snafu(display(r#"More than one measurement not yet supported: Saw new measurement {}, had been using measurement {}"#, actual, expected))]
    OnlyOneMeasurementSupported { expected: String, actual: String },

    #[snafu(display(r#"Error writing to TableWriter: {}"#, source))]
    Writing { source: TableError },

    #[snafu(display(r#"Error creating TableWriter: {}"#, source))]
    WriterCreation { source: TableError },

    #[snafu(display(r#"Error processing TSM File: {}"#, source))]
    TSMProcessing { source: TSMError },

    // TODO clean this error up
    #[snafu(display(r#"could not find ts column"#))]
    CouldNotFindTsColumn,

    // TODO clean this error up
    #[snafu(display(r#"could not find column"#))]
    CouldNotFindColumn,
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

                    let mut writer = MeasurementWriter::new(sampler.settings, schema, table_writer);

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

            let series_measurement = series.measurement.as_str();

            // do not use entry API to avoid copying the key unless it is not present
            let mut converter = match self.converters.get_mut(series_measurement) {
                Some(converter) => converter,
                None => {
                    self.converters.insert(
                        series_measurement.into(),
                        MeasurementConverter::UnknownSchema(MeasurementSampler::new(self.settings)),
                    );
                    self.converters.get_mut(series_measurement).unwrap()
                }
            };

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
        ensure!(!self.schema_sample.is_empty(), NeedsAtLeastOneLine);

        let mut builder = SchemaBuilder::new(self.schema_sample[0].series.measurement.as_str());

        for line in &self.schema_sample {
            let series = &line.series;

            let measurement_name = builder.get_measurement_name();
            ensure!(
                series.measurement == measurement_name,
                OnlyOneMeasurementSupported {
                    actual: measurement_name,
                    expected: &series.measurement,
                }
            );

            if let Some(tag_set) = &series.tag_set {
                for (tag_name, _) in tag_set {
                    builder = builder.tag(tag_name.as_str());
                }
            }
            for (field_name, field_value) in &line.field_set {
                let field_type = match field_value {
                    FieldValue::F64(_) => DataType::Float,
                    FieldValue::I64(_) => DataType::Integer,
                    FieldValue::String(_) => DataType::String,
                    FieldValue::Boolean(_) => DataType::Boolean,
                };
                builder = builder.field(field_name.as_str(), field_type);
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

/// Keeps track of if we have written a value to a particular row
struct PackersForRow<'a> {
    packer: &'a mut Packers,
    wrote_value_for_row: bool,
}

impl<'a> PackersForRow<'a> {
    fn new(packer: &'a mut Packers) -> Self {
        PackersForRow {
            packer,
            wrote_value_for_row: false,
        }
    }
    /// Retrieve the packer and note that we have written to this packer
    fn packer(&mut self) -> &mut Packers {
        assert!(
            !self.wrote_value_for_row,
            "Should only write one value to each column per row"
        );
        self.wrote_value_for_row = true;
        self.packer
    }
    /// Finish writing a row and prepare for the next. If no value has
    /// been written, write a NULL
    fn finish_row(&mut self) {
        if !self.wrote_value_for_row {
            self.packer.push_none();
        }
        self.wrote_value_for_row = false;
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
fn pack_lines<'a>(schema: &Schema, lines: &[ParsedLine<'a>]) -> Vec<Packers> {
    let col_defs = schema.get_col_defs();
    let mut packers: Vec<_> = col_defs
        .iter()
        .enumerate()
        .map(|(idx, col_def)| {
            debug!("  Column definition [{}] = {:?}", idx, col_def);

            // Initialise a Packer<T> for the matching data type wrapped in a
            // Packers enum variant to allow it to live in a vector.
            let mut packer = Packers::from(col_def.data_type);
            packer.reserve_exact(lines.len());
            packer
        })
        .collect();

    // map col_name -> PackerForRow;
    // Use a String as a key (rather than &String) so we can look up via str
    let mut packer_map: BTreeMap<_, _> = col_defs
        .iter()
        .map(|x| x.name.clone())
        .zip(packers.iter_mut().map(|packer| PackersForRow::new(packer)))
        .collect();

    for line in lines {
        let timestamp_col_name = schema.timestamp();

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
                if let Some(packer_for_row) = packer_map.get_mut(tag_name.as_str()) {
                    packer_for_row
                        .packer()
                        .str_packer_mut()
                        .push(ByteArray::from(tag_value.as_str()));
                } else {
                    panic!(
                        "tag {} seen in input that has no matching column in schema",
                        tag_name
                    )
                }
            }
        }

        for (field_name, field_value) in &line.field_set {
            if let Some(packer_for_row) = packer_map.get_mut(field_name.as_str()) {
                let packer = packer_for_row.packer();
                match *field_value {
                    FieldValue::F64(f) => {
                        packer.f64_packer_mut().push(f);
                    }
                    FieldValue::I64(i) => {
                        packer.i64_packer_mut().push(i);
                    }
                    FieldValue::String(ref s) => {
                        packer.str_packer_mut().push(ByteArray::from(s.as_str()));
                    }
                    FieldValue::Boolean(b) => {
                        packer.bool_packer_mut().push(b);
                    }
                }
            } else {
                panic!(
                    "field {} seen in input that has no matching column in schema",
                    field_name
                )
            }
        }

        if let Some(packer_for_row) = packer_map.get_mut(timestamp_col_name) {
            // The Rust implementation of the parquet writer doesn't support
            // Nanosecond precision for timestamps yet, so we downconvert them here
            // to microseconds
            let timestamp_micros = line.timestamp.map(|timestamp_nanos| timestamp_nanos / 1000);

            // TODO(edd) why would line _not_ have a timestamp??? We should always have them
            packer_for_row
                .packer()
                .i64_packer_mut()
                .push_option(timestamp_micros)
        } else {
            panic!("No {} field present in schema...", timestamp_col_name);
        }

        // Now, go over all packers and add missing values if needed
        for packer_for_row in packer_map.values_mut() {
            packer_for_row.finish_row();
        }
    }
    packers
}

/// Converts a TSM file into the delorean_table internal columnar
/// data format and then passes that converted data to a
/// `DeloreanTableWriter`
pub struct TSMFileConverter {
    table_writer_source: Box<dyn DeloreanTableWriterSource>,
}

impl TSMFileConverter {
    pub fn new(table_writer_source: Box<dyn DeloreanTableWriterSource>) -> Self {
        Self {
            table_writer_source,
        }
    }

    /// Given one or more sets of readers, converts the underlying TSM data into
    /// a set of Parquet files segmented by measurement name.
    pub fn convert<R>(
        &mut self,
        index_readers: Vec<(R, usize)>,
        mut block_readers: Vec<R>,
    ) -> Result<(), Error>
    where
        R: BufRead + Seek,
    {
        if index_readers.is_empty() {
            return Err(Error::TSMProcessing {
                source: TSMError {
                    description: "at least one reader required".to_string(),
                },
            });
        } else if index_readers.len() != block_readers.len() {
            return Err(Error::TSMProcessing {
                source: TSMError {
                    description: "different number of readers".to_string(),
                },
            });
        }

        let mut dst = vec![None; index_readers.len()];
        let mut mappers = Vec::with_capacity(index_readers.len());

        for (i, (reader, size)) in index_readers.into_iter().enumerate() {
            let index_reader = TSMIndexReader::try_new(reader, size).context(TSMProcessing)?;
            mappers.push(TSMMeasurementMapper::new(index_reader.peekable(), i));
        }

        // track all the block readers for each file, so that the correct reader
        // can be used to decode each block
        let mut block_reader = TSMBlockReader::new(block_readers.remove(0));
        for reader in block_readers.into_iter() {
            block_reader.add_reader(reader);
        }

        loop {
            dst = Self::refill_input_tables(&mut mappers, dst)?;
            let res = Self::merge_input_tables(dst)?;
            let next_measurement = res.0;
            dst = res.1;

            match next_measurement {
                Some(mut table) => {
                    // convert (potentially merged) measurement..
                    let (schema, packed_columns) =
                        Self::process_measurement_table(&mut block_reader, &mut table)?;
                    let mut table_writer = self
                        .table_writer_source
                        .next_writer(&schema)
                        .context(WriterCreation)?;

                    table_writer
                        .write_batch(&packed_columns)
                        .context(WriterCreation)?;
                    table_writer.close().context(WriterCreation)?;
                }
                None => break,
            }
        }
        Ok(())
    }

    // Given a set of input tables, identifies the next table (lexicographically)
    // and then merges any identical tables into the first one.
    //
    // Returns the merged table and the remaining set of inputs to be
    // subsequently processed.
    fn merge_input_tables(
        mut inputs: Vec<Option<MeasurementTable>>,
    ) -> Result<(Option<MeasurementTable>, Vec<Option<MeasurementTable>>), Error> {
        // track each table's position in the input vector. If tables are chosen
        // they will be moved out of the input vector later on.
        let mut input_map: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (i, table) in inputs.iter().enumerate() {
            if let Some(table) = table {
                let v = input_map.entry(table.name.clone()).or_default();
                v.push(i);
            }
        }

        // tables are now organised together by the table's measurement name and
        // ordered lexicographically. The first item in the sorted map is the
        // next measurement that needs to be merged and processed.
        if let Some((_, table_indexes)) = input_map.into_iter().next() {
            let mut iter = table_indexes.into_iter(); // merge each of these into first one

            // unwrap is safe because all hashmap entries have at least one table index.
            let first_table_index = iter.next().unwrap();
            // unwrap is safe because the indexes in iter all point to non-none
            // tables.
            let mut first_table = inputs[first_table_index].take().unwrap();

            // if there are multiple tables for this measurement merge them
            // into the first one.
            for idx in iter {
                // TODO(edd): perf - could calculate a more efficient merge
                // order.
                let mut next_table = inputs[idx].take().unwrap();
                first_table.merge(&mut next_table).context(TSMProcessing)?;
            }
            return Ok((Some(first_table), inputs));
        }

        // no measurements to process; all inputs drained
        Ok((None, inputs))
    }

    // Ensures that the destination vector has the next
    // measurement table for each input iterator.
    fn refill_input_tables(
        inputs: &mut Vec<impl Iterator<Item = Result<MeasurementTable, TSMError>>>,
        mut dst: Vec<Option<MeasurementTable>>,
    ) -> Result<Vec<Option<MeasurementTable>>, Error> {
        for (input, dst) in inputs.iter_mut().zip(dst.iter_mut()) {
            if dst.is_none() {
                match input.next() {
                    Some(res) => {
                        let table = res.context(TSMProcessing)?;
                        *dst = Some(table);
                    }
                    None => continue,
                }
            }
        }
        Ok(dst)
    }

    // Given a measurement table `process_measurement_table` produces an
    // appropriate schema and set of Packers.
    fn process_measurement_table(
        mut block_reader: impl BlockDecoder,
        m: &mut MeasurementTable,
    ) -> Result<(Schema, Vec<Packers>), Error> {
        let mut builder = SchemaBuilder::new(&m.name);
        let mut packed_columns: Vec<Packers> = Vec::new();

        let mut tks = Vec::new();
        for tag in m.tag_columns() {
            builder = builder.tag(tag);
            tks.push(tag.clone());
            packed_columns.push(Packers::String(Packer::new()));
        }

        let mut fks = Vec::new();
        for (field_key, block_type) in m.field_columns().to_owned() {
            builder = builder.field(&field_key, DataType::from(&block_type));
            fks.push((field_key.clone(), block_type));
            packed_columns.push(Packers::from(block_type));
        }

        // Account for timestamp
        packed_columns.push(Packers::Integer(Packer::new()));

        let schema = builder.build();

        // get mapping between named columns and packer indexes.
        let name_packer = schema
            .get_col_defs()
            .iter()
            .map(|c| (c.name.clone(), c.index as usize))
            .collect::<BTreeMap<String, usize>>();

        // Process the measurement to build out a table.
        //
        // The processing function we supply to `process` does the following:
        //
        //  - Append the timestamp column to the packer timestamp column
        //  - Materialise the same tag value for any tag key columns where the
        //    emitted section has a none-null value for that column.
        //  - Materialise NULL values for any tag key columns that we don't have
        //    data for in the emitted section.
        //  - Append the field columns to the packer field columns. The emitted
        //    section will already have fully materialised the data for these
        //    columns, including any NULL entries.
        //  - Materialise NULL values for any field columns that the emitted
        //    section does not have any data for.
        //
        m.process(
            &mut block_reader,
            |section: delorean_tsm::mapper::TableSection| -> Result<(), TSMError> {
                // number of rows in each column in this table section.
                let col_len = section.len();

                // if this is the first section of the table then we can avoid
                // extending slices and just move the slice over to the packer
                // vector.
                //
                // TODO(edd): will the compiler just figure this out for us w.r.t
                // `extend_from_slice`??
                let first_table_section = section.is_first();

                // Process the timestamp column.
                let ts_idx = name_packer
                    .get(schema.timestamp())
                    .context(CouldNotFindTsColumn)
                    .map_err(|e| TSMError {
                        description: e.to_string(),
                    })?;

                if section.is_first() {
                    packed_columns[*ts_idx] = Packers::from(section.ts);
                } else {
                    packed_columns[*ts_idx]
                        .i64_packer_mut()
                        .extend_from_slice(&section.ts);
                }

                // Process any tag columns that this section has values for.
                // We have to materialise the values for the column, which are
                // guaranteed to be the same.
                for (tag_key, tag_value) in &section.tag_cols {
                    let idx = name_packer
                        .get(tag_key)
                        .context(CouldNotFindColumn)
                        .map_err(|e| TSMError {
                            description: e.to_string(),
                        })?;

                    // this will create a column of repeated values.
                    if first_table_section {
                        packed_columns[*idx] = Packers::from_elem_str(tag_value, col_len);
                    } else {
                        packed_columns[*idx]
                            .str_packer_mut()
                            .extend_from_slice(&vec![ByteArray::from(tag_value.as_ref()); col_len]);
                    }
                }

                // Not all tag columns may be present in the section. For those
                // that are not present we need to materialise NULL values for
                // every row.
                let tag_keys = section
                    .tag_cols
                    .iter()
                    .map(|pair| pair.0.clone())
                    .collect::<BTreeSet<_>>();
                for key in &tks {
                    if tag_keys.contains(key) {
                        continue;
                    }

                    let idx = name_packer
                        .get(key)
                        .context(CouldNotFindColumn)
                        .map_err(|e| TSMError {
                            description: e.to_string(),
                        })?;

                    if first_table_section {
                        // creates a column of repeated None values.
                        let col: Vec<Option<Vec<u8>>> = vec![None; col_len];
                        packed_columns[*idx] = Packers::from(col);
                    } else {
                        // pad out column with None values because we don't have a
                        // value for it.
                        packed_columns[*idx]
                            .str_packer_mut()
                            .fill_with_null(col_len);
                    }
                }

                // Next we will write out all of the field columns for this
                // section.
                let mut got_field_cols = Vec::new();
                for (field_key, field_values) in section.field_cols {
                    let idx = name_packer
                        .get(&field_key)
                        .context(CouldNotFindColumn)
                        .map_err(|e| TSMError {
                            description: e.to_string(),
                        })?;

                    if first_table_section {
                        match field_values {
                            ColumnData::Float(v) => packed_columns[*idx] = Packers::from(v),
                            ColumnData::Integer(v) => packed_columns[*idx] = Packers::from(v),
                            ColumnData::Str(v) => packed_columns[*idx] = Packers::from(v),
                            ColumnData::Bool(v) => packed_columns[*idx] = Packers::from(v),
                            ColumnData::Unsigned(v) => packed_columns[*idx] = Packers::from(v),
                        }
                    } else {
                        match field_values {
                            ColumnData::Float(v) => packed_columns[*idx]
                                .f64_packer_mut()
                                .extend_from_option_slice(&v),
                            ColumnData::Integer(v) => packed_columns[*idx]
                                .i64_packer_mut()
                                .extend_from_option_slice(&v),
                            ColumnData::Str(values) => {
                                let col = packed_columns[*idx].str_packer_mut();
                                for value in values {
                                    match value {
                                        Some(v) => col.push(ByteArray::from(v)),
                                        None => col.push_option(None),
                                    }
                                }
                            }
                            ColumnData::Bool(v) => packed_columns[*idx]
                                .bool_packer_mut()
                                .extend_from_option_slice(&v),
                            ColumnData::Unsigned(values) => {
                                let col = packed_columns[*idx].i64_packer_mut();
                                for value in values {
                                    match value {
                                        Some(v) => col.push(v as i64),
                                        None => col.push_option(None),
                                    }
                                }
                            }
                        }
                    }
                    got_field_cols.push(field_key);
                }

                // Finally, materialise NULL values for all of the field columns
                // that this section does not have any values for
                for (key, field_type) in &fks {
                    if got_field_cols.contains(key) {
                        continue;
                    }

                    let idx = name_packer
                        .get(key)
                        .context(CouldNotFindColumn)
                        .map_err(|e| TSMError {
                            description: e.to_string(),
                        })?;

                    // this will create a column of repeated None values.
                    if first_table_section {
                        match field_type {
                            BlockType::Float => {
                                let col: Vec<Option<f64>> = vec![None; col_len];
                                packed_columns[*idx] = Packers::from(col);
                            }
                            BlockType::Integer => {
                                let col: Vec<Option<i64>> = vec![None; col_len];
                                packed_columns[*idx] = Packers::from(col);
                            }
                            BlockType::Bool => {
                                let col: Vec<Option<bool>> = vec![None; col_len];
                                packed_columns[*idx] = Packers::from(col);
                            }
                            BlockType::Str => {
                                let col: Vec<Option<Vec<u8>>> = vec![None; col_len];
                                packed_columns[*idx] = Packers::from(col);
                            }
                            BlockType::Unsigned => {
                                let col: Vec<Option<u64>> = vec![None; col_len];
                                packed_columns[*idx] = Packers::from(col);
                            }
                        }
                    } else {
                        match field_type {
                            BlockType::Float => {
                                packed_columns[*idx]
                                    .f64_packer_mut()
                                    .fill_with_null(col_len);
                            }
                            BlockType::Integer => {
                                packed_columns[*idx]
                                    .i64_packer_mut()
                                    .fill_with_null(col_len);
                            }
                            BlockType::Bool => {
                                packed_columns[*idx]
                                    .bool_packer_mut()
                                    .fill_with_null(col_len);
                            }
                            BlockType::Str => {
                                packed_columns[*idx]
                                    .str_packer_mut()
                                    .fill_with_null(col_len);
                            }
                            BlockType::Unsigned => {
                                packed_columns[*idx]
                                    .i64_packer_mut()
                                    .fill_with_null(col_len);
                            }
                        }
                    }
                }
                Ok(())
            },
        )
        .context(TSMProcessing)?;
        Ok((schema, packed_columns))
    }
}

impl std::fmt::Debug for TSMFileConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TSMFileConverter")
            .field("table_writer_source", &"DYNAMIC")
            .finish()
    }
}

#[cfg(test)]
mod delorean_ingest_tests {
    use super::*;
    use delorean_table::{
        DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError, Packers,
    };
    use delorean_table_schema::ColumnDefinition;
    use delorean_test_helpers::approximately_equal;
    use delorean_tsm::{
        reader::{BlockData, MockBlockDecoder},
        Block,
    };

    use libflate::gzip;
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;

    use std::sync::{Arc, Mutex};

    /// Record what happens when the writer is created so we can
    /// inspect it as part of the tests. It uses string manipulation
    /// for quick test writing and easy debugging
    struct WriterLog {
        events: Vec<String>,
    }

    impl WriterLog {
        fn new() -> Self {
            Self { events: Vec::new() }
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
            Self {
                log,
                measurement_name,
            }
        }
    }

    impl DeloreanTableWriter for NoOpWriter {
        fn write_batch(&mut self, packers: &[Packers]) -> Result<(), TableError> {
            if packers.is_empty() {
                log_event(
                    &self.log,
                    format!(
                        "[{}] Wrote no data; no packers passed",
                        self.measurement_name
                    ),
                );
            }

            let rows_written = packers
                .iter()
                .fold(packers[0].num_rows(), |cur_len, packer| {
                    assert_eq!(
                        packer.num_rows(),
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
            Box::new(Self { log })
        }
    }

    impl DeloreanTableWriterSource for NoOpWriterSource {
        fn next_writer(
            &mut self,
            schema: &Schema,
        ) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
            let measurement_name = schema.measurement();
            log_event(
                &self.log,
                format!("Created writer for measurement {}", measurement_name),
            );
            Ok(Box::new(NoOpWriter::new(
                self.log.clone(),
                measurement_name.to_string(),
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
            ColumnDefinition::new("time", 3, DataType::Timestamp)
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
            ColumnDefinition::new("time", 3, DataType::Timestamp)
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
            ColumnDefinition::new("time", 4, DataType::Timestamp)
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
            ColumnDefinition::new("time", 3, DataType::Timestamp)
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
            ColumnDefinition::new("time", 2, DataType::Timestamp)
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
            Err(Error::OnlyOneMeasurementSupported { .. })
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
    // as expected.
    //
    // Note this table has a row with a tag and each field type and
    // then a row where the tag, fields and timestamps each hold a null
    static LP_DATA: &str = r#"
               cpu,tag1=A int_field=64i,float_field=100.0,str_field="foo1",bool_field=t 1590488773254420000
               cpu,tag1=B int_field=65i,float_field=101.0,str_field="foo2",bool_field=t 1590488773254430000
               cpu        int_field=66i,float_field=102.0,str_field="foo3",bool_field=t 1590488773254440000
               cpu,tag1=C               float_field=103.0,str_field="foo4",bool_field=t 1590488773254450000
               cpu,tag1=D int_field=67i,str_field="foo5",bool_field=t                   1590488773254460000
               cpu,tag1=E int_field=68i,float_field=104.0,bool_field=t                  1590488773254470000
               cpu,tag1=F int_field=69i,float_field=105.0,str_field="foo6"              1590488773254480000
               cpu,tag1=G int_field=70i,float_field=106.0,str_field="foo7",bool_field=t
               cpu,tag1=H int_field=71i,float_field=107.0,str_field="foo8",bool_field=t 1590488773254490000
             "#;
    static EXPECTED_NUM_LINES: usize = 9;

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
        assert_eq!(cols.len(), 6);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("int_field", 1, DataType::Integer)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("float_field", 2, DataType::Float)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("str_field", 3, DataType::String)
        );
        assert_eq!(
            cols[4],
            ColumnDefinition::new("bool_field", 4, DataType::Boolean)
        );

        Ok(())
    }

    #[test]
    fn pack_data_value() -> Result<(), Error> {
        let mut sampler = parse_data_into_sampler()?;
        let schema = sampler.deduce_schema_from_sample()?;

        let packers = pack_lines(&schema, &sampler.schema_sample);

        // 6 columns so 6 packers
        assert_eq!(packers.len(), 6);

        // all packers should have packed all lines
        for p in &packers {
            assert_eq!(p.num_rows(), EXPECTED_NUM_LINES);
        }

        // Tag values
        let tag_packer = packers[0].str_packer();
        assert_eq!(tag_packer.get(0).unwrap(), &ByteArray::from("A"));
        assert_eq!(tag_packer.get(1).unwrap(), &ByteArray::from("B"));
        assert!(packers[0].is_null(2));
        assert_eq!(tag_packer.get(3).unwrap(), &ByteArray::from("C"));
        assert_eq!(tag_packer.get(4).unwrap(), &ByteArray::from("D"));
        assert_eq!(tag_packer.get(5).unwrap(), &ByteArray::from("E"));
        assert_eq!(tag_packer.get(6).unwrap(), &ByteArray::from("F"));
        assert_eq!(tag_packer.get(7).unwrap(), &ByteArray::from("G"));
        assert_eq!(tag_packer.get(8).unwrap(), &ByteArray::from("H"));

        // int_field values
        let int_field_packer = &packers[1].i64_packer();
        assert_eq!(int_field_packer.get(0).unwrap(), &64);
        assert_eq!(int_field_packer.get(1).unwrap(), &65);
        assert_eq!(int_field_packer.get(2).unwrap(), &66);
        assert!(int_field_packer.is_null(3));
        assert_eq!(int_field_packer.get(4).unwrap(), &67);
        assert_eq!(int_field_packer.get(5).unwrap(), &68);
        assert_eq!(int_field_packer.get(6).unwrap(), &69);
        assert_eq!(int_field_packer.get(7).unwrap(), &70);
        assert_eq!(int_field_packer.get(8).unwrap(), &71);

        // float_field values
        let float_field_packer = &packers[2].f64_packer();
        assert!(approximately_equal(
            *float_field_packer.get(0).unwrap(),
            100.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(1).unwrap(),
            101.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(2).unwrap(),
            102.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(3).unwrap(),
            103.0
        ));
        assert!(float_field_packer.is_null(4));
        assert!(approximately_equal(
            *float_field_packer.get(5).unwrap(),
            104.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(6).unwrap(),
            105.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(7).unwrap(),
            106.0
        ));
        assert!(approximately_equal(
            *float_field_packer.get(8).unwrap(),
            107.0
        ));

        // str_field values
        let str_field_packer = &packers[3].str_packer();
        assert_eq!(str_field_packer.get(0).unwrap(), &ByteArray::from("foo1"));
        assert_eq!(str_field_packer.get(1).unwrap(), &ByteArray::from("foo2"));
        assert_eq!(str_field_packer.get(2).unwrap(), &ByteArray::from("foo3"));
        assert_eq!(str_field_packer.get(3).unwrap(), &ByteArray::from("foo4"));
        assert_eq!(str_field_packer.get(4).unwrap(), &ByteArray::from("foo5"));
        assert!(str_field_packer.is_null(5));
        assert_eq!(str_field_packer.get(6).unwrap(), &ByteArray::from("foo6"));
        assert_eq!(str_field_packer.get(7).unwrap(), &ByteArray::from("foo7"));
        assert_eq!(str_field_packer.get(8).unwrap(), &ByteArray::from("foo8"));

        // bool_field values
        let bool_field_packer = &packers[4].bool_packer();
        assert_eq!(bool_field_packer.get(0).unwrap(), &true);
        assert_eq!(bool_field_packer.get(1).unwrap(), &true);
        assert_eq!(bool_field_packer.get(2).unwrap(), &true);
        assert_eq!(bool_field_packer.get(3).unwrap(), &true);
        assert_eq!(bool_field_packer.get(4).unwrap(), &true);
        assert_eq!(bool_field_packer.get(5).unwrap(), &true);
        assert!(bool_field_packer.is_null(6));
        assert_eq!(bool_field_packer.get(7).unwrap(), &true);
        assert_eq!(bool_field_packer.get(8).unwrap(), &true);

        // timestamp values (NB The timestamps are truncated to Microseconds)
        let timestamp_packer = &packers[5].i64_packer();
        assert_eq!(timestamp_packer.get(0).unwrap(), &1_590_488_773_254_420);
        assert_eq!(timestamp_packer.get(1).unwrap(), &1_590_488_773_254_430);
        assert_eq!(timestamp_packer.get(2).unwrap(), &1_590_488_773_254_440);
        assert_eq!(timestamp_packer.get(3).unwrap(), &1_590_488_773_254_450);
        assert_eq!(timestamp_packer.get(4).unwrap(), &1_590_488_773_254_460);
        assert_eq!(timestamp_packer.get(5).unwrap(), &1_590_488_773_254_470);
        assert_eq!(timestamp_packer.get(6).unwrap(), &1_590_488_773_254_480);
        assert!(timestamp_packer.is_null(7));
        assert_eq!(timestamp_packer.get(8).unwrap(), &1_590_488_773_254_490);

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

    // ----- Tests for TSM Data -----

    #[test]
    fn process_measurement_table() -> Result<(), Box<dyn std::error::Error>> {
        // Input data - in line protocol format
        //
        // cpu,region=east temp=1.2 0
        // cpu,region=east voltage=10.2 0
        //
        // cpu,region=east temp=1.2 1000
        // cpu,region=east voltage=10.2 1000
        //
        // cpu,region=east temp=1.4 2000
        // cpu,region=east voltage=10.4 2000
        // cpu,region=west,server=a temp=100.2 2000
        //
        // cpu,az=b watts=1000 3000
        // cpu,region=west,server=a temp=99.5 3000
        //
        // cpu,az=b watts=2000 4000
        // cpu,region=west,server=a temp=100.3 4000
        //
        // cpu,az=b watts=3000 5000

        // Expected output table
        //
        // |  az  | region | server |  temp | voltage |  watts  | time |
        // |------|--------|--------|-------|---------|---------|------|
        // | b    |  NULL  |  NULL  | NULL  |  NULL   |   1000  | 3000 |
        // | b    |  NULL  |  NULL  | NULL  |  NULL   |   2000  | 4000 |
        // | b    |  NULL  |  NULL  | NULL  |  NULL   |   3000  | 5000 |
        // | NULL |  east  |  NULL  | 1.2   |  10.2   |   NULL  | 0000 |  <-- notice series joined on ts column
        // | NULL |  east  |  NULL  | 1.2   |  10.2   |   NULL  | 1000 |  <-- notice series joined on ts column
        // | NULL |  east  |  NULL  | 1.4   |  10.4   |   NULL  | 2000 |  <-- notice series joined on ts column
        // | NULL |  west  |    a   | 100.2 |  NULL   |   NULL  | 2000 |
        // | NULL |  west  |    a   | 99.5  |  NULL   |   NULL  | 3000 |
        // | NULL |  west  |    a   | 100.3 |  NULL   |   NULL  | 4000 |

        let mut table = MeasurementTable::new("cpu".to_string(), 0);
        // cpu region=east temp=<all the block data for this key>
        table.add_series_data(
            vec![("region".to_string(), "east".to_string())],
            "temp".to_string(),
            Block {
                min_time: 0,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Float,
                reader_idx: 0,
            },
        )?;

        // cpu region=east voltage=<all the block data for this key>
        table.add_series_data(
            vec![("region".to_string(), "east".to_string())],
            "voltage".to_string(),
            Block {
                min_time: 1,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Float,
                reader_idx: 0,
            },
        )?;

        // cpu region=west,server=a temp=<all the block data for this key>
        table.add_series_data(
            vec![
                ("region".to_string(), "west".to_string()),
                ("server".to_string(), "a".to_string()),
            ],
            "temp".to_string(),
            Block {
                min_time: 2,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Float,
                reader_idx: 0,
            },
        )?;

        // cpu az=b watts=<all the block data for this key>
        table.add_series_data(
            vec![("az".to_string(), "b".to_string())],
            "watts".to_string(),
            Block {
                min_time: 3,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Unsigned,
                reader_idx: 0,
            },
        )?;

        let mut block_map = BTreeMap::new();
        block_map.insert(
            0,
            BlockData::Float {
                i: 0,
                ts: vec![0, 1000, 2000],
                values: vec![1.2, 1.2, 1.4],
            },
        );
        block_map.insert(
            1,
            BlockData::Float {
                i: 0,
                ts: vec![0, 1000, 2000],
                values: vec![10.2, 10.2, 10.4],
            },
        );
        block_map.insert(
            2,
            BlockData::Float {
                i: 0,
                ts: vec![2000, 3000, 4000],
                values: vec![100.2, 99.5, 100.3],
            },
        );
        block_map.insert(
            3,
            BlockData::Unsigned {
                i: 0,
                ts: vec![3000, 4000, 5000],
                values: vec![1000, 2000, 3000],
            },
        );

        let decoder = MockBlockDecoder::new(block_map);
        let (schema, packers) = TSMFileConverter::process_measurement_table(decoder, &mut table)?;

        let expected_defs = vec![
            ColumnDefinition::new("az", 0, DataType::String),
            ColumnDefinition::new("region", 1, DataType::String),
            ColumnDefinition::new("server", 2, DataType::String),
            ColumnDefinition::new("temp", 3, DataType::Float),
            ColumnDefinition::new("voltage", 4, DataType::Float),
            ColumnDefinition::new("watts", 5, DataType::Integer),
            ColumnDefinition::new("time", 6, DataType::Timestamp),
        ];

        assert_eq!(schema.get_col_defs(), expected_defs);
        // az column
        assert_eq!(
            packers[0],
            Packers::String(Packer::from(vec![
                Some(ByteArray::from("b")),
                Some(ByteArray::from("b")),
                Some(ByteArray::from("b")),
                None,
                None,
                None,
                None,
                None,
                None,
            ]))
        );
        // region column
        assert_eq!(
            packers[1],
            Packers::String(Packer::from(vec![
                None,
                None,
                None,
                Some(ByteArray::from("east")),
                Some(ByteArray::from("east")),
                Some(ByteArray::from("east")),
                Some(ByteArray::from("west")),
                Some(ByteArray::from("west")),
                Some(ByteArray::from("west")),
            ]))
        );
        // server column
        assert_eq!(
            packers[2],
            Packers::String(Packer::from(vec![
                None,
                None,
                None,
                None,
                None,
                None,
                Some(ByteArray::from("a")),
                Some(ByteArray::from("a")),
                Some(ByteArray::from("a")),
            ]))
        );
        // temp column
        assert_eq!(
            packers[3],
            Packers::Float(Packer::from(vec![
                None,
                None,
                None,
                Some(1.2),
                Some(1.2),
                Some(1.4),
                Some(100.2),
                Some(99.5),
                Some(100.3),
            ]))
        );
        // voltage column
        assert_eq!(
            packers[4],
            Packers::Float(Packer::from(vec![
                None,
                None,
                None,
                Some(10.2),
                Some(10.2),
                Some(10.4),
                None,
                None,
                None,
            ]))
        );
        // watts column
        assert_eq!(
            packers[5],
            Packers::Integer(Packer::from(vec![
                Some(1000),
                Some(2000),
                Some(3000),
                None,
                None,
                None,
                None,
                None,
                None,
            ]))
        );
        // timestamp column
        assert_eq!(
            packers[6],
            Packers::Integer(Packer::from(vec![
                Some(3),
                Some(4),
                Some(5),
                Some(0),
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(4),
            ]))
        );

        Ok(())
    }

    fn empty_block() -> Block {
        Block {
            min_time: 0,
            max_time: 0,
            offset: 0,
            size: 0,
            typ: BlockType::Float,
            reader_idx: 0,
        }
    }

    #[test]
    fn merge_input_tables() -> Result<(), Box<dyn std::error::Error>> {
        let mut inputs = vec![];
        let mut table = MeasurementTable::new("cpu".to_string(), 0);
        table.add_series_data(
            vec![("region".to_string(), "east".to_string())],
            "temp".to_string(),
            empty_block(),
        )?;
        inputs.push(Some(table.clone()));

        table = MeasurementTable::new("cpu".to_string(), 1);
        table.add_series_data(
            vec![("server".to_string(), "a".to_string())],
            "temp".to_string(),
            empty_block(),
        )?;
        inputs.push(Some(table.clone()));

        table = MeasurementTable::new("disk".to_string(), 2);
        table.add_series_data(
            vec![("region".to_string(), "west".to_string())],
            "temp".to_string(),
            empty_block(),
        )?;
        inputs.push(Some(table));

        let mut res = TSMFileConverter::merge_input_tables(inputs)?;
        let mut merged = res.0.unwrap();
        inputs = res.1;
        assert_eq!(merged.name, "cpu".to_string());
        assert_eq!(merged.tag_columns(), vec!["region", "server"]);
        assert_eq!(inputs[0], None);
        assert_eq!(inputs[1], None);

        res = TSMFileConverter::merge_input_tables(inputs)?;
        merged = res.0.unwrap();
        assert_eq!(merged.name, "disk".to_string());
        assert_eq!(res.1, vec![None, None, None]);
        Ok(())
    }

    #[test]
    fn conversion_tsm_file_single() -> Result<(), Error> {
        let file = File::open("../tests/fixtures/merge-tsm/merge_a.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let log = Arc::new(Mutex::new(WriterLog::new()));
        let mut converter = TSMFileConverter::new(NoOpWriterSource::new(log.clone()));
        let index_steam = BufReader::new(Cursor::new(&buf));
        let block_stream = BufReader::new(Cursor::new(&buf));
        converter
            .convert(vec![(index_steam, 39475)], vec![block_stream])
            .unwrap();

        // CPU columns: - tags: cpu, host. (2)
        //                fields: usage_guest, usage_guest_nice
        //                        usage_idle, usage_iowait, usage_irq,
        //                        usage_nice, usage_softirq, usage_steal,
        //                        usage_system, usage_user (10)
        //                timestamp (1)
        //
        // disk columns: - tags: device, fstype, host, mode, path (5)
        //                 fields: free, inodes_free, inodes_total, inodes_used,
        //                         total, used, used_percent (7)
        //                 timestamp (1)
        assert_eq!(
            get_events(&log),
            vec![
                "Created writer for measurement cpu",
                "[cpu] Wrote batch of 13 cols, 85 rows",
                "[cpu] Closed",
                "Created writer for measurement disk",
                "[disk] Wrote batch of 13 cols, 36 rows",
                "[disk] Closed"
            ],
        );

        Ok(())
    }

    #[test]
    fn conversion_tsm_files_none_overlapping() -> Result<(), Error> {
        let mut index_streams = Vec::new();
        let mut block_streams = Vec::new();

        let file_a = File::open("../tests/fixtures/merge-tsm/merge_a.tsm.gz");
        let mut decoder_a = gzip::Decoder::new(file_a.unwrap()).unwrap();
        let mut buf_a = Vec::new();
        decoder_a.read_to_end(&mut buf_a).unwrap();
        index_streams.push((BufReader::new(Cursor::new(&buf_a)), 39475));
        block_streams.push(BufReader::new(Cursor::new(&buf_a)));

        let file_b = File::open("../tests/fixtures/merge-tsm/merge_b.tsm.gz");
        let mut decoder_b = gzip::Decoder::new(file_b.unwrap()).unwrap();
        let mut buf_b = Vec::new();
        decoder_b.read_to_end(&mut buf_b).unwrap();
        index_streams.push((BufReader::new(Cursor::new(&buf_b)), 45501));
        block_streams.push(BufReader::new(Cursor::new(&buf_b)));

        let log = Arc::new(Mutex::new(WriterLog::new()));
        let mut converter = TSMFileConverter::new(NoOpWriterSource::new(log.clone()));

        converter.convert(index_streams, block_streams).unwrap();

        // CPU columns: - tags: cpu, host. (2)
        //                fields: usage_guest, usage_guest_nice
        //                        usage_idle, usage_iowait, usage_irq,
        //                        usage_nice, usage_softirq, usage_steal,
        //                        usage_system, usage_user (10)
        //                timestamp (1)
        //
        // disk columns: - tags: device, fstype, host, mode, path (5)
        //                 fields: free, inodes_free, inodes_total, inodes_used,
        //                         total, used, used_percent (7)
        //                 timestamp (1)
        //
        // In this case merge_a.tsm has 85 rows of data for cpu measurement and
        // merge_b.tsm 340.
        //
        // For the disk measurement merge_a.tsm has 36 rows and merge_b.tsm 126
        assert_eq!(
            get_events(&log),
            vec![
                "Created writer for measurement cpu",
                "[cpu] Wrote batch of 13 cols, 425 rows",
                "[cpu] Closed",
                "Created writer for measurement disk",
                "[disk] Wrote batch of 13 cols, 162 rows",
                "[disk] Closed"
            ],
        );

        Ok(())
    }
}
