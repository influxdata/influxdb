///! Types for mapping and converting series data from TSM indexes produced by
///! InfluxDB >= 2.x
use crate::reader::{BlockDecoder, TSMIndexReader};
use crate::{Block, BlockData, BlockType, TSMError};

use log::warn;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::i64;
use std::io::{BufRead, Seek};
use std::iter::Peekable;

/// `TSMMeasurementMapper` takes a TSM reader and produces an iterator that
/// collects all series data for a given measurement.
///
/// The main purpose of the `TSMMeasurementMapper` is to provide a
/// transformation step that allows one to convert per-series/per-field data
/// into measurement-oriented table data.
///
#[derive(Debug)]
pub struct TSMMeasurementMapper<R>
where
    R: BufRead + Seek,
{
    iter: Peekable<TSMIndexReader<R>>,
}

impl<R> TSMMeasurementMapper<R>
where
    R: BufRead + Seek,
{
    pub fn new(iter: Peekable<TSMIndexReader<R>>) -> Self {
        Self { iter }
    }
}

/// either assign a value from a `Result` or return an error wrapped in an Option.
macro_rules! try_or_some {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => return Some(Err(err)),
        }
    };
}

impl<R: BufRead + Seek> Iterator for TSMMeasurementMapper<R> {
    type Item = Result<MeasurementTable, TSMError>;

    fn next(&mut self) -> Option<Self::Item> {
        // `None` indicates the end of index iteration.
        let entry = try_or_some!(self.iter.next()?);

        let parsed_key = try_or_some!(entry.parse_key());
        let mut measurement: MeasurementTable = MeasurementTable::new(parsed_key.measurement);
        try_or_some!(measurement.add_series_data(
            parsed_key.tagset,
            parsed_key.field_key,
            entry.block_type,
            entry.block
        ));

        // The first index entry for the item has been processed, next keep
        // peeking at subsequent entries in the index until a yielded value is
        // for a different measurement. At that point we will return the
        // measurement.
        while let Some(res) = self.iter.peek() {
            match res {
                Ok(entry) => {
                    let parsed_key = try_or_some!(entry.parse_key());
                    if measurement.name != parsed_key.measurement {
                        // Next entry is for a different measurement.
                        return Some(Ok(measurement));
                    }
                    try_or_some!(measurement.add_series_data(
                        parsed_key.tagset,
                        parsed_key.field_key,
                        entry.block_type,
                        entry.block
                    ));
                }
                Err(e) => return Some(Err(e.clone())),
            }
            self.iter.next(); // advance iterator - we got what we needed from the peek
        }
        Some(Ok(measurement)) // final measurement in index.
    }
}

// FieldKeyBlocks is a mapping between a set of field keys and all of the blocks
// for those keys.
pub type FieldKeyBlocks = BTreeMap<String, Vec<Block>>;

#[derive(Clone, Debug)]
pub struct MeasurementTable {
    pub name: String,
    // Tagset for key --> map of fields with that tagset to their blocks.
    //
    // Here we are mapping each set of field keys (and their blocks) to a unique
    // tag set.
    //
    // One entry in `tag_set_fields_blocks` might be:
    //
    // key: vec![("region", "west"), ("server", "a")]
    // value: {
    //          {key: "temp": vec![*block1*, *block2*},
    //          {key: "current": value: vec![*block1*, *block1*, *block3*]}
    //          {key: "voltage": value: vec![*block1*]}
    // }
    //
    // All of the blocks and fields for `"server"="b"` would be kept under a
    // separate key on `tag_set_fields_blocks`.
    tag_set_fields_blocks: BTreeMap<Vec<(String, String)>, FieldKeyBlocks>,

    tag_columns: BTreeSet<String>,
    field_columns: BTreeMap<String, BlockType>,
}

impl MeasurementTable {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tag_set_fields_blocks: BTreeMap::new(),
            tag_columns: BTreeSet::new(),
            field_columns: BTreeMap::new(),
        }
    }

    pub fn tag_set_fields_blocks(
        &mut self,
    ) -> &mut BTreeMap<Vec<(String, String)>, FieldKeyBlocks> {
        &mut self.tag_set_fields_blocks
    }

    pub fn tag_columns(&self) -> Vec<&String> {
        self.tag_columns.iter().collect()
    }

    pub fn field_columns(&self) -> &BTreeMap<String, BlockType> {
        &self.field_columns
    }

    // updates the table with data from a single TSM index entry's block.
    fn add_series_data(
        &mut self,
        tagset: Vec<(String, String)>,
        field_key: String,
        block_type: BlockType,
        block: Block,
    ) -> Result<(), TSMError> {
        // Invariant: our data model does not support a field column for a
        // measurement table having multiple data types, even though this is
        // supported in InfluxDB.
        if let Some(fk) = self.field_columns.get(&field_key) {
            if *fk != block_type {
                warn!(
                    "Rejected block for field {:?} with type {:?}. \
                    Tagset: {:?}, measurement {:?}. \
                    Field exists with type: {:?}",
                    &field_key, block_type, tagset, self.name, *fk
                );
                return Ok(());
            }
        }

        // tags will be used as the key to a map, where the value will be a
        // collection of all the field keys for that tagset and the associated
        // blocks.
        self.field_columns.insert(field_key.clone(), block_type);
        for (k, _) in &tagset {
            self.tag_columns.insert(k.clone());
        }

        let field_key_blocks = self.tag_set_fields_blocks.entry(tagset).or_default();
        let blocks = field_key_blocks.entry(field_key).or_default();
        blocks.push(block);

        Ok(())
    }
}

impl Display for MeasurementTable {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Measurement: {}", self.name)?;
        writeln!(f, "\nTag Sets:")?;
        for (tagset, field_key_blocks) in &self.tag_set_fields_blocks {
            write!(f, "\t")?;
            for (key, value) in tagset {
                write!(f, "{}={} ", key, value)?;
            }

            writeln!(f, "\n\tField Keys:")?;
            for (field_key, blocks) in field_key_blocks {
                writeln!(f, "\t{}", field_key)?;
                for block in blocks {
                    writeln!(
                        f,
                        "\t\tBlock time-range: ({}, {}), Offset: {}, Size: {}",
                        block.min_time, block.max_time, block.offset, block.size
                    )?;
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

/// `ColumnData` describes various types of nullable block data.
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnData {
    // TODO(edd): perf - I expect it to be much better to track nulls in a
    // separate bitmap.
    Float(Vec<Option<f64>>),
    Integer(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
    Str(Vec<Option<Vec<u8>>>),
    Unsigned(Vec<Option<u64>>),
}

// ValuePair represents a single timestamp-value pair from a TSM block.
#[derive(Debug, PartialEq)]
enum ValuePair {
    F64((i64, f64)),
    I64((i64, i64)),
    Bool((i64, bool)),
    Str((i64, Vec<u8>)),
    U64((i64, u64)),
}

impl ValuePair {
    // returns the timestamp associated with the value pair.
    fn timestamp(&self) -> i64 {
        match *self {
            Self::F64((ts, _)) => ts,
            Self::I64((ts, _)) => ts,
            Self::Bool((ts, _)) => ts,
            Self::Str((ts, _)) => ts,
            Self::U64((ts, _)) => ts,
        }
    }
}

// Maps multiple columnar field blocks to a single tablular representation.
//
// Given a set of field keys and a set of blocks for each key,
// `map_field_columns` aligns each columnar block by the timestamp component to
// produce a single tablular output with one timestamp column, and each value
// column joined by the timestamp values.
//
// For example, here we have three blocks (one block for a different field):
//
// ┌───────────┬───────────┐     ┌───────────┬───────────┐    ┌───────────┬───────────┐
// │    TS     │   Temp    │     │    TS     │  Voltage  │    │    TS     │  Current  │
// ├───────────┼───────────┤     ├───────────┼───────────┤    ├───────────┼───────────┤
// │     1     │   10.2    │     │     1     │   1.23    │    │     2     │   0.332   │
// ├───────────┼───────────┤     ├───────────┼───────────┤    ├───────────┼───────────┤
// │     2     │   11.4    │     │     2     │   1.24    │    │     3     │    0.5    │
// ├───────────┼───────────┤     ├───────────┼───────────┤    ├───────────┼───────────┤
// │     3     │   10.2    │     │     3     │   1.26    │    │     5     │    0.6    │
// └───────────┼───────────┘     └───────────┼───────────┘    └───────────┼───────────┘
//             │                             │                            │
//             │                             │                            │
//             └─────────────────────────────┼────────────────────────────┘
//                                           │
//                                           │
//                                           │
//                                           ▼
//                     ┌──────────┐  ┌──────────┬─────────┬─────────┐
//                     │   Time   │  │ Current  │  Temp   │ Voltage │
//                     ├──────────┤  ├──────────┼─────────┼─────────┤
//                     │    1     │  │   NULL   │  10.2   │  1.23   │
//                     ├──────────┤  ├──────────┼─────────┼─────────┤
//                     │    2     │  │  0.332   │  11.4   │  1.24   │
//                     ├──────────┤  ├──────────┼─────────┼─────────┤
//                     │    3     │  │   0.5    │  10.2   │  1.26   │
//                     ├──────────┤  ├──────────┼─────────┼─────────┤
//                     │    5     │  │   0.6    │  NULL   │  NULL   │
//                     └──────────┘  └──────────┴─────────┴─────────┘
//
// We produce a single time column and a column for each field block. Notice
// that if there is no value for a timestamp that the column entry becomes NULL
// Currently we use an Option(None) variant to represent NULL values but in the
// the future this may be changed to a separate bitmap to track NULL values.
//
// An invariant of the TSM block format is that multiple blocks for the same
// input field will never overlap by time. Once we have mapped a single block
// for a field we can decode and pull the next block for the field and continue
// to build the output.
//
pub fn map_field_columns(
    mut decoder: impl BlockDecoder,
    field_blocks: &mut FieldKeyBlocks,
) -> Result<(Vec<i64>, BTreeMap<String, ColumnData>), TSMError> {
    // This function maintains two main buffers. The first holds the next
    // decoded block for each field in the input fields. `refill_block_buffer`
    // is responsible for determining if each value in the buffer (a decoded
    // block) needs refilling. Refilling involves physically decoding a TSM block
    // using the reader.
    //
    // The second buffer holds the "head" of each of the blocks in the first
    // buffer; these values are tuples of time-stamp and value. Using these
    // values we can essentially do a k-way "join" on the timestamp parts of the
    // tuples, and construct an output row where each field (plus time) are
    // columns.

    // This buffer holds the next decoded block for each input field.
    let mut input_block_buffer = BTreeMap::new();
    refill_block_buffer(&mut decoder, field_blocks, &mut input_block_buffer)?;

    // This buffer holds the head (ts, value) pair in each decoded input block
    // of the input block buffer.
    let mut block_value_buffer: Vec<Option<ValuePair>> = Vec::new();
    block_value_buffer.resize_with(input_block_buffer.len(), || None);
    refill_value_pair_buffer(&mut input_block_buffer, &mut block_value_buffer);

    // Create output columns for each field.
    let mut result = BTreeMap::new();
    for (field_key, block) in &input_block_buffer {
        match block {
            BlockData::Float { .. } => {
                result.insert(field_key.clone(), ColumnData::Float(vec![]));
            }
            BlockData::Integer { .. } => {
                result.insert(field_key.clone(), ColumnData::Integer(vec![]));
            }
            BlockData::Bool { .. } => {
                result.insert(field_key.clone(), ColumnData::Bool(vec![]));
            }
            BlockData::Str { .. } => {
                result.insert(field_key.clone(), ColumnData::Str(vec![]));
            }
            BlockData::Unsigned { .. } => {
                result.insert(field_key.clone(), ColumnData::Unsigned(vec![]));
            }
        }
    }

    // Each iteration of this loop will result in the creation of one output
    // row. Every input block maps to a single column (field) in the output, but
    // a block does not have to have a value for every row. Buffers are only
    // refilled if values have been used during the loop iteration.
    //
    // When all inputs have been drained there is no timestamp available to
    // create a row with and iteration stops.
    let mut timestamps = Vec::new(); // TODO(edd): get hint for pre-allocate
    while let Some(min_ts) = map_blocks_to_columns(&mut block_value_buffer, &mut result) {
        //
        // TODO(edd): Convert nanoseconds into microseconds for Parquet support.
        // Address this in https://github.com/influxdata/delorean/issues/167
        //
        timestamps.push(min_ts / 1000);
        refill_block_buffer(&mut decoder, field_blocks, &mut input_block_buffer)?;
        refill_value_pair_buffer(&mut input_block_buffer, &mut block_value_buffer);
    }

    Ok((timestamps, result))
}

// Given a set of input blocks, where each block comprises two equally sized
// arrays of timestamps and values, join the head of each input block's value
// array by the head of the corresponding timestamp column.
//
fn map_blocks_to_columns(
    blocks: &mut [Option<ValuePair>],
    dst: &mut BTreeMap<String, ColumnData>,
) -> Option<i64> {
    // First determine the minimum timestamp in any of the input blocks or return
    // None if all of the blocks have been drained.
    let min_ts = blocks.iter().flatten().map(ValuePair::timestamp).min()?;

    for (i, column) in dst.values_mut().enumerate() {
        match &mut blocks[i] {
            Some(pair) => {
                // If this candidate has the `min_ts` time-stamp then emit its
                // value to the output column, otherwise emit a None value.
                match pair {
                    ValuePair::F64((ts, value)) => {
                        if let ColumnData::Float(vs) = column {
                            if *ts == min_ts {
                                vs.push(Some(*value));
                                blocks[i] = None;
                            } else {
                                vs.push(None); // block has a value available but timestamp doesn't join
                            }
                        };
                    }
                    ValuePair::I64((ts, value)) => {
                        if let ColumnData::Integer(vs) = column {
                            if *ts == min_ts {
                                vs.push(Some(*value));
                                blocks[i] = None;
                            } else {
                                vs.push(None); // block has a value available but timestamp doesn't join
                            }
                        };
                    }
                    ValuePair::Bool((ts, value)) => {
                        if let ColumnData::Bool(vs) = column {
                            if *ts == min_ts {
                                vs.push(Some(*value));
                                blocks[i] = None;
                            } else {
                                vs.push(None); // block has a value available but timestamp doesn't join
                            }
                        };
                    }
                    ValuePair::Str((ts, value)) => {
                        if let ColumnData::Str(vs) = column {
                            // TODO(edd): perf - Remove this cloning....
                            if *ts == min_ts {
                                vs.push(Some(value.clone()));
                                blocks[i] = None;
                            } else {
                                vs.push(None); // block has a value available but timestamp doesn't join
                            }
                        };
                    }
                    ValuePair::U64((ts, value)) => {
                        if let ColumnData::Unsigned(vs) = column {
                            if *ts == min_ts {
                                vs.push(Some(*value));
                                blocks[i] = None;
                            } else {
                                vs.push(None); // block has a value available but timestamp doesn't join
                            }
                        };
                    }
                }
            }
            // This field value pair doesn't have a value for the min time-stamp
            None => match column {
                ColumnData::Float(vs) => {
                    vs.push(None);
                }
                ColumnData::Integer(vs) => {
                    vs.push(None);
                }
                ColumnData::Bool(vs) => {
                    vs.push(None);
                }
                ColumnData::Str(vs) => {
                    vs.push(None);
                }
                ColumnData::Unsigned(vs) => {
                    vs.push(None);
                }
            },
        }
    }
    Some(min_ts)
}

// Ensures that the next available block for a field is materialised in the
// destination container.
fn refill_block_buffer(
    decoder: &mut impl BlockDecoder,
    field_blocks: &mut FieldKeyBlocks,
    dst: &mut BTreeMap<String, BlockData>,
) -> Result<(), TSMError> {
    // Determine for each input block if the destination container needs
    // refilling.
    for (field, blocks) in field_blocks.iter_mut() {
        if blocks.is_empty() {
            continue; // drained input block
        }

        if let Some(dst_block) = dst.get(field) {
            if !dst_block.is_empty() {
                continue; // not ready to be refilled.
            }
        };

        // Either no block data for field in dst, or the block data that is
        // present has been drained.
        //
        // Pop the next input block for this field key, decode it and refill dst
        // with it.
        let decoded_block = decoder.decode(&blocks.remove(0))?;
        dst.insert(field.clone(), decoded_block);
    }
    Ok(())
}

// Fills any empty (consumed) values from the destination vector with the next
// value from the input set of blocks.
fn refill_value_pair_buffer(
    blocks: &mut BTreeMap<String, BlockData>,
    dst: &mut Vec<Option<ValuePair>>,
) {
    for (i, block) in blocks.values_mut().enumerate() {
        // TODO(edd): seems like this could be DRY'd up a bit??
        // TODO(edd): PERF - removing from vector will shift elements. Better off
        // tracking an index that's been read up to?
        match dst[i] {
            Some(_) => {}
            None => match block {
                BlockData::Float { ts, values } => {
                    if ts.is_empty() {
                        continue;
                    }
                    dst[i] = Some(ValuePair::F64((ts.remove(0), values.remove(0))))
                }
                BlockData::Integer { ts, values } => {
                    if ts.is_empty() {
                        continue;
                    }
                    dst[i] = Some(ValuePair::I64((ts.remove(0), values.remove(0))))
                }
                BlockData::Bool { ts, values } => {
                    if ts.is_empty() {
                        continue;
                    }
                    dst[i] = Some(ValuePair::Bool((ts.remove(0), values.remove(0))))
                }
                BlockData::Str { ts, values } => {
                    if ts.is_empty() {
                        continue;
                    }
                    dst[i] = Some(ValuePair::Str((ts.remove(0), values.remove(0))))
                }
                BlockData::Unsigned { ts, values } => {
                    if ts.is_empty() {
                        continue;
                    }
                    dst[i] = Some(ValuePair::U64((ts.remove(0), values.remove(0))))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::*;

    use libflate::gzip;

    use std::fs::File;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;

    const TSM_FIXTURE_SIZE: usize = 4_222_248;

    #[test]
    fn map_tsm_index() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader =
            TSMIndexReader::try_new(BufReader::new(Cursor::new(buf)), TSM_FIXTURE_SIZE).unwrap();
        let mapper = TSMMeasurementMapper::new(reader.peekable());

        // Although there  are over 2,000 series keys in the TSM file, there are
        // only 121 unique measurements.
        assert_eq!(mapper.count(), 121);
    }

    #[test]
    fn map_field_columns_file() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let index_reader =
            TSMIndexReader::try_new(BufReader::new(Cursor::new(&buf)), TSM_FIXTURE_SIZE).unwrap();
        let mut mapper = TSMMeasurementMapper::new(index_reader.peekable());

        let mut block_reader = TSMBlockReader::new(BufReader::new(Cursor::new(&buf)));

        let mut cpu = mapper
            .find(|m| m.as_ref().unwrap().name == "cpu")
            .unwrap()
            .unwrap();

        // cpu measurement has these 10 field keys on each tagset combination
        let exp_field_keys = vec![
            "usage_guest",
            "usage_guest_nice",
            "usage_idle",
            "usage_iowait",
            "usage_irq",
            "usage_nice",
            "usage_softirq",
            "usage_steal",
            "usage_system",
            "usage_user",
        ];

        for field_blocks in cpu.tag_set_fields_blocks.values_mut() {
            let (_, field_cols) =
                super::map_field_columns(&mut block_reader, field_blocks).unwrap();
            let keys: Vec<_> = field_cols.keys().collect();

            // Every mapping between field blocks should result in columns
            // for every field.
            assert_eq!(keys, exp_field_keys);
        }
    }

    #[test]
    fn measurement_table_columns() {
        let file = File::open("../tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader =
            TSMIndexReader::try_new(BufReader::new(Cursor::new(buf)), TSM_FIXTURE_SIZE).unwrap();
        let mut mapper = TSMMeasurementMapper::new(reader.peekable());

        let cpu = mapper
            .find(|table| table.as_ref().unwrap().name == "cpu")
            .unwrap()
            .unwrap();

        assert_eq!(cpu.tag_columns(), vec!["cpu", "host"]);
        assert_eq!(
            cpu.field_columns().keys().collect::<Vec<_>>(),
            vec![
                "usage_guest",
                "usage_guest_nice",
                "usage_idle",
                "usage_iowait",
                "usage_irq",
                "usage_nice",
                "usage_softirq",
                "usage_steal",
                "usage_system",
                "usage_user"
            ]
        );
    }

    #[test]
    fn conflicting_field_types() -> Result<(), TSMError> {
        let mut table = MeasurementTable::new("cpu".to_string());
        table.add_series_data(
            vec![("region".to_string(), "west".to_string())],
            "value".to_string(),
            BlockType::Float,
            Block {
                max_time: 0,
                min_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Float,
            },
        )?;

        table.add_series_data(
            vec![],
            "value".to_string(),
            BlockType::Integer,
            Block {
                max_time: 0,
                min_time: 0,
                offset: 0,
                size: 0,
                typ: BlockType::Integer,
            },
        )?;

        // The block type for the value field should be Float becuase the
        // conflicting integer field should be ignored.
        assert_eq!(
            *table.field_columns().get("value").unwrap(),
            BlockType::Float,
        );
        Ok(())
    }

    #[test]
    fn fill_value_buffer() {
        // pairs is a helper to generate expected values.
        let pairs = |values: &[(i64, i64)]| -> Vec<Option<ValuePair>> {
            values
                .iter()
                .map(|(t, v)| Some(ValuePair::I64((*t, *v))))
                .collect::<Vec<_>>()
        };

        let mut input = BTreeMap::new();
        input.insert(
            "a".to_string(),
            BlockData::Integer {
                ts: vec![1, 2],
                values: vec![1, 2],
            },
        );

        input.insert(
            "b".to_string(),
            BlockData::Integer {
                ts: vec![1, 2, 3],
                values: vec![10, 20, 30],
            },
        );

        input.insert(
            "c".to_string(),
            BlockData::Integer {
                ts: vec![1, 2, 3],
                values: vec![100, 200, 300],
            },
        );

        let mut dst: Vec<Option<ValuePair>> = vec![None, None, None];

        super::refill_value_pair_buffer(&mut input, &mut dst);
        assert_eq!(dst, pairs(&[(1, 1), (1, 10), (1, 100)]));

        // If the buffer wasn't drained then no new values will be added.
        super::refill_value_pair_buffer(&mut input, &mut dst);
        assert_eq!(dst, pairs(&[(1, 1), (1, 10), (1, 100)]));

        // use up a value
        dst[2] = None;
        super::refill_value_pair_buffer(&mut input, &mut dst);
        assert_eq!(dst, pairs(&[(1, 1), (1, 10), (2, 200)]));

        // consume multiple values
        dst = vec![None, None, None];
        super::refill_value_pair_buffer(&mut input, &mut dst);
        assert_eq!(dst, pairs(&[(2, 2), (2, 20), (3, 300)]));

        // consume values to drain the first and last input
        dst = vec![None, None, None];
        super::refill_value_pair_buffer(&mut input, &mut dst);
        let mut exp = pairs(&[(2, 2), (3, 30), (3, 300)]);
        exp[0] = None;
        exp[2] = None;
        assert_eq!(dst, exp);

        // drain remaining input
        dst = vec![None, None, None];
        super::refill_value_pair_buffer(&mut input, &mut dst);
        assert_eq!(dst, vec![None, None, None]);
    }
}
