//! Types for mapping and converting series data from TSM indexes produced by
//! InfluxDB >= 2.x
use crate::storage::tsm::{Block, TSMReader};
use crate::storage::StorageError;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::io::{BufRead, Seek};
use std::iter::FromIterator;
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
    iter: Peekable<TSMReader<R>>,
}

impl<R> TSMMeasurementMapper<R>
where
    R: BufRead + Seek,
{
    pub fn new(iter: Peekable<TSMReader<R>>) -> TSMMeasurementMapper<R> {
        TSMMeasurementMapper { iter }
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
    type Item = Result<MeasurementTable, StorageError>;

    fn next(&mut self) -> Option<Result<MeasurementTable, StorageError>> {
        let entry = match self.iter.next() {
            Some(entry) => match entry {
                Ok(entry) => entry,
                Err(e) => return Some(Err(e)),
            },
            None => return None, // End of index iteration.
        };

        let parsed_key = match entry.parse_key() {
            Ok(key) => key,
            Err(e) => return Some(Err(e)),
        };
        let mut measurement: MeasurementTable = MeasurementTable::new(parsed_key.measurement);
        try_or_some!(measurement.add_series_data(
            parsed_key.tagset,
            parsed_key.field_key,
            entry.block
        ));

        // The first index entry for the item has been processed, next keep
        // peeking at subsequent entries in the index until a yielded value is
        // for a different measurement. At that point we will
        while let Some(res) = self.iter.peek() {
            match res {
                Ok(entry) => {
                    match entry.parse_key() {
                        Ok(parsed_key) => {
                            if measurement.name != parsed_key.measurement {
                                // Next entry is for a different measurement.
                                return Some(Ok(measurement));
                            }

                            try_or_some!(measurement.add_series_data(
                                parsed_key.tagset,
                                parsed_key.field_key,
                                entry.block
                            ));
                        }
                        Err(e) => return Some(Err(e)),
                    };
                }
                Err(e) => return Some(Err(e.clone())),
            }
            self.iter.next(); // advance iterator - we got what we needed from the peek
        }
        Some(Ok(measurement)) // final measurement in index.
    }
}

// FieldKeyBlocks is a mapping between field keys and all of the blocks
// associated with those keys in a TSM index.
type FieldKeyBlocks = BTreeMap<String, Vec<Block>>;

#[derive(Clone, Debug)]
pub struct MeasurementTable {
    pub name: String,
    // tagset for key --> map of fields with that tagset to their blocks.
    tag_set_fields_blocks: BTreeMap<Vec<(String, String)>, FieldKeyBlocks>,

    // TODO(edd): not sure yet if we need separate sets for tags and fields.
    tag_columns: BTreeSet<String>,
    field_columns: BTreeSet<String>,
}

impl MeasurementTable {
    fn new(name: String) -> Self {
        Self {
            name,
            tag_set_fields_blocks: BTreeMap::new(),
            tag_columns: BTreeSet::new(),
            field_columns: BTreeSet::new(),
        }
    }

    pub fn tag_columns(&self) -> Vec<&String> {
        Vec::from_iter(self.tag_columns.iter())
    }

    pub fn field_columns(&self) -> Vec<&String> {
        Vec::from_iter(self.field_columns.iter())
    }

    // updates the table with data from a single TSM index entry's block.
    fn add_series_data(
        &mut self,
        tagset: Vec<(String, String)>,
        field_key: String,
        block: Block,
    ) -> Result<(), StorageError> {
        // tags will be used as the key to a map, where the value will be a
        // collection of all the field keys for that tagset and the associated
        // blocks.
        self.field_columns.insert(field_key.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use libflate::gzip;
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;

    #[test]
    fn map_tsm_index() {
        let file = File::open("tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader = TSMReader::try_new(BufReader::new(Cursor::new(buf)), 4_222_248).unwrap();
        let mapper = TSMMeasurementMapper::new(reader.peekable());

        // Although there  are over 2,000 series keys in the TSM file, there are
        // only 121 unique measurements.
        assert_eq!(mapper.count(), 121);
    }

    #[test]
    fn measurement_table_columns() {
        let file = File::open("tests/fixtures/000000000000005-000000002.tsm.gz");
        let mut decoder = gzip::Decoder::new(file.unwrap()).unwrap();
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();

        let reader = TSMReader::try_new(BufReader::new(Cursor::new(buf)), 4_222_248).unwrap();
        let mut mapper = TSMMeasurementMapper::new(reader.peekable());

        let cpu = mapper
            .find(|table| table.as_ref().unwrap().name == "cpu")
            .unwrap()
            .unwrap();

        assert_eq!(cpu.tag_columns(), vec!["cpu", "host"]);
        assert_eq!(
            cpu.field_columns(),
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
}
