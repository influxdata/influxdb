//! Code to decode [`MutableBatch`] from pbdata protobuf

use generated_types::influxdata::pbdata::v1::{
    column::{SemanticType, Values as PbValues},
    Column as PbColumn, DatabaseBatch, PackedStrings, TableBatch,
};
use hashbrown::{HashMap, HashSet};
use mutable_batch::{writer::Writer, MutableBatch};
use schema::{InfluxColumnType, InfluxFieldType, TIME_COLUMN_NAME};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

/// Error type for line protocol conversion
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("error writing column {}: {}", column, source))]
    Write {
        source: mutable_batch::writer::Error,
        column: String,
    },

    #[snafu(display("duplicate column name: {}", column))]
    DuplicateColumnName { column: String },

    #[snafu(display("table batch must contain time column"))]
    MissingTime,

    #[snafu(display("time column must not contain nulls"))]
    NullTime,

    #[snafu(display("column with no values: {}", column))]
    EmptyColumn { column: String },

    #[snafu(display("column missing dictionary: {}", column))]
    MissingDictionary { column: String },

    #[snafu(display(
        "column \"{}\" contains invalid offset {} at index {}",
        column,
        offset,
        index
    ))]
    InvalidOffset {
        column: String,
        offset: usize,
        index: usize,
    },

    #[snafu(display("column \"{}\" contains more than one type of values", column))]
    MultipleValues { column: String },

    #[snafu(display("cannot infer type for column: {}", column))]
    InvalidType { column: String },
}

/// Result type for pbdata conversion
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Decodes a [`DatabaseBatch`] to a map of [`MutableBatch`] keyed by table ID
pub fn decode_database_batch(database_batch: &DatabaseBatch) -> Result<HashMap<i64, MutableBatch>> {
    let mut id_to_data = HashMap::with_capacity(database_batch.table_batches.len());

    for table_batch in &database_batch.table_batches {
        let batch = id_to_data.entry(table_batch.table_id).or_default();

        write_table_batch(batch, table_batch)?;
    }

    Ok(id_to_data)
}

/// Writes the provided [`TableBatch`] to a [`MutableBatch`] on error any changes made
/// to `batch` are reverted
pub fn write_table_batch(batch: &mut MutableBatch, table_batch: &TableBatch) -> Result<()> {
    let to_insert = table_batch.row_count as usize;
    if to_insert == 0 {
        return Ok(());
    }

    // Verify columns are unique
    let mut columns = HashSet::with_capacity(table_batch.columns.len());
    for col in &table_batch.columns {
        ensure!(
            columns.insert(col.column_name.as_str()),
            DuplicateColumnNameSnafu {
                column: &col.column_name
            }
        );
    }

    // Batch must contain a time column
    ensure!(columns.contains(TIME_COLUMN_NAME), MissingTimeSnafu);

    let mut writer = Writer::new(batch, to_insert);
    for column in &table_batch.columns {
        let influx_type = pb_column_type(column)?;
        let valid_mask = compute_valid_mask(&column.null_mask, to_insert);
        let valid_mask = valid_mask.as_deref();

        // Already verified has values
        let values = column.values.as_ref().unwrap();

        match influx_type {
            InfluxColumnType::Field(InfluxFieldType::Float) => writer.write_f64(
                &column.column_name,
                valid_mask,
                RepeatLastElement::new(values.f64_values.iter().cloned()),
            ),
            InfluxColumnType::Field(InfluxFieldType::Integer) => writer.write_i64(
                &column.column_name,
                valid_mask,
                RepeatLastElement::new(values.i64_values.iter().cloned()),
            ),
            InfluxColumnType::Field(InfluxFieldType::UInteger) => writer.write_u64(
                &column.column_name,
                valid_mask,
                RepeatLastElement::new(values.u64_values.iter().cloned()),
            ),
            InfluxColumnType::Tag => {
                if let Some(interned) = values.interned_string_values.as_ref() {
                    let dictionary =
                        interned
                            .dictionary
                            .as_ref()
                            .context(MissingDictionarySnafu {
                                column: &column.column_name,
                            })?;
                    validate_packed_string(&column.column_name, dictionary)?;
                    writer.write_tag_dict(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(interned.values.iter().map(|x| *x as usize)),
                        packed_strings_iter(dictionary),
                    )
                } else if let Some(packed) = values.packed_string_values.as_ref() {
                    validate_packed_string(&column.column_name, packed)?;
                    writer.write_tag(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(packed_strings_iter(packed)),
                    )
                } else {
                    writer.write_tag(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(values.string_values.iter().map(|x| x.as_str())),
                    )
                }
            }
            InfluxColumnType::Field(InfluxFieldType::String) => {
                if let Some(interned) = values.interned_string_values.as_ref() {
                    let dictionary =
                        interned
                            .dictionary
                            .as_ref()
                            .context(MissingDictionarySnafu {
                                column: &column.column_name,
                            })?;

                    validate_packed_string(&column.column_name, dictionary)?;
                    writer.write_string(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(
                            interned
                                .values
                                .iter()
                                .map(|x| packed_string_idx(dictionary, *x as usize)),
                        ),
                    )
                } else if let Some(packed) = values.packed_string_values.as_ref() {
                    validate_packed_string(&column.column_name, packed)?;
                    writer.write_string(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(packed_strings_iter(packed)),
                    )
                } else {
                    writer.write_string(
                        &column.column_name,
                        valid_mask,
                        RepeatLastElement::new(values.string_values.iter().map(|x| x.as_str())),
                    )
                }
            }
            InfluxColumnType::Field(InfluxFieldType::Boolean) => writer.write_bool(
                &column.column_name,
                valid_mask,
                RepeatLastElement::new(values.bool_values.iter().cloned()),
            ),
            InfluxColumnType::Timestamp => {
                ensure!(valid_mask.is_none(), NullTimeSnafu);
                writer.write_time(
                    &column.column_name,
                    RepeatLastElement::new(values.i64_values.iter().cloned()),
                )
            }
        }
        .context(WriteSnafu {
            column: &column.column_name,
        })?;
    }

    writer.commit();
    Ok(())
}

/// Inner state of [`RepeatLastElement`].
enum RepeatLastElementInner<I>
where
    I: Iterator,
    I::Item: Clone,
{
    /// The iteration is running and the iterator hasn't ended yet.
    Running { it: I, next: I::Item },

    /// The iterator has ended and we're repeating the last element by cloning it.
    Repeating { element: I::Item },

    /// The iterator was empty.
    Empty,
}

/// Iterator wrapper that repeats the last element forever.
///
/// This will just yield `None` if the wrapped iterator was empty.
struct RepeatLastElement<I>
where
    I: Iterator,
    I::Item: Clone,
{
    /// Inner state, wrapped into an option to make the borrow-checker happy.
    inner: Option<RepeatLastElementInner<I>>,
}

impl<I> RepeatLastElement<I>
where
    I: Iterator,
    I::Item: Clone,
{
    fn new(mut it: I) -> Self {
        let inner = match it.next() {
            Some(next) => RepeatLastElementInner::Running { it, next },
            None => RepeatLastElementInner::Empty,
        };

        Self { inner: Some(inner) }
    }
}

impl<I> Iterator for RepeatLastElement<I>
where
    I: Iterator,
    I::Item: Clone,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.take().expect("should be set") {
            RepeatLastElementInner::Running { mut it, next } => {
                match it.next() {
                    Some(next2) => {
                        self.inner = Some(RepeatLastElementInner::Running { it, next: next2 });
                    }
                    None => {
                        self.inner = Some(RepeatLastElementInner::Repeating {
                            element: next.clone(),
                        });
                    }
                }
                Some(next)
            }
            RepeatLastElementInner::Repeating { element } => {
                let element_cloned = element.clone();
                self.inner = Some(RepeatLastElementInner::Repeating { element });
                Some(element_cloned)
            }
            RepeatLastElementInner::Empty => {
                self.inner = Some(RepeatLastElementInner::Empty);
                None
            }
        }
    }
}

/// Validates that the packed strings array is valid
fn validate_packed_string(column: &str, strings: &PackedStrings) -> Result<()> {
    let mut last_offset = match strings.offsets.first() {
        Some(first) => *first as usize,
        None => return Ok(()),
    };

    for (index, offset) in strings.offsets.iter().enumerate().skip(1) {
        let offset = *offset as usize;
        if offset < last_offset || !strings.values.is_char_boundary(offset) {
            return InvalidOffsetSnafu {
                column,
                offset,
                index,
            }
            .fail();
        }
        last_offset = offset;
    }
    Ok(())
}

/// Indexes a [`PackedStrings`]
///
/// # Panic
///
/// - if the index is beyond the bounds
/// - if the index is not at a UTF-8 character boundary
fn packed_string_idx(strings: &PackedStrings, idx: usize) -> &str {
    let start_offset = strings.offsets[idx] as usize;
    let end_offset = strings.offsets[idx + 1] as usize;
    &strings.values[start_offset..end_offset]
}

/// Returns an iterator over the strings in a [`PackedStrings`]
///
/// # Panic
///
/// If the offsets array is not an increasing sequence of numbers less than
/// the length of the strings array
fn packed_strings_iter(strings: &PackedStrings) -> impl Iterator<Item = &str> + '_ {
    let mut last_offset = strings.offsets.first().cloned().unwrap_or_default() as usize;
    strings.offsets.iter().skip(1).map(move |next_offset| {
        let next_offset = *next_offset as usize;
        let string = &strings.values[last_offset..next_offset];
        last_offset = next_offset;
        string
    })
}

/// Converts a potentially truncated null mask to a valid mask
fn compute_valid_mask(null_mask: &[u8], to_insert: usize) -> Option<Vec<u8>> {
    if null_mask.is_empty() || null_mask.iter().all(|x| *x == 0) {
        return None;
    }

    // The expected length of the validity mask
    let expected_len = (to_insert + 7) >> 3;

    // The number of bits over the byte boundary
    let overrun = to_insert & 7;

    let mut mask: Vec<_> = (0..expected_len)
        .map(|x| match null_mask.get(x) {
            Some(v) => !*v,
            None => 0xFF,
        })
        .collect();

    if overrun != 0 {
        *mask.last_mut().unwrap() &= (1 << overrun) - 1;
    }

    Some(mask)
}

fn pb_column_type(col: &PbColumn) -> Result<InfluxColumnType> {
    let values = col.values.as_ref().context(EmptyColumnSnafu {
        column: &col.column_name,
    })?;

    let value_type = pb_value_type(&col.column_name, values)?;
    let semantic_type = SemanticType::from_i32(col.semantic_type);

    match (semantic_type, value_type) {
        (Some(SemanticType::Tag), InfluxFieldType::String) => Ok(InfluxColumnType::Tag),
        (Some(SemanticType::Field), field) => Ok(InfluxColumnType::Field(field)),
        (Some(SemanticType::Time), InfluxFieldType::Integer)
            if col.column_name.as_str() == TIME_COLUMN_NAME =>
        {
            Ok(InfluxColumnType::Timestamp)
        }
        _ => InvalidTypeSnafu {
            column: &col.column_name,
        }
        .fail(),
    }
}

fn pb_value_type(column: &str, values: &PbValues) -> Result<InfluxFieldType> {
    let mut ret = None;
    let mut set_type = |field: InfluxFieldType| -> Result<()> {
        match ret {
            Some(_) => MultipleValuesSnafu { column }.fail(),
            None => {
                ret = Some(field);
                Ok(())
            }
        }
    };

    if !values.string_values.is_empty() {
        set_type(InfluxFieldType::String)?;
    }

    if values.packed_string_values.is_some() {
        set_type(InfluxFieldType::String)?;
    }

    if values.interned_string_values.is_some() {
        set_type(InfluxFieldType::String)?;
    }

    if !values.i64_values.is_empty() {
        set_type(InfluxFieldType::Integer)?;
    }

    if !values.u64_values.is_empty() {
        set_type(InfluxFieldType::UInteger)?;
    }

    if !values.f64_values.is_empty() {
        set_type(InfluxFieldType::Float)?;
    }

    if !values.bool_values.is_empty() {
        set_type(InfluxFieldType::Boolean)?;
    }

    ret.context(EmptyColumnSnafu { column })
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;
    use generated_types::influxdata::pbdata::v1::InternedStrings;
    use schema::Projection;

    use super::*;

    fn column(name: &str, semantic_type: SemanticType) -> PbColumn {
        PbColumn {
            column_name: name.to_string(),
            semantic_type: semantic_type as _,
            values: None,
            null_mask: vec![],
        }
    }

    fn empty_values() -> PbValues {
        PbValues {
            i64_values: vec![],
            f64_values: vec![],
            u64_values: vec![],
            string_values: vec![],
            bool_values: vec![],
            bytes_values: vec![],
            packed_string_values: None,
            interned_string_values: None,
        }
    }

    fn with_strings(mut column: PbColumn, values: Vec<&str>, nulls: Vec<u8>) -> PbColumn {
        let mut v = empty_values();
        v.string_values = values.iter().map(ToString::to_string).collect();
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_packed_strings(
        mut column: PbColumn,
        values: PackedStrings,
        nulls: Vec<u8>,
    ) -> PbColumn {
        let mut v = empty_values();
        v.packed_string_values = Some(values);
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_interned_strings(
        mut column: PbColumn,
        values: InternedStrings,
        nulls: Vec<u8>,
    ) -> PbColumn {
        let mut v = empty_values();
        v.interned_string_values = Some(values);
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_i64(mut column: PbColumn, values: Vec<i64>, nulls: Vec<u8>) -> PbColumn {
        let mut v = empty_values();
        v.i64_values = values;
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_u64(mut column: PbColumn, values: Vec<u64>, nulls: Vec<u8>) -> PbColumn {
        let mut v = empty_values();
        v.u64_values = values;
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_f64(mut column: PbColumn, values: Vec<f64>, nulls: Vec<u8>) -> PbColumn {
        let mut v = empty_values();
        v.f64_values = values;
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    fn with_bool(mut column: PbColumn, values: Vec<bool>, nulls: Vec<u8>) -> PbColumn {
        let mut v = empty_values();
        v.bool_values = values;
        column.null_mask = nulls;
        column.values = Some(v);
        column
    }

    #[test]
    fn test_packed_strings_iter() {
        let s = PackedStrings {
            values: "".to_string(),
            offsets: vec![],
        };
        assert_eq!(packed_strings_iter(&s).count(), 0);

        let s = PackedStrings {
            values: "".to_string(),
            offsets: vec![0],
        };
        assert_eq!(packed_strings_iter(&s).count(), 0);

        let s = PackedStrings {
            values: "fooboo".to_string(),
            offsets: vec![0, 3, 6],
        };
        let r: Vec<_> = packed_strings_iter(&s).collect();

        assert_eq!(r, vec!["foo", "boo"]);
    }

    #[test]
    fn test_column_type() {
        let mut column = column("test", SemanticType::Time);

        let e = pb_column_type(&column).unwrap_err().to_string();
        assert_eq!(e, "column with no values: test");

        let mut values = empty_values();
        values.i64_values = vec![2];
        values.f64_values = vec![32.];
        column.values = Some(values);

        let e = pb_column_type(&column).unwrap_err().to_string();
        assert_eq!(e, "column \"test\" contains more than one type of values");

        let mut values = empty_values();
        values.string_values = vec!["hello".to_string()];
        values.packed_string_values = Some(PackedStrings {
            values: "".to_string(),
            offsets: vec![],
        });
        column.values = Some(values);

        let e = pb_column_type(&column).unwrap_err().to_string();
        assert_eq!(e, "column \"test\" contains more than one type of values");

        let mut values = empty_values();
        values.string_values = vec!["hello".to_string()];
        values.interned_string_values = Some(InternedStrings {
            dictionary: None,
            values: vec![],
        });
        column.values = Some(values);

        let e = pb_column_type(&column).unwrap_err().to_string();
        assert_eq!(e, "column \"test\" contains more than one type of values");
    }

    #[test]
    fn test_basic() {
        let mut table_batch = TableBatch {
            columns: vec![
                with_strings(
                    column("tag1", SemanticType::Tag),
                    vec!["v1", "v1", "v2", "v2", "v1"],
                    vec![],
                ),
                with_strings(
                    column("tag2", SemanticType::Tag),
                    vec!["v2", "v3"],
                    vec![0b00010101],
                ),
                with_f64(
                    column("f64", SemanticType::Field),
                    vec![3., 5.],
                    vec![0b00001101],
                ),
                with_i64(
                    column("i64", SemanticType::Field),
                    vec![56, 2],
                    vec![0b00001110],
                ),
                with_i64(
                    column("time", SemanticType::Time),
                    vec![1, 2, 3, 4, 5],
                    vec![0b00000000],
                ),
                with_u64(
                    column("u64", SemanticType::Field),
                    vec![4, 3, 2, 1],
                    vec![0b00000100],
                ),
            ],
            row_count: 5,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();

        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+-----+-----+------+------+--------------------------------+-----+",
            "| f64 | i64 | tag1 | tag2 | time                           | u64 |",
            "+-----+-----+------+------+--------------------------------+-----+",
            "|     | 56  | v1   |      | 1970-01-01T00:00:00.000000001Z | 4   |",
            "| 3   |     | v1   | v2   | 1970-01-01T00:00:00.000000002Z | 3   |",
            "|     |     | v2   |      | 1970-01-01T00:00:00.000000003Z |     |",
            "|     |     | v2   | v3   | 1970-01-01T00:00:00.000000004Z | 2   |",
            "| 5   | 2   | v1   |      | 1970-01-01T00:00:00.000000005Z | 1   |",
            "+-----+-----+------+------+--------------------------------+-----+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        table_batch.columns.push(table_batch.columns[0].clone());

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "duplicate column name: tag1");

        table_batch.columns.pop();

        // Missing time column -> error
        let mut time = table_batch.columns.remove(4);
        assert_eq!(time.column_name.as_str(), "time");

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "table batch must contain time column");

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        // Nulls in time column -> error
        time.null_mask = vec![1];
        table_batch.columns.push(time);

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "time column must not contain nulls");

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        // Missing values -> error
        table_batch.columns[0].values.take().unwrap();

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "column with no values: tag1");

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        // No data -> error
        table_batch.columns[0].values = Some(PbValues {
            i64_values: vec![],
            f64_values: vec![],
            u64_values: vec![],
            string_values: vec![],
            bool_values: vec![],
            bytes_values: vec![],
            packed_string_values: None,
            interned_string_values: None,
        });

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "column with no values: tag1");

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
    }

    #[test]
    fn test_strings() {
        let table_batch = TableBatch {
            columns: vec![
                with_packed_strings(
                    column("tag1", SemanticType::Tag),
                    PackedStrings {
                        values: "helloinfluxdata".to_string(),
                        offsets: vec![0, 5, 11, 11, 15],
                    },
                    vec![0b000010010],
                ),
                with_packed_strings(
                    column("tag2", SemanticType::Tag),
                    PackedStrings {
                        values: "helloworld".to_string(),
                        offsets: vec![0, 5, 10],
                    },
                    vec![0b000111010],
                ),
                with_packed_strings(
                    column("s1", SemanticType::Field),
                    PackedStrings {
                        values: "cupcakesareawesome".to_string(),
                        offsets: vec![0, 8, 11, 18],
                    },
                    vec![0b000110010],
                ),
                with_interned_strings(
                    column("tag3", SemanticType::Tag),
                    InternedStrings {
                        dictionary: Some(PackedStrings {
                            values: "tag1tag2".to_string(),
                            offsets: vec![0, 4, 8, 8],
                        }),
                        values: vec![0, 1, 1, 0, 2, 1],
                    },
                    vec![0b000000000],
                ),
                with_interned_strings(
                    column("s2", SemanticType::Field),
                    InternedStrings {
                        dictionary: Some(PackedStrings {
                            values: "v1v2v3".to_string(),
                            offsets: vec![0, 2, 4, 6],
                        }),
                        values: vec![0, 1, 2],
                    },
                    vec![0b000011010],
                ),
                with_i64(
                    column("time", SemanticType::Time),
                    vec![1, 2, 3, 4, 5, 6],
                    vec![],
                ),
            ],
            row_count: 6,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();
        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+----------+----+--------+-------+------+--------------------------------+",
            "| s1       | s2 | tag1   | tag2  | tag3 | time                           |",
            "+----------+----+--------+-------+------+--------------------------------+",
            "| cupcakes | v1 | hello  | hello | tag1 | 1970-01-01T00:00:00.000000001Z |",
            "|          |    |        |       | tag2 | 1970-01-01T00:00:00.000000002Z |",
            "| are      | v2 | influx | world | tag2 | 1970-01-01T00:00:00.000000003Z |",
            "| awesome  |    |        |       | tag1 | 1970-01-01T00:00:00.000000004Z |",
            "|          |    |        |       |      | 1970-01-01T00:00:00.000000005Z |",
            "|          | v3 | data   |       | tag2 | 1970-01-01T00:00:00.000000006Z |",
            "+----------+----+--------+-------+------+--------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        // Try to write 6 rows expecting an error
        let mut try_write = |other: PbColumn, expected_err: &str| {
            let table_batch = TableBatch {
                columns: vec![
                    with_i64(
                        column("time", SemanticType::Time),
                        vec![1, 2, 3, 4, 5, 6],
                        vec![],
                    ),
                    other,
                ],
                row_count: 6,
                table_id: 42,
            };

            let err = write_table_batch(&mut batch, &table_batch)
                .unwrap_err()
                .to_string();

            assert_eq!(err, expected_err);
            assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
        };

        try_write(
            with_packed_strings(
                column("s1", SemanticType::Tag),
                PackedStrings {
                    values: "helloworld".to_string(),
                    offsets: vec![0, 5, 11],
                },
                vec![0b000111010],
            ),
            "column \"s1\" contains invalid offset 11 at index 2",
        );

        try_write(
            with_packed_strings(
                column("s1", SemanticType::Field),
                PackedStrings {
                    values: "helloworld".to_string(),
                    offsets: vec![0, 5, 4],
                },
                vec![0b000111010],
            ),
            "column \"s1\" contains invalid offset 4 at index 2",
        );

        try_write(
            with_packed_strings(
                column("tag2", SemanticType::Field),
                PackedStrings {
                    values: "helloworld".to_string(),
                    offsets: vec![0, 5, 10],
                },
                vec![0b000111010],
            ),
            "error writing column tag2: Unable to insert iox::column_type::field::string type into column tag2 with type iox::column_type::tag",
        );

        try_write(
            with_packed_strings(
                column("tag2", SemanticType::Tag),
                PackedStrings {
                    values: "hello😀world".to_string(),
                    offsets: vec![0, 6, 10],
                },
                vec![0b000111010],
            ),
            "column \"tag2\" contains invalid offset 6 at index 1",
        );

        try_write(
            with_interned_strings(
                column("tag3", SemanticType::Tag),
                InternedStrings {
                    dictionary: Some(PackedStrings {
                        values: "tag1tag2".to_string(),
                        offsets: vec![0, 4, 8, 8],
                    }),
                    values: vec![0, 1, 3, 0, 2, 1],
                },
                vec![0b000000000],
            ),
            "error writing column tag3: Key not found in dictionary: 3",
        );

        try_write(
            with_interned_strings(
                column("tag3", SemanticType::Tag),
                InternedStrings {
                    dictionary: Some(PackedStrings {
                        values: "tag1tag2".to_string(),
                        offsets: vec![0, 4, 3, 8],
                    }),
                    values: vec![0, 1, 1, 0, 2, 1],
                },
                vec![0b000000000],
            ),
            "column \"tag3\" contains invalid offset 3 at index 2",
        );
    }

    #[test]
    fn test_optimization_trim_null_masks() {
        // See https://github.com/influxdata/influxdb-pb-data-protocol#optimization-1-trim-null-masks
        let table_batch = TableBatch {
            columns: vec![
                with_i64(
                    column("i64", SemanticType::Field),
                    vec![1, 2, 3, 4, 5, 6, 7],
                    vec![0b11000001],
                ),
                with_i64(
                    column("time", SemanticType::Time),
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    vec![0b00000000],
                ),
            ],
            row_count: 10,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();

        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+-----+--------------------------------+",
            "| i64 | time                           |",
            "+-----+--------------------------------+",
            "|     | 1970-01-01T00:00:00.000000001Z |",
            "| 1   | 1970-01-01T00:00:00.000000002Z |",
            "| 2   | 1970-01-01T00:00:00.000000003Z |",
            "| 3   | 1970-01-01T00:00:00.000000004Z |",
            "| 4   | 1970-01-01T00:00:00.000000005Z |",
            "| 5   | 1970-01-01T00:00:00.000000006Z |",
            "|     | 1970-01-01T00:00:00.000000007Z |",
            "|     | 1970-01-01T00:00:00.000000008Z |",
            "| 6   | 1970-01-01T00:00:00.000000009Z |",
            "| 7   | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
    }

    #[test]
    fn test_optimization_omit_null_masks() {
        // See https://github.com/influxdata/influxdb-pb-data-protocol#optimization-1b-omit-empty-null-masks
        let table_batch = TableBatch {
            columns: vec![with_i64(
                column("time", SemanticType::Time),
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                vec![],
            )],
            row_count: 9,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();

        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+--------------------------------+",
            "| time                           |",
            "+--------------------------------+",
            "| 1970-01-01T00:00:00.000000001Z |",
            "| 1970-01-01T00:00:00.000000002Z |",
            "| 1970-01-01T00:00:00.000000003Z |",
            "| 1970-01-01T00:00:00.000000004Z |",
            "| 1970-01-01T00:00:00.000000005Z |",
            "| 1970-01-01T00:00:00.000000006Z |",
            "| 1970-01-01T00:00:00.000000007Z |",
            "| 1970-01-01T00:00:00.000000008Z |",
            "| 1970-01-01T00:00:00.000000009Z |",
            "+--------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
    }

    #[test]
    fn test_optimization_trim_repeated_tail_values() {
        // See https://github.com/influxdata/influxdb-pb-data-protocol#optimization-2-trim-repeated-tail-values
        let table_batch = TableBatch {
            columns: vec![
                with_strings(
                    column("f_s", SemanticType::Field),
                    vec!["s1", "s2", "s3"],
                    vec![0b11000001],
                ),
                with_interned_strings(
                    column("f_i", SemanticType::Field),
                    InternedStrings {
                        dictionary: Some(PackedStrings {
                            values: "s1s2".to_string(),
                            offsets: vec![0, 2, 4],
                        }),
                        values: vec![0, 1, 0],
                    },
                    vec![0b11000001],
                ),
                with_packed_strings(
                    column("f_p", SemanticType::Field),
                    PackedStrings {
                        values: "s1s2s3".to_string(),
                        offsets: vec![0, 2, 4, 6],
                    },
                    vec![0b11000001],
                ),
                with_strings(
                    column("t_s", SemanticType::Tag),
                    vec!["s1", "s2", "s3"],
                    vec![0b11000001],
                ),
                with_interned_strings(
                    column("t_i", SemanticType::Tag),
                    InternedStrings {
                        dictionary: Some(PackedStrings {
                            values: "s1s2".to_string(),
                            offsets: vec![0, 2, 4],
                        }),
                        values: vec![0, 1, 0],
                    },
                    vec![0b11000001],
                ),
                with_packed_strings(
                    column("t_p", SemanticType::Tag),
                    PackedStrings {
                        values: "s1s2s3".to_string(),
                        offsets: vec![0, 2, 4, 6],
                    },
                    vec![0b11000001],
                ),
                with_bool(
                    column("bool", SemanticType::Field),
                    vec![false, false, true],
                    vec![0b11000001],
                ),
                with_f64(
                    column("f64", SemanticType::Field),
                    vec![1.1, 2.2, 3.3],
                    vec![0b11000001],
                ),
                with_i64(
                    column("i64", SemanticType::Field),
                    vec![1, 2, 3],
                    vec![0b11000001],
                ),
                with_u64(
                    column("u64", SemanticType::Field),
                    vec![1, 2, 3],
                    vec![0b11000001],
                ),
                with_i64(column("time", SemanticType::Time), vec![1, 2, 3], vec![]),
            ],
            row_count: 9,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();

        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+-------+-----+-----+-----+-----+-----+-----+-----+-----+--------------------------------+-----+",
            "| bool  | f64 | f_i | f_p | f_s | i64 | t_i | t_p | t_s | time                           | u64 |",
            "+-------+-----+-----+-----+-----+-----+-----+-----+-----+--------------------------------+-----+",
            "|       |     |     |     |     |     |     |     |     | 1970-01-01T00:00:00.000000001Z |     |",
            "| false | 1.1 | s1  | s1  | s1  | 1   | s1  | s1  | s1  | 1970-01-01T00:00:00.000000002Z | 1   |",
            "| false | 2.2 | s2  | s2  | s2  | 2   | s2  | s2  | s2  | 1970-01-01T00:00:00.000000003Z | 2   |",
            "| true  | 3.3 | s1  | s3  | s3  | 3   | s1  | s3  | s3  | 1970-01-01T00:00:00.000000003Z | 3   |",
            "| true  | 3.3 | s1  | s3  | s3  | 3   | s1  | s3  | s3  | 1970-01-01T00:00:00.000000003Z | 3   |",
            "| true  | 3.3 | s1  | s3  | s3  | 3   | s1  | s3  | s3  | 1970-01-01T00:00:00.000000003Z | 3   |",
            "|       |     |     |     |     |     |     |     |     | 1970-01-01T00:00:00.000000003Z |     |",
            "|       |     |     |     |     |     |     |     |     | 1970-01-01T00:00:00.000000003Z |     |",
            "| true  | 3.3 | s1  | s3  | s3  | 3   | s1  | s3  | s3  | 1970-01-01T00:00:00.000000003Z | 3   |",
            "+-------+-----+-----+-----+-----+-----+-----+-----+-----+--------------------------------+-----+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

        // we need at least one value though
        let table_batch = TableBatch {
            columns: vec![with_i64(column("time", SemanticType::Time), vec![], vec![])],
            row_count: 9,
            table_id: 42,
        };

        let mut batch = MutableBatch::new();

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "column with no values: time");
    }
}
