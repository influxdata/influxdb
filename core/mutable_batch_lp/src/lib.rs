//! Code to convert line protocol to [`MutableBatch`]

#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
use workspace_hack as _;

use hashbrown::{HashMap, HashSet, hash_map::Entry};
use influxdb_line_protocol::{FieldValue, ParsedLine, parse_lines};
use mutable_batch::MutableBatch;
use mutable_batch::writer::Writer;
use schema::builder::ColumnInsertValidator;
use snafu::{ResultExt, Snafu};

/// A limit on the number of errors to return from a partial LP write.
const MAXIMUM_RETURNED_ERRORS: usize = 100;

/// Error type for a conversion attempt on a set of line protocol lines
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display(
        "errors encountered on line(s):\n{}",
        itertools::join(lines.iter(), "\n")
    ))]
    PerLine { lines: Vec<LineError> },

    #[snafu(display("empty write payload"))]
    EmptyPayload,
}

/// Errors which occur independently per line
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum LineError {
    #[snafu(display("error parsing line {} (1-based): {}", line, source))]
    LineProtocol {
        source: influxdb_line_protocol::Error,
        line: usize,
    },

    #[snafu(display("error writing line {} (1-based): {}", line, source))]
    Write { source: LineWriteError, line: usize },

    #[snafu(display("timestamp overflows i64 on line {} (1-based)", line))]
    TimestampOverflow { line: usize },
}

/// Result type for line protocol conversion
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Statistics about a line-protocol payload
#[derive(Debug, Copy, Clone, Default)]
pub struct PayloadStatistics {
    /// The number of fields
    pub num_fields: usize,
    /// The number of lines
    pub num_lines: usize,
}

/// Converts line protocol to a set of [`MutableBatch`]
#[derive(Debug)]
pub struct LinesConverter {
    /// The timestamp for non-timestamped rows
    default_time: i64,
    /// The multiplier to convert input timestamps to nanoseconds
    timestamp_base: i64,
    /// The statistics
    stats: PayloadStatistics,
    /// The current batches
    batches: HashMap<String, MutableBatch>,
}

impl LinesConverter {
    /// Create a new [`LinesConverter`]
    pub fn new(default_time: i64) -> Self {
        Self {
            default_time,
            timestamp_base: 1,
            stats: Default::default(),
            batches: Default::default(),
        }
    }

    /// Write some line protocol data.
    ///
    /// If a field / tag name appears more than once in a single line, the
    /// following semantics apply:
    ///
    ///   * duplicate fields, same value: do nothing/coalesce
    ///   * duplicate fields, different value: last occurrence wins
    ///   * duplicate fields, different types: return
    ///     [`LineWriteError::ConflictedFieldTypes`]
    ///   * duplicate tags, same value: return [`LineWriteError::DuplicateTag`]
    ///   * duplicate tags, different value: return
    ///     [`LineWriteError::DuplicateTag`]
    ///   * duplicate tags, different types: return
    ///     [`LineWriteError::DuplicateTag`]
    ///   * same name for tag and field: return
    ///     [`mutable_batch::writer::Error::TypeMismatch`]
    ///   * same name for tag and field, different type :
    ///     [`mutable_batch::writer::Error::TypeMismatch`]
    ///
    pub fn write_lp(&mut self, lines: &str) -> Result<()> {
        let mut errors = Vec::new();
        for line_err in parse_lines(lines)
            .enumerate()
            .filter_map(|(line_idx, maybe_line)| {
                maybe_line
                    .context(LineProtocolSnafu { line: line_idx + 1 })
                    .and_then(|line| self.rebase_timestamp(line, line_idx))
                    .and_then(|line| self.add_line_to_batch(line, line_idx))
                    .err()
            })
        {
            if errors.len() < MAXIMUM_RETURNED_ERRORS {
                errors.push(line_err);
            }
        }

        if !errors.is_empty() {
            return Err(Error::PerLine { lines: errors });
        }
        Ok(())
    }

    fn rebase_timestamp<'a>(
        &self,
        mut line: ParsedLine<'a>,
        line_idx: usize,
    ) -> Result<ParsedLine<'a>, LineError> {
        if let Some(t) = line.timestamp.as_mut() {
            let updated_timestamp = match t.checked_mul(self.timestamp_base) {
                Some(t) => t,
                None => return Err(LineError::TimestampOverflow { line: line_idx + 1 }),
            };
            *t = updated_timestamp;
        }
        Ok(line)
    }

    /// Push the given line - the timestamp in this line should already be rebased through
    /// [`Self::rebase_timestamp`]. Part of [`Self::write_lp`], but usable for when you have already
    /// parsed your line.
    pub fn add_line_to_batch(
        &mut self,
        line: ParsedLine<'_>,
        line_idx: usize,
    ) -> Result<Option<Warning>, LineError> {
        let measurement = line.series.measurement.as_str();

        let (_, batch) = self
            .batches
            .raw_entry_mut()
            .from_key(measurement)
            .or_insert_with(|| (measurement.to_string(), MutableBatch::new()));

        // TODO: Reuse writer
        let mut writer = Writer::new(batch, 1);
        let warning = write_line(&mut writer, &line, self.default_time)
            .context(WriteSnafu { line: line_idx + 1 })?;

        writer.commit();
        self.stats.num_lines += 1;
        self.stats.num_fields += line.field_set.len();

        Ok(warning)
    }

    /// Consume this [`LinesConverter`] returning the [`MutableBatch`]
    /// and the [`PayloadStatistics`] for the written data
    pub fn finish(self) -> Result<(HashMap<String, MutableBatch>, PayloadStatistics)> {
        let Self { batches, stats, .. } = self;

        // Keep only batches that have rows. If add_line_to_batch returned a WriteError for all
        // lines of that table, there will be an empty mutable batch in `batches` that will violate
        // the assumptions that the partitioner makes later.
        let nonempty_batches: HashMap<_, _> = batches
            .into_iter()
            .filter(|(_table, batch)| batch.rows() > 0)
            .collect();

        // If there aren't any nonempty batches, then we have an empty payload.
        match nonempty_batches.is_empty() {
            false => Ok((nonempty_batches, stats)),
            true => Err(Error::EmptyPayload),
        }
    }
}

/// A warning can be produced by the [`write_line`] function. Such a warning indicates that writing
/// the individual line didn't completely fail, but did something that the user should be warned
/// about.
#[derive(Debug, Clone)]
#[must_use = "Warnings should be bubbled up to the user"]
pub enum Warning {
    /// If the user passes in a field named `time`, the `time field must be simply stripped out,
    /// while the rest of the batch moves on as expected. See
    /// <https://github.com/influxdata/influxdb_iox/issues/15710#issuecomment-3633907650>
    BogusTimeStrippedOut {
        /// The measurement from which the field was stripped out
        measurement: String,
    },
}

/// Converts the provided lines of line protocol to a set of [`MutableBatch`]
/// keyed by measurement name
pub fn lines_to_batches(lines: &str, default_time: i64) -> Result<HashMap<String, MutableBatch>> {
    let mut converter = LinesConverter::new(default_time);
    converter.write_lp(lines)?;
    Ok(converter.finish()?.0)
}

/// An error applying an already-parsed line protocol line ([`ParsedLine`]) to a
/// [`MutableBatch`].
#[derive(Debug, Snafu)]
pub enum LineWriteError {
    /// A transparent error wrapper over the underling mutable batch error.
    #[snafu(display("{}", source))]
    MutableBatch {
        /// The underlying error
        source: mutable_batch::writer::Error,
    },

    /// The specified tag name appears twice in one LP line, with conflicting
    /// values.
    #[snafu(display(
        "the tag '{}' is specified more than once with conflicting values",
        name
    ))]
    DuplicateTag {
        /// The duplicated tag name.
        name: String,
    },

    /// The specified field name appears twice in one LP line, with conflicting
    /// types.
    #[snafu(display(
        "the field '{}' is specified more than once with conflicting types",
        name
    ))]
    ConflictedFieldTypes {
        /// The duplicated field name.
        name: String,
    },

    /// Using `time` as a tag is not allowed. See
    /// <https://github.com/influxdata/influxdb_iox/issues/15710#issuecomment-3633907650>
    /// As stated in <https://github.com/influxdata/influxdb_iox/issues/15710#issuecomment-3607311952>,
    /// since this is a v1 compatibility thing, we want to match the v1 error string as closely as
    /// we can, so this error string should stay exactly the same unless we explicitly decide
    /// otherwise.
    #[snafu(display("input tag \"time\" on measurement \"{measurement}\" is invalid"))]
    TimeTagNotAllowed {
        /// The measurement the tag was trying to be written for
        measurement: String,
    },

    /// This can occur when someone submits a write that only has `time` as a field, which is then
    /// stripped out due to <https://github.com/influxdata/influxdb_iox/issues/15710>, and leaves
    /// the line with no fields. In this case, we don't write the line and instead return an error
    /// that displays to the user the same as [`Warning::BogusTimeStrippedOut`] does. We want to
    /// keep this exact string for the error so that it matches with v1, just like we're doing with
    /// [`Self::TimeTagNotAllowed`].
    #[snafu(display(
        "invalid field name: input field \"time\" on measurement \"{measurement}\" is invalid"
    ))]
    TimeWasOnlyField {
        /// The measurement that they were trying to write for
        measurement: String,
    },
}

/// Writes the [`ParsedLine`] to the [`MutableBatch`], respecting the edge case
/// semantics described in [`LinesConverter::write_lp()`].
pub fn write_line<T>(
    writer: &mut Writer<'_, T>,
    line: &ParsedLine<'_>,
    default_time: i64,
) -> Result<Option<Warning>, LineWriteError>
where
    T: ColumnInsertValidator,
{
    if line.tag_value("time").is_some() {
        return Err(LineWriteError::TimeTagNotAllowed {
            measurement: line.series.measurement.to_string(),
        });
    }

    if let [(field_name, _)] = &*line.field_set
        && *field_name == "time"
    {
        return Err(LineWriteError::TimeWasOnlyField {
            measurement: line.series.measurement.to_string(),
        });
    }

    let mut warning = None;

    // Only allocate the seen tags hashset if there are tags.
    if let Some(tags) = &line.series.tag_set {
        let mut seen = HashSet::with_capacity(tags.len());
        for (tag_key, tag_value) in tags {
            // Check if a field with this name has been observed previously.
            if !seen.insert(tag_key) {
                // This tag_key appears more than once, with differing values.
                //
                // This is always an error.
                return Err(LineWriteError::DuplicateTag {
                    name: tag_key.to_string(),
                });
            }
            writer
                .write_tag(tag_key.as_str(), None, std::iter::once(tag_value.as_str()))
                .context(MutableBatchSnafu)?
        }
    }

    // In order to maintain parity with TSM, if a field within a single line is
    // repeated, the last occurrence is retained - for example, in the following
    // line protocol write:
    //
    //               table v=2,bananas=42,v=3,platanos=24
    //                      ▲              ▲
    //                      └───────┬──────┘
    //                              │
    //                     duplicate field "v"
    //
    // The duplicate "v" field should be collapsed into a single "v=3" field,
    // yielding an effective write of:
    //
    //                table bananas=42,v=3,platanos=24
    //
    // To do this in O(n) time, the code below walks backwards (from right to
    // left) when visiting parsed fields, and tracks which fields it has
    // observed. Any time a previously observed field is visited, it is skipped:
    //
    //                                visit direction
    //                           ◀─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
    //
    //               table v=2,bananas=42,v=3,platanos=24
    //                      ▲              ▲
    //                      │              │
    //                     skip           keep
    //
    // A notable exception of this "last value wins" rule is if the types differ
    // between each occurrence of "v":
    //
    //                        table v=2i,v=3u
    //
    // In this instance we break from established TSM behaviour and return an
    // error. IOx features schema enforcement, and as such is expected to reject
    // conflicting types for writes. See the github issue[1] for the desired
    // semantics.
    //
    // Tests below codify each of the scenarios described in the ticket.
    //
    // [1]: https://github.com/influxdata/influxdb_iox/issues/4326

    let mut seen = HashMap::<_, &FieldValue<'_>>::with_capacity(line.field_set.len());
    for (field_key, field_value) in line.field_set.iter().rev() {
        if *field_key == "time" {
            warning = Some(Warning::BogusTimeStrippedOut {
                measurement: line.series.measurement.to_string(),
            });
            continue;
        }

        // Check if a field with this name has been observed previously.
        match seen.entry(field_key) {
            Entry::Occupied(e) if e.get().is_same_type(field_value) => {
                // This field_value, and the "last" occurrence of this field_key
                // (the first visited) are of the same type - this occurrence is
                // skipped.
                continue;
            }
            Entry::Occupied(_) => {
                // This occurrence of "field_key" is of a different type to that
                // of the "last" (fist visited) occurrence. This is an
                // internally type-conflicted line and should be rejected.
                return Err(LineWriteError::ConflictedFieldTypes {
                    name: field_key.to_string(),
                });
            }
            Entry::Vacant(v) => {
                v.insert(field_value);
            }
        };

        match field_value {
            FieldValue::I64(value) => writer.write_i64s_from_slice(field_key.as_str(), &[*value]),
            FieldValue::U64(value) => writer.write_u64s_from_slice(field_key.as_str(), &[*value]),
            FieldValue::F64(value) => writer.write_f64s_from_slice(field_key.as_str(), &[*value]),
            FieldValue::String(value) => {
                writer.write_string(field_key.as_str(), None, std::iter::once(value.as_str()))
            }
            FieldValue::Boolean(value) => {
                writer.write_bool(field_key.as_str(), None, std::iter::once(*value))
            }
        }
        .context(MutableBatchSnafu)?;
    }

    let time = line.timestamp.unwrap_or(default_time);
    writer
        .write_time("time", std::iter::once(time))
        .context(MutableBatchSnafu)?;

    Ok(warning)
}

/// Test helper utilities
pub mod test_helpers {
    use mutable_batch::MutableBatch;

    /// Converts the line protocol for a single table into a WritePayload
    pub fn lp_to_mutable_batch(lp: &str) -> (String, MutableBatch) {
        let batches = super::lines_to_batches(lp, 0).unwrap();
        assert_eq!(batches.len(), 1);

        batches.into_iter().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;
    use ::test_helpers::assert_error;
    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use schema::Projection;

    #[test]
    fn test_basic() {
        let lp = r#"cpu,tag1=v1,tag2=v2 val=2i 0
        cpu,tag1=v4,tag2=v1 val=2i 0
        mem,tag1=v2 ival=3i 0
        cpu,tag2=v2 val=3i 1
        cpu,tag1=v1,tag2=v2 fval=2.0
        mem,tag1=v5 ival=2i 1
        "#;

        let mut batches = lines_to_batches(lp, 5).unwrap();
        assert_eq!(batches.len(), 2);

        assert_batches_eq!(
            &[
                "+------+------+------+--------------------------------+-----+",
                "| fval | tag1 | tag2 | time                           | val |",
                "+------+------+------+--------------------------------+-----+",
                "|      | v1   | v2   | 1970-01-01T00:00:00Z           | 2   |",
                "|      | v4   | v1   | 1970-01-01T00:00:00Z           | 2   |",
                "|      |      | v2   | 1970-01-01T00:00:00.000000001Z | 3   |",
                "| 2.0  | v1   | v2   | 1970-01-01T00:00:00.000000005Z |     |",
                "+------+------+------+--------------------------------+-----+",
            ],
            &[batches
                .remove("cpu")
                .unwrap()
                .try_into_arrow(Projection::All)
                .unwrap()]
        );

        assert_batches_eq!(
            &[
                "+------+------+--------------------------------+",
                "| ival | tag1 | time                           |",
                "+------+------+--------------------------------+",
                "| 3    | v2   | 1970-01-01T00:00:00Z           |",
                "| 2    | v5   | 1970-01-01T00:00:00.000000001Z |",
                "+------+------+--------------------------------+",
            ],
            &[batches
                .remove("mem")
                .unwrap()
                .try_into_arrow(Projection::All)
                .unwrap()]
        );
    }

    #[test]
    fn test_partial_line_conversion() {
        let lp = r#"cpu,tag1=v1,tag2=v2 val=2i 0
        cpu,tag1=v4,tag2=v1 val=2i 0
        mem,tag1=v2 ival=3i 0
        ,tag2=v2 val=3i 1
        cpu,tag1=v1,tag2=v2 fval=2.0
        bad_line
        mem,tag1=v5 ival=2i 1
        "#;

        let mut converter = LinesConverter::new(5);
        let result = converter.write_lp(lp);
        assert_matches!(
            result,
            Err(Error::PerLine { lines }) if matches!(&lines[..], [LineError::LineProtocol { .. }, LineError::LineProtocol { .. }]),
            "expected an error returned from write_lp(), but found {:?}", result
        );
        let (mut batches, _) = converter.finish().unwrap();
        assert_eq!(
            batches.len(),
            2,
            "expected both batches are written, instead found {:?}",
            batches.len(),
        );

        assert_batches_eq!(
            &[
                "+------+------+------+--------------------------------+-----+",
                "| fval | tag1 | tag2 | time                           | val |",
                "+------+------+------+--------------------------------+-----+",
                "|      | v1   | v2   | 1970-01-01T00:00:00Z           | 2   |",
                "|      | v4   | v1   | 1970-01-01T00:00:00Z           | 2   |",
                "| 2.0  | v1   | v2   | 1970-01-01T00:00:00.000000005Z |     |",
                "+------+------+------+--------------------------------+-----+",
            ],
            &[batches
                .remove("cpu")
                .unwrap()
                .try_into_arrow(Projection::All)
                .unwrap()]
        );

        assert_batches_eq!(
            &[
                "+------+------+--------------------------------+",
                "| ival | tag1 | time                           |",
                "+------+------+--------------------------------+",
                "| 3    | v2   | 1970-01-01T00:00:00Z           |",
                "| 2    | v5   | 1970-01-01T00:00:00.000000001Z |",
                "+------+------+--------------------------------+",
            ],
            &[batches
                .remove("mem")
                .unwrap()
                .try_into_arrow(Projection::All)
                .unwrap()]
        );
    }

    #[test]
    fn test_nulls_string_and_float() {
        let lp = r#"m f0="cat" 1639612800000000000
m f1=10i 1639612800000000000
        "#;

        let mut batches = lines_to_batches(lp, 5).unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches
            .remove("m")
            .unwrap()
            .try_into_arrow(Projection::All)
            .unwrap();
        assert_batches_eq!(
            &[
                "+-----+----+----------------------+",
                "| f0  | f1 | time                 |",
                "+-----+----+----------------------+",
                "| cat |    | 2021-12-16T00:00:00Z |",
                "|     | 10 | 2021-12-16T00:00:00Z |",
                "+-----+----+----------------------+",
            ],
            slice::from_ref(&batch)
        );

        // Verify the nullness of the string column ("" not the same as null)
        let f0 = &batch.columns()[0];
        assert!(f0.is_valid(0));
        assert!(!f0.is_valid(1));

        // Verify the nullness of the f1 column ("" not the same as null)
        let f1 = &batch.columns()[1];
        assert!(!f1.is_valid(0));
        assert!(f1.is_valid(1));
    }

    #[test]
    fn test_nulls_int_and_uint_and_bool() {
        let lp = r#"m i=1i 1639612800000000000
m u=2u 1639612800000000000
m b=t 1639612800000000000
        "#;

        let batches = lines_to_batches(lp, 5).unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches["m"]
            .clone()
            .try_into_arrow(Projection::All)
            .unwrap();
        assert_batches_eq!(
            &[
                "+------+---+----------------------+---+",
                "| b    | i | time                 | u |",
                "+------+---+----------------------+---+",
                "|      | 1 | 2021-12-16T00:00:00Z |   |",
                "|      |   | 2021-12-16T00:00:00Z | 2 |",
                "| true |   | 2021-12-16T00:00:00Z |   |",
                "+------+---+----------------------+---+",
            ],
            slice::from_ref(&batch)
        );

        // Verify the nullness of the int column
        let b = &batch.columns()[0];
        assert!(!b.is_valid(0));
        assert!(!b.is_valid(1));
        assert!(b.is_valid(2));

        // Verify the nullness of the int column
        let i = &batch.columns()[1];
        assert!(i.is_valid(0));
        assert!(!i.is_valid(1));
        assert!(!i.is_valid(2));

        // Verify the nullness of the uint column
        let u = &batch.columns()[3];
        assert!(!u.is_valid(0));
        assert!(u.is_valid(1));
        assert!(!u.is_valid(2));
    }

    // https://github.com/influxdata/influxdb_iox/issues/4326
    mod issue4326 {
        use super::*;

        #[test]
        fn test_duplicate_field_same_value() {
            let lp = "m1 val=2i,val=2i 0";

            let mut batches = lines_to_batches(lp, 5).unwrap();
            assert_eq!(batches.len(), 1);

            assert_batches_eq!(
                &[
                    "+----------------------+-----+",
                    "| time                 | val |",
                    "+----------------------+-----+",
                    "| 1970-01-01T00:00:00Z | 2   |",
                    "+----------------------+-----+",
                ],
                &[batches
                    .remove("m1")
                    .unwrap()
                    .try_into_arrow(Projection::All)
                    .unwrap()]
            );
        }

        #[test]
        fn test_duplicate_field_different_values() {
            let lp = "m1 val=1i,val=2i 0";

            let mut batches = lines_to_batches(lp, 5).unwrap();
            assert_eq!(batches.len(), 1);

            // "last value wins"
            assert_batches_eq!(
                &[
                    "+----------------------+-----+",
                    "| time                 | val |",
                    "+----------------------+-----+",
                    "| 1970-01-01T00:00:00Z | 2   |",
                    "+----------------------+-----+",
                ],
                &[batches
                    .remove("m1")
                    .unwrap()
                    .try_into_arrow(Projection::All)
                    .unwrap()]
            );
        }

        #[test]
        fn test_duplicate_fields_different_type() {
            let lp = "m1 val=1i,val=2.0 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(&lines[..],
                [LineError::Write {
                    source: LineWriteError::ConflictedFieldTypes { name },
                    line: 1
                }] if name == "val"
            ));
        }

        #[test]
        fn test_duplicate_tags_same_value() {
            let lp = "m1,tag=1,tag=1 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("duplicate tag write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(
                    &lines[..],
                    [LineError::Write {
                        source: LineWriteError::DuplicateTag { name },
                        line: 1
                    }] if name == "tag"
            ));
        }

        #[test]
        fn test_duplicate_tags_different_value() {
            let lp = "m1,tag=1,tag=2 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("duplicate tag write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(
                    &lines[..],
                    [LineError::Write {
                        source: LineWriteError::DuplicateTag { name },
                        line: 1
                    }] if name == "tag"
            ));
        }

        // NOTE: All tags are strings, so this should never be a type conflict.
        #[test]
        fn test_duplicate_tags_different_type() {
            let lp = "m1,tag=1,tag=2.0 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(
                    &lines[..],
                    [LineError::Write {
                        source: LineWriteError::DuplicateTag { name },
                        line: 1
                    }] if name == "tag"
            ));
        }

        // NOTE: disallowed in IOx but accepted in TSM
        //
        // https://github.com/influxdata/influxdb_iox/issues/3150
        #[test]
        fn test_duplicate_is_tag_and_field() {
            let lp = "m1,v=1i v=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(
                    &lines[..],
                    [LineError::Write {
                        source: LineWriteError::MutableBatch { .. },
                        line: 1
                    }]
            ));
        }

        #[test]
        fn test_duplicate_is_tag_and_field_different_types() {
            let lp = "m1,v=1i v=1.0 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::PerLine { lines } if matches!(
                    &lines[..],
                    [LineError::Write {
                        source: LineWriteError::MutableBatch { .. },
                        line: 1
                    }]
            ));
        }
    }

    #[test]
    fn dont_add_batches_when_there_are_write_errors() {
        let lp = r#"6,,=0,,=^/+\---6,,=yY\w\w\,y-/- (="
\_/1 (=""#;

        let mut converter = LinesConverter::new(10);
        let _errors = match converter.write_lp(lp) {
            Ok(_) => vec![],
            Err(Error::PerLine { lines }) => lines,
            Err(other) => panic!("unexpected error: `{other}` input: `{lp}`"),
        };

        assert_error!(converter.finish(), Error::EmptyPayload);
    }

    #[test]
    fn dont_add_stats_when_there_are_write_errors() {
        let lp = "cpu,tag1=v1,tag2=v2 val=2i 0
cpu val=4u";

        let mut converter = LinesConverter::new(10);
        // The second line has a different type for val
        converter.write_lp(lp).unwrap_err();
        let (batches, stats) = converter.finish().unwrap();

        let total_rows: usize = batches.iter().map(|(_table, batch)| batch.rows()).sum();
        assert_eq!(stats.num_lines, total_rows);
    }

    #[test]
    fn duplicate_field_names_when_one_contains_optional_escaping_doesnt_panic() {
        let lp = "table ,field=33,\\,field=333";
        lines_to_batches(lp, 5).unwrap();
    }

    #[test]
    fn batch_creation_continues_after_line_error_limit() {
        const WANT_GOOD_ROWS: usize = 5;

        // Create a LP payload that exceeds the error limit for "bad lines",
        // but then contains a few good lines.
        //
        // Converting it should result in errors returned up to, but not over
        // the limit. All good lines following the limit must still be
        // converted.
        let lp = (0..=MAXIMUM_RETURNED_ERRORS)
            .map(|i| format!("bananas,foo=bar foo=42i {i}"))
            .chain(
                (1..=WANT_GOOD_ROWS)
                    .map(|i| format!("bananas,baz=qux life=42i {}", i + MAXIMUM_RETURNED_ERRORS)),
            )
            .collect::<Vec<_>>()
            .join("\n");

        let mut converter = LinesConverter::new(0);
        assert_matches!(converter.write_lp(&lp), Err(Error::PerLine { lines }) => {
            assert_eq!(lines.len(), MAXIMUM_RETURNED_ERRORS);
        });
        let (batches, _) = converter.finish().unwrap();

        // 1 table, with 5 rows.
        assert_eq!(batches.len(), 1);
        let total_rows: usize = batches.iter().map(|(_table, batch)| batch.rows()).sum();
        assert_eq!(total_rows, WANT_GOOD_ROWS);
    }
}
