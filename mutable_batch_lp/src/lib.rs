//! Code to convert line protocol to [`MutableBatch`]

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
use workspace_hack as _;

use hashbrown::{hash_map::Entry, HashMap, HashSet};
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use mutable_batch::writer::Writer;
use mutable_batch::MutableBatch;
use snafu::{ResultExt, Snafu};

/// Error type for line protocol conversion
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("error parsing line {} (1-based): {}", line, source))]
    LineProtocol {
        source: influxdb_line_protocol::Error,
        line: usize,
    },

    #[snafu(display("error writing line {}: {}", line, source))]
    Write { source: LineWriteError, line: usize },

    #[snafu(display("empty write payload"))]
    EmptyPayload,

    #[snafu(display("timestamp overflows i64"))]
    TimestampOverflow,
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

    /// Sets a multiplier to convert line protocol timestamps to nanoseconds
    pub fn set_timestamp_base(&mut self, timestamp_base: i64) {
        self.timestamp_base = timestamp_base
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
        for (line_idx, maybe_line) in parse_lines(lines).enumerate() {
            let mut line = maybe_line.context(LineProtocolSnafu { line: line_idx + 1 })?;

            if let Some(t) = line.timestamp.as_mut() {
                *t = t
                    .checked_mul(self.timestamp_base)
                    .ok_or(Error::TimestampOverflow)?;
            }

            self.stats.num_lines += 1;
            self.stats.num_fields += line.field_set.len();

            let measurement = line.series.measurement.as_str();

            let (_, batch) = self
                .batches
                .raw_entry_mut()
                .from_key(measurement)
                .or_insert_with(|| (measurement.to_string(), MutableBatch::new()));

            // TODO: Reuse writer
            let mut writer = Writer::new(batch, 1);
            write_line(&mut writer, &line, self.default_time)
                .context(WriteSnafu { line: line_idx + 1 })?;
            writer.commit();
        }
        Ok(())
    }

    /// Consume this [`LinesConverter`] returning the [`MutableBatch`]
    /// and the [`PayloadStatistics`] for the written data
    pub fn finish(self) -> Result<(HashMap<String, MutableBatch>, PayloadStatistics)> {
        match self.batches.is_empty() {
            false => Ok((self.batches, self.stats)),
            true => Err(Error::EmptyPayload),
        }
    }
}

/// Converts the provided lines of line protocol to a set of [`MutableBatch`]
/// keyed by measurement name
pub fn lines_to_batches(lines: &str, default_time: i64) -> Result<HashMap<String, MutableBatch>> {
    Ok(lines_to_batches_stats(lines, default_time)?.0)
}

/// Converts the provided lines of line protocol to a set of [`MutableBatch`]
/// keyed by measurement name, and a set of statistics about the converted line protocol
pub fn lines_to_batches_stats(
    lines: &str,
    default_time: i64,
) -> Result<(HashMap<String, MutableBatch>, PayloadStatistics)> {
    let mut converter = LinesConverter::new(default_time);
    converter.write_lp(lines)?;
    converter.finish()
}

/// An error applying an already-parsed line protocol line ([`ParsedLine`]) to a
/// [`MutableBatch`].
#[allow(missing_copy_implementations)]
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
}

/// Writes the [`ParsedLine`] to the [`MutableBatch`], respecting the edge case
/// semantics described in [`LinesConverter::write_lp()`].
pub fn write_line(
    writer: &mut Writer<'_>,
    line: &ParsedLine<'_>,
    default_time: i64,
) -> Result<(), LineWriteError> {
    // Only allocate the seen tags hashset if there are tags.
    if let Some(tags) = &line.series.tag_set {
        let mut seen = HashSet::with_capacity(tags.len());
        for (tag_key, tag_value) in tags.iter() {
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
            FieldValue::I64(value) => {
                writer.write_i64(field_key.as_str(), None, std::iter::once(*value))
            }
            FieldValue::U64(value) => {
                writer.write_u64(field_key.as_str(), None, std::iter::once(*value))
            }
            FieldValue::F64(value) => {
                writer.write_f64(field_key.as_str(), None, std::iter::once(*value))
            }
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

    Ok(())
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
    use super::*;
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

        let batches = lines_to_batches(lp, 5).unwrap();
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
            &[batches["cpu"].to_arrow(Projection::All).unwrap()]
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
            &[batches["mem"].to_arrow(Projection::All).unwrap()]
        );
    }

    #[test]
    fn test_nulls_string_and_float() {
        let lp = r#"m f0="cat" 1639612800000000000
m f1=10i 1639612800000000000
        "#;

        let batches = lines_to_batches(lp, 5).unwrap();
        assert_eq!(batches.len(), 1);

        let batch = batches["m"].to_arrow(Projection::All).unwrap();
        assert_batches_eq!(
            &[
                "+-----+----+----------------------+",
                "| f0  | f1 | time                 |",
                "+-----+----+----------------------+",
                "| cat |    | 2021-12-16T00:00:00Z |",
                "|     | 10 | 2021-12-16T00:00:00Z |",
                "+-----+----+----------------------+",
            ],
            &[batch.clone()]
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

        let batch = batches["m"].to_arrow(Projection::All).unwrap();
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
            &[batch.clone()]
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

            let batches = lines_to_batches(lp, 5).unwrap();
            assert_eq!(batches.len(), 1);

            assert_batches_eq!(
                &[
                    "+----------------------+-----+",
                    "| time                 | val |",
                    "+----------------------+-----+",
                    "| 1970-01-01T00:00:00Z | 2   |",
                    "+----------------------+-----+",
                ],
                &[batches["m1"].to_arrow(Projection::All).unwrap()]
            );
        }

        #[test]
        fn test_duplicate_field_different_values() {
            let lp = "m1 val=1i,val=2i 0";

            let batches = lines_to_batches(lp, 5).unwrap();
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
                &[batches["m1"].to_arrow(Projection::All).unwrap()]
            );
        }

        #[test]
        fn test_duplicate_fields_different_type() {
            let lp = "m1 val=1i,val=2.0 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::Write {
                    source: LineWriteError::ConflictedFieldTypes { name },
                    line: 1
                }
            => {
                assert_eq!(name, "val");
            });
        }

        #[test]
        fn test_duplicate_tags_same_value() {
            let lp = "m1,tag=1,tag=1 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("duplicate tag write should fail");
            assert_matches!(err,
                Error::Write {
                    source: LineWriteError::DuplicateTag { name },
                    line: 1
                }
            => {
                assert_eq!(name, "tag");
            });
        }

        #[test]
        fn test_duplicate_tags_different_value() {
            let lp = "m1,tag=1,tag=2 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("duplicate tag write should fail");
            assert_matches!(err,
                Error::Write {
                    source: LineWriteError::DuplicateTag { name },
                    line: 1
                }
            => {
                assert_eq!(name, "tag");
            });
        }

        // NOTE: All tags are strings, so this should never be a type conflict.
        #[test]
        fn test_duplicate_tags_different_type() {
            let lp = "m1,tag=1,tag=2.0 val=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(err,
                Error::Write {
                    source: LineWriteError::DuplicateTag { name },
                    line: 1
                }
            => {
                assert_eq!(name, "tag");
            });
        }

        // NOTE: disallowed in IOx but accepted in TSM
        //
        // https://github.com/influxdata/influxdb_iox/issues/3150
        #[test]
        fn test_duplicate_is_tag_and_field() {
            let lp = "m1,v=1i v=1i 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(
                err,
                Error::Write {
                    source: LineWriteError::MutableBatch { .. },
                    line: 1
                }
            );
        }

        #[test]
        fn test_duplicate_is_tag_and_field_different_types() {
            let lp = "m1,v=1i v=1.0 0";

            let err = lines_to_batches(lp, 5).expect_err("type conflicted write should fail");
            assert_matches!(
                err,
                Error::Write {
                    source: LineWriteError::MutableBatch { .. },
                    line: 1
                }
            );
        }
    }
}
