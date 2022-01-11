//! Code to convert line protocol to [`MutableBatch`]

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use hashbrown::HashMap;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use mutable_batch::writer::Writer;
use mutable_batch::MutableBatch;
use snafu::{ensure, ResultExt, Snafu};

/// Error type for line protocol conversion
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("error parsing line {}: {}", line, source))]
    LineProtocol {
        source: influxdb_line_protocol::Error,
        line: usize,
    },

    #[snafu(display("error writing line {}: {}", line, source))]
    Write {
        source: mutable_batch::writer::Error,
        line: usize,
    },

    #[snafu(display("empty write payload"))]
    EmptyPayload,
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
    let mut stats = PayloadStatistics::default();
    let mut batches = HashMap::new();
    for (line_idx, maybe_line) in parse_lines(lines).enumerate() {
        let line = maybe_line.context(LineProtocolSnafu { line: line_idx + 1 })?;

        stats.num_lines += 1;
        stats.num_fields += line.field_set.len();

        let measurement = line.series.measurement.as_str();

        let (_, batch) = batches
            .raw_entry_mut()
            .from_key(measurement)
            .or_insert_with(|| (measurement.to_string(), MutableBatch::new()));

        // TODO: Reuse writer
        let mut writer = Writer::new(batch, 1);
        write_line(&mut writer, &line, default_time).context(WriteSnafu { line: line_idx + 1 })?;
        writer.commit();
    }
    ensure!(!batches.is_empty(), EmptyPayloadSnafu);

    Ok((batches, stats))
}

/// Writes the [`ParsedLine`] to the [`MutableBatch`]
pub fn write_line(
    writer: &mut Writer<'_>,
    line: &ParsedLine<'_>,
    default_time: i64,
) -> mutable_batch::writer::Result<()> {
    for (tag_key, tag_value) in line.series.tag_set.iter().flatten() {
        writer.write_tag(tag_key.as_str(), None, std::iter::once(tag_value.as_str()))?
    }

    for (field_key, field_value) in &line.field_set {
        match field_value {
            FieldValue::I64(value) => {
                writer.write_i64(field_key.as_str(), None, std::iter::once(*value))?;
            }
            FieldValue::U64(value) => {
                writer.write_u64(field_key.as_str(), None, std::iter::once(*value))?;
            }
            FieldValue::F64(value) => {
                writer.write_f64(field_key.as_str(), None, std::iter::once(*value))?;
            }
            FieldValue::String(value) => {
                writer.write_string(field_key.as_str(), None, std::iter::once(value.as_str()))?;
            }
            FieldValue::Boolean(value) => {
                writer.write_bool(field_key.as_str(), None, std::iter::once(*value))?;
            }
        }
    }

    let time = line.timestamp.unwrap_or(default_time);
    writer.write_time("time", std::iter::once(time))?;

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
    use schema::selection::Selection;

    #[test]
    fn test_basic() {
        let lp = r#"cpu,tag1=v1,tag2=v2 val=2i 0
        cpu,tag1=v4,tag2=v1 val=2i 0
        mem,tag1=v2 ival=3i 0
        cpu,tag2=v2 val=3i 1
        cpu,tag1=v1,tag2=v2 fval=2.0
        mem,tag1=v5 ival=2i 1
        "#;

        let batch = lines_to_batches(lp, 5).unwrap();
        assert_eq!(batch.len(), 2);

        assert_batches_eq!(
            &[
                "+------+------+------+--------------------------------+-----+",
                "| fval | tag1 | tag2 | time                           | val |",
                "+------+------+------+--------------------------------+-----+",
                "|      | v1   | v2   | 1970-01-01T00:00:00Z           | 2   |",
                "|      | v4   | v1   | 1970-01-01T00:00:00Z           | 2   |",
                "|      |      | v2   | 1970-01-01T00:00:00.000000001Z | 3   |",
                "| 2    | v1   | v2   | 1970-01-01T00:00:00.000000005Z |     |",
                "+------+------+------+--------------------------------+-----+",
            ],
            &[batch["cpu"].to_arrow(Selection::All).unwrap()]
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
            &[batch["mem"].to_arrow(Selection::All).unwrap()]
        );
    }
}
