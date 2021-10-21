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

use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use mutable_batch::writer::Writer;
use mutable_batch::MutableBatch;
use snafu::{ResultExt, Snafu};

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
}

/// Result type for line protocol conversion
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts the provided lines of line protocol to a [`MutableBatch`]
pub fn lines_to_batch(lines: &str, default_time: i64) -> Result<MutableBatch> {
    let mut batch = MutableBatch::new();
    for (line_idx, maybe_line) in parse_lines(lines).enumerate() {
        let line = maybe_line.context(LineProtocol { line: line_idx + 1 })?;

        // TODO: Reuse writer
        let mut writer = Writer::new(&mut batch, 1);
        write_line(&mut writer, line, default_time).context(Write { line: line_idx + 1 })?;
        writer.commit();
    }
    Ok(batch)
}

fn write_line(
    writer: &mut Writer<'_>,
    line: ParsedLine<'_>,
    default_time: i64,
) -> mutable_batch::writer::Result<()> {
    for (tag_key, tag_value) in line.series.tag_set.into_iter().flatten() {
        writer.write_tag(tag_key.as_str(), None, std::iter::once(tag_value.as_str()))?
    }

    for (field_key, field_value) in line.field_set {
        match field_value {
            FieldValue::I64(value) => {
                writer.write_i64(field_key.as_str(), None, std::iter::once(value))?;
            }
            FieldValue::U64(value) => {
                writer.write_u64(field_key.as_str(), None, std::iter::once(value))?;
            }
            FieldValue::F64(value) => {
                writer.write_f64(field_key.as_str(), None, std::iter::once(value))?;
            }
            FieldValue::String(value) => {
                writer.write_string(field_key.as_str(), None, std::iter::once(value.as_str()))?;
            }
            FieldValue::Boolean(value) => {
                writer.write_bool(field_key.as_str(), None, std::iter::once(value))?;
            }
        }
    }

    let time = line.timestamp.unwrap_or(default_time);
    writer.write_time("time", std::iter::once(time))?;

    Ok(())
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
        cpu,tag2=v2 val=3i 1
        cpu,tag1=v1,tag2=v2 fval=2.0"#;

        let batch = lines_to_batch(lp, 5).unwrap();

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
            &[batch.to_arrow(Selection::All).unwrap()]
        );
    }
}
