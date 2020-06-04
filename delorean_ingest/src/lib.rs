//! Library with code for (aspirationally) ingesting various data formats into Delorean
use log::debug;
use snafu::{OptionExt, Snafu};

use delorean_line_parser::{FieldValue, ParsedLine};

use line_protocol_schema::{DataType, Schema, SchemaBuilder};

/// Handles converting raw line protocol `ParsedLine` structures into Delorean format.
pub struct LineProtocolConverter {
    // Schema is used in tests and will be used to actually convert data shortly
    schema: Schema,
}

#[derive(Snafu, Debug)]
pub enum Error {
    /// Conversion needs at least one line of data
    NeedsAtLeastOneLine,

    // Only a single line protocol measurement field is currently supported
    #[snafu(display(r#"More than one measurement not yet supported: {}"#, message))]
    OnlyOneMeasurementSupported { message: String },
}

/// `LineProtocolConverter` are used to
/// converting an iterator of `ParsedLines` into the  delorean
/// internal columnar data format (exactly what this is is TBD).
///
impl LineProtocolConverter {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Create a new `LineProtocolConverter` by extracting the implied
    /// schema from an iterator of ParsedLines.
    ///
    /// The converter can subsequently be used for any `ParsedLine`'s
    /// that have the same schema (e.g. tag names, field names,
    /// measurements).
    ///
    pub fn new(lines: &[ParsedLine<'_>]) -> Result<LineProtocolConverter, Error> {
        let mut peekable_iter = lines.iter().peekable();
        let first_line = peekable_iter.peek().context(NeedsAtLeastOneLine)?;

        let mut builder = SchemaBuilder::new(&first_line.series.measurement);

        for line in peekable_iter {
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

        let schema = builder.build();
        debug!("Deduced line protocol schema: {:#?}", schema);
        Ok(LineProtocolConverter { schema })
    }
}

#[cfg(test)]
mod delorean_ingest_tests {
    use super::*;
    use line_protocol_schema::ColumnDefinition;

    fn only_good_lines(data: &str) -> Vec<ParsedLine<'_>> {
        delorean_line_parser::parse_lines(data)
            .filter_map(|r| {
                assert!(r.is_ok());
                r.ok()
            })
            .collect()
    }

    #[test]
    fn no_lines() {
        let parsed_lines = only_good_lines("");
        let converter_result = LineProtocolConverter::new(&parsed_lines);

        assert!(matches!(converter_result, Err(Error::NeedsAtLeastOneLine)));
    }

    #[test]
    fn one_line() {
        let parsed_lines =
            only_good_lines("cpu,host=A,region=west usage_system=64i 1590488773254420000");

        let converter = LineProtocolConverter::new(&parsed_lines).expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        let cols = converter.schema.get_col_defs();
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
    fn multi_line_same_schema() {
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_system=67i 1590488773254430000"#,
        );

        let converter = LineProtocolConverter::new(&parsed_lines).expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        let cols = converter.schema.get_col_defs();
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
    fn multi_line_new_field() {
        // given two lines of protocol data that have different field names
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_user=61.32 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines).expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // then both field names appear in the resulting schema
        let cols = converter.schema.get_col_defs();
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
    fn multi_line_new_tags() {
        // given two lines of protocol data that have different tags
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A,fail_group=Z usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines).expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // Then both tag names appear in the resulting schema
        let cols = converter.schema.get_col_defs();
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
    fn multi_line_field_changed() {
        // given two lines of protocol data that have apparently different data types for the field:
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A usage_system=61.1 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines).expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // Then the first field type appears in the resulting schema (TBD is this what we want??)
        let cols = converter.schema.get_col_defs();
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
    fn multi_line_measurement_changed() {
        // given two lines of protocol data for two different measurements
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            vcpu,host=A usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let converter_result = LineProtocolConverter::new(&parsed_lines);

        // Then the converter does not support it
        assert!(matches!(
            converter_result,
            Err(Error::OnlyOneMeasurementSupported { message: _ })
        ));
    }
}
