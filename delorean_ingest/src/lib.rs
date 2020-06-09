#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

//! Library with code for (aspirationally) ingesting various data formats into Delorean
//! TODO move this to delorean/src/ingest/line_protocol.rs?
use log::debug;
use snafu::{OptionExt, Snafu};

use std::collections::BTreeMap;

use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_table::packers::Packer;
use delorean_table_schema::{DataType, Schema, SchemaBuilder};

/// Handles converting raw line protocol `ParsedLine` structures into Delorean format.
#[derive(Debug)]
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

    /// Packs a sequence of `ParsedLine`s (which are row-based) from a
    /// single measurement into a columnar memory format. Among other
    /// things, this format is suitable for writing data out column by
    /// column (e.g. parquet).
    ///
    /// FIXME: the plan is to switch from `Packer` to something based
    /// on Apache Arrow.
    pub fn pack_lines<'a>(&self, lines: impl Iterator<Item = ParsedLine<'a>>) -> Vec<Packer> {
        let col_defs = self.schema.get_col_defs();
        let mut packers: Vec<Packer> = col_defs
            .iter()
            .enumerate()
            .map(|(idx, col_def)| {
                debug!("  Column definition [{}] = {:?}", idx, col_def);
                Packer::new(col_def.data_type)
            })
            .collect();

        // map col_name -> Packer;
        let mut packer_map: BTreeMap<&String, &mut Packer> = col_defs
            .iter()
            .map(|x| &x.name)
            .zip(packers.iter_mut())
            .collect();

        // for each parsed input line
        // for each tag we expect to see, add an appropriate entry

        for line in lines {
            let timestamp_col_name = self.schema.timestamp();

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

            // TODO handle data from different measurements
            assert_eq!(
                series.measurement.to_string(),
                self.schema.measurement(),
                "Different measurements in same line protocol stream not supported"
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
                        starting_len+1, packer.len(),
                    )
                }
            }

            // Should have added one value to all packers
            assert!(
                packer_map.values().all(|x| x.len() == starting_len + 1),
                "Should have added 1 row to all packers"
            );
        }

        packers
    }
}

#[cfg(test)]
mod delorean_ingest_tests {
    use super::*;
    use delorean_table_schema::ColumnDefinition;
    use delorean_test_helpers::approximately_equal;

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

    #[test]
    fn pack_data_schema() -> Result<(), Error> {
        let parsed_lines = only_good_lines(LP_DATA);
        assert_eq!(parsed_lines.len(), EXPECTED_NUM_LINES);

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines)?;

        // Then the correct schema is extracted
        let cols = converter.schema.get_col_defs();
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
        let parsed_lines = only_good_lines(LP_DATA);
        assert_eq!(parsed_lines.len(), EXPECTED_NUM_LINES);

        // when we extract the schema and pack the values
        let converter = LineProtocolConverter::new(&parsed_lines)?;
        let packers = converter.pack_lines(parsed_lines.into_iter());

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
}
