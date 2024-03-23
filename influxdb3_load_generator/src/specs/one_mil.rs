//! Spec for the 1 million series use case

use crate::specification::*;
use crate::specs::BuiltInSpec;

pub(crate) fn spec() -> BuiltInSpec {
    let description =
        r#"1 million series in a single table use case. If you run this with -writer-count=100
           you'll get all 1M series written every sampling interval. Our primary test is interval=10s"#.to_string();
    let write_spec = DataSpec {
        name: "one_mil".to_string(),
        measurements: vec![MeasurementSpec {
            name: "measurement_data".to_string(),
            tags: vec![TagSpec {
                key: "series_id".to_string(),
                copies: None,
                append_copy_id: None,
                value: Some("series-number-".to_string()),
                append_writer_id: None,
                cardinality: Some(1_000_000),
            }],
            fields: vec![
                FieldSpec {
                    key: "int_val".to_string(),
                    copies: Some(10),
                    null_probability: None,
                    bool: None,
                    string: None,
                    string_random: None,
                    integer: None,
                    integer_range: Some((1, 100_000_000)),
                    float: None,
                    float_range: None,
                },
                FieldSpec {
                    key: "float_val".to_string(),
                    copies: Some(10),
                    null_probability: None,
                    bool: None,
                    string: None,
                    string_random: None,
                    integer: None,
                    integer_range: None,
                    float: None,
                    float_range: Some((1.0, 100.0)),
                },
            ],
            copies: None,
            lines_per_sample: Some(10_000),
        }],
    };

    BuiltInSpec {
        description,
        write_spec,
    }
}
