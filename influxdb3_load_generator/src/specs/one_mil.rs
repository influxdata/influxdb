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
                value: Some("series-number-".to_string()),
                cardinality: Some(1_000_000),
                ..Default::default()
            }],
            fields: vec![
                FieldSpec {
                    key: "int_val".to_string(),
                    copies: Some(10),
                    null_probability: None,
                    field: FieldKind::IntegerRange(1, 100_000_000),
                },
                FieldSpec {
                    key: "float_val".to_string(),
                    copies: Some(10),
                    null_probability: None,
                    field: FieldKind::FloatRange(1.0, 100.0),
                },
            ],
            copies: None,
            lines_per_sample: Some(10_000),
        }],
    };

    let query_spec = QuerierSpec {
        name: "one_mil".to_string(),
        queries: vec![QuerySpec {
            query: "SELECT int_val, float_val FROM measurement_data WHERE series_id = $sid"
                .to_string(),
            params: vec![ParamSpec {
                name: "sid".to_string(),
                param: ParamKind::Cardinality {
                    base: Some("some-value-".to_string()),
                    cardinality: 1_000_000,
                },
            }],
        }],
    };

    BuiltInSpec {
        description,
        write_spec,
        query_spec,
    }
}
