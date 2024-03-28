//! Spec that shows the various elements of the data generator. Gets printed to console when
//! the generator is run without a spec specified.

use serde_json::json;

use crate::specification::*;
use crate::specs::BuiltInSpec;

pub(crate) fn spec() -> BuiltInSpec {
    let description =
        r#"Example that shows the various elements of the data generator."#.to_string();

    let write_spec = DataSpec {
        name: "sample_spec".to_string(),
        measurements: vec![
            MeasurementSpec {
                name: "some_measurement".to_string(),
                tags: vec![
                    TagSpec {
                        key: "some_tag".to_string(),
                        value: Some("a-value-here".to_string()),
                        ..Default::default()
                    },
                    TagSpec {
                        key: "random_data_tag".to_string(),
                        value: Some("card-val-".to_string()),
                        cardinality: Some(2),
                        ..Default::default()
                    },
                    TagSpec {
                        key: "higher_cardinality_data_tag".to_string(),
                        value: Some("card-val-".to_string()),
                        cardinality: Some(6),
                        ..Default::default()
                    },
                    TagSpec {
                        key: "copied_tag".to_string(),
                        copies: Some(3),
                        append_copy_id: Some(true),
                        value: Some("copy-val-".to_string()),
                        ..Default::default()
                    },
                    TagSpec {
                        key: "writer_id".to_string(),
                        value: Some("writer-id-".to_string()),
                        append_writer_id: Some(true),
                        ..Default::default()
                    },
                ],
                fields: vec![
                    FieldSpec {
                        key: "f1".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::Float(1.2),
                    },
                    FieldSpec {
                        key: "i1".to_string(),
                        copies: None,
                        null_probability: Some(0.6),
                        field: FieldKind::Integer(5),
                    },
                ],
                copies: None,
                lines_per_sample: None,
            },
            MeasurementSpec {
                name: "copied_measurement".to_string(),
                tags: vec![],
                fields: vec![
                    FieldSpec {
                        key: "random_string".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::StringRandom(10),
                    },
                    FieldSpec {
                        key: "constant_string".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::String("a constant string".to_string()),
                    },
                    FieldSpec {
                        key: "random_integer".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::IntegerRange(1, 100),
                    },
                    FieldSpec {
                        key: "constant_integer".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::Integer(42),
                    },
                    FieldSpec {
                        key: "random_float".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::FloatRange(1.0, 100.0),
                    },
                    FieldSpec {
                        key: "constant_float".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::Float(6.8),
                    },
                    FieldSpec {
                        key: "random_bool".to_string(),
                        copies: None,
                        null_probability: None,
                        field: FieldKind::Bool(true),
                    },
                ],
                copies: Some(2),
                lines_per_sample: None,
            },
        ],
    };

    let query_spec = QuerierSpec {
        name: "sample_spec".to_string(),
        queries: vec![QuerySpec {
            query: "SELECT f1, i1 FROM some_measurement WHERE some_tag = $some_val".to_string(),
            params: vec![ParamSpec {
                name: "some_val".to_string(),
                param: ParamKind::Static(json!("a-value-here")),
            }],
        }],
    };

    BuiltInSpec {
        description,
        write_spec,
        query_spec,
    }
}
