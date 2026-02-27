use chrono::TimeZone;
use rand::{SeedableRng, rngs::SmallRng};

use super::*;
use crate::specification::{FieldSpec, TagSpec};
#[test]
fn example_spec_lp() {
    let spec = DataSpec {
        name: "foo".to_string(),
        measurements: vec![MeasurementSpec {
            name: "m".to_string(),
            tags: vec![TagSpec {
                key: "t".to_string(),
                copies: Some(2),
                append_copy_id: None,
                value: Some("w".to_string()),
                append_writer_id: None,
                cardinality: Some(10),
            }],
            fields: vec![
                FieldSpec {
                    key: "i".to_string(),
                    copies: Some(2),
                    null_probability: None,
                    field: FieldKind::Integer(42),
                },
                FieldSpec {
                    key: "f".to_string(),
                    copies: None,
                    null_probability: None,
                    field: FieldKind::Float(6.8),
                },
                FieldSpec {
                    key: "s".to_string(),
                    copies: None,
                    null_probability: None,
                    field: FieldKind::String("hello".to_string()),
                },
            ],
            copies: Some(1),
            lines_per_sample: Some(2),
        }],
    };
    let mut generators = create_generators(&spec, 2).unwrap();
    let mut rng = SmallRng::from_entropy();

    let t = Local.timestamp_millis_opt(123).unwrap();
    let lp = generators.get_mut(0).unwrap().dry_run(t, &mut rng);
    let actual: Vec<&str> = lp.split('\n').collect();
    let expected: Vec<&str> = vec![
        "m,t=w1,t_2=w1 i=42i,i_2=42i,f=6.8,s=\"hello\" 123000000",
        "m,t=w2,t_2=w2 i=42i,i_2=42i,f=6.8,s=\"hello\" 123000001",
        "",
    ];
    assert_eq!(actual, expected);

    let t = Local.timestamp_millis_opt(567).unwrap();
    let lp = generators.get_mut(1).unwrap().dry_run(t, &mut rng);
    let actual: Vec<&str> = lp.split('\n').collect();
    let expected: Vec<&str> = vec![
        "m,t=w6,t_2=w6 i=42i,i_2=42i,f=6.8,s=\"hello\" 567000000",
        "m,t=w7,t_2=w7 i=42i,i_2=42i,f=6.8,s=\"hello\" 567000001",
        "",
    ];
    assert_eq!(actual, expected);
}
