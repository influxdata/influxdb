use super::*;

#[test]
fn tag_spec_splits_cardinality_for_writers() {
    let mut tag_spec = TagSpec {
        key: "".to_string(),
        copies: None,
        append_copy_id: None,
        value: None,
        append_writer_id: None,
        cardinality: Some(100),
    };

    let (min, max) = tag_spec.cardinality_min_max(1, 10).unwrap();
    assert_eq!(min, 1);
    assert_eq!(max, 10);
    let (min, max) = tag_spec.cardinality_min_max(2, 10).unwrap();
    assert_eq!(min, 11);
    assert_eq!(max, 20);
    let (min, max) = tag_spec.cardinality_min_max(10, 10).unwrap();
    assert_eq!(min, 91);
    assert_eq!(max, 100);

    // if the cardinality is not evenly divisible by the number of writers, the last writer
    // will go over the cardinality set
    tag_spec.cardinality = Some(30);
    let (min, max) = tag_spec.cardinality_min_max(1, 7).unwrap();
    assert_eq!(min, 1);
    assert_eq!(max, 5);
    let (min, max) = tag_spec.cardinality_min_max(4, 7).unwrap();
    assert_eq!(min, 16);
    assert_eq!(max, 20);
    let (min, max) = tag_spec.cardinality_min_max(7, 7).unwrap();
    assert_eq!(min, 31);
    assert_eq!(max, 35);
}
