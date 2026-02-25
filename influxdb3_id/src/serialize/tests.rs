use indexmap::{IndexMap, IndexSet};

use super::{SerdeVecMap, SerdeVecSet};

#[test]
fn serde_vec_map_with_json() {
    let map = IndexMap::<u32, &str>::from_iter([(0, "foo"), (1, "bar"), (2, "baz")]);
    let serde_vec_map = SerdeVecMap::from(map);
    // test round-trip to JSON:
    let s = serde_json::to_string(&serde_vec_map).unwrap();
    assert_eq!(r#"[[0,"foo"],[1,"bar"],[2,"baz"]]"#, s);
    let d: SerdeVecMap<u32, &str> = serde_json::from_str(&s).unwrap();
    assert_eq!(d, serde_vec_map);
}

#[test]
fn test_no_duplicates() {
    let json_str = r#"[[0, "foo"], [0, "bar"]]"#;
    let err = serde_json::from_str::<SerdeVecMap<u8, &str>>(json_str).unwrap_err();
    assert!(err.to_string().contains("duplicate key found"));
}

#[test]
fn serde_vec_set_with_json() {
    let set = IndexSet::<u32>::from_iter([0, 1, 2]);
    let serde_vec_set = SerdeVecSet::from(set);
    // test round-trip to JSON:
    let s = serde_json::to_string(&serde_vec_set).unwrap();
    assert_eq!(r#"[0,1,2]"#, s);
    let d: SerdeVecSet<u32> = serde_json::from_str(&s).unwrap();
    assert_eq!(d, serde_vec_set);
}

#[test]
fn test_set_duplicates_ignored() {
    let json_str = r#"[0, 1, 0, 2, 1]"#;
    let set: SerdeVecSet<u8> = serde_json::from_str(json_str).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&0));
    assert!(set.contains(&1));
    assert!(set.contains(&2));
}
