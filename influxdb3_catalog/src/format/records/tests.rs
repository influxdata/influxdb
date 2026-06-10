use crate::format::REGISTRY;

#[test]
fn record_names_are_unique() {
    let mut names: Vec<&str> = REGISTRY.all().map(|r| r.name).collect();
    names.sort();
    let original_len = names.len();
    names.dedup();
    assert_eq!(
        names.len(),
        original_len,
        "all record names should be unique"
    );
}
