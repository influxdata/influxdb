use crate::{CatalogId, QueryGroupId};

#[test]
fn query_group_id_has_catalog_id_behavior() {
    let id = QueryGroupId::new(42);

    assert_eq!(id.get(), 42);
    assert_eq!(id.next(), QueryGroupId::new(43));
    assert_eq!(id.checked_next(), Some(QueryGroupId::new(43)));
    assert_eq!(QueryGroupId::from(42), id);
    assert_eq!(id.to_string(), "42");
    assert_eq!("42".parse::<QueryGroupId>().unwrap(), id);
}
