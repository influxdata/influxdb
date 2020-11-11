pub mod rle;

pub use self::rle::RLE;

/// The encoded id for a NULL value.
pub const NULL_ID: u32 = 0;

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::column::{cmp, RowIDs};

    #[test]
    fn size() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional(Some("north".to_string()), 1);
        drle.push_additional(Some("east".to_string()), 5);
        drle.push_additional(Some("south".to_string()), 2);
        drle.push_none();
        drle.push_none();
        drle.push_none();
        drle.push_none();

        // keys - 18 bytes.
        // entry_index is 24 + ((24+4) * 3) + 14 == 122
        // index_entry is 24 + (24*4) + 14 == 134
        // index_row_ids is 24 + (4 + 0?? * 4) == 40 ??????
        // run lengths is 24 + (8*5) == 64
        // 360

        // TODO(edd): there some mystery bytes in the bitmap implementation.
        // need to figure out how to measure these
        assert_eq!(drle.size(), 397);
    }

    #[test]
    fn rle_push() {
        let mut drle = super::RLE::from(vec!["hello", "hello", "hello", "hello"]);
        drle.push_additional(Some("hello".to_string()), 1);
        drle.push_additional(None, 3);
        drle.push("world".to_string());

        assert_eq!(
            drle.all_values(vec![]),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                None,
                None,
                None,
                Some(&"world".to_string()),
            ]
        );

        drle.push_additional(Some("zoo".to_string()), 3);
        drle.push_none();
        assert_eq!(
            drle.all_values(vec![]),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                None,
                None,
                None,
                Some(&"world".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
                None,
            ]
        );
    }

    // tests a defect I discovered.
    #[test]
    fn push_additional_first_run_length() {
        let arr = vec!["world".to_string(), "hello".to_string()];

        let mut drle = super::RLE::with_dictionary(arr.into_iter().collect::<BTreeSet<String>>());
        drle.push_additional(Some("world".to_string()), 1);
        drle.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            drle.all_values(vec![]),
            vec![Some(&"world".to_string()), Some(&"hello".to_string())]
        );
        assert_eq!(drle.all_encoded_values(vec![]), vec![2, 1]);

        drle = super::RLE::default();
        drle.push_additional(Some("hello".to_string()), 1);
        drle.push_additional(Some("world".to_string()), 1);

        assert_eq!(
            drle.all_values(vec![]),
            vec![Some(&"hello".to_string()), Some(&"world".to_string())]
        );
        assert_eq!(drle.all_encoded_values(vec![]), vec![1, 2]);
    }

    #[test]
    #[should_panic]
    fn rle_push_wrong_order() {
        let mut drle = super::RLE::default();
        drle.push("b".to_string());
        drle.push("a".to_string());
    }

    #[test]
    fn row_ids_filter_equal() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_none(); // 9
        drle.push_additional(Some("south".to_string()), 2); // 10, 11

        let ids = drle.row_ids_filter(&"east", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"south", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![10, 11]));

        let ids = drle.row_ids_filter(&"foo", &cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert!(ids.is_empty());

        // != some value not in the column should exclude the NULL value.
        let ids = drle.row_ids_filter(&"foo", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11]));

        let ids = drle.row_ids_filter(&"east", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 10, 11]));
    }

    #[test]
    fn row_ids_filter_equal_no_null() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 2);
        drle.push_additional(Some("west".to_string()), 1);

        let ids = drle.row_ids_filter(&"abba", &cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2]));
    }

    #[test]
    fn row_ids_filter_cmp() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_additional(Some("west".to_string()), 1); // 11
        drle.push_additional(Some("north".to_string()), 1); // 12
        drle.push_none(); // 13
        drle.push_additional(Some("west".to_string()), 5); // 14, 15, 16, 17, 18

        let ids = drle.row_ids_filter(&"east", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"east", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty());

        let ids = drle.row_ids_filter(&"north", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![9, 10, 11, 14, 15, 16, 17, 18]));

        let ids = drle.row_ids_filter(&"north", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        // The encoding also supports comparisons on values that don't directly exist in the column.
        let ids = drle.row_ids_filter(&"abba", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ])
        );

        let ids = drle.row_ids_filter(&"east1", &cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        let ids = drle.row_ids_filter(&"east1", &cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        let ids = drle.row_ids_filter(&"east1", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"region", &cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 12]));

        let ids = drle.row_ids_filter(&"zoo", &cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ])
        );
    }

    #[test]
    fn row_ids_not_null() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(None, 3); // 3, 4, 5
        drle.push_additional(Some("north".to_string()), 1); // 6
        drle.push_additional(None, 2); // 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10

        // essentially `WHERE value IS NULL`
        let ids = drle.row_ids_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 4, 5, 7, 8]));

        // essentially `WHERE value IS NOT NULL`
        let ids = drle.row_ids_not_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 6, 9, 10]));
    }

    #[test]
    fn value() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        assert_eq!(drle.value(3), Some(&"north".to_string()));
        assert_eq!(drle.value(0), Some(&"east".to_string()));
        assert_eq!(drle.value(10), Some(&"south".to_string()));

        assert_eq!(drle.value(11), None);
        assert_eq!(drle.value(22), None);
    }

    #[test]
    fn dictionary() {
        let mut drle = super::RLE::default();
        assert!(drle.dictionary().is_empty());

        drle.push_additional(Some("east".to_string()), 23);
        drle.push_additional(Some("west".to_string()), 2);
        drle.push_none();
        drle.push_additional(Some("zoo".to_string()), 1);

        assert_eq!(
            drle.dictionary(),
            &["east".to_string(), "west".to_string(), "zoo".to_string()]
        );
    }

    #[test]
    fn values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let mut dst = Vec::with_capacity(1000);
        dst = drle.values(&[0, 1, 3, 4], dst);
        assert_eq!(
            dst,
            vec![Some("east"), Some("east"), Some("north"), Some("east"),]
        );

        dst = drle.values(&[8, 10, 11], dst);
        assert_eq!(dst, vec![Some("east"), Some("south"), None]);

        assert_eq!(dst.capacity(), 1000);

        assert!(drle.values(&[1000], dst).is_empty());
    }

    #[test]
    fn all_values() {
        let mut drle = super::RLE::from(vec!["hello", "zoo"]);
        drle.push_none();

        let zoo = "zoo".to_string();
        let dst = vec![Some(&zoo), Some(&zoo), Some(&zoo), Some(&zoo)];
        let got = drle.all_values(dst);

        assert_eq!(
            got,
            [Some(&"hello".to_string()), Some(&"zoo".to_string()), None]
        );
        assert_eq!(got.capacity(), 4);
    }

    #[test]
    fn distinct_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 100);

        let values = drle.distinct_values((0..100).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"east".to_string())]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let values = drle.distinct_values((0..12).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![
                None,
                Some(&"east".to_string()),
                Some(&"north".to_string()),
                Some(&"south".to_string()),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values((0..4).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"east".to_string()), Some(&"north".to_string()),]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values(&[3, 10], BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"north".to_string()), Some(&"south".to_string()),]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values(&[100], BTreeSet::new());
        assert!(values.is_empty());
    }

    #[test]
    fn contains_other_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let east = &"east".to_string();
        let north = &"north".to_string();
        let south = &"south".to_string();

        let mut others = BTreeSet::new();
        others.insert(Some(east));
        others.insert(Some(north));

        assert!(drle.contains_other_values(&others));

        let f1 = "foo".to_string();
        others.insert(Some(&f1));
        assert!(drle.contains_other_values(&others));

        others.insert(Some(&south));
        others.insert(None);
        assert!(!drle.contains_other_values(&others));

        let f2 = "bar".to_string();
        others.insert(Some(&f2));
        assert!(!drle.contains_other_values(&others));

        assert!(drle.contains_other_values(&BTreeSet::new()));
    }

    #[test]
    fn has_non_null_value() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        assert!(drle.has_non_null_value(&[0]));
        assert!(drle.has_non_null_value(&[0, 1, 2]));
        assert!(drle.has_non_null_value(&[10]));

        assert!(!drle.has_non_null_value(&[11]));
        assert!(!drle.has_non_null_value(&[11, 12, 100]));

        // Pure NULL column...
        drle = super::RLE::default();
        drle.push_additional(None, 10);
        assert!(!drle.has_non_null_value(&[0]));
        assert!(!drle.has_non_null_value(&[4, 7]));
    }

    #[test]
    fn encoded_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let mut encoded = drle.encoded_values(&[0], vec![]);
        assert_eq!(encoded, vec![1]);

        encoded = drle.encoded_values(&[1, 3, 5, 6], vec![]);
        assert_eq!(encoded, vec![1, 2, 1, 1]);

        encoded = drle.encoded_values(&[9, 10, 11], vec![]);
        assert_eq!(encoded, vec![3, 3, 0]);
    }

    #[test]
    fn all_encoded_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional(None, 2);
        drle.push_additional(Some("north".to_string()), 2);

        let dst = Vec::with_capacity(100);
        let dst = drle.all_encoded_values(dst);
        assert_eq!(dst, vec![1, 1, 1, 0, 0, 2, 2]);
        assert_eq!(dst.capacity(), 100);
    }

    #[test]
    fn min() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(None, 2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.min(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(drle.min(&[0, 1, 2, 3, 4, 5, 6]), Some(&"east".to_string()));
        assert_eq!(drle.min(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.min(&[3]), None);
        assert_eq!(drle.min(&[3, 4]), None);

        let mut drle = super::RLE::default();
        drle.push_additional(None, 10);
        assert_eq!(drle.min(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn max() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(None, 2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.max(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(drle.max(&[0, 1, 2, 3, 4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.max(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.max(&[3]), None);
        assert_eq!(drle.max(&[3, 4]), None);

        let drle = super::RLE::default();
        assert_eq!(drle.max(&[0]), None);

        let mut drle = super::RLE::default();
        drle.push_additional(None, 10);
        assert_eq!(drle.max(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn count() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(None, 2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.count(&[0, 1, 2]), 3);
        assert_eq!(drle.count(&[0, 1, 2, 3, 4, 5, 6]), 5);
        assert_eq!(drle.count(&[4, 5, 6]), 2);
        assert_eq!(drle.count(&[3]), 0);
        assert_eq!(drle.count(&[3, 4]), 0);

        let mut drle = super::RLE::default();
        drle.push_additional(None, 10);
        assert_eq!(drle.count(&[2, 3, 6, 8]), 0);
    }
}
