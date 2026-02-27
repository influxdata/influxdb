// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow_util::assert_batches_eq;
use mutable_batch::MutableBatch;
use mutable_batch::writer::Writer;
use schema::Projection;

#[test]
fn test_new_column() {
    let mut batch = MutableBatch::new();
    let mut writer = Writer::new(&mut batch, 2);

    writer
        .write_bool("b1", None, vec![true, false].into_iter())
        .unwrap();

    writer.commit();

    let expected = &[
        "+-------+",
        "| b1    |",
        "+-------+",
        "| true  |",
        "| false |",
        "+-------+",
    ];

    assert_batches_eq!(
        expected,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let mut writer = Writer::new(&mut batch, 1);
    writer
        .write_string("tag1", None, vec!["v1"].into_iter())
        .unwrap();

    std::mem::drop(writer);

    // Should not include tag1 column
    assert_batches_eq!(
        expected,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );
}
