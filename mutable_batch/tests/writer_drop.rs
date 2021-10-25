use arrow_util::assert_batches_eq;
use mutable_batch::writer::Writer;
use mutable_batch::MutableBatch;
use schema::selection::Selection;

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

    assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

    let mut writer = Writer::new(&mut batch, 1);
    writer
        .write_string("tag1", None, vec!["v1"].into_iter())
        .unwrap();

    std::mem::drop(writer);

    // Should not include tag1 column
    assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);
}
