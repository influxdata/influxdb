use arrow_util::assert_batches_eq;
use mutable_batch::MutableBatch;
use mutable_batch_pb::{decode::write_table_batch, encode::encode_batch};
use schema::selection::Selection;

#[test]
fn test_encode_decode() {
    let (_, batch) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(
        r#"
        foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1
        foo,t1=bar uv=1u,fv=32.0,bv=true 2
        foo,t1=bar iv=1i,uv=1u,fv=1.0,sv="bye" 3
        foo iv=-3405i,uv=566u,bv=false,sv="hi" 4
        foo,t1=asdf iv=1i,fv=1.23,bv=true,sv="hi" 5
    "#,
    );

    let expected = &[
        "+-------+------+-------+-----+------+--------------------------------+-----+",
        "| bv    | fv   | iv    | sv  | t1   | time                           | uv  |",
        "+-------+------+-------+-----+------+--------------------------------+-----+",
        "| true  | 1    | 1     | hi  | asdf | 1970-01-01T00:00:00.000000001Z | 774 |",
        "| true  | 32   |       |     | bar  | 1970-01-01T00:00:00.000000002Z | 1   |",
        "|       | 1    | 1     | bye | bar  | 1970-01-01T00:00:00.000000003Z | 1   |",
        "| false |      | -3405 | hi  |      | 1970-01-01T00:00:00.000000004Z | 566 |",
        "| true  | 1.23 | 1     | hi  | asdf | 1970-01-01T00:00:00.000000005Z |     |",
        "+-------+------+-------+-----+------+--------------------------------+-----+",
    ];

    assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

    let encoded = encode_batch("foo", &batch);

    let mut batch = MutableBatch::new();
    write_table_batch(&mut batch, &encoded).unwrap();

    assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);
}
