use arrow_util::assert_batches_eq;
use data_types::{PartitionKey, PartitionTemplate, TemplatePart};
use mutable_batch::{writer::Writer, MutableBatch, PartitionWrite, WritePayload};
use mutable_batch_pb::{decode::write_table_batch, encode::encode_batch};
use schema::Projection;

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
        "| true  | 1.0  | 1     | hi  | asdf | 1970-01-01T00:00:00.000000001Z | 774 |",
        "| true  | 32.0 |       |     | bar  | 1970-01-01T00:00:00.000000002Z | 1   |",
        "|       | 1.0  | 1     | bye | bar  | 1970-01-01T00:00:00.000000003Z | 1   |",
        "| false |      | -3405 | hi  |      | 1970-01-01T00:00:00.000000004Z | 566 |",
        "| true  | 1.23 | 1     | hi  | asdf | 1970-01-01T00:00:00.000000005Z |     |",
        "+-------+------+-------+-----+------+--------------------------------+-----+",
    ];

    assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

    let encoded = encode_batch(42, &batch);
    assert_eq!(encoded.table_id, 42);

    let mut batch = MutableBatch::new();
    write_table_batch(&mut batch, &encoded).unwrap();

    assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
}

// This test asserts columns containing no values do not prevent an encoded
// batch from being deserialize:
//
//  https://github.com/influxdata/influxdb_iox/issues/4272
//
// The test constructs a table such that after partitioning, one entire column
// is NULL within a partition. In this test case, the following table is
// partitioned by YMD:
//
//
//    | time       | A    | B    |
//    | ---------- | ---- | ---- |
//    | 1970-01-01 | 1    | NULL |
//    | 1970-07-05 | NULL | 1    |
//
// Yielding two partitions:
//
//    | time       | A    | B    |
//    | ---------- | ---- | ---- |
//    | 1970-01-01 | 1    | NULL |
//
// and:
//
//    | time       | A    | B    |
//    | ---------- | ---- | ---- |
//    | 1970-07-05 | NULL | 1    |
//
// In both partitions, one column is composed entirely of NULL values.
//
// Encoding each of these partitions succeeds, but decoding the partition fails
// due to the inability to infer a column type from the serialized format which
// contains no values:
//
// ```
//  Column {
//      column_name: "B",
//      semantic_type: Field,
//      values: Some(
//          Values {
//              i64_values: [],
//              f64_values: [],
//              u64_values: [],
//              string_values: [],
//              bool_values: [],
//              bytes_values: [],
//              packed_string_values: None,
//              interned_string_values: None,
//          },
//      ),
//      null_mask: [
//          1,
//      ],
//  },
// ```
//
// In a column that is not entirely NULL, one of the "Values" fields would be
// non-empty, and the decoder would use this to infer the type of the column.
//
// Because we have chosen to not differentiate between "NULL" and "empty" in our
// proto encoding, the decoder cannot infer which field within the "Values"
// struct the column belongs to - all are valid, but empty. This causes
// [`Error::EmptyColumn`] to be returned during deserialisation.
//
// This is fixed by skipping entirely-null columns when encoding the batch.
#[test]
fn test_encode_decode_null_columns_issue_4272() {
    let mut batch = MutableBatch::new();
    let mut writer = Writer::new(&mut batch, 2);

    writer
        // Yielding partition keys: ["1970-01-01", "1970-07-05"]
        .write_time("time", [160, 16007561568756160].into_iter())
        .unwrap();
    writer
        .write_i64("A", Some(&[0b00000001]), [1].into_iter())
        .unwrap();
    writer
        .write_i64("B", Some(&[0b00000010]), [1].into_iter())
        .unwrap();
    writer.commit();

    let mut partitions = PartitionWrite::partition(
        "test",
        &batch,
        &PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        },
    );

    // There should be two partitions, one with for the timestamp 160, and
    // one for the other timestamp.
    assert_eq!(partitions.len(), 2);

    // Round-trip the "1970-01-01" partition
    let mut got = MutableBatch::default();
    partitions
        .remove::<PartitionKey>(&"1970-01-01".into())
        .expect("partition not found")
        .write_to_batch(&mut got)
        .expect("should write");

    let encoded = encode_batch(24, &got);
    assert_eq!(encoded.table_id, 24);

    let mut batch = MutableBatch::new();
    // Without the fix for #4272 this deserialisation call would fail.
    write_table_batch(&mut batch, &encoded).unwrap();

    let expected = &[
        "+---+--------------------------------+",
        "| A | time                           |",
        "+---+--------------------------------+",
        "| 1 | 1970-01-01T00:00:00.000000160Z |",
        "+---+--------------------------------+",
    ];
    assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);

    // And finally assert the "1970-07-05" round-trip
    let mut got = MutableBatch::default();
    partitions
        .remove::<PartitionKey>(&"1970-07-05".into())
        .expect("partition not found")
        .write_to_batch(&mut got)
        .expect("should write");

    let encoded = encode_batch(42, &got);
    assert_eq!(encoded.table_id, 42);

    let mut batch = MutableBatch::new();
    // Without the fix for #4272 this deserialisation call would fail.
    write_table_batch(&mut batch, &encoded).unwrap();

    let expected = &[
        "+---+--------------------------------+",
        "| B | time                           |",
        "+---+--------------------------------+",
        "| 1 | 1970-07-05T06:32:41.568756160Z |",
        "+---+--------------------------------+",
    ];
    assert_batches_eq!(expected, &[batch.to_arrow(Projection::All).unwrap()]);
}
