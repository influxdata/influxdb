use delorean_parquet::writer::DeloreanParquetTableWriter;

use delorean_table::{packers::Packer, DeloreanTableWriter};
use delorean_table_schema::DataType;
use std::fs;

#[test]
fn test_write_parquet_data() {
    let schema = delorean_table_schema::SchemaBuilder::new("measurement_name")
        .tag("tag1")
        .field("string_field", delorean_table_schema::DataType::String)
        .field("float_field", delorean_table_schema::DataType::Float)
        .field("int_field", delorean_table_schema::DataType::Integer)
        .field("bool_field", delorean_table_schema::DataType::Boolean)
        .build();

    assert_eq!(schema.get_col_defs().len(), 6);
    let mut packers = vec![
        Packer::new(DataType::String),  // 0: tag1
        Packer::new(DataType::String),  // 1: string_field
        Packer::new(DataType::Float),   // 2: float_field
        Packer::new(DataType::Integer), // 3: int_field
        Packer::new(DataType::Boolean), // 4: bool_field
        Packer::new(DataType::Integer), // 5: timstamp
    ];

    // create this data:

    // row 0: "tag1_val0", "str_val0", 1.0, 100, true, 900000000000
    // row 1: null,       null     , null, null, null, null
    // row 2: "tag1_val2", "str_val2", 2.0, 200, false, 9100000000000
    packers[0].pack_str(Some("tag1_val0"));
    packers[1].pack_str(Some("str_val0"));
    packers[2].pack_f64(Some(1.0));
    packers[3].pack_i64(Some(100));
    packers[4].pack_bool(Some(true));
    packers[5].pack_i64(Some(900000000000));

    packers[0].pack_none();
    packers[1].pack_none();
    packers[2].pack_none();
    packers[3].pack_none();
    packers[4].pack_none();
    packers[5].pack_none();

    packers[0].pack_str(Some("tag1_val2"));
    packers[1].pack_str(Some("str_val2"));
    packers[2].pack_f64(Some(2.0));
    packers[3].pack_i64(Some(200));
    packers[4].pack_bool(Some(true));
    packers[5].pack_i64(Some(910000000000));

    // write the data out to the parquet file
    let output_path = delorean_test_helpers::tempfile::Builder::new()
        .prefix("delorean_parquet_e2e")
        .suffix(".parquet")
        .tempfile()
        .expect("error creating temp file")
        .into_temp_path();
    let output_file = fs::File::create(&output_path).expect("can't open temp file for writing");

    let mut parquet_writer =
        DeloreanParquetTableWriter::new(&schema, output_file).expect("can't create parquet writer");
    parquet_writer
        .write_batch(&packers)
        .expect("can't write batch");
    parquet_writer.close().expect("can't close writer");

    // verify the file has some non zero content
    let file_meta = fs::metadata(&output_path).expect("Can't get file metadata");
    assert!(file_meta.is_file());
    assert!(file_meta.len() > 0, "Length was {}", file_meta.len());

    // TODO: implement a parquet reader to read the file back in
    // then read the data back in and verify it came out successfully
    let output_path_string: String = output_path.to_string_lossy().to_string();
    output_path.close().expect("error deleting the temp file");

    assert!(
        fs::metadata(&output_path_string).is_err(),
        "temp file was not cleaned up {:?}",
        &output_path_string
    );
}
