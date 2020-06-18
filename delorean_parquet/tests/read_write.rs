use delorean_parquet::writer::DeloreanParquetTableWriter;
use delorean_table::{packers::Packer, DeloreanTableWriter, Packers};

use parquet::data_type::ByteArray;
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
    let mut packers: Vec<Packers> = vec![
        Packers::String(Packer::new()),  // 0: tag1
        Packers::String(Packer::new()),  // 1: string_field
        Packers::Float(Packer::new()),   // 2: float_field
        Packers::Integer(Packer::new()), // 3: int_field
        Packers::Boolean(Packer::new()), // 4: bool_field
        Packers::Integer(Packer::new()), // 5: timestamp
    ];

    // create this data:

    // row 0: "tag1_val0", "str_val0", 1.0, 100, true, 900000000000
    // row 1: null,       null     , null, null, null, null
    // row 2: "tag1_val2", "str_val2", 2.0, 200, false, 9100000000000
    packers[0]
        .str_packer_mut()
        .push(ByteArray::from("tag1_val0"));
    packers[1]
        .str_packer_mut()
        .push(ByteArray::from("str_val0"));
    packers[2].f64_packer_mut().push(1.0);
    packers[3].i64_packer_mut().push(100);
    packers[4].bool_packer_mut().push(true);
    packers[5].i64_packer_mut().push(900000000000);

    packers[0].push_none();
    packers[1].push_none();
    packers[2].push_none();
    packers[3].push_none();
    packers[4].push_none();
    packers[5].push_none();

    packers[0]
        .str_packer_mut()
        .push(ByteArray::from("tag1_val2"));
    packers[1]
        .str_packer_mut()
        .push(ByteArray::from("str_val2"));
    packers[2].f64_packer_mut().push(2.0);
    packers[3].i64_packer_mut().push(200);
    packers[4].bool_packer_mut().push(true);
    packers[5].i64_packer_mut().push(910000000000);

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
