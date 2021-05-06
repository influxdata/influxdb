//! Provide storage statistics for parquet files
use super::error::{ParquetLibraryError, Result};
use arrow::datatypes::DataType;
use parquet::{
    file::reader::{ChunkReader, FileReader, SerializedFileReader},
    schema,
};
use snafu::ResultExt;

pub fn parquet_schema_as_string(parquet_schema: &schema::types::Type) -> String {
    let mut parquet_schema_string = Vec::new();
    schema::printer::print_schema(&mut parquet_schema_string, parquet_schema);
    String::from_utf8_lossy(&parquet_schema_string).to_string()
}

/// Maps from parquet types to table schema types
pub fn data_type_from_parquet_type(parquet_type: parquet::basic::Type) -> DataType {
    use parquet::basic::Type::*;

    match parquet_type {
        BOOLEAN => DataType::Boolean,
        INT64 => DataType::Int64,
        DOUBLE => DataType::Float64,
        BYTE_ARRAY => DataType::Utf8,
        _ => {
            unimplemented!("Unsupported parquet datatype: {:?}", parquet_type);
        }
    }
}

/// Print parquet metadata that can be read from `input`, with a total
/// size of `input_size` byes
pub fn print_parquet_metadata<R: 'static>(input: R) -> Result<()>
where
    R: ChunkReader,
{
    let input_len = input.len();

    let reader = SerializedFileReader::new(input).context(ParquetLibraryError {
        message: "Creating parquet reader",
    })?;

    let parquet_metadata = reader.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let num_columns = file_metadata.schema_descr().num_columns();

    println!("Parquet file size: {} bytes", input_len);
    println!(
        "Parquet file Schema: {}",
        parquet_schema_as_string(file_metadata.schema()).trim_end()
    );
    println!("Parquet file metadata:");
    println!("  row groups: {}", parquet_metadata.num_row_groups());
    if let Some(created_by) = file_metadata.created_by() {
        println!("  created by: {:?}", created_by);
    }
    println!("  version: {}", file_metadata.version());
    println!("  num_rows: {}", file_metadata.num_rows());
    println!("  num_columns: {}", num_columns);
    let kv_meta_len = match file_metadata.key_value_metadata() {
        Some(v) => v.len(),
        None => 0,
    };
    println!("  key_value_meta.len: {:?}", kv_meta_len);
    if let Some(column_orders) = file_metadata.column_orders() {
        println!("  column_orders:");
        for (idx, col) in column_orders.iter().enumerate() {
            println!("    column_order[{}]: {:?}", idx, col.sort_order());
        }
    }

    println!("Row groups:");
    for (rg_idx, rg_metadata) in parquet_metadata.row_groups().iter().enumerate() {
        println!("  Row Group [{}]:", rg_idx);
        println!(
            "    total uncompressed byte size: {}",
            rg_metadata.total_byte_size()
        );
        for (cc_idx, cc_metadata) in rg_metadata.columns().iter().enumerate() {
            println!("  Column Chunk [{}]:", cc_idx);
            println!("    file_offset: {}", cc_metadata.file_offset());
            println!("    column_type: {:?}", cc_metadata.column_type());
            println!("    column_path: {}", cc_metadata.column_path().string());
            println!("    num_values: {}", cc_metadata.num_values());
            println!("    encodings: {:?}", cc_metadata.encodings());
            println!("    compression: {:?}", cc_metadata.compression());
            println!("    compressed_size: {}", cc_metadata.compressed_size());
            println!("    uncompressed_size: {}", cc_metadata.uncompressed_size());
            println!("    data_page_offset: {}", cc_metadata.data_page_offset());
            println!("    has_index_page: {}", cc_metadata.has_index_page());
            if let Some(index_page_offset) = cc_metadata.index_page_offset() {
                println!("    index_page_offset: {}", index_page_offset);
            }
            println!(
                "    has_dictionary_page: {}",
                cc_metadata.has_dictionary_page()
            );
            if let Some(dictionary_page_offset) = cc_metadata.dictionary_page_offset() {
                println!("    dictionary_page_offset: {}", dictionary_page_offset);
            }
            if let Some(statistics) = cc_metadata.statistics() {
                println!("    statistics: {:?}", statistics);
            } else {
                println!("    NO STATISTICS");
            }
        }
    }

    Ok(())
}
