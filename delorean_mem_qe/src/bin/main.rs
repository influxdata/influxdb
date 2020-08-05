use std::{fs::File, path::Path};

use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array, array::Array, datatypes, ipc};

use delorean_mem_qe::column;
use delorean_mem_qe::column::Column;
use delorean_mem_qe::segment::Segment;
use delorean_mem_qe::Store;

// use snafu::ensure;
use snafu::Snafu;

#[derive(Snafu, Debug, Clone, Copy, PartialEq)]
pub enum Error {
    // #[snafu(display(r#"Too many sort columns specified"#))]
// TooManyColumns,

// #[snafu(display(r#"Same column specified as sort column multiple times"#))]
// RepeatedColumns { index: usize },

// #[snafu(display(r#"Specified column index is out bounds"#))]
// OutOfBoundsColumn { index: usize },
}

fn main() {
    let mut store = Store::default();
    read_arrow_file(&mut store);

    println!(
        "total segments {:?} with total size {:?}",
        store.segment_total(),
        store.size(),
    );
}

fn read_arrow_file(store: &mut Store) {
    let r = File::open(Path::new("/Users/edd/work/InfluxData/delorean_misc/in-memory-sort/env_role_path_time/http_api_requests_total.arrow")).unwrap();
    let mut reader = ipc::reader::StreamReader::try_new(r).unwrap();
    while let Some(batch) = reader.next_batch().unwrap() {
        let segment = record_batch_to_segment(&batch).unwrap();
        store.add_segment(segment);
    }
}

fn record_batch_to_segment(rb: &RecordBatch) -> Result<Segment, Error> {
    let mut segment = Segment::default();

    // println!("cols {:?} rows {:?}", rb.num_columns(), rb.num_rows());
    for (i, column) in rb.columns().iter().enumerate() {
        match *column.data_type() {
            datatypes::DataType::Float64 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<array::Float64Array>()
                    .unwrap();
                let column = Column::from(arr.value_slice(0, rb.num_rows()));
                segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Int64 => {
                let arr = column.as_any().downcast_ref::<array::Int64Array>().unwrap();
                let column = Column::from(arr.value_slice(0, rb.num_rows()));
                segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap();

                let mut column = column::String::default();
                let mut prev = arr.value(0);
                let mut count = 1_u64;
                for j in 1..arr.len() {
                    let next = arr.value(j);
                    if prev == next {
                        count += 1;
                    } else {
                        column.add_additional(prev, count);
                        prev = next;
                        count = 1;
                    }
                }
                segment.add_column(rb.schema().field(i).name(), Column::String(column));
            }
            datatypes::DataType::Boolean => {
                panic!("unsupported");
            }
            _ => panic!("unsupported datatype"),
        }
    }
    Ok(segment)
}
