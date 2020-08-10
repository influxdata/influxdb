use std::{fs::File, path::Path};

use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array, array::Array, datatypes, ipc};

use delorean_mem_qe::column;
use delorean_mem_qe::column::{Column, Scalar};
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
    let r = File::open(Path::new("/Users/edd/work/InfluxData/delorean_misc/in-memory-sort/env_role_path_time/http_api_requests_total.arrow")).unwrap();
    let reader = ipc::reader::StreamReader::try_new(r).unwrap();

    let mut store = Store::default();
    build_store(reader, &mut store).unwrap();

    println!(
        "total segments {:?} with total size {:?}",
        store.segment_total(),
        store.size(),
    );

    // time_column_min_time(&store);
    // time_column_max_time(&store);
    // time_column_first(&store);
    let segments = store.segments();
    let res = segments.last("host").unwrap();
    println!("{:?}", res);

    // let segments = segments
    //     .filter_by_time(1590036110000000, 1590044410000000)
    //     .filter_by_predicate_eq("env", &column::Scalar::String("prod01-eu-central-1"));
    // let res = segments.first(
    //     "env",
    //     &column::Scalar::String("prod01-eu-central-1"),
    //     1590036110000000,
    // );
    // println!("{:?}", res);
    // let segments = segments.filter_by_time(1590036110000000, 1590044410000000);
    // println!("{:?}", segments.last("host"));
    // println!("{:?}", segments.segments().last().unwrap().row(14899));

    // time_row_by_last_ts(&store);

    // let rows = segments
    //     .segments()
    //     .last()
    //     .unwrap()
    //     .filter_by_predicate_eq(
    //         Some((1590040770000000, 1590040790000000)),
    //         vec![
    //             ("env", Some(&column::Scalar::String("prod01-us-west-2"))),
    //             ("method", Some(&column::Scalar::String("GET"))),
    //             (
    //                 "host",
    //                 Some(&column::Scalar::String("queryd-v1-75bc6f7886-57pxd")),
    //             ),
    //         ],
    //     )
    //     .unwrap();

    // for row_id in rows.iter() {
    //     println!(
    //         "{:?} - {:?}",
    //         row_id,
    //         segments.segments().last().unwrap().row(row_id as usize)
    //     );
    // }
    // println!("{:?}", rows.cardinality());

    // time_row_by_preds(&store);
    loop {
        let mut total_count = 0.0;
        let now = std::time::Instant::now();
        for segment in segments.segments() {
            let (min, max) = segment.time_range();
            let time_ids = segment.filter_by_predicates_eq((min, max), vec![]).unwrap();

            let group_ids = segment.group_by_column_ids("env").unwrap();
            for (col_values, row_ids) in group_ids {
                // filter ids by time
                let mut result = row_ids.and(&time_ids);
                // let
                // println!(
                //     "({:?}, {:?}) SUM OF COLUMN env={:?} is {:?} (count is {:?})",
                //     min,
                //     max,
                //     col_values,
                //     segment.sum_column(&"counter", &result),
                //     result.cardinality(),
                // );
                if let column::Scalar::Float(x) =
                    segment.sum_column(&"counter", &mut result).unwrap()
                {
                    total_count += x;
                }
            }
        }
        println!("Done ({:?}) in {:?}", total_count, now.elapsed());
    }
}

fn build_store(
    mut reader: arrow::ipc::reader::StreamReader<File>,
    store: &mut Store,
) -> Result<(), Error> {
    while let Some(rb) = reader.next_batch().unwrap() {
        let segment = convert_record_batch(rb)?;
        store.add_segment(segment);
    }
    Ok(())
}

fn convert_record_batch(rb: RecordBatch) -> Result<Segment, Error> {
    let mut segment = Segment::new(rb.num_rows());

    // println!("cols {:?} rows {:?}", rb.num_columns(), rb.num_rows());
    for (i, column) in rb.columns().iter().enumerate() {
        match *column.data_type() {
            datatypes::DataType::Float64 => {
                if column.null_count() > 0 {
                    panic!("null floats");
                }
                let arr = column
                    .as_any()
                    .downcast_ref::<array::Float64Array>()
                    .unwrap();

                let column = Column::from(arr.value_slice(0, rb.num_rows()));
                segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Int64 => {
                if column.null_count() > 0 {
                    panic!("null times");
                }
                let arr = column.as_any().downcast_ref::<array::Int64Array>().unwrap();

                let column = Column::from(arr.value_slice(0, rb.num_rows()));
                segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap();

                let mut c = column::String::default();
                let mut prev = if !column.is_null(0) {
                    Some(arr.value(0))
                } else {
                    None
                };

                let mut count = 1_u64;
                for j in 1..arr.len() {
                    let next = if column.is_null(j) {
                        None
                    } else {
                        Some(arr.value(j))
                    };

                    if prev == next {
                        count += 1;
                        continue;
                    }

                    match prev {
                        Some(x) => c.add_additional(Some(x.to_string()), count),
                        None => c.add_additional(None, count),
                    }
                    prev = next;
                    count = 1;
                }

                // Add final batch to column if any
                match prev {
                    Some(x) => c.add_additional(Some(x.to_string()), count),
                    None => c.add_additional(None, count),
                }

                segment.add_column(rb.schema().field(i).name(), Column::String(c));
            }
            datatypes::DataType::Boolean => {
                panic!("unsupported");
            }
            _ => panic!("unsupported datatype"),
        }
    }
    Ok(segment)
}

fn time_column_min_time(store: &Store) {
    let repeat = 1000;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut total_min = 0;
    for _ in 1..repeat {
        let now = std::time::Instant::now();
        let segments = store.segments();
        let min = segments.column_min("time").unwrap();
        total_time += now.elapsed();

        if let Scalar::Integer(v) = min {
            total_min += v
        }
    }
    println!(
        "Ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        total_min
    );
}

fn time_column_max_time(store: &Store) {
    let repeat = 1000;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut total_max = 0;
    for _ in 1..repeat {
        let now = std::time::Instant::now();
        let segments = store.segments();
        let max = segments.column_max("time").unwrap();
        total_time += now.elapsed();

        if let Scalar::Integer(v) = max {
            total_max += v
        }
    }
    println!(
        "Ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        total_max
    );
}

fn time_column_first(store: &Store) {
    let repeat = 100000;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut total_max = 0;
    for _ in 1..repeat {
        let now = std::time::Instant::now();
        let segments = store.segments();
        let res = segments.first("host").unwrap();
        total_time += now.elapsed();
        total_max += res.0;
    }
    println!(
        "Ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        total_max
    );
}

fn time_row_by_last_ts(store: &Store) {
    let repeat = 100000;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut total_max = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let (_, _, row_id) = segments.last("time").unwrap();
        let res = segments.segments().last().unwrap().row(row_id).unwrap();
        total_time += now.elapsed();
        total_max += res.len();
    }
    println!(
        "Ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        total_max
    );
}

fn time_row_by_preds(store: &Store) {
    let repeat = 100000;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut total_max = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let rows = segments
            .segments()
            .last()
            .unwrap()
            .filter_by_predicates_eq(
                (1590040770000000, 1590040790000000),
                vec![
                    ("env", Some(&column::Scalar::String("prod01-us-west-2"))),
                    ("method", Some(&column::Scalar::String("GET"))),
                    (
                        "host",
                        Some(&column::Scalar::String("queryd-v1-75bc6f7886-57pxd")),
                    ),
                ],
            )
            .unwrap();

        // for row_id in rows.iter() {
        //     println!(
        //         "{:?} - {:?}",
        //         row_id,
        //         segments.segments().last().unwrap().row(row_id as usize)
        //     );
        // }

        total_time += now.elapsed();
        total_max += rows.cardinality();
    }
    println!(
        "Ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        total_max
    );
}
