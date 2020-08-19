use std::{fs, fs::File, path::PathBuf, rc::Rc, sync::Arc};

use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array, array::Array, datatypes};

use delorean_mem_qe::column;
use delorean_mem_qe::column::Column;
use delorean_mem_qe::segment::{Aggregate, GroupingStrategy, Segment};
use delorean_mem_qe::{adapter::DeloreanQueryEngine, Store};
use parquet::arrow::arrow_reader::ArrowReader;

// use snafu::ensure;
use datatypes::TimeUnit;
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

fn format_size(sz: usize) -> String {
    human_format::Formatter::new().format(sz as f64)
}

fn main() {
    env_logger::init();

    //let r = File::open(Path::new("/Users/edd/work/InfluxData/delorean_misc/in-memory-sort/env_role_path_time/http_api_requests_total.arrow")).unwrap();

    //let path = PathBuf::from("/Users/alamb/Software/query_testing/cloud2_sli_dashboard_query.ingested/data/000000000089174-000000004/http_api_requests_total.parquet");

    // smaller file to test with
    let path = PathBuf::from("/Users/alamb/Software/query_testing/cloud2_sli_dashboard_query.ingested/data/000000000068644-000000002/http_api_requests_total.parquet");

    let r = File::open(&path).unwrap();
    let file_size = fs::metadata(&path).expect("read metadata").len();
    println!(
        "Reading {} ({}) bytes of parquet from {:?}....",
        format_size(file_size as usize),
        file_size,
        path
    );

    //let r = File::open("/Users/alamb/Software/query_testing/cloud2_sli_dashboard_query.ingested/data/000000000095062-000000006/http_api_requests_total.parquet").unwrap();
    let parquet_reader = parquet::file::reader::SerializedFileReader::new(r).unwrap();
    let mut reader =
        parquet::arrow::arrow_reader::ParquetFileArrowReader::new(Rc::new(parquet_reader));
    let batch_size = 60000;
    let record_batch_reader = reader.get_record_reader(batch_size).unwrap();

    //let reader = ipc::reader::StreamReader::try_new(r).unwrap();

    let mut store = Store::default();
    build_store(record_batch_reader, &mut store).unwrap();

    println!(
        "total segments {:?} with total size {} ({})",
        store.segment_total(),
        format_size(store.size()),
        store.size()
    );
    let store = Arc::new(store);

    time_select_with_pred(&store);
    time_datafusion_select_with_pred(store.clone());
    time_first_host(&store);
    time_sum_range(&store);
    time_count_range(&store);
    time_group_single_with_pred(&store);
    time_group_by_multi_agg_count(&store);
    time_group_by_multi_agg_sorted_count(&store);

    // time_column_min_time(&store);
    // time_column_max_time(&store);
    // time_column_first(&store);
    // let segments = store.segments();
    // let res = segments.last("host").unwrap();
    // println!("{:?}", res);

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

    // let segments = store.segments();
    // let columns = segments.read_filter_eq(
    //     (1590036110000000, 1590040770000000),
    //     &[("env", Some(&column::Scalar::String("prod01-eu-central-1")))],
    //     vec![
    //         "env".to_string(),
    //         "method".to_string(),
    //         "host".to_string(),
    //         "counter".to_string(),
    //         "time".to_string(),
    //     ],
    // );

    // for (k, v) in columns {
    //     println!("COLUMN {:?}", k);
    //     // println!("ROWS ({:?}) {:?}", v.len(), 0);
    //     println!("ROWS ({}) {:?}", v, v.len());
    // }

    // loop {
    //     let now = std::time::Instant::now();
    //     let segments = store.segments();
    //     let groups = segments.read_group_eq(
    //         (0, 1590044410000000),
    //         &[],
    //         vec!["env".to_string(), "role".to_string()],
    //         vec![
    //             ("counter".to_string(), Aggregate::Sum),
    //             // ("counter".to_string(), Aggregate::Count),
    //         ],
    //     );
    //     println!("{:?} {:?}", groups, now.elapsed());
    // }

    // loop {
    //     let mut total_count = 0.0;
    //     let now = std::time::Instant::now();
    //     for segment in segments.segments() {
    //         let (min, max) = segment.time_range();
    //         let time_ids = segment
    //             .filter_by_predicates_eq((min, max), &vec![])
    //             .unwrap();

    //         let group_ids = segment.group_by_column_ids("env").unwrap();
    //         for (col_values, row_ids) in group_ids {
    //             // filter ids by time
    //             let mut result = row_ids.and(&time_ids);
    //             // let
    //             // println!(
    //             //     "({:?}, {:?}) SUM OF COLUMN env={:?} is {:?} (count is {:?})",
    //             //     min,
    //             //     max,
    //             //     col_values,
    //             //     segment.sum_column(&"counter", &result),
    //             //     result.cardinality(),
    //             // );
    //             if let column::Scalar::Float(x) =
    //                 segment.sum_column(&"counter", &mut result).unwrap()
    //             {
    //                 total_count += x;
    //             }
    //         }
    //     }
    //     println!("Done ({:?}) in {:?}", total_count, now.elapsed());
    // }
}

fn build_store(mut reader: impl RecordBatchReader, store: &mut Store) -> Result<(), Error> {
    let mut total_rows_read = 0;
    let start = std::time::Instant::now();
    loop {
        let rb = reader.next_batch();
        match rb {
            Err(e) => println!("WARNING: error reading batch: {:?}, SKIPPING", e),
            Ok(Some(rb)) => {
                // if i < 363 {
                //     i += 1;
                //     continue;
                // }
                total_rows_read += rb.num_rows();
                let segment = convert_record_batch(rb)?;
                store.add_segment(segment);
            }
            Ok(None) => {
                let now = std::time::Instant::now();
                println!(
                    "Completed loading {} rows in {:?}",
                    total_rows_read,
                    now - start
                );
                return Ok(());
            }
        }
    }
}

fn convert_record_batch(rb: RecordBatch) -> Result<Segment, Error> {
    let mut segment = Segment::new(rb.num_rows(), rb.schema().clone());

    println!(
        "Loading record batch: cols {:?} rows {:?}",
        rb.num_columns(),
        rb.num_rows()
    );
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
            datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => {
                if column.null_count() > 0 {
                    panic!("null times");
                }
                let arr = column
                    .as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                    .unwrap();

                let column = Column::from(arr.value_slice(0, rb.num_rows()));
                segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap();

                // IMPORTANT - build a set of values (dictionary) ahead of
                // time so we can ensure we encoded the column in an ordinally
                // correct manner.
                //
                // We can use a trick where encoded integers are ordered according
                // to the decoded values, making sorting, comparison and grouping
                // more efficient.
                //
                let mut dictionary: std::collections::BTreeSet<Option<String>> =
                    std::collections::BTreeSet::new();
                for j in 1..arr.len() {
                    let next = if column.is_null(j) {
                        None
                    } else {
                        Some(arr.value(j).to_string())
                    };
                    dictionary.insert(next);
                }

                let mut c = column::String::with_dictionary(dictionary);
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
            ref d @ _ => panic!("unsupported datatype: {:?}", d),
        }
    }
    Ok(segment)
}

//
// SELECT FIRST(host) FROM measurement
//
fn time_first_host(store: &Store) {
    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let (ts, _, _) = segments.first("host").unwrap();

        total_time += now.elapsed();
        track += ts;
    }
    println!(
        "time_first_host ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

//
// SELECT SUM(counter) FROM measurement
// WHERE time >= "2020-05-07 06:48:00" AND time < "2020-05-21 07:00:10"
//
fn time_sum_range(store: &Store) {
    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let segments = store.segments();
    let mut track = 0.0;
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        for segment in segments.segments() {
            let filtered_ids =
                segment.filter_by_predicates_eq((1588834080000000, 1590044410000000), &[]);
            if let Some(mut row_ids) = filtered_ids {
                if let column::Scalar::Float(v) =
                    segment.sum_column("counter", &mut row_ids).unwrap()
                {
                    track += v;
                }
            }
        }

        total_time += now.elapsed();
    }
    println!(
        "time_sum_range ran {:?} in {:?} {:?} / total {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

//
// SELECT COUNT(counter) FROM measurement
// WHERE time >= "2020-05-07 06:48:00" AND time < "2020-05-21 07:00:10"
//
fn time_count_range(store: &Store) {
    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        for segment in segments.segments() {
            let filtered_ids =
                segment.filter_by_predicates_eq((1588834080000000, 1590044410000000), &[]);
            if let Some(mut row_ids) = filtered_ids {
                track += segment.count_column("counter", &mut row_ids).unwrap();
            }
        }

        total_time += now.elapsed();
    }
    println!(
        "time_count_range ran {:?} in {:?} {:?} / total {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

//
// SELECT env, method, host, counter, time
// FROM measurement
// WHERE time >= "2020-05-21 04:41:50" AND time < "2020-05-21 05:59:30"
// AND "env" = "prod01-eu-central-1"
fn time_select_with_pred(store: &Store) {
    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let columns = segments.read_filter_eq(
            (1590036110000000, 1590040770000000),
            &[("env", Some(&column::Scalar::String("prod01-eu-central-1")))],
            vec![
                "env".to_string(),
                "method".to_string(),
                "host".to_string(),
                "counter".to_string(),
                "time".to_string(),
            ],
        );

        total_time += now.elapsed();
        track += columns.len();
    }
    println!(
        "time_select_with_pred ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

/// DataFusion implementation of
//
// SELECT env, method, host, counter, time
// FROM measurement
// WHERE time >= "2020-05-21 04:41:50" AND time < "2020-05-21 05:59:30"
// AND "env" = "prod01-eu-central-1"
//
// Use the hard coded timestamp values 1590036110000000, 1590040770000000

fn time_datafusion_select_with_pred(store: Arc<Store>) {
    let mut query_engine = DeloreanQueryEngine::new(store);

    let sql_string = r#"SELECT env, method, host, counter, time
               FROM measurement
               WHERE time::BIGINT >= 1590036110000000
               AND time::BIGINT < 1590040770000000
               AND env = 'prod01-eu-central-1'
     "#;

    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    for _ in 0..repeat {
        let now = std::time::Instant::now();
        track += query_engine.run_sql(&sql_string);
        total_time += now.elapsed();
    }
    println!(
        "time_datafusion_select_with_pred ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

//
// SELECT env, method, host, counter, time
// FROM measurement
// WHERE time >= "2020-05-21 04:41:50" AND time < "2020-05-21 05:59:30"
// AND "env" = "prod01-eu-central-1"
//
fn time_group_single_with_pred(store: &Store) {
    let repeat = 100;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        for segment in segments.segments() {
            let results = segment.group_single_agg_by_predicate_eq(
                (1588834080000000, 1590044410000000),
                &[],
                &"env".to_string(),
                &vec![("counter".to_string(), Aggregate::Count)],
            );
            track += results.len();
        }

        total_time += now.elapsed();
    }
    println!(
        "time_group_single_with_pred ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

fn time_group_by_multi_agg_count(store: &Store) {
    let strats = vec![
        GroupingStrategy::HashGroup,
        GroupingStrategy::HashGroupConcurrent,
        GroupingStrategy::SortGroup,
        GroupingStrategy::SortGroupConcurrent,
    ];

    for strat in &strats {
        let repeat = 10;
        let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
        let mut total_max = 0;
        let segments = store.segments();
        for _ in 0..repeat {
            let now = std::time::Instant::now();

            let groups = segments.read_group_eq(
                (1589000000000001, 1590044410000000),
                &[],
                vec!["status".to_string(), "method".to_string()],
                vec![("counter".to_string(), Aggregate::Count)],
                strat,
            );

            total_time += now.elapsed();
            total_max += groups.len();
        }
        println!(
            "time_group_by_multi_agg_count_{:?} ran {:?} in {:?} {:?} / call {:?}",
            strat,
            repeat,
            total_time,
            total_time / repeat,
            total_max
        );
    }
}

fn time_group_by_multi_agg_sorted_count(store: &Store) {
    let strats = vec![
        GroupingStrategy::HashGroup,
        GroupingStrategy::HashGroupConcurrent,
        GroupingStrategy::SortGroup,
        GroupingStrategy::SortGroupConcurrent,
    ];

    for strat in &strats {
        let repeat = 10;
        let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
        let mut total_max = 0;
        let segments = store.segments();
        for _ in 0..repeat {
            let now = std::time::Instant::now();

            let groups = segments.read_group_eq(
                (1589000000000001, 1590044410000000),
                &[],
                vec!["env".to_string(), "role".to_string()],
                vec![("counter".to_string(), Aggregate::Count)],
                strat,
            );

            total_time += now.elapsed();
            total_max += groups.len();
        }
        println!(
            "time_group_by_multi_agg_SORTED_count_{:?} ran {:?} in {:?} {:?} / call {:?}",
            strat,
            repeat,
            total_time,
            total_time / repeat,
            total_max
        );
    }
}
