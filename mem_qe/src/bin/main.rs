use std::{
    env,
    ffi::OsStr,
    fs,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use datatypes::TimeUnit;
use observability_deps::tracing::debug;
use snafu::Snafu;

use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array, array::Array, datatypes, ipc};
use mem_qe::column;
use mem_qe::column::{AggregateType, Column};
use mem_qe::segment::{ColumnType, GroupingStrategy, Schema, Segment};
use mem_qe::Store;
use parquet::arrow::arrow_reader::ArrowReader;

#[derive(Snafu, Debug, Clone, Copy, PartialEq)]
pub enum Error {}

fn format_size(sz: usize) -> String {
    human_format::Formatter::new().format(sz as f64)
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    let path = &args[1];
    let mut sort_order = vec![];
    if let Some(arg) = args.get(2) {
        sort_order = arg.split(',').collect::<Vec<_>>();
        println!("sort is {:?}", sort_order);
    };

    let mut store = Store::default();
    match Path::new(path).extension().and_then(OsStr::to_str) {
        Some("arrow") => build_arrow_store(path, &mut store, sort_order).unwrap(),
        Some("parquet") => build_parquet_store(path, &mut store, sort_order).unwrap(),
        _ => panic!("unsupported file type"),
    }

    println!(
        "total segments {:?} with total size {} ({})",
        store.segment_total(),
        format_size(store.size()),
        store.size()
    );
    let store = Arc::new(store);

    time_select_with_pred(&store);
    // time_datafusion_select_with_pred(store.clone());
    time_first_host(&store);
    time_sum_range(&store);
    time_count_range(&store);
    time_group_single_with_pred(&store);
    time_group_by_multi_agg_count(&store);
    time_group_by_multi_agg_sorted_count(&store);
    time_window_agg_count(&store);
    time_tag_keys_with_pred(&store);
    time_tag_values_with_pred(&store);
    time_group_by_different_columns(&store);
}

fn build_parquet_store(path: &str, store: &mut Store, sort_order: Vec<&str>) -> Result<(), Error> {
    let path = PathBuf::from(path);
    let r = File::open(&path).unwrap();
    let file_size = fs::metadata(&path).expect("read metadata").len();
    println!(
        "Reading {} ({}) bytes of Parquet from {:?}....",
        format_size(file_size as usize),
        file_size,
        path
    );

    let parquet_reader = parquet::file::reader::SerializedFileReader::new(r).unwrap();
    let mut reader =
        parquet::arrow::arrow_reader::ParquetFileArrowReader::new(Arc::new(parquet_reader));
    let batch_size = 60000;
    let record_batch_reader = reader.get_record_reader(batch_size).unwrap();
    build_store(record_batch_reader, store, sort_order)
}

fn build_arrow_store(path: &str, store: &mut Store, sort_order: Vec<&str>) -> Result<(), Error> {
    let r = File::open(Path::new(path)).unwrap();
    let file_size = fs::metadata(&path).expect("read metadata").len();
    println!(
        "Reading {} ({}) bytes of Arrow from {:?}....",
        format_size(file_size as usize),
        file_size,
        path
    );

    let reader = ipc::reader::StreamReader::try_new(r).unwrap();
    build_store(reader, store, sort_order)
}

fn build_store(
    mut reader: impl RecordBatchReader,
    store: &mut Store,
    sort_order: Vec<&str>,
) -> Result<(), Error> {
    let mut total_rows_read = 0;
    let start = std::time::Instant::now();
    loop {
        let rb = reader.next().transpose();
        match rb {
            Err(e) => println!("WARNING: error reading batch: {:?}, SKIPPING", e),
            Ok(Some(rb)) => {
                // if i < 360 {
                //     i += 1;
                //     continue;
                // }
                let schema = Schema::with_sort_order(
                    rb.schema(),
                    sort_order.iter().map(|s| s.to_string()).collect(),
                );

                total_rows_read += rb.num_rows();
                let mut segment = Segment::new(rb.num_rows(), schema);
                convert_record_batch(rb, &mut segment);

                debug!("{}", &segment);
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

fn convert_record_batch(rb: RecordBatch, segment: &mut Segment) {
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
                let column = Column::from(arr.values());
                segment.add_column(rb.schema().field(i).name(), ColumnType::Field(column));

                // TODO(edd): figure out how to get ownership here without
                // cloning
                // let arr: array::Float64Array =
                // arrow::array::PrimitiveArray::from(column.data());
                // let column = Column::from(arr);
                // segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Int64 => {
                if column.null_count() > 0 {
                    panic!("null integers not expected in testing");
                }
                let arr = column.as_any().downcast_ref::<array::Int64Array>().unwrap();
                let column = Column::from(arr.values());
                segment.add_column(rb.schema().field(i).name(), ColumnType::Time(column));

                // TODO(edd): figure out how to get ownership here without
                // cloning
                // let arr: array::Int64Array =
                // arrow::array::PrimitiveArray::from(column.data());
                // let column = Column::from(arr);
                // segment.add_column(rb.schema().field(i).name(), column);
            }
            datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => {
                if column.null_count() > 0 {
                    panic!("null timestamps not expected in testing");
                }
                let arr = column
                    .as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                    .unwrap();
                let column = Column::from(arr.values());
                segment.add_column(rb.schema().field(i).name(), ColumnType::Time(column));

                // TODO(edd): figure out how to get ownership here without
                // cloning
                // let arr: array::TimestampMicrosecondArray =
                //     arrow::array::PrimitiveArray::from(column.data());
                // let column = Column::from(arr);
                // segment.add_column(rb.schema().field(i).name(), column);
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

                segment.add_column(
                    rb.schema().field(i).name(),
                    ColumnType::Tag(Column::String(c)),
                );
            }
            datatypes::DataType::Boolean => {
                panic!("unsupported");
            }
            _ => panic!("unsupported datatype"),
        }
    }
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
            &[("env", "prod01-eu-central-1")],
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

// fn time_datafusion_select_with_pred(store: Arc<Store>) {
//     let mut query_engine = IOxQueryEngine::new(store);

//     let sql_string = r#"SELECT env, method, host, counter, time
//                FROM measurement
//                WHERE time::BIGINT >= 1590036110000000
//                AND time::BIGINT < 1590040770000000
//                AND env = 'prod01-eu-central-1'
//      "#;

//     let repeat = 100;
//     let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
//     let mut track = 0;
//     for _ in 0..repeat {
//         let now = std::time::Instant::now();
//         track += query_engine.run_sql(&sql_string);
//         total_time += now.elapsed();
//     }
//     println!(
//         "time_datafusion_select_with_pred ran {:?} in {:?} {:?} / call {:?}",
//         repeat,
//         total_time,
//         total_time / repeat,
//         track
//     );
// }

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
                &[("counter".to_string(), AggregateType::Count)],
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

//
// SELECT COUNT(counter)
// FROM measurement
// WHERE time >= "2020-05-21 04:41:50" AND time < "2020-05-21 05:59:30"
// GROUP BY "status", "method"
//
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
                (1589000000000001, 1590044410000001),
                &[],
                vec!["status".to_string(), "method".to_string()],
                vec![("counter".to_string(), AggregateType::Count)],
                0,
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

//
// SELECT COUNT(counter)
// FROM measurement
// WHERE time >= "2020-05-21 04:41:50" AND time < "2020-05-21 05:59:30"
// GROUP BY "env", "role"
//
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
                vec![("counter".to_string(), AggregateType::Count)],
                0,
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

fn time_window_agg_count(store: &Store) {
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
                vec![("counter".to_string(), AggregateType::Count)],
                60000000 * 10, // 10 minutes,
                strat,
            );

            total_time += now.elapsed();
            total_max += groups.len();
        }
        println!(
            "time_window_agg_count {:?} ran {:?} in {:?} {:?} / call {:?}",
            strat,
            repeat,
            total_time,
            total_time / repeat,
            total_max
        );
    }
}

//
// SHOW TAG KEYS WHERE time >= x and time < y AND "env" = 'prod01-eu-central-1'
fn time_tag_keys_with_pred(store: &Store) {
    let repeat = 10;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let columns = segments.tag_keys(
            (1588834080000000, 1590044410000000),
            &[("env", "prod01-eu-central-1")],
        );

        total_time += now.elapsed();
        track += columns.len();
        // println!("{:?}", columns);
    }
    println!(
        "time_tag_keys_with_pred ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

//
// SHOW TAG VALUES ON "host", "method" WHERE time >= x and time < y AND "env" =
// 'prod01-us-west-1'
fn time_tag_values_with_pred(store: &Store) {
    let repeat = 10;
    let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
    let mut track = 0;
    let segments = store.segments();
    for _ in 0..repeat {
        let now = std::time::Instant::now();

        let tag_values = segments.tag_values(
            (1588834080000000, 1590044410000000),
            &[("env", "prod01-us-west-2")],
            &["host".to_string(), "method".to_string()],
        );

        total_time += now.elapsed();
        track += tag_values.len();
    }
    println!(
        "time_tag_values_with_pred ran {:?} in {:?} {:?} / call {:?}",
        repeat,
        total_time,
        total_time / repeat,
        track
    );
}

// This is for a performance experiment where I wanted to show the performance
// change as more columns are grouped on.
//
// This only shows good peformance when the input file is ordered on all of the
// columns below.
fn time_group_by_different_columns(store: &Store) {
    let strats = vec![
        GroupingStrategy::HashGroup,
        GroupingStrategy::HashGroupConcurrent,
        GroupingStrategy::SortGroup,
        GroupingStrategy::SortGroupConcurrent,
    ];

    let cols = vec![
        "status".to_string(),
        "method".to_string(),
        "url".to_string(),
        "env".to_string(),
        "handler".to_string(),
        "role".to_string(),
        "user_agent".to_string(),
        "path".to_string(),
        "nodename".to_string(),
        "host".to_string(),
        "hostname".to_string(),
    ];

    for strat in &strats {
        let repeat = 10;
        let mut total_time: std::time::Duration = std::time::Duration::new(0, 0);
        let segments = store.segments();
        for i in 1..=cols.len() {
            for _ in 0..repeat {
                let now = std::time::Instant::now();

                segments.read_group_eq(
                    (1589000000000001, 1590044410000000),
                    &[],
                    cols[0..i].to_vec(),
                    vec![("counter".to_string(), AggregateType::Count)],
                    0,
                    strat,
                );

                total_time += now.elapsed();
            }
            println!(
                "time_group_by_different_columns{:?} cols: {:?} ran {:?} in {:?} {:?}",
                strat,
                i,
                repeat,
                total_time,
                total_time / repeat,
            );
        }
    }
}
