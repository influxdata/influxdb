use std::{sync::Arc, thread};

use arrow::{
    array::{StringViewBuilder, StructBuilder, UInt64Builder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
};
use arrow_array::{ArrayRef, RecordBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use influxdb3_sys_events::{Event, RingBuffer, SysEventStore, ToRecordBatch};
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::debug;
use rand::Rng;

const MAX_WRITE_ITERATIONS: u32 = 100_000;

#[allow(dead_code)]
#[derive(Debug)]
struct SampleSysEvent {
    pub data_fetched_1: u64,
    pub data_fetched_2: u64,
    pub data_fetched_3: u64,
    pub data_fetched_4: u64,
    pub data_fetched_5: u64,
    pub data_fetched_6: u64,
    pub data_fetched_7: u64,
    pub data_fetched_8: u64,
    pub data_fetched_9: u64,
    pub data_fetched_10: u64,
    pub data_fetched_11: u64,
    pub data_fetched_12: u64,
    pub data_fetched_13: u64,
    pub data_fetched_14: u64,
    pub data_fetched_15: u64,
    pub data_fetched_16: u64,
    pub data_fetched_17: u64,
    pub data_fetched_18: u64,
    pub data_fetched_19: u64,
    pub data_fetched_20: u64,
    pub string_field: String,
}

impl ToRecordBatch<SampleSysEvent> for SampleSysEvent {
    fn to_record_batch(
        items: Option<&RingBuffer<Event<SampleSysEvent>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        items.map(|buf| {
            let iter = buf.in_order();
            let mut event_time_arr = StringViewBuilder::with_capacity(1000);
            let mut struct_builder = StructBuilder::from_fields(
                vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ],
                1000,
            );
            for event in iter {
                event_time_arr.append_value("2024-12-01T23:59:59.000Z");
                let time_taken_builder = struct_builder.field_builder::<UInt64Builder>(0).unwrap();
                time_taken_builder.append_value(event.data.data_fetched_1);

                let num_files_fetched_builder =
                    struct_builder.field_builder::<UInt64Builder>(1).unwrap();
                num_files_fetched_builder.append_value(event.data.data_fetched_10);

                struct_builder.append(true);
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(struct_builder.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }

    fn schema() -> Schema {
        let columns = vec![
            Field::new("event_time", DataType::Utf8View, false),
            Field::new(
                "event_data",
                DataType::Struct(Fields::from(vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ])),
                false,
            ),
        ];
        Schema::new(columns)
    }
}

impl SampleSysEvent {
    pub(crate) fn new() -> Self {
        let rand_start_range = 0..100_000_000;
        let start = rand::thread_rng().gen_range(rand_start_range);
        SampleSysEvent {
            data_fetched_1: start + 1,
            data_fetched_2: start + 2,
            data_fetched_3: start + 3,
            data_fetched_4: start + 4,
            data_fetched_5: start + 5,
            data_fetched_6: start + 6,
            data_fetched_7: start + 7,
            data_fetched_8: start + 8,
            data_fetched_9: start + 9,
            data_fetched_10: start + 10,
            data_fetched_11: start + 11,
            data_fetched_12: start + 12,
            data_fetched_13: start + 13,
            data_fetched_14: start + 14,
            data_fetched_15: start + 15,
            data_fetched_16: start + 16,
            data_fetched_17: start + 17,
            data_fetched_18: start + 18,
            data_fetched_19: start + 19,
            data_fetched_20: start + 20,
            string_field: format!("str-{start}"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct SampleSysEvent2 {
    pub data_fetched_1: u64,
    pub data_fetched_2: u64,
    pub data_fetched_3: u64,
    pub data_fetched_4: u64,
    pub data_fetched_5: u64,
    pub data_fetched_6: u64,
    pub data_fetched_7: u64,
    pub data_fetched_8: u64,
    pub data_fetched_9: u64,
    pub data_fetched_10: u64,
    pub data_fetched_11: u64,
    pub data_fetched_12: u64,
    pub data_fetched_13: u64,
    pub data_fetched_14: u64,
    pub data_fetched_15: u64,
    pub data_fetched_16: u64,
    pub data_fetched_17: u64,
    pub data_fetched_18: u64,
    pub data_fetched_19: u64,
    pub data_fetched_20: u64,
    pub string_field: String,
}

impl ToRecordBatch<SampleSysEvent2> for SampleSysEvent2 {
    fn to_record_batch(
        items: Option<&RingBuffer<Event<SampleSysEvent2>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        items.map(|buf| {
            let iter = buf.in_order();
            let mut event_time_arr = StringViewBuilder::with_capacity(1000);
            let mut struct_builder = StructBuilder::from_fields(
                vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ],
                1000,
            );
            for event in iter {
                event_time_arr.append_value("2024-12-01T23:59:59.000Z");
                let time_taken_builder = struct_builder.field_builder::<UInt64Builder>(0).unwrap();
                time_taken_builder.append_value(event.data.data_fetched_1);

                let num_files_fetched_builder =
                    struct_builder.field_builder::<UInt64Builder>(1).unwrap();
                num_files_fetched_builder.append_value(event.data.data_fetched_10);

                struct_builder.append(true);
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(struct_builder.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }

    fn schema() -> Schema {
        let columns = vec![
            Field::new("event_time", DataType::Utf8View, false),
            Field::new(
                "event_data",
                DataType::Struct(Fields::from(vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ])),
                false,
            ),
        ];
        Schema::new(columns)
    }
}

impl SampleSysEvent2 {
    pub(crate) fn new() -> Self {
        let rand_start_range = 0..100_000_000;
        let start = rand::thread_rng().gen_range(rand_start_range);
        SampleSysEvent2 {
            data_fetched_1: start + 1,
            data_fetched_2: start + 2,
            data_fetched_3: start + 3,
            data_fetched_4: start + 4,
            data_fetched_5: start + 5,
            data_fetched_6: start + 6,
            data_fetched_7: start + 7,
            data_fetched_8: start + 8,
            data_fetched_9: start + 9,
            data_fetched_10: start + 10,
            data_fetched_11: start + 11,
            data_fetched_12: start + 12,
            data_fetched_13: start + 13,
            data_fetched_14: start + 14,
            data_fetched_15: start + 15,
            data_fetched_16: start + 16,
            data_fetched_17: start + 17,
            data_fetched_18: start + 18,
            data_fetched_19: start + 19,
            data_fetched_20: start + 20,
            string_field: format!("str-{start}"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct SampleSysEvent3 {
    pub data_fetched_1: u64,
    pub data_fetched_2: u64,
    pub data_fetched_3: u64,
    pub data_fetched_4: u64,
    pub data_fetched_5: u64,
    pub data_fetched_6: u64,
    pub data_fetched_7: u64,
    pub data_fetched_8: u64,
    pub data_fetched_9: u64,
    pub data_fetched_10: u64,
    pub string_field: String,
}

impl ToRecordBatch<SampleSysEvent3> for SampleSysEvent3 {
    fn to_record_batch(
        items: Option<&RingBuffer<Event<SampleSysEvent3>>>,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        items.map(|buf| {
            let iter = buf.in_order();
            let mut event_time_arr = StringViewBuilder::with_capacity(1000);
            let mut struct_builder = StructBuilder::from_fields(
                vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ],
                1000,
            );
            for event in iter {
                event_time_arr.append_value("2024-12-01T23:59:59.000Z");
                let time_taken_builder = struct_builder.field_builder::<UInt64Builder>(0).unwrap();
                time_taken_builder.append_value(event.data.data_fetched_1);

                let num_files_fetched_builder =
                    struct_builder.field_builder::<UInt64Builder>(1).unwrap();
                num_files_fetched_builder.append_value(event.data.data_fetched_10);

                struct_builder.append(true);
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(event_time_arr.finish()),
                Arc::new(struct_builder.finish()),
            ];
            RecordBatch::try_new(Arc::new(Self::schema()), columns)
        })
    }

    fn schema() -> Schema {
        let columns = vec![
            Field::new("event_time", DataType::Utf8View, false),
            Field::new(
                "event_data",
                DataType::Struct(Fields::from(vec![
                    Field::new("time_taken", DataType::UInt64, false),
                    Field::new("total_fetched", DataType::UInt64, false),
                ])),
                false,
            ),
        ];
        Schema::new(columns)
    }
}

impl SampleSysEvent3 {
    pub(crate) fn new() -> Self {
        let rand_start_range = 0..100_000_000;
        let start = rand::thread_rng().gen_range(rand_start_range);
        SampleSysEvent3 {
            data_fetched_1: start + 1,
            data_fetched_2: start + 2,
            data_fetched_3: start + 3,
            data_fetched_4: start + 4,
            data_fetched_5: start + 5,
            data_fetched_6: start + 6,
            data_fetched_7: start + 7,
            data_fetched_8: start + 8,
            data_fetched_9: start + 9,
            data_fetched_10: start + 10,
            string_field: format!("str-{start}"),
        }
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("syseventstore_writes_reads_t6");
    group.bench_function(BenchmarkId::new("array", 0), |b| {
        b.iter(|| {
            array_sys_event_store();
        })
    });
    // group.bench_function(BenchmarkId::new("vec", 0), |b| b.iter(|| {
    //     vec_sys_event_store();
    // }));
    group.finish();
}

fn array_sys_event_store() {
    let time_provider: Arc<dyn TimeProvider> = Arc::new(SystemProvider::new());
    let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));
    let store_clone_1 = Arc::clone(&sys_event_store);
    let store_clone_2 = Arc::clone(&sys_event_store);
    let store_clone_3 = Arc::clone(&sys_event_store);
    let store_clone_4 = Arc::clone(&sys_event_store);
    let store_clone_5 = Arc::clone(&sys_event_store);
    let store_clone_6 = Arc::clone(&sys_event_store);
    let t1 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            store_clone_1.record(SampleSysEvent::new());
        }
    });

    let t2 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            store_clone_2.record(SampleSysEvent2::new());
        }
    });

    let t3 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            store_clone_3.record(SampleSysEvent3::new());
        }
    });

    let t4 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            let vals = store_clone_4.as_record_batch::<SampleSysEvent>();
            debug!("result {:?}", vals.is_some());
        }
    });

    let t5 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            let vals = store_clone_5.as_record_batch::<SampleSysEvent2>();
            debug!("result {:?}", vals.is_some());
        }
    });

    let t6 = thread::spawn(move || {
        for _ in 0..MAX_WRITE_ITERATIONS {
            let vals = store_clone_6.as_record_batch::<SampleSysEvent3>();
            debug!("result {:?}", vals.is_some());
        }
    });

    let _ = t1.join();
    let _ = t2.join();
    let _ = t3.join();
    let _ = t4.join();
    let _ = t5.join();
    let _ = t6.join();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
