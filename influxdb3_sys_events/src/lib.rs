use std::{
    any::{Any, TypeId},
    mem::replace,
    sync::Arc,
};
use std::{fmt::Debug, ops::Deref};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_array::{RecordBatch, StructArray};
use async_trait::async_trait;
use bevy_reflect::Reflect;
use dashmap::DashMap;
use datafusion::{
    error::DataFusionError,
    logical_expr::{col, BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
use iox_system_tables::IoxSystemTable;
use iox_time::TimeProvider;

const MAX_CAPACITY: usize = 1000;
const EVENT_TYPE_PREDICATE: &str = "event_type";

// `select * from system.events_descr`
// foo | Capturing...
//
// `system.events` -> where event_type = 'Foo'
// Foo is event that has foo `system.events.foo` table to query
// Foo can have various props (should it be map array? / struct array?)
//
// event_time | event_data
// xx         | {time: "2024-12-11T23:59:59.000", generation_id: 0}
// xx         | [{time: "2024-12-11T23:59:59.000"}, {generation_id: 0}]
// xx         | [{time: "2024-12-11T23:59:59.000"}, {generation_id: 0}]
// xx         | [{time: "2024-12-11T23:59:59.000"}, {generation_id: 0}]
// xx         | [{time: "2024-12-11T23:59:59.000"}, {generation_id: 0}]
// xx         | [{time: "2024-12-11T23:59:59.000"}, {generation_id: 0}]
// system.events.foo => foo returns event_time and data (map array?)
//
pub trait SysEvent {
    fn event_type(&self) -> &'static str;
}

// SystemSchemaProvider -> tables.insert each time new event comes in
//
// Event -> impl IoxSystemTable
//
#[derive(Clone)]
struct Event1 {
    foo: u64,
    bar: String,
}

impl SysEvent for Event1 {
    fn event_type(&self) -> &'static str {
        "event_1"
    }
}

#[derive(Clone)]
struct Event2 {
    baz: u64,
    boo: String,
}

impl SysEvent for Event2 {
    fn event_type(&self) -> &'static str {
        "event_2"
    }
}

struct RingBuffer<T> {
    buf: Vec<T>,
    max: usize,
    write_index: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            max: capacity,
            write_index: 0,
        }
    }

    pub fn push(&mut self, val: T) {
        if !self.reached_max() {
            self.buf.push(val);
        } else {
            let _ = replace(&mut self.buf[self.write_index], val);
        }
        self.write_index = (self.write_index + 1) % self.max;
    }

    pub fn in_order(&self) -> impl Iterator<Item = &'_ T> {
        let (head, tail) = self.buf.split_at(self.write_index);
        tail.iter().chain(head.iter())
    }

    fn reached_max(&mut self) -> bool {
        self.buf.len() >= self.max
    }
}

#[allow(dead_code)]
pub struct EventSystemTable {
    sys_event_store: SysEventStore,
}

pub fn error() -> DataFusionError {
    DataFusionError::Plan(format!(
        "must provide a {EVENT_TYPE_PREDICATE} = '<event_type>' predicate in queries to \
            system.events"
    ))
}

#[async_trait]
impl IoxSystemTable for EventSystemTable {
    fn schema(&self) -> SchemaRef {
        let columns = vec![
            Field::new("event_time", DataType::Utf8, false),
            Field::new("event_data", DataType::Struct(Fields::empty()), false),
        ];
        Arc::new(Schema::new(columns))
    }

    async fn scan(
        &self,
        filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let event_type = filters
            .ok_or_else(error)?
            .iter()
            .find_map(|f| match f {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    if left.deref() == &col(EVENT_TYPE_PREDICATE) && op == &Operator::Eq {
                        match right.deref() {
                            Expr::Literal(
                                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)),
                            ) => Some(s.to_owned()),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .ok_or_else(error)?;

        // need type here, use type registry?
        // let results = self.sys_event_store.query(&event_type);

        unimplemented!()
    }
}

#[derive(Default, Clone, Debug, Reflect)]
pub struct Event<D> {
    time: i64,
    data: D,
}

pub struct SysEventStore {
    // events: dashmap::DashMap<TypeId, Box<dyn Any + Send + Sync>>,
    events: dashmap::DashMap<Arc<str>, Box<dyn Any + Send + Sync>>,
    // events: dashmap::DashMap<Arc<str>, RingBuffer<Event<AllSysEvents>>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl SysEventStore {
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            events: DashMap::new(),
            time_provider,
        }
    }

    pub fn add<E: 'static + Send + Sync + SysEvent>(&self, val: E) {
        let mut buf = self
            .events
            .entry(Arc::from(val.event_type()))
            .or_insert_with(|| Box::new(RingBuffer::<Event<E>>::new(MAX_CAPACITY)));
        let wrapped = Event {
            time: self.time_provider.now().timestamp_nanos(),
            data: val,
        };
        buf.downcast_mut::<RingBuffer<Event<E>>>().unwrap().push(wrapped);
    }

    pub fn query<E: 'static + Clone + Send + Sync + SysEvent>(&self, event_type: &str) -> Vec<Event<E>> {
        let mut vec = vec![];
        if let Some(event) = self.events.get(&Arc::from(event_type)) {
            for ev in event.downcast_ref::<RingBuffer<Event<E>>>().unwrap().in_order() {
                vec.push(ev.clone());
            }
        };
        vec
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::sync::Arc;

    use arrow::{
        array::{
            BooleanBuilder, Int16Builder, Int64Builder, Int8Builder, StringBuilder, StructBuilder,
            UInt64Builder,
        },
        datatypes::{DataType, Field},
    };
    use bevy_reflect::{GetField, Reflect};
    use iox_time::{MockProvider, Time};

    use crate::{Event, RingBuffer, SysEvent, SysEventStore};

    #[derive(Default, Clone, Debug, Reflect)]
    struct SampleEvent1 {
        start_time: i64,
        time_taken: u64,
        total_fetched: u64,
        random_name: String,
    }

    impl SysEvent for SampleEvent1 {
        fn event_type(&self) -> &'static str {
            "sample_event_1"
        }
    }

    #[derive(Default, Clone, Debug, Reflect)]
    struct SampleEvent2 {
        start_time: i64,
        time_taken: u64,
        generation_id: u64,
    }

    impl SysEvent for SampleEvent2 {
        fn event_type(&self) -> &'static str {
            "sample_event_2"
        }
    }

    #[test]
    fn test_ring_buffer_not_full_at_less_than_max() {
        let mut buf = RingBuffer::new(2);
        buf.push(1);

        let all_results: Vec<&u64> = buf.in_order().collect();
        let first = *all_results.first().unwrap();

        assert_eq!(1, all_results.len());
        assert_eq!(&1, first);
    }

    #[test]
    fn test_ring_buffer_not_full_at_max() {
        let mut buf = RingBuffer::new(2);
        buf.push(1);
        buf.push(2);

        let all_results: Vec<&u64> = buf.in_order().collect();
        let first = *all_results.first().unwrap();
        let second = *all_results.get(1).unwrap();

        assert_eq!(2, all_results.len());
        assert_eq!(&1, first);
        assert_eq!(&2, second);
    }

    #[test]
    fn test_ring_buffer() {
        let mut buf = RingBuffer::new(2);
        buf.push(1);
        buf.push(2);
        buf.push(3);

        let all_results: Vec<&u64> = buf.in_order().collect();
        let first = *all_results.first().unwrap();
        let second = *all_results.get(1).unwrap();

        assert_eq!(2, all_results.len());
        assert_eq!(&2, first);
        assert_eq!(&3, second);
    }

    #[test]
    fn test_event_store() {
        let event_data = SampleEvent1 {
            start_time: 0,
            time_taken: 10,
            total_fetched: 10,
            random_name: "foo".to_owned(),
        };

        let event_data2 = SampleEvent2 {
            start_time: 0,
            time_taken: 10,
            generation_id: 100,
        };

        let event_data3 = SampleEvent1 {
            start_time: 0,
            time_taken: 10,
            total_fetched: 10,
            random_name: "boo".to_owned(),
        };

        let time_provider = MockProvider::new(Time::from_timestamp_nanos(100));

        let event_store = SysEventStore::new(Arc::new(time_provider));
        event_store.add(event_data);
        event_store.add(event_data2);
        event_store.add(event_data3);
        assert_eq!(2, event_store.events.len());

        let all_events = event_store.query::<SampleEvent1>("sample_event_1");
        assert_eq!(2, all_events.len());
        println!("{:?}", all_events);

        let all_events = event_store.query::<SampleEvent2>("sample_event_2");
        assert_eq!(1, all_events.len());
        println!("{:?}", all_events);
    }

    #[test]
    fn test_struct_builder() {
        let event_data = SampleEvent1 {
            start_time: 0,
            time_taken: 10,
            total_fetched: 10,
            random_name: "foo".to_owned(),
        };
        let event_data2 = SampleEvent1 {
            start_time: 0,
            time_taken: 10,
            total_fetched: 10,
            random_name: "boo".to_owned(),
        };

        let event = Event {
            time: 0,
            data: event_data.clone(),
        };

        let event2 = Event {
            time: 0,
            data: event_data2,
        };

        let mut time_arr = Int64Builder::with_capacity(2);
        // let mut list_arr = GenericListBuilder::new();
        // let struct_arrays: Vec<StructArray> = [event, event2].iter().map(|e| e.data.to_struct_array()).collect();
        // concat(&struct_arrays);
        let field_infos = derive_schema(&event_data);
        let len = field_infos.len();
        let schema: Vec<Field> = field_infos.iter().map(|f| f.field.clone()).collect();
        let mut struct_builder = StructBuilder::from_fields(schema, len);

        for i in &[event, event2] {
            time_arr.append_value(i.time);
            let data = &i.data;
            // let arr = i.data.to_struct_array();
            // println!(">>>> full struct array {:?}", arr);

            for (idx, schema) in field_infos.iter().enumerate() {
                match schema.field.data_type() {
                    DataType::Null => {
                        // let get_field = data.get_field::<None>(schema.field.name());
                        // let value = get_field.unwrap();
                        // struct_builder.field_builder::<BooleanBuilder>(idx).unwrap().append_value(value.to_owned());
                    }
                    DataType::Boolean => {
                        let value = data.get_field::<bool>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<BooleanBuilder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::Int8 => {
                        let value = data.get_field::<i8>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<Int8Builder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::Int16 => {
                        let value = data.get_field::<i16>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<Int16Builder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::Int32 => {
                        let value = data.get_field::<i64>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<Int64Builder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::Int64 => {
                        let value = data.get_field::<i64>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<Int64Builder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::UInt8 => todo!(),
                    DataType::UInt16 => todo!(),
                    DataType::UInt32 => todo!(),
                    DataType::UInt64 => {
                        let value = data.get_field::<u64>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<UInt64Builder>(idx)
                            .unwrap()
                            .append_value(value.to_owned());
                    }
                    DataType::Float16 => todo!(),
                    DataType::Float32 => todo!(),
                    DataType::Float64 => todo!(),
                    DataType::Timestamp(_, _) => todo!(),
                    DataType::Date32 => todo!(),
                    DataType::Date64 => todo!(),
                    DataType::Time32(_) => todo!(),
                    DataType::Time64(_) => todo!(),
                    DataType::Duration(_) => todo!(),
                    DataType::Interval(_) => todo!(),
                    DataType::Binary => todo!(),
                    DataType::FixedSizeBinary(_) => todo!(),
                    DataType::LargeBinary => todo!(),
                    DataType::BinaryView => todo!(),
                    DataType::Utf8 => {
                        let value = data.get_field::<String>(schema.field.name()).unwrap();
                        struct_builder
                            .field_builder::<StringBuilder>(idx)
                            .unwrap()
                            .append_value(value);
                    }
                    DataType::LargeUtf8 => todo!(),
                    DataType::Utf8View => todo!(),
                    DataType::List(_) => todo!(),
                    DataType::ListView(_) => todo!(),
                    DataType::FixedSizeList(_, _) => todo!(),
                    DataType::LargeList(_) => todo!(),
                    DataType::LargeListView(_) => todo!(),
                    DataType::Struct(_) => todo!(),
                    DataType::Union(_, _) => todo!(),
                    DataType::Dictionary(_, _) => todo!(),
                    DataType::Decimal128(_, _) => todo!(),
                    DataType::Decimal256(_, _) => todo!(),
                    DataType::Map(_, _) => todo!(),
                    DataType::RunEndEncoded(_, _) => todo!(),
                };
            }

            let type_info = i.get_represented_type_info().unwrap();
            println!(" Got the type info {:?}", type_info);
            match type_info {
                bevy_reflect::TypeInfo::Struct(struct_info) => {
                    for i in struct_info.iter() {
                        println!(" Field name {:?} and type {:?}", i.name(), i.type_path());
                    }
                }
                _ => panic!("Not allowed"),
            };
        }
    }

    #[test]
    fn test_derive_schema() {
        let foo = "foo";
        let s = Sample {
            x: foo.to_string(),
            y: Some(foo.to_string()),
            z: foo,
        };
        derive_schema(&s);
    }

    #[derive(Reflect)]
    struct Sample<'a> {
        x: String,
        y: Option<String>,
        z: &'a str,
    }

    fn derive_schema<T: Reflect>(event_data: &T) -> Vec<FieldInfo> {
        let mut fields = vec![];
        let type_info = event_data.get_represented_type_info().unwrap();
        println!(" Got the type info {:?}", type_info);
        match type_info {
            bevy_reflect::TypeInfo::Struct(struct_info) => {
                for i in struct_info.iter() {
                    println!(" Field name {:?} and type {:?}", i.name(), i.type_path());
                    if i.type_path() == "i64" {
                        fields.push(FieldInfo {
                            name: i.name(),
                            field: Field::new(i.name(), DataType::Int64, true),
                        });
                    } else if i.type_path() == "u64" {
                        fields.push(FieldInfo {
                            name: i.name(),
                            field: Field::new(i.name(), DataType::UInt64, true),
                        });
                    } else if i.type_path() == "alloc::string::String" {
                        fields.push(FieldInfo {
                            name: i.name(),
                            field: Field::new(i.name(), DataType::Utf8, true),
                        });
                    } else {
                        println!("Unsupported field");
                    }
                }
            }
            _ => panic!("Not allowed"),
        };
        fields
    }

    #[allow(dead_code)]
    struct FieldInfo<'a> {
        name: &'a str,
        field: Field,
    }
}
