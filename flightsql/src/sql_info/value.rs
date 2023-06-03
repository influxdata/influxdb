//! Dynamic value support for SqlInfo

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayData, BooleanBuilder, Int32Builder, Int64Builder, Int8Builder,
        ListBuilder, StringBuilder, UnionArray,
    },
    datatypes::{DataType, Field, UnionFields, UnionMode},
};
use arrow_flight::sql::SqlInfo;
use once_cell::sync::Lazy;

/// Represents a dynamic value
#[derive(Debug, Clone, PartialEq)]
pub enum SqlInfoValue {
    String(String),
    Bool(bool),
    BigInt(i64),
    Bitmask(i32),
    StringList(Vec<String>),
    // TODO support more exotic metadata that requires the map of lists
    //ListMap(BTreeMap<i32, Vec<i32>>),
}

impl From<&str> for SqlInfoValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for SqlInfoValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for SqlInfoValue {
    fn from(value: i32) -> Self {
        Self::Bitmask(value)
    }
}

impl From<i64> for SqlInfoValue {
    fn from(value: i64) -> Self {
        Self::BigInt(value)
    }
}

impl From<&[&str]> for SqlInfoValue {
    fn from(values: &[&str]) -> Self {
        let values = values.iter().map(|s| s.to_string()).collect();
        Self::StringList(values)
    }
}

/// Something that can be converted into u32 (the represenation of a
/// [`SqlInfo`] name)
pub trait SqlInfoName {
    fn as_u32(&self) -> u32;
}

impl SqlInfoName for SqlInfo {
    fn as_u32(&self) -> u32 {
        // SqlInfos are u32 in the flight spec, but for some reason
        // SqlInfo repr is an i32, so convert between them
        u32::try_from(i32::from(*self)).expect("SqlInfo fit into u32")
    }
}

// Allow passing u32 directly into to with_sql_info
impl SqlInfoName for u32 {
    fn as_u32(&self) -> u32 {
        *self
    }
}

/// Handles creating the dense [`UnionArray`] described by [flightsql]
///
///
/// NOT YET COMPLETE: The int32_to_int32_list_map
///
/// ```text
/// *  value: dense_union<
/// *              string_value: utf8,
/// *              bool_value: bool,
/// *              bigint_value: int64,
/// *              int32_bitmask: int32,
/// *              string_list: list<string_data: utf8>
/// *              int32_to_int32_list_map: map<key: int32, value: list<$data$: int32>>
/// * >
/// ```
///[flightsql]: (https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/FlightSql.proto#L32-L43
pub struct SqlInfoUnionBuilder {
    // Values for each child type
    string_values: StringBuilder,
    bool_values: BooleanBuilder,
    bigint_values: Int64Builder,
    int32_bitmask_values: Int32Builder,
    string_list_values: ListBuilder<StringBuilder>,

    /// incrementally build types/offset of the dense union,
    ///
    /// See [Union Spec] for details.
    ///
    /// [Union Spec]: https://arrow.apache.org/docs/format/Columnar.html#dense-union
    type_ids: Int8Builder,
    offsets: Int32Builder,
}

/// [`DataType`] for the output union array
static UNION_TYPE: Lazy<DataType> = Lazy::new(|| {
    let nullable = false;
    let fields = vec![
        Field::new("string_value", DataType::Utf8, nullable),
        Field::new("bool_value", DataType::Boolean, nullable),
        Field::new("bigint_value", DataType::Int64, nullable),
        Field::new("int32_bitmask", DataType::Int32, nullable),
        // treat list as nullable b/c that is what hte builders make
        Field::new(
            "string_list",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ];

    // create "type ids", one for each type
    // assume they go from 0 .. num_fields
    let type_ids: Vec<i8> = (0..fields.len()).map(|v| v as i8).collect();

    DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
});

impl SqlInfoUnionBuilder {
    pub fn new() -> Self {
        Self {
            string_values: StringBuilder::new(),
            bool_values: BooleanBuilder::new(),
            bigint_values: Int64Builder::new(),
            int32_bitmask_values: Int32Builder::new(),
            string_list_values: ListBuilder::new(StringBuilder::new()),
            type_ids: Int8Builder::new(),
            offsets: Int32Builder::new(),
        }
    }

    /// Returns the DataType created by this builder
    pub fn schema() -> &'static DataType {
        &UNION_TYPE
    }

    /// Append the specified value to this builder
    pub fn append_value(&mut self, v: &SqlInfoValue) {
        // typeid is which child and len is the child array's length
        // *after* adding the value
        let (type_id, len) = match v {
            SqlInfoValue::String(v) => {
                self.string_values.append_value(v);
                (0, self.string_values.len())
            }
            SqlInfoValue::Bool(v) => {
                self.bool_values.append_value(*v);
                (1, self.bool_values.len())
            }
            SqlInfoValue::BigInt(v) => {
                self.bigint_values.append_value(*v);
                (2, self.bigint_values.len())
            }
            SqlInfoValue::Bitmask(v) => {
                self.int32_bitmask_values.append_value(*v);
                (3, self.int32_bitmask_values.len())
            }
            SqlInfoValue::StringList(values) => {
                // build list
                for v in values {
                    self.string_list_values.values().append_value(v);
                }
                // complete the list
                self.string_list_values.append(true);
                (4, self.string_list_values.len())
            }
        };

        self.type_ids.append_value(type_id);
        let len = i32::try_from(len).expect("offset fit in i32");
        self.offsets.append_value(len - 1);
    }

    /// Complete the construction and build the [`UnionArray`]
    pub fn finish(self) -> UnionArray {
        let Self {
            mut string_values,
            mut bool_values,
            mut bigint_values,
            mut int32_bitmask_values,
            mut string_list_values,
            mut type_ids,
            mut offsets,
        } = self;
        let type_ids = type_ids.finish();
        let offsets = offsets.finish();

        // form the correct ArrayData

        let len = offsets.len();
        let null_bit_buffer = None;
        let offset = 0;

        let buffers = vec![
            type_ids.into_data().buffers()[0].clone(),
            offsets.into_data().buffers()[0].clone(),
        ];

        let child_data = vec![
            string_values.finish().into_data(),
            bool_values.finish().into_data(),
            bigint_values.finish().into_data(),
            int32_bitmask_values.finish().into_data(),
            string_list_values.finish().into_data(),
        ];

        let data = ArrayData::try_new(
            UNION_TYPE.clone(),
            len,
            null_bit_buffer,
            offset,
            buffers,
            child_data,
        )
        .expect("Correctly created UnionArray");

        UnionArray::from(data)
    }
}
