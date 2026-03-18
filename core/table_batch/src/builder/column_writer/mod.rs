use generated_types::influxdata::pbdata::v1::{
    Column,
    column::{SemanticType, Values},
};
use influxdb_line_protocol::{EscapedStr, FieldValue};
use schema::{InfluxColumnType, InfluxFieldType};

use crate::builder::{Builder, StringBuffer};

use super::null_mask::{NullMaskBuilder, Value};

pub(crate) mod dictionary;
pub(crate) mod string;

/// An error representing when someone tries to give a value which is not of the expected
/// variant/type.
#[derive(Debug, PartialEq)]
pub struct SchemaConflict<'a> {
    /// The type that we were expecting
    pub expected: InfluxColumnType,
    /// The value we got (which will not be of the same type as `expected`)
    pub got: ColumnValue<'a>,
}

/// A struct representing an error when someone tries to push a null to a timestamp column, which is
/// not allowed since every single row must have an accompanying timestamp.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct NullPushedToTime;

/// A builder for an array-like structure of values. The values are stored in `value_buf`, and the
/// `mask` is used to indicate the actual structure of nulls and valid values. See the documentation
/// for [`NullMaskBuilder`] to see how it does so.
#[derive(Debug)]
pub struct ColumnBuilder {
    /// The builder for the null mask
    mask: NullMaskBuilder,
    /// The builder for the values
    pub value_buf: ValueBuffer,
}

impl ColumnBuilder {
    /// Create a new builder to contain values of the same variant as `value`, with
    /// `preceding_null_values` coming before `value` as the initial value
    pub fn new(value: ColumnValue<'_>, preceding_null_values: usize) -> Self {
        Self {
            mask: NullMaskBuilder::with_nulls_before_single_valid(preceding_null_values),
            value_buf: ValueBuffer::new_from_value(value),
        }
    }

    /// Push a single value. If it is not of the same type as we are already storing in this
    /// builder, this function will return an error.
    pub fn push<'a>(&mut self, value: ColumnValue<'a>) -> Result<(), SchemaConflict<'a>> {
        self.value_buf.push(value)?;
        self.mask.push(Value::NonNull);
        Ok(())
    }

    /// Push a null value. If this builder is building a time column, this function will return an
    /// error since time columns must have no null values.
    pub fn push_null(&mut self) -> Result<(), NullPushedToTime> {
        match self.value_buf {
            ValueBuffer::Time(_) => Err(NullPushedToTime),
            _ => {
                self.mask.push(Value::Null);
                Ok(())
            }
        }
    }

    /// Produce the final value which will then be stored in a [`TableBatch`]. This will return
    /// [`None`] if there were no nonnull values pushed to this column at all.
    ///
    /// [`TableBatch`]: generated_types::influxdata::pbdata::v1::TableBatch
    pub fn finish(self, column_name: String) -> Option<Column> {
        let semantic_type = match self.value_buf {
            ValueBuffer::Tag(_) => SemanticType::Tag,
            ValueBuffer::Time(_) => SemanticType::Time,
            _ => SemanticType::Field,
        };

        let mut values = Values::default();
        match self.value_buf {
            ValueBuffer::I64(b) => values.i64_values = <Vec<i64> as Builder<i64>>::finish(b)?,
            ValueBuffer::U64(b) => values.u64_values = <Vec<u64> as Builder<u64>>::finish(b)?,
            ValueBuffer::F64(b) => values.f64_values = <Vec<f64> as Builder<f64>>::finish(b)?,
            ValueBuffer::Boolean(b) => {
                values.bool_values = <Vec<bool> as Builder<bool>>::finish(b)?
            }
            ValueBuffer::String(b) => values.packed_string_values = Some(b.finish()?),
            ValueBuffer::Time(b) => values.i64_values = <Vec<i64> as Builder<i64>>::finish(b)?,
            ValueBuffer::Tag(b) => values.interned_string_values = Some(b.finish()?),
        }

        Some(Column {
            column_name,
            semantic_type: semantic_type.into(),
            values: Some(values),
            null_mask: self.mask.finish(),
        })
    }

    /// Remove a single value, be it null or nonnull, from the builder.
    pub fn drop_last_value(&mut self) {
        if self.mask.pop() == Value::NonNull {
            self.value_buf.drop_last_value();
        }
    }

    /// Check how many values/rows are currently being stored in this builder (including both nulls
    /// and nonnulls)
    pub fn num_rows(&self) -> usize {
        self.mask.len()
    }
}

/// A value that can be stored in a column
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnValue<'a> {
    /// A tag, represented by a string
    Tag(&'a EscapedStr<'a>),
    /// A field, which can be any variant of [`FieldValue`]
    Field(&'a FieldValue<'a>),
    /// A timestamp, which is an i64.
    Timestamp(i64),
}

/// A buffer for building, essentially, an array of values.
#[derive(Debug)]
pub enum ValueBuffer {
    /// For i64s
    I64(Vec<i64>),
    /// For u64s
    U64(Vec<u64>),
    /// For f64s
    F64(Vec<f64>),
    /// For bools
    Boolean(Vec<bool>),
    /// For strings within fields
    String(StringBuffer),
    /// For tag values
    Tag(dictionary::DictionaryBuffer),
    /// For timestamps
    Time(Vec<i64>),
}

impl ValueBuffer {
    /// Create a new value buffer containing only the given `value`.
    pub(crate) fn new_from_value(value: ColumnValue<'_>) -> Self {
        match value {
            ColumnValue::Field(f) => match f {
                FieldValue::I64(v) => Self::I64(vec![*v]),
                FieldValue::U64(v) => Self::U64(vec![*v]),
                FieldValue::F64(v) => Self::F64(vec![*v]),
                FieldValue::Boolean(v) => Self::Boolean(vec![*v]),
                FieldValue::String(v) => Self::String(StringBuffer::new(v)),
            },
            ColumnValue::Tag(t) => Self::Tag(dictionary::DictionaryBuffer::new(t)),
            ColumnValue::Timestamp(ts) => Self::Time(vec![ts]),
        }
    }

    /// Try to push the given value to `self`, returning a [`SchemaConflict`] if the given value
    /// isn't of the same variant as the other values in `self`.
    pub(crate) fn push<'a>(&mut self, value: ColumnValue<'a>) -> Result<(), SchemaConflict<'a>> {
        match (self, value) {
            (Self::Tag(buf), ColumnValue::Tag(s)) => buf.push_str(s),
            (Self::Time(buf), ColumnValue::Timestamp(s)) => buf.push(s),
            (Self::I64(buf), ColumnValue::Field(FieldValue::I64(i))) => buf.push(*i),
            (Self::U64(buf), ColumnValue::Field(FieldValue::U64(u))) => buf.push(*u),
            (Self::F64(buf), ColumnValue::Field(FieldValue::F64(f))) => buf.push(*f),
            (Self::Boolean(buf), ColumnValue::Field(FieldValue::Boolean(b))) => buf.push(*b),
            (Self::String(buf), ColumnValue::Field(FieldValue::String(s))) => {
                buf.push_str(s.as_str())
            }
            (slf, got) => {
                return Err(SchemaConflict {
                    expected: slf.expected_ty(),
                    got,
                });
            }
        }

        Ok(())
    }

    /// Get the [`InfluxColumnType`] representing the type of value that we can push to this
    /// builder.
    fn expected_ty(&self) -> InfluxColumnType {
        match self {
            Self::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
            Self::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
            Self::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
            Self::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
            Self::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
            Self::Tag(_) => InfluxColumnType::Tag,
            Self::Time(_) => InfluxColumnType::Timestamp,
        }
    }

    /// Just remove the last value from this builder
    pub fn drop_last_value(&mut self) {
        match self {
            Self::Tag(tag_buf) => tag_buf.drop_last_value(),
            Self::Time(time_buf) => _ = time_buf.pop(),
            Self::I64(b) => _ = b.pop(),
            Self::U64(b) => _ = b.pop(),
            Self::F64(b) => _ = b.pop(),
            Self::Boolean(b) => _ = b.pop(),
            Self::String(b) => _ = b.pop(),
        }
    }

    /// Get how many values are currently being stored here
    pub fn num_values(&self) -> usize {
        match self {
            Self::Tag(tag_buf) => tag_buf.len(),
            Self::Time(time_buf) => time_buf.len(),
            Self::I64(b) => b.len(),
            Self::U64(b) => b.len(),
            Self::F64(b) => b.len(),
            Self::Boolean(b) => b.len(),
            Self::String(b) => b.len(),
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use generated_types::influxdata::pbdata::v1::{InternedStrings, PackedStrings};

    use super::*;

    use proptest::{prelude::*, prop_oneof};

    /// A value buffer operation.
    #[derive(Debug, Clone)]
    pub(super) enum Op<T> {
        Push(T),
        Drop,
    }

    pub(super) fn arbitrary_op<T>() -> impl Strategy<Value = Op<T>>
    where
        T: Arbitrary + Clone,
    {
        prop_oneof![any::<T>().prop_map(Op::Push), Just(Op::Drop),]
    }

    fn ensure_schema_conflict(
        initial: ColumnValue<'static>,
        second: ColumnValue<'static>,
        expected: InfluxColumnType,
    ) {
        let mut value_buf = ValueBuffer::new_from_value(initial);
        let res = value_buf.push(second.clone()).unwrap_err();

        assert_eq!(
            res,
            SchemaConflict {
                expected,
                got: second
            }
        );
    }

    #[test]
    fn value_buf_rejects_heterogeneous_types() {
        ensure_schema_conflict(
            ColumnValue::Field(&FieldValue::I64(0)),
            ColumnValue::Field(&FieldValue::U64(1)),
            InfluxColumnType::Field(InfluxFieldType::Integer),
        );
    }

    #[test]
    fn value_buf_rejects_same_underlying_type_but_different_semantic_type() {
        ensure_schema_conflict(
            ColumnValue::Timestamp(0),
            ColumnValue::Field(&FieldValue::I64(1)),
            InfluxColumnType::Timestamp,
        );

        ensure_schema_conflict(
            ColumnValue::Tag(&EscapedStr::SingleSlice("a")),
            ColumnValue::Field(&FieldValue::String(EscapedStr::SingleSlice("b"))),
            InfluxColumnType::Tag,
        );
    }

    const COLUMN_NAME: &str = "a";

    #[derive(Debug, PartialEq)]
    enum FromValsErr<'a> {
        SchemaConflict(SchemaConflict<'a>),
        NullPushedToTime(NullPushedToTime),
    }

    fn column_from_vals<'a>(
        vals: impl IntoIterator<Item = Option<ColumnValue<'a>>>,
    ) -> Result<Option<Column>, FromValsErr<'a>> {
        let vals = vals.into_iter();
        let mut builder = None;

        let mut preceding_nulls = 0;
        for val in vals {
            match (val, builder.as_mut()) {
                (Some(val), None) => builder = Some(ColumnBuilder::new(val, preceding_nulls)),
                (Some(val), Some(builder)) => {
                    builder.push(val).map_err(FromValsErr::SchemaConflict)?
                }
                (None, None) => preceding_nulls += 1,
                (None, Some(builder)) => {
                    builder.push_null().map_err(FromValsErr::NullPushedToTime)?
                }
            }
        }

        let Some(builder) = builder else {
            return Ok(None);
        };

        Ok(builder.finish(COLUMN_NAME.to_string()))
    }

    #[test]
    fn basic_integers_functionality() {
        let col = column_from_vals(
            [1, 2, 3, 4, 5]
                .map(FieldValue::I64)
                .iter()
                .map(|i| Some(ColumnValue::Field(i))),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            col,
            Column {
                column_name: COLUMN_NAME.to_string(),
                semantic_type: SemanticType::Field.into(),
                values: Some(Values {
                    i64_values: vec![1, 2, 3, 4, 5],
                    ..Values::default()
                }),
                null_mask: vec![]
            }
        );

        let col = column_from_vals(
            [None, None, None, Some(1), None, None, Some(1), None, None]
                .map(|o| o.map(FieldValue::I64))
                .iter()
                .map(|o| o.as_ref().map(ColumnValue::Field)),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            col,
            Column {
                column_name: COLUMN_NAME.to_string(),
                semantic_type: SemanticType::Field.into(),
                values: Some(Values {
                    i64_values: vec![1, 1],
                    ..Values::default()
                }),
                null_mask: vec![0b10110111, 0b00000001]
            }
        );
    }

    #[test]
    fn basic_times() {
        let col = column_from_vals([12, 2, 3].map(ColumnValue::Timestamp).map(Some))
            .unwrap()
            .unwrap();

        assert_eq!(
            col,
            Column {
                column_name: COLUMN_NAME.to_string(),
                semantic_type: SemanticType::Time.into(),
                values: Some(Values {
                    i64_values: vec![12, 2, 3],
                    ..Values::default()
                }),
                null_mask: vec![]
            }
        );
    }

    #[test]
    fn fails_with_null_times() {
        let res = column_from_vals([Some(1), None, Some(2)].map(|o| o.map(ColumnValue::Timestamp)))
            .unwrap_err();

        assert_eq!(res, FromValsErr::NullPushedToTime(NullPushedToTime));
    }

    #[test]
    fn basic_strings() {
        let col = column_from_vals(
            [
                EscapedStr::SingleSlice("hello"),
                EscapedStr::CopiedValue("friend\nguy".to_string()),
                EscapedStr::CopiedValue("and\tother".to_string()),
                EscapedStr::SingleSlice("hello"),
            ]
            .map(FieldValue::String)
            .iter()
            .map(|s| Some(ColumnValue::Field(s)))
            .chain(std::iter::once(None)),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            col,
            Column {
                column_name: COLUMN_NAME.to_string(),
                semantic_type: SemanticType::Field.into(),
                values: Some(Values {
                    packed_string_values: Some(PackedStrings {
                        values: "hellofriend\nguyand\totherhello".to_string(),
                        offsets: vec![0, 5, 15, 24, 29]
                    }),
                    ..Values::default()
                }),
                null_mask: vec![0b00010000]
            }
        );
    }

    #[test]
    fn basic_tags() {
        let col = column_from_vals(
            [
                None,
                Some("big"),
                Some("small"),
                Some("expensive"),
                None,
                None,
                None,
                None,
                None,
                Some("cheap"),
                Some("big"),
            ]
            .map(|o| o.map(EscapedStr::SingleSlice))
            .iter()
            .map(|o| o.as_ref().map(ColumnValue::Tag)),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            col,
            Column {
                column_name: COLUMN_NAME.to_string(),
                semantic_type: SemanticType::Tag.into(),
                values: Some(Values {
                    interned_string_values: Some(InternedStrings {
                        dictionary: Some(PackedStrings {
                            values: "bigsmallexpensivecheap".to_string(),
                            offsets: vec![0, 3, 8, 17, 22]
                        }),
                        values: vec![0, 1, 2, 3, 0]
                    }),
                    ..Values::default()
                }),
                null_mask: vec![0b11110001, 0b00000001]
            }
        );
    }
}
