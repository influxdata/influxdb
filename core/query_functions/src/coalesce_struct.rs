//! `COALESCE`, but works for structs.
//!
//! Candidate for upstreaming as per <https://github.com/apache/arrow-datafusion/issues/6074>.
//!
//! For struct types, this preforms a recursive "first none-null" filling,
//! taking the first non-null field from argument list.
//!
//! For scalar (non-struct) types (like uint32) this works like the normal `coalesce`
//! function.
//!
//! # Example: Scalars
//!
//! Both `coalesce` and `coalesce_struct` return the first non null value
//! from their arguments:
//!
//! ```sql
//! coalesce(NULL, 2) = 2
//! coalesce_struct(NULL, 2) = 2
//! ```
//!
//! # Example: Struct
//!
//! When invoked on struct arguments, `coalesce_struct` returns a struct whose
//! *fields* contain the first non null value from the fields of its arguments:
//!
//! ```sql
//! coalesce_struct(
//!   { a: 1, b: NULL }, -- first argument, b is null
//!   { a: 2, b: 20 },   -- second argument, b is 20
//! )
//!
//! =
//!
//! { a: 1, b: 20 } -- the null from the first b was replaced with the `20`
//! ```
//!
//! # Example: Deeply Nested Struct
//!
//! `coalesce_struct` also works recursively on structs which themselves
//! contain structs, taking the first non-null value from each field:
//!
//! ```sql
//! coalesce_struct(
//!   NULL,
//!   {
//!     a: 1,
//!     b: NULL,
//!     c: NULL,
//!     d: NULL,
//!   },
//!   {
//!     a: 2,
//!     b: NULL,
//!     c: {a: NULL},
//!     d: {a: 2, b: NULL},
//!   },
//!   {
//!     a: 3,
//!     b: NULL,
//!     c: NULL,
//!     d: {a: 3, b: 3},
//!   },
//! )
//!
//! =
//!
//! {
//!   a: 1,
//!   b: NULL,
//!   c: {a: NULL},
//!   d: {a: 2, b: 3},
//! }
//! ```
use std::{
    any::Any,
    sync::{Arc, LazyLock},
};

use arrow::{
    array::{Array, ArrayRef, StructArray},
    compute::{is_null, kernels::zip::zip},
    datatypes::DataType,
};
use datafusion::{
    common::{cast::as_struct_array, internal_err},
    error::{DataFusionError, Result},
    logical_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    physical_plan::ColumnarValue,
    prelude::Expr,
    scalar::ScalarValue,
};

/// The name of the `coalesce_struct` UDF given to DataFusion.
pub const COALESCE_STRUCT_UDF_NAME: &str = "coalesce_struct";

#[derive(Debug, PartialEq, Eq, Hash)]
/// See [module-level docs](self) for more documentation.
struct CoalesceStructUDF {
    signature: Signature,
}

impl ScalarUDFImpl for CoalesceStructUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        COALESCE_STRUCT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{COALESCE_STRUCT_UDF_NAME} expects at least 1 argument"
            )));
        }
        let first_dt = &arg_types[0];

        for (idx, dt) in arg_types.iter().enumerate() {
            if dt != first_dt {
                let idx = idx + 1;
                return Err(DataFusionError::Plan(format!(
                    "{COALESCE_STRUCT_UDF_NAME} expects all arguments to have the same type, but first arg is '{first_dt}' and arg {idx} (1-based) is '{dt}'",
                )));
            }
        }

        Ok(first_dt.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        #[expect(clippy::manual_try_fold)]
        args.args.iter().enumerate().fold(Ok(None), |accu, (pos, arg)| {
            let Some(accu) = accu? else {return Ok(Some(arg.clone()))};

            if accu.data_type() != arg.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "{} expects all arguments to have the same type, but first arg is '{}' and arg {} (1-based) is '{}'",
                    COALESCE_STRUCT_UDF_NAME,
                    accu.data_type(),
                    pos + 1,
                    arg.data_type(),
                )));
            }

            let (array1, array2) = match (accu, arg) {
                (ColumnarValue::Scalar(scalar1), ColumnarValue::Scalar(scalar2)) =>  {
                    let ScalarValue::Struct(array1) = scalar1 else {
                        return internal_err!("Expected struct, got {:?}", scalar1);
                    };
                    let ScalarValue::Struct(array2) = scalar2 else {
                        return internal_err!("Expected struct, got {:?}", scalar2);
                    };
                    (Arc::clone(&array1) as ArrayRef, Arc::clone(array2) as ArrayRef)
                }
                (ColumnarValue::Scalar(s), ColumnarValue::Array(array2)) => {
                    let array1 = s.to_array_of_size(array2.len())?;
                    (array1, Arc::clone(array2))
                }
                (ColumnarValue::Array(array1), ColumnarValue::Scalar(s)) => {
                    let array2 = s.to_array_of_size(array1.len())?;
                    (array1, array2)
                }
                (ColumnarValue::Array(array1), ColumnarValue::Array(array2)) => {
                    (array1, Arc::clone(array2))
                }
            };

            let array = arrow_coalesce_struct(&array1, &array2)?;
            Ok(Some(ColumnarValue::Array(array)))
        })?.ok_or_else(|| DataFusionError::Plan(format!(
                "{COALESCE_STRUCT_UDF_NAME} expects at least 1 argument"
            )))
    }
}

/// Implementation of `coalesce_struct`.
///
/// See [module-level docs](self) for more information.
pub static COALESCE_STRUCT_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(CoalesceStructUDF {
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
});

/// Recursively fold [`Array`]s.
fn arrow_coalesce_struct(
    array1: &dyn Array,
    array2: &dyn Array,
) -> Result<Arc<dyn Array>, DataFusionError> {
    if matches!(array1.data_type(), DataType::Struct(_)) {
        let array1 = as_struct_array(array1)?;
        let array2 = as_struct_array(array2)?;

        let cols = array1
            .columns()
            .iter()
            .zip(array2.columns())
            .zip(array1.fields())
            .map(|((col1, col2), field)| {
                let out = arrow_coalesce_struct(&col1, &col2)?;
                Ok((Arc::clone(field), out)) as Result<_, DataFusionError>
            })
            .collect::<Result<Vec<_>, _>>()?;

        let array = StructArray::from(cols);
        Ok(Arc::new(array))
    } else {
        let is_null = is_null(array1)?;
        let array = zip(&is_null, &array2, &array1)?;
        Ok(array)
    }
}

/// Create logical `coalesce_struct` expression.
///
/// See [module-level docs](self) for more information.
pub fn coalesce_struct(args: Vec<Expr>) -> Expr {
    COALESCE_STRUCT_UDF.call(args)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::FieldRef;
    use arrow::{
        datatypes::{Field, Fields, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::prelude::SessionContext;
    use datafusion::{
        assert_batches_eq,
        common::assert_contains,
        common::scalar::ScalarStructBuilder,
        prelude::{col, lit},
        scalar::ScalarValue,
    };

    use super::*;

    #[tokio::test]
    async fn test() {
        let field_ba: FieldRef = Field::new("ba", DataType::UInt64, true).into();
        let field_bb: FieldRef = Field::new("bb", DataType::UInt64, true).into();

        let fields_b = Fields::from(vec![Arc::clone(&field_ba), Arc::clone(&field_bb)]);

        let field_a: FieldRef = Field::new("a", DataType::UInt64, true).into();
        let field_b: FieldRef = Field::new("b", DataType::Struct(fields_b.clone()), true).into();

        let fields = Fields::from(vec![Arc::clone(&field_a), Arc::clone(&field_b)]);
        let dt = DataType::Struct(fields.clone());

        assert_case_ok(
            [
                ColumnarValue::Array(ScalarValue::UInt64(None).to_array().unwrap()),
                ColumnarValue::Array(ScalarValue::UInt64(Some(1)).to_array().unwrap()),
                ColumnarValue::Array(ScalarValue::UInt64(Some(2)).to_array().unwrap()),
            ],
            &DataType::UInt64,
            ["+-----+", "| out |", "+-----+", "| 1   |", "+-----+"],
        )
        .await;

        assert_case_ok(
            [ColumnarValue::Array(
                ScalarStructBuilder::new_null(&fields).to_array().unwrap(),
            )],
            &dt,
            ["+-----+", "| out |", "+-----+", "|     |", "+-----+"],
        )
        .await;

        assert_case_ok(
            [
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields).to_array().unwrap()),
                ColumnarValue::Array(
                    ScalarStructBuilder::new()
                        .with_scalar(&field_a, ScalarValue::from(1u64))
                        .with_scalar(&field_b, ScalarStructBuilder::new_null(&fields_b))
                        .build()
                        .unwrap()
                        .to_array()
                        .unwrap(),
                ),
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields).to_array().unwrap()),
                ColumnarValue::Array(
                    ScalarStructBuilder::new()
                        .with_scalar(&field_a, ScalarValue::from(2u64))
                        .with_scalar(
                            &field_b,
                            ScalarStructBuilder::new()
                                .with_scalar(&field_ba, ScalarValue::from(3u64))
                                .with_scalar(&field_bb, ScalarValue::UInt64(None))
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap()
                        .to_array()
                        .unwrap(),
                ),
            ],
            &dt,
            [
                "+--------------------------+",
                "| out                      |",
                "+--------------------------+",
                "| {a: 1, b: {ba: 3, bb: }} |",
                "+--------------------------+",
            ],
        )
        .await;
        // same case as above, but with ColumnarValue::Scalar
        assert_case_ok(
            [
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields)),
                ColumnarValue::Scalar(
                    ScalarStructBuilder::new()
                        .with_scalar(&field_a, ScalarValue::from(1u64))
                        .with_scalar(&field_b, ScalarStructBuilder::new_null(&fields_b))
                        .build()
                        .unwrap(),
                ),
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields)),
                ColumnarValue::Scalar(
                    ScalarStructBuilder::new()
                        .with_scalar(&field_a, ScalarValue::from(2u64))
                        .with_scalar(
                            &field_b,
                            ScalarStructBuilder::new()
                                .with_scalar(&field_ba, ScalarValue::from(3u64))
                                .with_scalar(&field_bb, ScalarValue::UInt64(None))
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                ),
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields).to_array().unwrap()),
            ],
            &dt,
            [
                "+--------------------------+",
                "| out                      |",
                "+--------------------------+",
                "| {a: 1, b: {ba: 3, bb: }} |",
                "+--------------------------+",
            ],
        )
        .await;

        assert_case_err(
            [],
            &dt,
            "No function matches the given name and argument types 'coalesce_struct()'",
        )
        .await;

        assert_case_err(
            [],
            &dt,
            // make sure the error message is helpful
            "coalesce_struct(Any, .., Any)",
        )
        .await;

        assert_case_err(
            [
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields).to_array().unwrap()),
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields_b).to_array().unwrap())
            ],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields).to_array().unwrap()),
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields_b)),
            ],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields)),
                ColumnarValue::Array(ScalarStructBuilder::new_null(&fields_b).to_array().unwrap())
            ],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields)),
                ColumnarValue::Scalar(ScalarStructBuilder::new_null(&fields_b))
            ],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;
    }

    async fn assert_case_ok<const N: usize, const M: usize>(
        vals: [ColumnarValue; N],
        dt: &DataType,
        expected: [&'static str; M],
    ) {
        let actual = run_plan(vals.to_vec(), dt).await.unwrap();
        assert_batches_eq!(expected, &actual);
    }

    async fn assert_case_err<const N: usize>(
        vals: [ColumnarValue; N],
        dt: &DataType,
        expected: &'static str,
    ) {
        let actual = run_plan(vals.to_vec(), dt).await.unwrap_err();
        assert_contains!(actual.to_string(), expected);
    }

    async fn run_plan(
        vals: Vec<ColumnarValue>,
        dt: &DataType,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let col_names = (0..vals.len())
            .map(|idx| format!("col{idx}"))
            .collect::<Vec<_>>();

        let cols = vals
            .iter()
            .zip(&col_names)
            .filter_map(|(val, col_name)| match val {
                ColumnarValue::Array(a) => Some((col_name.as_str(), Arc::clone(a))),
                ColumnarValue::Scalar(_) => None,
            })
            .collect::<Vec<_>>();
        let rb = if cols.is_empty() {
            RecordBatch::new_empty(Arc::new(Schema::new([])))
        } else {
            RecordBatch::try_from_iter(cols.into_iter())?
        };

        let ctx = SessionContext::new();
        ctx.register_batch("t", rb).unwrap();
        let df = ctx.table("t").await?;
        let df = df.select(vec![
            coalesce_struct(
                vals.iter()
                    .zip(col_names)
                    .map(|(val, col_name)| match val {
                        ColumnarValue::Array(_) => col(col_name),
                        ColumnarValue::Scalar(s) => lit(s.clone()),
                    })
                    .collect(),
            )
            .alias("out"),
        ])?;

        // execute the query
        let batches: Vec<RecordBatch> = df.collect().await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        for batch in &batches {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.column(0).data_type(), dt);
        }

        Ok(batches)
    }
}
