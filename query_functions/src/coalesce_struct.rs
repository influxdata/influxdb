//! `COALESCE`, but works for structs.
//!
//! Candidate for upstreaming as per <https://github.com/apache/arrow-datafusion/issues/6074>.
//!
//! For struct types, this preforms a recursive "first none-null" filling.
//!
//! For non-struct types (like uint32) this works like the normal `coalesce` function.
//!
//! # Example
//!
//! ```sql
//! coalesce_nested(
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
use std::sync::Arc;

use arrow::{
    array::{Array, StructArray},
    compute::{is_null, kernels::zip::zip},
    datatypes::DataType,
};
use datafusion::{
    common::cast::as_struct_array,
    error::DataFusionError,
    logical_expr::{
        ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, Volatility,
    },
    physical_plan::ColumnarValue,
    prelude::Expr,
    scalar::ScalarValue,
};
use once_cell::sync::Lazy;

/// The name of the `coalesce_struct` UDF given to DataFusion.
pub const COALESCE_STRUCT_UDF_NAME: &str = "coalesce_struct";

/// Implementation of `coalesce_struct`.
///
/// See [module-level docs](self) for more information.
pub static COALESCE_STRUCT_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    let return_type: ReturnTypeFunction = Arc::new(move |arg_types| {
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

        Ok(Arc::new(first_dt.clone()))
    });

    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        #[allow(clippy::manual_try_fold)]
        args.iter().enumerate().fold(Ok(None), |accu, (pos, arg)| {
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
                    return Ok(Some(ColumnarValue::Scalar(scalar_coalesce_struct(scalar1, scalar2))));
                }
                (ColumnarValue::Scalar(s), ColumnarValue::Array(array2)) => {
                    let array1 = s.to_array_of_size(array2.len());
                    (array1, Arc::clone(array2))
                }
                (ColumnarValue::Array(array1), ColumnarValue::Scalar(s)) => {
                    let array2 = s.to_array_of_size(array1.len());
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
    });

    Arc::new(ScalarUDF::new(
        COALESCE_STRUCT_UDF_NAME,
        &Signature::variadic_any(Volatility::Immutable),
        &return_type,
        &fun,
    ))
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
        let array = zip(&is_null(array1)?, array2, array1)?;
        Ok(array)
    }
}

/// Recursively fold [`ScalarValue`]s.
fn scalar_coalesce_struct(scalar1: ScalarValue, scalar2: &ScalarValue) -> ScalarValue {
    match (scalar1, scalar2) {
        (ScalarValue::Struct(Some(vals1), fields1), ScalarValue::Struct(Some(vals2), _)) => {
            let vals = vals1
                .into_iter()
                .zip(vals2)
                .map(|(v1, v2)| scalar_coalesce_struct(v1, v2))
                .collect();
            ScalarValue::Struct(Some(vals), fields1)
        }
        (scalar1, scalar2) if scalar1.is_null() => scalar2.clone(),
        (scalar1, _) => scalar1,
    }
}

/// Create logical `coalesce_struct` expression.
///
/// See [module-level docs](self) for more information.
pub fn coalesce_struct(args: Vec<Expr>) -> Expr {
    Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF {
        fun: Arc::clone(&COALESCE_STRUCT_UDF),
        args,
    })
}

#[cfg(test)]
mod tests {
    use arrow::{
        datatypes::{Field, Fields, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        assert_batches_eq,
        common::assert_contains,
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use datafusion_util::context_with_table;

    use super::*;

    #[tokio::test]
    async fn test() {
        let fields_b = Fields::from(vec![
            Field::new("ba", DataType::UInt64, true),
            Field::new("bb", DataType::UInt64, true),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Struct(fields_b.clone()), true),
        ]);
        let dt = DataType::Struct(fields.clone());

        assert_case_ok(
            [
                ColumnarValue::Array(ScalarValue::UInt64(None).to_array()),
                ColumnarValue::Array(ScalarValue::UInt64(Some(1)).to_array()),
                ColumnarValue::Array(ScalarValue::UInt64(Some(2)).to_array()),
            ],
            &DataType::UInt64,
            ["+-----+", "| out |", "+-----+", "| 1   |", "+-----+"],
        )
        .await;

        assert_case_ok(
            [ColumnarValue::Array(
                ScalarValue::Struct(None, fields.clone()).to_array(),
            )],
            &dt,
            ["+-----+", "| out |", "+-----+", "|     |", "+-----+"],
        )
        .await;

        assert_case_ok(
            [
                ColumnarValue::Array(ScalarValue::Struct(None, fields.clone()).to_array()),
                ColumnarValue::Array(
                    ScalarValue::Struct(
                        Some(vec![
                            ScalarValue::UInt64(Some(1)),
                            ScalarValue::Struct(None, fields_b.clone()),
                        ]),
                        fields.clone(),
                    )
                    .to_array(),
                ),
                ColumnarValue::Array(ScalarValue::Struct(None, fields.clone()).to_array()),
                ColumnarValue::Array(
                    ScalarValue::Struct(
                        Some(vec![
                            ScalarValue::UInt64(Some(2)),
                            ScalarValue::Struct(
                                Some(vec![
                                    ScalarValue::UInt64(Some(3)),
                                    ScalarValue::UInt64(None),
                                ]),
                                fields_b.clone(),
                            ),
                        ]),
                        fields.clone(),
                    )
                    .to_array(),
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
                ColumnarValue::Scalar(ScalarValue::Struct(None, fields.clone())),
                ColumnarValue::Scalar(ScalarValue::Struct(
                    Some(vec![
                        ScalarValue::UInt64(Some(1)),
                        ScalarValue::Struct(None, fields_b.clone()),
                    ]),
                    fields.clone(),
                )),
                ColumnarValue::Scalar(ScalarValue::Struct(None, fields.clone())),
                ColumnarValue::Scalar(ScalarValue::Struct(
                    Some(vec![
                        ScalarValue::UInt64(Some(2)),
                        ScalarValue::Struct(
                            Some(vec![
                                ScalarValue::UInt64(Some(3)),
                                ScalarValue::UInt64(None),
                            ]),
                            fields_b.clone(),
                        ),
                    ]),
                    fields.clone(),
                )),
                ColumnarValue::Array(ScalarValue::Struct(None, fields.clone()).to_array()),
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
            "Error during planning: coalesce_struct expects at least 1 argument",
        )
        .await;

        assert_case_err(
            [ColumnarValue::Array(ScalarValue::Struct(None, fields.clone()).to_array()), ColumnarValue::Array(ScalarValue::Struct(None, fields_b.clone()).to_array())],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [ColumnarValue::Array(ScalarValue::Struct(None, fields.clone()).to_array()), ColumnarValue::Scalar(ScalarValue::Struct(None, fields_b.clone()))],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [ColumnarValue::Scalar(ScalarValue::Struct(None, fields.clone())), ColumnarValue::Array(ScalarValue::Struct(None, fields_b.clone()).to_array())],
            &dt,
            "Error during planning: coalesce_struct expects all arguments to have the same type, but first arg is"
        )
        .await;

        assert_case_err(
            [ColumnarValue::Scalar(ScalarValue::Struct(None, fields.clone())), ColumnarValue::Scalar(ScalarValue::Struct(None, fields_b.clone()))],
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

        let ctx = context_with_table(rb);
        let df = ctx.table("t").await?;
        let df = df.select(vec![coalesce_struct(
            vals.iter()
                .zip(col_names)
                .map(|(val, col_name)| match val {
                    ColumnarValue::Array(_) => col(col_name),
                    ColumnarValue::Scalar(s) => lit(s.clone()),
                })
                .collect(),
        )
        .alias("out")])?;

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
