use crate::NUMERICS;
use crate::non_negative::NonNegativeUDWF;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, Schema, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, PartitionEvaluator, Signature, TIMEZONE_WILDCARD, TypeSignature,
    Volatility, WindowUDF, WindowUDFImpl,
};
use std::sync::{Arc, LazyLock};

/// Name of the derivative window function.
pub const DERIVATIVE_UDWF_NAME: &str = "derivative";

/// Name of the non_negative_derivative window function.
pub const NON_NEGATIVE_DERIVATIVE_UDWF_NAME: &str = "non_negative_derivative";

/// Static instance of the derivative window function.
pub static DERIVATIVE_UDWF: LazyLock<Arc<WindowUDF>> =
    LazyLock::new(|| Arc::new(WindowUDF::new_from_impl(DerivativeUDWF::new())));

/// Static instance of the non_negative_derivative window function.
pub static NON_NEGATIVE_DERIVATIVE_UDWF: LazyLock<Arc<WindowUDF>> = LazyLock::new(|| {
    Arc::new(WindowUDF::new_from_impl(
        NonNegativeUDWF::new(NON_NEGATIVE_DERIVATIVE_UDWF_NAME, DerivativeUDWF::new())
            .with_documentation(&NON_NEGATIVE_DERIVATIVE_DOCUMENTATION),
    ))
});

static DERIVATIVE_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        r#"Computes the rate of change between consecutive non-null rows.

The first row with a non-null value and time will produce a NULL result,
for each subsequent row the result is calculated as the difference in values
divided by the difference in timestamps scaled by the unit interval (default 1s).

The result will be NULL if:
 - either the value or time column contains a NULL.
 - it is the first row with a non-null value and time.
"#,
        "derivative(value, time[, unit])",
    )
    .with_argument("value", "Numeric column (Int64, UInt64, or Float64) to compute the derivative of.")
    .with_argument("time", "Timestamp column (nanosecond precision) used to compute the time delta.")
    .with_argument("unit", "Optional interval that sets the derivative time unit. Defaults to 1 second. Must be literal and not contain months or days.")
    .with_sql_example(r#"```sql
SELECT name, time, derivative(reads, time) OVER (PARTITION BY name ORDER BY time) as "δr/δt" FROM diskio
```"#)
    .with_related_udf("non_negative_derivative")
    .with_related_udf("difference")
    .build()
});

static NON_NEGATIVE_DERIVATIVE_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        r#"Computes the non-negative rate of change between consecutive non-null rows.

Behaves like `derivative`, but any negative result is replaced with NULL.
This is useful for monitoring monotonically increasing counters that may
reset, where negative deltas are not meaningful.

The result will be NULL if:
 - the `derivative` result would be NULL.
 - the computed derivative is negative.
"#,
        "non_negative_derivative(value, time[, unit])",
    )
    .with_argument("value", "Numeric column (Int64, UInt64, or Float64) to compute the derivative of.")
    .with_argument("time", "Timestamp column (nanosecond precision) used to compute the time delta.")
    .with_argument("unit", "Optional interval that sets the derivative time unit. Defaults to 1 second. Must be literal and not contain months or days.")
    .with_sql_example(r#"```sql
SELECT name, time, non_negative_derivative(reads, time) OVER (PARTITION BY name ORDER BY time) as "δr/δt" FROM diskio
```"#)
    .with_related_udf("derivative")
    .with_related_udf("non_negative_difference")
    .build()
});

/// Implementation of the derivative window function.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DerivativeUDWF {
    signature: Signature,
}

impl DerivativeUDWF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                NUMERICS
                    .iter()
                    .flat_map(|dt| {
                        vec![
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Timestamp(
                                    TimeUnit::Nanosecond,
                                    Some(TIMEZONE_WILDCARD.into()),
                                ),
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                                DataType::Interval(IntervalUnit::MonthDayNano),
                            ]),
                            TypeSignature::Exact(vec![
                                dt.clone(),
                                DataType::Timestamp(
                                    TimeUnit::Nanosecond,
                                    Some(TIMEZONE_WILDCARD.into()),
                                ),
                                DataType::Interval(IntervalUnit::MonthDayNano),
                            ]),
                        ]
                    })
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for DerivativeUDWF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        DERIVATIVE_UDWF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DERIVATIVE_DOCUMENTATION)
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs<'_>,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let exprs = args.input_exprs();
        let unit_ns = if exprs.len() > 2 {
            if let Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(unit)))) =
                exprs[2].evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))
            {
                if unit.months > 0 || unit.days > 0 {
                    return exec_err!("derivative unit parameter cannot contain months or days");
                } else if unit.nanoseconds < 1 {
                    return exec_err!("derivative unit parameter must be positive");
                } else {
                    unit.nanoseconds as f64
                }
            } else {
                return exec_err!("derivative unit parameter must be a non-null literal interval");
            }
        } else {
            1_000_000_000_f64
        };

        Ok(Box::new(DerivativePartitionEvaluator { unit_ns }))
    }
}

/// PartitionEvaluator which returns the derivative between input values,
/// in the provided units.
#[derive(Debug)]
struct DerivativePartitionEvaluator {
    unit_ns: f64,
}

impl PartitionEvaluator for DerivativePartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<Arc<dyn Array>> {
        assert!(values.len() >= 2);

        let array = Arc::clone(&values[0]);
        let times = Arc::clone(&values[1]);

        let mut idx: usize = 0;
        let mut last: ScalarValue = array.data_type().try_into()?;
        let mut last_time: ScalarValue = times.data_type().try_into()?;
        let mut derivative: Vec<ScalarValue> = vec![];

        while idx < array.len() {
            last = ScalarValue::try_from_array(&array, idx)?;
            last_time = ScalarValue::try_from_array(&times, idx)?;
            derivative.push(ScalarValue::Float64(None));
            idx += 1;
            if !last.is_null() && !last_time.is_null() {
                break;
            }
        }
        while idx < array.len() {
            let v = ScalarValue::try_from_array(&array, idx)?;
            let t = ScalarValue::try_from_array(&times, idx)?;
            if v.is_null() || t.is_null() {
                derivative.push(ScalarValue::Float64(None));
            } else {
                let dv = delta(&v, &last)?;
                let dt = delta_time(&t, &last_time, self.unit_ns)?;
                derivative.push(ScalarValue::Float64(Some(dv / dt)));
                last = v.clone();
                last_time = t.clone();
            }
            idx += 1;
        }
        Ok(Arc::new(ScalarValue::iter_to_array(derivative)?))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

/// Calculate the absolute difference of two numerical values.
fn delta(curr: &ScalarValue, prev: &ScalarValue) -> Result<f64> {
    match (curr, prev) {
        (ScalarValue::Float64(Some(curr)), ScalarValue::Float64(Some(prev))) => Ok(*curr - *prev),
        (ScalarValue::Int64(Some(curr)), ScalarValue::Int64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        (ScalarValue::UInt64(Some(curr)), ScalarValue::UInt64(Some(prev))) => {
            Ok(*curr as f64 - *prev as f64)
        }
        _ => exec_err!("derivative attempted on unsupported values"),
    }
}

/// Calculate the nanosecond time difference, scaled to the unit.
fn delta_time(curr: &ScalarValue, prev: &ScalarValue, unit: f64) -> Result<f64> {
    if let (
        ScalarValue::TimestampNanosecond(Some(curr), _),
        ScalarValue::TimestampNanosecond(Some(prev), _),
    ) = (curr, prev)
    {
        Ok(((*curr).saturating_sub(*prev) as f64) / unit)
    } else {
        exec_err!("time delta attempted on unsupported values")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        physical_expr::expressions::{col, lit},
        physical_plan::PhysicalExpr,
    };

    /// Build the exprs and fields needed to create a `PartitionEvaluatorArgs`
    /// for `DerivativeUDWF`. If `interval` is `Some`, a third argument is
    /// included for the unit parameter.
    fn evaluator_args(
        interval: Option<ScalarValue>,
    ) -> (Vec<Arc<dyn PhysicalExpr>>, Vec<FieldRef>) {
        let mut exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> = vec![
            lit(1.0_f64),
            lit(ScalarValue::TimestampNanosecond(Some(0), None)),
        ];
        let mut fields: Vec<FieldRef> = vec![
            Arc::new(Field::new("f", DataType::Float64, true)),
            Arc::new(Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )),
        ];
        if let Some(iv) = interval {
            exprs.push(lit(iv));
            fields.push(Arc::new(Field::new(
                "u",
                DataType::Interval(IntervalUnit::MonthDayNano),
                true,
            )));
        }
        (exprs, fields)
    }

    #[test]
    fn delta_unsupported_types() {
        let err = delta(
            &ScalarValue::Utf8(Some("a".into())),
            &ScalarValue::Utf8(Some("b".into())),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("derivative attempted on unsupported values"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn delta_mismatched_types() {
        let err = delta(
            &ScalarValue::Float64(Some(1.0)),
            &ScalarValue::Int64(Some(2)),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("derivative attempted on unsupported values"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn delta_time_unsupported_types() {
        let err = delta_time(
            &ScalarValue::Int64(Some(100)),
            &ScalarValue::Int64(Some(50)),
            1_000_000_000_f64,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("time delta attempted on unsupported values"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn delta_time_null_values() {
        let err = delta_time(
            &ScalarValue::TimestampNanosecond(None, None),
            &ScalarValue::TimestampNanosecond(Some(100), None),
            1_000_000_000_f64,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("time delta attempted on unsupported values"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn partition_evaluator_interval_with_months() {
        let udwf = DerivativeUDWF::new();
        let (exprs, fields) = evaluator_args(Some(ScalarValue::new_interval_mdn(1, 0, 0)));
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let err = udwf.partition_evaluator(args).unwrap_err();
        assert!(
            err.to_string().contains("cannot contain months or days"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn partition_evaluator_interval_with_days() {
        let udwf = DerivativeUDWF::new();
        let (exprs, fields) = evaluator_args(Some(ScalarValue::new_interval_mdn(0, 1, 0)));
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let err = udwf.partition_evaluator(args).unwrap_err();
        assert!(
            err.to_string().contains("cannot contain months or days"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn partition_evaluator_column_interval() {
        let udwf = DerivativeUDWF::new();
        let schema = Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("u", DataType::Interval(IntervalUnit::MonthDayNano), true),
        ]);
        let exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> = vec![
            col("f", &schema).unwrap(),
            col("t", &schema).unwrap(),
            col("u", &schema).unwrap(),
        ];
        let fields: Vec<FieldRef> = schema.fields().to_vec();
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let err = udwf.partition_evaluator(args).unwrap_err();
        assert!(
            err.to_string()
                .contains("must be a non-null literal interval"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn partition_evaluator_null_interval() {
        let udwf = DerivativeUDWF::new();
        let (exprs, fields) = evaluator_args(Some(ScalarValue::IntervalMonthDayNano(None)));
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let err = udwf.partition_evaluator(args).unwrap_err();
        assert!(
            err.to_string()
                .contains("must be a non-null literal interval"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn evaluate_identical_times_returns_infinity() {
        use arrow::array::{Float64Array, TimestampNanosecondArray};

        let udwf = DerivativeUDWF::new();
        let (exprs, fields) = evaluator_args(None);
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let mut evaluator = udwf.partition_evaluator(args).unwrap();

        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(TimestampNanosecondArray::from(vec![100, 100, 100])),
        ];
        let result = evaluator.evaluate_all(&values, 3).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected Float64Array");

        assert!(result.is_null(0)); // first row is always null
        assert_eq!(result.value(1), f64::INFINITY); // dt == 0, dv > 0 => +inf
        assert_eq!(result.value(2), f64::INFINITY); // dt == 0, dv > 0 => +inf
    }

    #[test]
    fn evaluate_identical_times_zero_delta_returns_nan() {
        use arrow::array::{Float64Array, TimestampNanosecondArray};

        let udwf = DerivativeUDWF::new();
        let (exprs, fields) = evaluator_args(None);
        let args = PartitionEvaluatorArgs::new(&exprs, &fields, false, false);
        let mut evaluator = udwf.partition_evaluator(args).unwrap();

        let values: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![5.0, 5.0])),
            Arc::new(TimestampNanosecondArray::from(vec![100, 100])),
        ];
        let result = evaluator.evaluate_all(&values, 2).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected Float64Array");

        assert!(result.is_null(0)); // first row is always null
        assert!(result.value(1).is_nan()); // dt == 0, dv == 0 => NaN
    }
}
