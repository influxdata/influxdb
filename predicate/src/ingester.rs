//! Convert from the protobuf predicate used by the ingester's query API.

use crate::{BinaryExpr, Predicate};
use data_types::timestamp::TimestampRange;
use datafusion::{
    logical_plan::{
        abs, acos, asin, atan, ceil, concat, cos, digest, exp, floor, ln, log10, log2, round,
        signum, sin, sqrt, tan, trunc,
        window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
        Column, Expr, Operator,
    },
    physical_plan::{aggregates, functions, window_functions},
    prelude::{
        array, date_part, date_trunc, lower, ltrim, md5, octet_length, rtrim, sha224, sha256,
        sha384, sha512, trim, upper,
    },
};
use generated_types::{
    google::{FieldViolation, OptionalField},
    influxdata::iox::ingester::v1 as proto,
};

impl From<Predicate> for proto::Predicate {
    fn from(_pred: Predicate) -> Self {
        unimplemented!()
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = FieldViolation;

    fn try_from(proto: proto::Predicate) -> Result<Self, Self::Error> {
        let proto::Predicate {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        } = proto;

        let field_columns = if field_columns.is_empty() {
            None
        } else {
            Some(field_columns.into_iter().collect())
        };

        let range = range.map(|r| TimestampRange::new(r.start, r.end));

        let exprs = exprs
            .into_iter()
            .map(from_proto_expr)
            .collect::<Result<Vec<_>, _>>()?;

        let value_expr = value_expr
            .into_iter()
            .map(|ve| {
                let left = ve.left.unwrap_field("left")?;
                let right = ve.right.unwrap_field("right")?;

                Ok(BinaryExpr {
                    left: from_proto_column(left),
                    op: from_proto_binary_op("op", &ve.op)?,
                    right: from_proto_expr(right)?,
                })
            })
            .collect::<Result<Vec<BinaryExpr>, FieldViolation>>()?;

        Ok(Self {
            field_columns,
            partition_key,
            range,
            exprs,
            value_expr,
        })
    }
}

fn from_proto_column(proto: proto::Column) -> Column {
    Column {
        relation: proto.relation.map(|r| r.relation),
        name: proto.name,
    }
}

fn from_proto_binary_op(field: &'static str, op: &str) -> Result<Operator, FieldViolation> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
        other => Err(proto_error(
            field,
            format!("Unsupported binary operator '{:?}'", other),
        )),
    }
}

fn boxed_expr(
    expr: Option<Box<proto::LogicalExprNode>>,
    field_name: &'static str,
) -> Result<Box<Expr>, FieldViolation> {
    Ok(Box::new(from_proto_expr(*expr.unwrap_field(field_name)?)?))
}

fn from_proto_expr(proto: proto::LogicalExprNode) -> Result<Expr, FieldViolation> {
    use proto::logical_expr_node::ExprType;

    let expr_type = proto.expr_type.unwrap_field("expr_type")?;
    match expr_type {
        ExprType::BinaryExpr(binary_expr) => Ok(Expr::BinaryExpr {
            left: boxed_expr(binary_expr.l, "l")?,
            op: from_proto_binary_op("op", &binary_expr.op)?,
            right: boxed_expr(binary_expr.r, "r")?,
        }),
        ExprType::Column(column) => Ok(Expr::Column(from_proto_column(column))),
        ExprType::Literal(literal) => Ok(Expr::Literal(from_proto_scalar_value(literal)?)),
        ExprType::WindowExpr(expr) => {
            let window_function = expr.window_function.unwrap_field("window_function")?;
            let partition_by = expr
                .partition_by
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = expr
                .order_by
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?;
            let window_frame = expr
                .window_frame
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    if proto::WindowFrameUnits::Range == window_frame.window_frame_units()
                        && order_by.len() != 1
                    {
                        Err(proto_error("order_by", "With window frame of type RANGE, the order by expression must be of length 1"))
                    } else {
                        from_proto_window_frame(window_frame)
                    }
                })
                .transpose()?;

            match window_function {
                proto::window_expr_node::WindowFunction::AggrFunction(i) => {
                    let aggr_function = proto::AggregateFunction::from_i32(i).ok_or_else(|| {
                        proto_error(
                            "window_function",
                            format!("Received an unknown aggregate window function: {}", i),
                        )
                    })?;

                    Ok(Expr::WindowFunction {
                        fun: window_functions::WindowFunction::AggregateFunction(
                            from_proto_aggr_function(aggr_function)?,
                        ),
                        args: vec![from_proto_expr(*expr.expr.unwrap_field("expr")?)?],
                        partition_by,
                        order_by,
                        window_frame,
                    })
                }
                proto::window_expr_node::WindowFunction::BuiltInFunction(i) => {
                    use proto::BuiltInWindowFunction::*;
                    use window_functions::BuiltInWindowFunction;

                    let built_in = proto::BuiltInWindowFunction::from_i32(i)
                        .unwrap_field("built_in_window_function")?;
                    let built_in_function = match built_in {
                        Unspecified => {
                            return Err(proto_error("built_in_window_function", "not specified"))
                        }
                        RowNumber => BuiltInWindowFunction::RowNumber,
                        Rank => BuiltInWindowFunction::Rank,
                        PercentRank => BuiltInWindowFunction::PercentRank,
                        DenseRank => BuiltInWindowFunction::DenseRank,
                        Lag => BuiltInWindowFunction::Lag,
                        Lead => BuiltInWindowFunction::Lead,
                        FirstValue => BuiltInWindowFunction::FirstValue,
                        CumeDist => BuiltInWindowFunction::CumeDist,
                        Ntile => BuiltInWindowFunction::Ntile,
                        NthValue => BuiltInWindowFunction::NthValue,
                        LastValue => BuiltInWindowFunction::LastValue,
                    };

                    Ok(Expr::WindowFunction {
                        fun: window_functions::WindowFunction::BuiltInWindowFunction(
                            built_in_function,
                        ),
                        args: vec![from_proto_expr(*expr.expr.unwrap_field("expr")?)?],
                        partition_by,
                        order_by,
                        window_frame,
                    })
                }
            }
        }
        ExprType::AggregateExpr(expr) => {
            use aggregates::AggregateFunction;
            use proto::AggregateFunction::*;

            let aggr_function = proto::AggregateFunction::from_i32(expr.aggr_function)
                .unwrap_field("aggr_function")?;
            let fun = match aggr_function {
                Unspecified => return Err(proto_error("aggr_function", "not specified")),
                Min => AggregateFunction::Min,
                Max => AggregateFunction::Max,
                Sum => AggregateFunction::Sum,
                Avg => AggregateFunction::Avg,
                Count => AggregateFunction::Count,
                ApproxDistinct => AggregateFunction::ApproxDistinct,
                ArrayAgg => AggregateFunction::ArrayAgg,
                Variance => AggregateFunction::Variance,
                VariancePop => AggregateFunction::VariancePop,
                Covariance => AggregateFunction::Covariance,
                CovariancePop => AggregateFunction::CovariancePop,
                Stddev => AggregateFunction::Stddev,
                StddevPop => AggregateFunction::StddevPop,
                Correlation => AggregateFunction::Correlation,
                ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
                ApproxMedian => AggregateFunction::ApproxMedian,
            };

            Ok(Expr::AggregateFunction {
                fun,
                args: expr
                    .expr
                    .into_iter()
                    .map(from_proto_expr)
                    .collect::<Result<Vec<_>, _>>()?,
                distinct: false, //TODO
            })
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(
            boxed_expr(alias.expr, "expr")?,
            alias.alias.clone(),
        )),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(boxed_expr(is_null.expr, "expr")?)),
        ExprType::IsNotNullExpr(is_not_null) => {
            Ok(Expr::IsNotNull(boxed_expr(is_not_null.expr, "expr")?))
        }
        ExprType::NotExpr(not) => Ok(Expr::Not(boxed_expr(not.expr, "expr")?)),
        ExprType::Between(between) => Ok(Expr::Between {
            expr: boxed_expr(between.expr, "expr")?,
            negated: between.negated,
            low: boxed_expr(between.low, "low")?,
            high: boxed_expr(between.high, "high")?,
        }),
        ExprType::CaseNode(case) => {
            let proto::CaseNode {
                expr,
                when_then_expr,
                else_expr,
            } = *case;
            let when_then_expr = when_then_expr
                .into_iter()
                .map(|e| {
                    let proto::WhenThen {
                        when_expr,
                        then_expr,
                    } = e;
                    Ok((
                        Box::new(from_proto_expr(when_expr.unwrap_field("when_expr")?)?),
                        Box::new(from_proto_expr(then_expr.unwrap_field("then_expr")?)?),
                    ))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, FieldViolation>>()?;
            let expr: Option<Result<_, FieldViolation>> =
                expr.map(|e| Ok(Box::new(from_proto_expr(*e)?)));
            let else_expr: Option<Result<_, FieldViolation>> =
                else_expr.map(|e| Ok(Box::new(from_proto_expr(*e)?)));
            Ok(Expr::Case {
                expr: expr.transpose()?,
                when_then_expr,
                else_expr: else_expr.transpose()?,
            })
        }
        ExprType::Cast(cast) => {
            let expr = boxed_expr(cast.expr, "expr")?;
            let arrow_type = cast.arrow_type.unwrap_field("arrow_type")?;
            let arrow_type_enum = arrow_type.arrow_type_enum.unwrap_field("arrow_type_enum")?;

            let data_type = from_proto_arrow_type(arrow_type_enum)?;
            Ok(Expr::Cast { expr, data_type })
        }
        ExprType::TryCast(cast) => {
            let expr = boxed_expr(cast.expr, "expr")?;
            let arrow_type = cast.arrow_type.unwrap_field("arrow_type")?;
            let arrow_type_enum = arrow_type.arrow_type_enum.unwrap_field("arrow_type_enum")?;

            let data_type = from_proto_arrow_type(arrow_type_enum)?;
            Ok(Expr::TryCast { expr, data_type })
        }
        ExprType::Sort(sort) => Ok(Expr::Sort {
            expr: boxed_expr(sort.expr, "expr")?,
            asc: sort.asc,
            nulls_first: sort.nulls_first,
        }),
        ExprType::Negative(negative) => Ok(Expr::Negative(boxed_expr(negative.expr, "expr")?)),
        ExprType::InList(in_list) => Ok(Expr::InList {
            expr: boxed_expr(in_list.expr, "expr")?,
            list: in_list
                .list
                .into_iter()
                .map(from_proto_expr)
                .collect::<Result<Vec<_>, _>>()?,
            negated: in_list.negated,
        }),
        ExprType::Wildcard(_) => Ok(Expr::Wildcard),
        ExprType::ScalarFunction(expr) => {
            let scalar_function = proto::ScalarFunction::from_i32(expr.fun).unwrap_field("fun")?;
            let mut args = expr
                .args
                .into_iter()
                .map(from_proto_expr)
                .rev()
                .collect::<Result<Vec<_>, _>>()?;

            let scalar_function = match scalar_function {
                proto::ScalarFunction::Unspecified => {
                    return Err(proto_error("fun", "not specified"))
                }
                // unary - must have one argument, otherwise error
                proto::ScalarFunction::Sqrt => sqrt(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sin => sin(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Cos => cos(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Tan => tan(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Asin => asin(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Acos => acos(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Atan => atan(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Exp => exp(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log2 => log2(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ln => ln(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log => log10(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Log10 => log10(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Floor => floor(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ceil => ceil(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Round => round(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Trunc => trunc(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Abs => abs(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Signum => signum(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Octetlength => octet_length(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Lower => lower(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Upper => upper(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Trim => trim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Ltrim => ltrim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Rtrim => rtrim(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Md5 => md5(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha224 => sha224(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha256 => sha256(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha384 => sha384(args.pop().unwrap_field("arg")?),
                proto::ScalarFunction::Sha512 => sha512(args.pop().unwrap_field("arg")?),

                // binary - error if there aren't two arguments
                proto::ScalarFunction::Datepart => date_part(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),
                proto::ScalarFunction::Datetrunc => date_trunc(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),
                proto::ScalarFunction::Digest => digest(
                    args.pop().unwrap_field("arg")?,
                    args.pop().unwrap_field("arg")?,
                ),

                // pass the vec of arguments through without checking how many
                proto::ScalarFunction::Concat => concat(&args),
                proto::ScalarFunction::Array => array(args),
                proto::ScalarFunction::Totimestamp => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::ToTimestamp,
                    args,
                },
                proto::ScalarFunction::Totimestampmillis => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::ToTimestampMillis,
                    args,
                },
                proto::ScalarFunction::Nullif => Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::NullIf,
                    args,
                },
            };

            Ok(scalar_function)
        }
    }
}

fn from_proto_scalar_value(
    proto: proto::ScalarValue,
) -> Result<datafusion::scalar::ScalarValue, FieldViolation> {
    use datafusion::scalar::ScalarValue;
    use proto::scalar_value::Value::*;

    let value = proto.value.unwrap_field("value")?;

    let result = match value {
        BoolValue(v) => ScalarValue::Boolean(Some(v)),
        Utf8Value(v) => ScalarValue::Utf8(Some(v)),
        LargeUtf8Value(v) => ScalarValue::LargeUtf8(Some(v)),
        Int8Value(v) => ScalarValue::Int8(Some(v as i8)),
        Int16Value(v) => ScalarValue::Int16(Some(v as i16)),
        Int32Value(v) => ScalarValue::Int32(Some(v)),
        Int64Value(v) => ScalarValue::Int64(Some(v)),
        Uint8Value(v) => ScalarValue::UInt8(Some(v as u8)),
        Uint16Value(v) => ScalarValue::UInt16(Some(v as u16)),
        Uint32Value(v) => ScalarValue::UInt32(Some(v)),
        Uint64Value(v) => ScalarValue::UInt64(Some(v)),
        Float32Value(v) => ScalarValue::Float32(Some(v)),
        Float64Value(v) => ScalarValue::Float64(Some(v)),
        Date32Value(v) => ScalarValue::Date32(Some(v)),
        TimeMicrosecondValue(v) => ScalarValue::TimestampMicrosecond(Some(v), None),
        TimeNanosecondValue(v) => ScalarValue::TimestampNanosecond(Some(v), None),
        ListValue(_v) => unimplemented!("this is nontrivial"),
        NullListValue(_v) => unimplemented!("this is nontrivial"),
        NullValue(_null_enum) => unimplemented!("this is nontrivial"),
        Decimal128Value(_v) => unimplemented!("this is nontrivial"),
        Date64Value(v) => ScalarValue::Date64(Some(v)),
        TimeSecondValue(v) => ScalarValue::TimestampSecond(Some(v), None),
        TimeMillisecondValue(v) => ScalarValue::TimestampMillisecond(Some(v), None),
        IntervalYearmonthValue(v) => ScalarValue::IntervalYearMonth(Some(v)),
        IntervalDaytimeValue(v) => ScalarValue::IntervalDayTime(Some(v)),
    };

    Ok(result)
}

fn from_proto_aggr_function(
    proto: proto::AggregateFunction,
) -> Result<aggregates::AggregateFunction, FieldViolation> {
    use aggregates::AggregateFunction;
    let agg = match proto {
        proto::AggregateFunction::Unspecified => {
            return Err(proto_error("aggregate_function", "not specified"))
        }
        proto::AggregateFunction::Min => AggregateFunction::Min,
        proto::AggregateFunction::Max => AggregateFunction::Max,
        proto::AggregateFunction::Sum => AggregateFunction::Sum,
        proto::AggregateFunction::Avg => AggregateFunction::Avg,
        proto::AggregateFunction::Count => AggregateFunction::Count,
        proto::AggregateFunction::ApproxDistinct => AggregateFunction::ApproxDistinct,
        proto::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
        proto::AggregateFunction::Variance => AggregateFunction::Variance,
        proto::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
        proto::AggregateFunction::Covariance => AggregateFunction::Covariance,
        proto::AggregateFunction::CovariancePop => AggregateFunction::CovariancePop,
        proto::AggregateFunction::Stddev => AggregateFunction::Stddev,
        proto::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
        proto::AggregateFunction::Correlation => AggregateFunction::Correlation,
        proto::AggregateFunction::ApproxPercentileCont => AggregateFunction::ApproxPercentileCont,
        proto::AggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
    };

    Ok(agg)
}

fn from_proto_window_frame(proto: proto::WindowFrame) -> Result<WindowFrame, FieldViolation> {
    let units = proto.window_frame_units();
    let units = match units {
        proto::WindowFrameUnits::Unspecified => {
            return Err(proto_error("window_frame_units", "not specified"))
        }
        proto::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
        proto::WindowFrameUnits::Range => WindowFrameUnits::Range,
        proto::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
    };

    let start_bound =
        from_proto_window_frame_bound(proto.start_bound.unwrap_field("start_bound")?)?;

    let end_bound: Option<Result<_, FieldViolation>> =
        proto.end_bound.map(from_proto_window_frame_bound);
    let end_bound = end_bound
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);

    Ok(WindowFrame {
        units,
        start_bound,
        end_bound,
    })
}

fn from_proto_window_frame_bound(
    proto: proto::WindowFrameBound,
) -> Result<WindowFrameBound, FieldViolation> {
    let bound_type = proto.window_frame_bound_type();

    let bound = match bound_type {
        proto::WindowFrameBoundType::Unspecified => {
            return Err(proto_error("window_frame_bound_type", "not specified"))
        }
        proto::WindowFrameBoundType::CurrentRow => WindowFrameBound::CurrentRow,
        proto::WindowFrameBoundType::Preceding => WindowFrameBound::Preceding(proto.bound_value),
        proto::WindowFrameBoundType::Following => WindowFrameBound::Following(proto.bound_value),
    };

    Ok(bound)
}

fn from_proto_interval_unit(
    proto: proto::IntervalUnit,
) -> Result<arrow::datatypes::IntervalUnit, FieldViolation> {
    use arrow::datatypes::IntervalUnit;
    let iu = match proto {
        proto::IntervalUnit::Unspecified => {
            return Err(proto_error("interval_unit", "not specified"))
        }
        proto::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
        proto::IntervalUnit::DayTime => IntervalUnit::DayTime,
        proto::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
    };

    Ok(iu)
}

fn from_proto_time_unit(
    proto: proto::TimeUnit,
) -> Result<arrow::datatypes::TimeUnit, FieldViolation> {
    use arrow::datatypes::TimeUnit;
    let unit = match proto {
        proto::TimeUnit::Unspecified => return Err(proto_error("time_unit", "not specified")),
        proto::TimeUnit::Second => TimeUnit::Second,
        proto::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
        proto::TimeUnit::Microsecond => TimeUnit::Microsecond,
        proto::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
    };

    Ok(unit)
}

fn from_proto_arrow_type(
    proto: proto::arrow_type::ArrowTypeEnum,
) -> Result<arrow::datatypes::DataType, FieldViolation> {
    use arrow::datatypes::{DataType, UnionMode};
    use proto::arrow_type::ArrowTypeEnum;

    let ty = match proto {
        ArrowTypeEnum::None(_) => DataType::Null,
        ArrowTypeEnum::Bool(_) => DataType::Boolean,
        ArrowTypeEnum::Uint8(_) => DataType::UInt8,
        ArrowTypeEnum::Int8(_) => DataType::Int8,
        ArrowTypeEnum::Uint16(_) => DataType::UInt16,
        ArrowTypeEnum::Int16(_) => DataType::Int16,
        ArrowTypeEnum::Uint32(_) => DataType::UInt32,
        ArrowTypeEnum::Int32(_) => DataType::Int32,
        ArrowTypeEnum::Uint64(_) => DataType::UInt64,
        ArrowTypeEnum::Int64(_) => DataType::Int64,
        ArrowTypeEnum::Float16(_) => DataType::Float16,
        ArrowTypeEnum::Float32(_) => DataType::Float32,
        ArrowTypeEnum::Float64(_) => DataType::Float64,
        ArrowTypeEnum::Utf8(_) => DataType::Utf8,
        ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
        ArrowTypeEnum::Binary(_) => DataType::Binary,
        ArrowTypeEnum::FixedSizeBinary(size) => DataType::FixedSizeBinary(size),
        ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
        ArrowTypeEnum::Date32(_) => DataType::Date32,
        ArrowTypeEnum::Date64(_) => DataType::Date64,
        ArrowTypeEnum::Duration(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Duration(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Timestamp(proto::Timestamp {
            time_unit,
            timezone,
        }) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Timestamp(
                from_proto_time_unit(time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone),
                },
            )
        }
        ArrowTypeEnum::Time32(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Time32(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Time64(time_unit) => {
            let time_unit = proto::TimeUnit::from_i32(time_unit).unwrap_field("time_unit")?;
            DataType::Time64(from_proto_time_unit(time_unit)?)
        }
        ArrowTypeEnum::Interval(interval_unit) => {
            let interval_unit =
                proto::IntervalUnit::from_i32(interval_unit).unwrap_field("interval_unit")?;
            DataType::Interval(from_proto_interval_unit(interval_unit)?)
        }
        ArrowTypeEnum::Decimal(proto::Decimal { whole, fractional }) => {
            DataType::Decimal(whole as usize, fractional as usize)
        }
        ArrowTypeEnum::List(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::List(Box::new(from_proto_field(*list_type)?))
        }
        ArrowTypeEnum::LargeList(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::LargeList(Box::new(from_proto_field(*list_type)?))
        }
        ArrowTypeEnum::FixedSizeList(list) => {
            let list_type = list.field_type.unwrap_field("field_type")?;
            DataType::FixedSizeList(Box::new(from_proto_field(*list_type)?), list.list_size)
        }
        ArrowTypeEnum::Struct(strct) => DataType::Struct(
            strct
                .sub_field_types
                .into_iter()
                .map(from_proto_field)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ArrowTypeEnum::Union(union) => {
            let union_mode =
                proto::UnionMode::from_i32(union.union_mode).unwrap_field("union_mode")?;
            let union_mode = match union_mode {
                proto::UnionMode::Unspecified => {
                    return Err(proto_error("union_mode", "not specified"))
                }
                proto::UnionMode::Dense => UnionMode::Dense,
                proto::UnionMode::Sparse => UnionMode::Sparse,
            };
            let union_types = union
                .union_types
                .into_iter()
                .map(from_proto_field)
                .collect::<Result<Vec<_>, _>>()?;
            DataType::Union(union_types, union_mode)
        }
        ArrowTypeEnum::Dictionary(dict) => {
            let key_datatype = dict
                .key
                .map(|t| from_proto_arrow_type(t.arrow_type_enum.unwrap_field("arrow_type_enum")?))
                .transpose()?
                .unwrap_field("key")?;
            let value_datatype = dict
                .value
                .map(|t| from_proto_arrow_type(t.arrow_type_enum.unwrap_field("arrow_type_enum")?))
                .transpose()?
                .unwrap_field("value")?;
            DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
        }
    };

    Ok(ty)
}

fn from_proto_field(proto: proto::Field) -> Result<arrow::datatypes::Field, FieldViolation> {
    let data_type = proto
        .arrow_type
        .unwrap_field("arrow_type")?
        .arrow_type_enum
        .unwrap_field("arrow_type_enum")?;
    let data_type = from_proto_arrow_type(data_type)?;
    Ok(arrow::datatypes::Field::new(
        &proto.name,
        data_type,
        proto.nullable,
    ))
}

fn proto_error(field: impl Into<String>, description: impl Into<String>) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: description.into(),
    }
}
