mod internal;

pub use internal::{Duration, Window};

use std::sync::Arc;

use arrow_deps::{
    arrow::{
        array::{ArrayRef, Int64Array, Int64Builder},
        datatypes::DataType,
    },
    datafusion::{logical_plan::Expr, physical_plan::functions::make_scalar_function, prelude::*},
};

use crate::group_by::WindowDuration;

// Reuse DataFusion error and Result types for this module
pub use arrow_deps::datafusion::error::{DataFusionError as Error, Result};

/// This is the implementation of the `window_bounds` user defined
/// function used in IOx to compute window boundaries when doing
/// grouping by windows.
fn window_bounds(
    args: &[ArrayRef],
    every: &WindowDuration,
    offset: &WindowDuration,
) -> Result<ArrayRef> {
    // Note:  At the time of writing, DataFusion creates arrays of constants for
    // constant arguments (which 4 of 5 arguments to window bounds are). We
    // should eventually contribute some way back upstream to make DataFusion
    // pass 4 constants rather than 4 arrays of constants.

    // There are any number of ways this function could also be further
    // optimized, which we leave as an exercise to our future selves

    // `args` and output are dynamically-typed Arrow arrays, which means that we
    // need to:
    //
    // 1. cast the values to the type we want
    // 2. perform the window_bounds calculation for every element in the
    //     timestamp array
    // 3. construct the resulting array

    // this is guaranteed by DataFusion based on the function's signature.
    assert_eq!(args.len(), 1);

    let time = &args[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast of time failed");

    // Note: the Go code uses the `Stop` field of the `GetEarliestBounds` call as
    // the window boundary https://github.com/influxdata/influxdb/blob/master/storage/reads/array_cursor.gen.go#L546

    // Note window doesn't use the period argument
    let period = internal::Duration::from_nsecs(0);
    let window = internal::Window::new(every.into(), period, offset.into());

    // calculate the output times, one at a time, one element at a time
    let mut builder = Int64Builder::new(time.len());
    time.iter().try_for_each(|ts| match ts {
        Some(ts) => {
            let bounds = window.get_earliest_bounds(ts);
            builder.append_value(bounds.stop)
        }
        None => builder.append_null(),
    })?;

    Ok(Arc::new(builder.finish()))
}

/// Create a DataFusion `Expr` that invokes `window_bounds` with the
/// appropriate every and offset arguments at runtime
pub fn make_window_bound_expr(
    time_arg: Expr,
    every: &WindowDuration,
    offset: &WindowDuration,
) -> Expr {
    // Bind a copy of the arguments in a closure
    let every = every.clone();
    let offset = offset.clone();

    // TODO provide optimized implementations (that took every/offset
    // as a constant rather than arrays)
    let func_ptr = make_scalar_function(move |args| window_bounds(args, &every, &offset));

    let udf = create_udf(
        "window_bounds",
        vec![DataType::Int64],     // argument types
        Arc::new(DataType::Int64), // return type
        func_ptr,
    );

    udf.call(vec![time_arg])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_bounds() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(100),
            None,
            Some(200),
            Some(300),
            Some(400),
        ]));

        let every = WindowDuration::from_nanoseconds(200);
        let offset = WindowDuration::from_nanoseconds(50);

        let bounds_array =
            window_bounds(&[input], &every, &offset).expect("window_bounds executed correctly");

        let expected_array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(250),
            None,
            Some(250),
            Some(450),
            Some(450),
        ]));

        assert_eq!(
            &expected_array, &bounds_array,
            "Expected:\n{:?}\nActual:\n{:?}",
            expected_array, bounds_array,
        );
    }
}
