use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use futures::executor::block_on;

use crate::first_error::check_for_first_error;

/// Batches with `sizes` rows, then an error if `fail`.
fn stream(sizes: &[usize], fail: bool) -> SendableRecordBatchStream {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let mut items: Vec<_> = sizes
        .iter()
        .map(|&n| {
            let col = Arc::new(Int32Array::from(vec![1; n]));
            Ok(RecordBatch::try_new(Arc::clone(&schema), vec![col]).unwrap())
        })
        .collect();
    if fail {
        items.push(Err(DataFusionError::ResourcesExhausted("test".into())));
    }
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(items),
    ))
}

/// Row counts of the batches a stream yields, and whether it ended in an error.
fn drain(stream: SendableRecordBatchStream) -> (Vec<usize>, bool) {
    let items = block_on(stream.collect::<Vec<_>>());
    let failed = items.iter().any(Result::is_err);
    let rows = items.into_iter().flatten().map(|b| b.num_rows()).collect();
    (rows, failed)
}

#[test]
fn error_before_first_rows_is_returned() {
    for sizes in [&[][..], &[0, 0]] {
        let result = block_on(check_for_first_error(stream(sizes, true)));
        assert!(result.is_err(), "{sizes:?}");
    }
}

#[test]
fn error_after_first_rows_stays_in_the_stream() {
    let stream = block_on(check_for_first_error(stream(&[0, 2, 3], true))).unwrap();
    assert_eq!(drain(stream), (vec![0, 2, 3], true));
}

/// A selective scan can emit empty batches until its end; only the first is kept.
#[test]
fn only_the_first_leading_empty_batch_is_kept() {
    for (sizes, expected) in [
        (&[][..], vec![]),
        (&[0, 0, 0], vec![0]),
        (&[0, 0, 0, 1, 0], vec![0, 1, 0]),
    ] {
        let stream = block_on(check_for_first_error(stream(sizes, false))).unwrap();
        assert_eq!(drain(stream), (expected, false), "{sizes:?}");
    }
}
