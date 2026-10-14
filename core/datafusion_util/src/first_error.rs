//! Surface a query's early errors while the caller can still report them.

use datafusion::common::Result;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

/// Poll `stream` until its first batch with rows (or its end), returning any
/// error raised before then. Errors before the first output, such as a sort that
/// must consume all its input, can then be reported before a response is
/// committed, e.g. as an HTTP status. The returned stream replays the polled
/// batches and continues with the rest of `stream`.
///
/// Only the first leading empty batch is kept: a consumer may need it (it can be
/// the only batch), but a selective scan can emit empty batches until its end.
pub async fn check_for_first_error(
    mut stream: SendableRecordBatchStream,
) -> Result<SendableRecordBatchStream> {
    let mut peeked = Vec::new();
    while let Some(batch) = stream.next().await.transpose()? {
        let has_rows = batch.num_rows() > 0;
        if has_rows || peeked.is_empty() {
            peeked.push(Ok(batch));
        }
        if has_rows {
            break;
        }
    }
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        stream.schema(),
        futures::stream::iter(peeked).chain(stream),
    )))
}
