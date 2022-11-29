use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, stream::BoxStream, Stream, StreamExt};
use generated_types::{read_response::Frame, ReadResponse};

/// Chunk given [`Frame`]s -- while preserving the order -- into [`ReadResponse`]s that shall at max have the
/// given `size_limit`, in bytes.
pub struct ChunkReadResponses {
    inner: BoxStream<'static, Result<Frame, tonic::Status>>,
    size_limit: usize,
    finished: bool,
    frames: Vec<Frame>,
    /// Current size of `frames`, in bytes
    frames_size: usize,
}

impl ChunkReadResponses {
    /// Create new stream wrapper.
    ///
    /// # Panic
    /// Panics if `size_limit` is 0.
    pub fn new<S>(inner: S, size_limit: usize) -> Self
    where
        S: Stream<Item = Result<Frame, tonic::Status>> + Send + 'static,
    {
        assert!(size_limit > 0, "zero size limit");

        Self {
            inner: inner.boxed(),
            size_limit,
            finished: false,
            frames: Vec::default(),
            frames_size: 0,
        }
    }
}

impl Stream for ChunkReadResponses {
    type Item = Result<ReadResponse, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(this.inner.poll_next_unpin(cx)) {
                Some(Ok(frame)) => {
                    let fsize = frame_size(&frame);

                    if fsize > this.size_limit {
                        this.finished = true;
                        return Poll::Ready(Some(Err(tonic::Status::resource_exhausted(format!(
                            "Oversized frame in read response: frame_size={}, size_limit={}",
                            fsize, this.size_limit
                        )))));
                    }

                    // flush?
                    if this.frames_size + fsize > this.size_limit {
                        this.frames_size = fsize;
                        let mut tmp = vec![frame];
                        std::mem::swap(&mut tmp, &mut this.frames);
                        return Poll::Ready(Some(Ok(ReadResponse { frames: tmp })));
                    }

                    this.frames.push(frame);
                    this.frames_size += fsize;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    this.finished = true;

                    // final flush
                    if !this.frames.is_empty() {
                        this.frames_size = 0;
                        return Poll::Ready(Some(Ok(ReadResponse {
                            frames: std::mem::take(&mut this.frames),
                        })));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

fn frame_size(frame: &Frame) -> usize {
    frame
        .data
        .as_ref()
        .map(|data| data.encoded_len())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use generated_types::influxdata::platform::storage::read_response::{
        frame::Data, BooleanPointsFrame,
    };

    use super::*;

    #[test]
    #[should_panic(expected = "zero size limit")]
    fn test_new_panics() {
        ChunkReadResponses::new(futures::stream::empty(), 0);
    }

    #[tokio::test]
    async fn test_ok() {
        let frame1 = Frame {
            data: Some(Data::BooleanPoints(BooleanPointsFrame {
                timestamps: vec![1, 2, 3],
                values: vec![false, true, false],
            })),
        };
        let frame2 = Frame {
            data: Some(Data::BooleanPoints(BooleanPointsFrame {
                timestamps: vec![4],
                values: vec![true],
            })),
        };
        let fsize1 = frame_size(&frame1);
        let fsize2 = frame_size(&frame2);

        // no respones
        assert_eq!(
            ChunkReadResponses::new(futures::stream::empty(), 1)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![],
        );

        // split
        assert_eq!(
            ChunkReadResponses::new(
                futures::stream::iter([
                    Ok(frame1.clone()),
                    Ok(frame1.clone()),
                    Ok(frame2.clone()),
                    Ok(frame2.clone()),
                    Ok(frame1.clone()),
                ]),
                fsize1 + fsize1 + fsize2,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
            vec![
                ReadResponse {
                    frames: vec![frame1.clone(), frame1.clone(), frame2.clone()],
                },
                ReadResponse {
                    frames: vec![frame2.clone(), frame1.clone()],
                },
            ],
        );

        // single response
        assert_eq!(
            ChunkReadResponses::new(
                futures::stream::iter(
                    [Ok(frame1.clone()), Ok(frame2.clone()), Ok(frame2.clone()),]
                ),
                fsize1 + fsize2 + fsize2,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
            vec![ReadResponse {
                frames: vec![frame1.clone(), frame2.clone(), frame2.clone()],
            },],
        );

        // multiple responses
        assert_eq!(
            ChunkReadResponses::new(
                futures::stream::iter([
                    Ok(frame1.clone()),
                    Ok(frame1.clone()),
                    Ok(frame2.clone()),
                    Ok(frame2.clone()),
                    Ok(frame1.clone()),
                    Ok(frame1.clone()),
                    Ok(frame2.clone()),
                ]),
                fsize1 + fsize1 + fsize2,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
            vec![
                ReadResponse {
                    frames: vec![frame1.clone(), frame1.clone(), frame2.clone()],
                },
                ReadResponse {
                    frames: vec![frame2.clone(), frame1.clone(), frame1],
                },
                ReadResponse {
                    frames: vec![frame2],
                },
            ],
        );
    }

    #[tokio::test]
    async fn test_err_stream() {
        let frame = Frame {
            data: Some(Data::BooleanPoints(BooleanPointsFrame {
                timestamps: vec![1, 2, 3],
                values: vec![false, true, false],
            })),
        };
        let fsize = frame_size(&frame);

        // split
        let res = ChunkReadResponses::new(
            futures::stream::iter(vec![
                Ok(frame.clone()),
                Ok(frame.clone()),
                Ok(frame.clone()),
                Ok(frame.clone()),
                Err(tonic::Status::internal("foo")),
                Ok(frame.clone()),
            ]),
            2 * fsize,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(res.len(), 4);

        assert_eq!(
            res[0].as_ref().unwrap(),
            &ReadResponse {
                frames: vec![frame.clone(), frame.clone()],
            },
        );

        // error comes two frames early because the package wasn't full yet
        assert_eq!(res[1].as_ref().unwrap_err().code(), tonic::Code::Internal,);
        assert_eq!(res[1].as_ref().unwrap_err().message(), "foo",);

        assert_eq!(
            res[2].as_ref().unwrap(),
            &ReadResponse {
                frames: vec![frame.clone(), frame.clone()],
            },
        );
        assert_eq!(
            res[3].as_ref().unwrap(),
            &ReadResponse {
                frames: vec![frame],
            },
        );
    }

    #[tokio::test]
    async fn test_err_oversized() {
        let frame = Frame {
            data: Some(Data::BooleanPoints(BooleanPointsFrame {
                timestamps: vec![1, 2, 3],
                values: vec![false, true, false],
            })),
        };

        // split
        let res = ChunkReadResponses::new(
            futures::stream::iter([Ok(frame.clone()), Ok(frame.clone())]),
            1,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(res.len(), 1);

        assert_eq!(
            res[0].as_ref().unwrap_err().code(),
            tonic::Code::ResourceExhausted,
        );
        assert_eq!(
            res[0].as_ref().unwrap_err().message(),
            "Oversized frame in read response: frame_size=33, size_limit=1",
        );
    }
}
