use super::SeriesChunk;
use crate::Result;
use crate::error::Error;
use arrow::array::RecordBatch;
use arrow::compute::partition;
use arrow::datatypes::SchemaRef;
use futures::{Stream, ready};
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use schema::INFLUXQL_METADATA_KEY;
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Stream that processes a stream of [SeriesChunk]s, merging them
/// subsequet chunks that are for the same series, and splitting chunks
/// that are larger that the specified chunk size.
pub(crate) struct SeriesChunkMergeStream<S> {
    input: S,
    chunk_size: Option<NonZeroUsize>,

    current: Option<SeriesChunk>,
}

impl<S> SeriesChunkMergeStream<S> {
    pub(crate) fn new(input: S, chunk_size: Option<NonZeroUsize>) -> Self {
        Self {
            input,
            chunk_size,
            current: None,
        }
    }
}

impl<S> Stream for SeriesChunkMergeStream<S>
where
    S: Stream<Item = Result<SeriesChunk>> + Unpin,
{
    type Item = Result<SeriesChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut input = Pin::new(&mut this.input);
        loop {
            if let Some(chunk_size) = this.chunk_size {
                let chunk_size: usize = chunk_size.into();
                if let Some(current) = this.current.take() {
                    if current.num_rows() > chunk_size {
                        let (mut left, right) = current.split_at(chunk_size);
                        left.partial = true;
                        this.current = Some(right);
                        return Poll::Ready(Some(Ok(left)));
                    } else {
                        this.current = Some(current);
                    }
                }
            }

            match ready!(input.as_mut().poll_next(cx)) {
                None => {
                    return if let Some(current) = this.current.take() {
                        Poll::Ready(Some(Ok(current)))
                    } else {
                        Poll::Ready(None)
                    };
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(chunk)) => {
                    match this.current {
                        Some(ref mut current) => {
                            if current.series() == chunk.series() {
                                current.merge(chunk);
                            } else {
                                let current = this.current.take().unwrap(); // safe unwrap due to check above
                                this.current = Some(chunk);
                                return Poll::Ready(Some(Ok(current)));
                            }
                        }
                        None => {
                            this.current = Some(chunk);
                        }
                    }
                }
            }
        }
    }
}

/// SeriesChunkStream processes a stream of [RecordBatch]es, breaking
/// each one into [SeriesChunk]s. Each [SeriesChunk] contains the data
/// from a single Series.
pub(crate) struct SeriesChunkStream<S> {
    /// A stream that returns [RecordBatch]es
    record_batch_stream: S,

    measurement: usize,
    tag_columns: Arc<BTreeMap<Arc<str>, usize>>,
    value_columns: Arc<[(Arc<str>, usize)]>,

    batch: RecordBatch,
    partitions: Vec<Range<usize>>,
    current_partition: usize,
}

impl<S> SeriesChunkStream<S> {
    pub(crate) fn try_new(record_batch_stream: S, schema: SchemaRef) -> Result<Self> {
        let md = schema.metadata.get(INFLUXQL_METADATA_KEY).ok_or(
            datafusion::error::DataFusionError::Internal(
                "Missing INFLUXQL_METADATA in RecordBatch schema".to_owned(),
            ),
        )?;
        let iox_metadata: InfluxQlMetadata = serde_json::from_str(md.as_str())
            .map_err(|x| datafusion::error::DataFusionError::Internal(x.to_string()))?;

        let measurement = iox_metadata.measurement_column_index as usize;
        let mut elided_columns = BTreeSet::new();
        elided_columns.insert(measurement);
        let tags = iox_metadata
            .tag_key_columns
            .iter()
            .inspect(|x| {
                if !x.is_projected {
                    elided_columns.insert(x.column_index as usize);
                }
            })
            .map(|x| (Arc::from(x.tag_key.as_str()), x.column_index as usize))
            .collect::<BTreeMap<_, _>>();
        let mut columns = Vec::new();

        schema.fields().iter().enumerate().for_each(|(i, f)| {
            if !elided_columns.contains(&i) {
                columns.push((Arc::from(f.name().as_str()), i));
            }
        });

        Ok(Self {
            record_batch_stream,
            measurement,
            tag_columns: Arc::from(tags),
            value_columns: Arc::from(columns),
            batch: RecordBatch::new_empty(schema),
            partitions: Vec::new(),
            current_partition: 0,
        })
    }

    fn chunk(&self, range: &Range<usize>, batch: &RecordBatch) -> SeriesChunk {
        let batch = batch.slice(range.start, range.end - range.start);
        SeriesChunk::new(
            self.measurement,
            Arc::clone(&self.tag_columns),
            Arc::clone(&self.value_columns),
            batch,
        )
    }

    fn get_partitions_from_record_batch(
        &self,
        batch: &RecordBatch,
    ) -> datafusion::common::Result<Vec<Range<usize>>> {
        let mut tag_keys_columns = Vec::with_capacity(self.tag_columns.len() + 1);
        tag_keys_columns.push(Arc::clone(batch.column(self.measurement)));
        for (_, idx) in self.tag_columns.iter() {
            tag_keys_columns.push(Arc::clone(batch.column(*idx)));
        }
        Ok(partition(tag_keys_columns.as_slice())?.ranges())
    }
}

impl<S> SeriesChunkStream<S>
where
    S: Stream<Item = Result<RecordBatch, datafusion::common::DataFusionError>> + Unpin,
{
    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        Pin::new(&mut self.record_batch_stream)
            .poll_next(cx)
            .map_err(Error::from)
    }
}

impl<S> Stream for SeriesChunkStream<S>
where
    S: Stream<Item = Result<RecordBatch, datafusion::common::DataFusionError>> + Unpin,
{
    type Item = Result<SeriesChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while this.current_partition >= this.partitions.len() {
            match ready!(this.poll_next_inner(cx)) {
                None => return Poll::Ready(None),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(batch)) => {
                    this.partitions = this.get_partitions_from_record_batch(&batch)?;
                    this.batch = batch;
                    this.current_partition = 0;
                }
            }
        }
        let chunk = this.chunk(&this.partitions[this.current_partition], &this.batch);
        this.current_partition += 1;
        Poll::Ready(Some(Ok(chunk)))
    }
}

#[cfg(test)]
mod tests {
    use crate::response::SeriesChunkSerializer;

    use super::super::tests::{Column, make_schema_and_batches};
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::Int32Type;
    use datafusion_util::MemoryStream;
    use futures::TryStreamExt;

    macro_rules! insta_assert_yaml_snapshot {
        ($INPUT:expr) => {
            let chunks = $INPUT
                .iter()
                .map(|s| SeriesChunkSerializer::new(s, None, true))
                .collect::<Vec<_>>();

            insta::assert_yaml_snapshot!(chunks);
        };
    }

    #[tokio::test]
    async fn test_no_batch() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: false,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],

            data: vec![], // no record batch
            chunk_size: None,
        })
        .await;

        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn test_no_group_by_single_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            // single record batch
            data: vec![vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                // single series: a1
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ]],
            chunk_size: None, // single chunk
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_no_group_by_multi_chunks() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            // single record batch
            data: vec![vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                // single series: a1
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ]],
            chunk_size: Some(2), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_no_group_by_zero_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: false, // no group by
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            // single record batch
            data: vec![vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                // single series: a1
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ]],
            chunk_size: Some(0), // this is equivalent to None, i.e. single chunk
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_single_batch_single_series_single_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true, // group by tag0
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true, // group by tag1
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            // single record batch
            data: vec![vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                // single series: a1
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ]],
            chunk_size: None, // single chunk
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_single_batch_single_series_multi_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true, // group by tag0
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true, // group by tag1
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            // single record batch
            data: vec![vec![
                Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1000000000, 2000000000, 3000000000,
                ])),
                // single series: a1
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ]],
            chunk_size: Some(2), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_single_series_single_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // single series: a1
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "a", "a", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "1", "1", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: None, // single chunk
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_single_series_multi_chunk_gt() {
        // Testing batch size > chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // single series: a1
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "a", "a", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "1", "1", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(2), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_single_series_multi_chunk_eq() {
        // Testing batch size = chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // single series: a1
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "a", "a", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "1", "1", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(3), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_single_series_multi_chunk_lt() {
        // Testing batch size < chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // single series: a1
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "a", "a", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "1", "1", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(12), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_multi_series_single_chunk() {
        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // multi series: a1, b2
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "b"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "2"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "b", "b", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "2", "2", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: None, // single chunk
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_multi_series_multi_chunk_lt() {
        // Testing batch size < chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // multi series: a1, b2
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "b"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "2"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "b", "b", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "2", "2", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(12), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_multi_series_multi_chunk_eq() {
        // Testing batch size = chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // multi series: a1, b2
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "b"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "2"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "b", "b", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "2", "2", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(3), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    #[tokio::test]
    async fn test_many_batches_multi_series_multi_chunk_gt() {
        // Testing batch size > chunk size

        let chunks = make_chunks(TestParams {
            columns: vec![
                Column::Measurement,
                Column::Time,
                Column::Tag {
                    name: "tag0",
                    group_by: true,
                    projected: false,
                },
                Column::Tag {
                    name: "tag1",
                    group_by: true,
                    projected: false,
                },
                Column::Field { name: "val" },
            ],
            data: vec![
                // record batch 1
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1000000000, 2000000000, 3000000000,
                    ])),
                    // multi series: a1, b2
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "b"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "2"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
                // record batch 2
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        4000000000, 5000000000, 6000000000, 7000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "b", "b", "a", "a",
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec![
                        "2", "2", "1", "1",
                    ])),
                    Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
                ],
                // record batch 3
                vec![
                    Arc::new(StringArray::from(vec!["m1", "m1", "m1"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        8000000000,
                        9000000000,
                        10000000000,
                    ])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["a", "a", "a"])),
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(vec!["1", "1", "1"])),
                    Arc::new(Int64Array::from(vec![8, 9, 10])),
                ],
            ],
            chunk_size: Some(2), // multi chunks
        })
        .await;

        insta_assert_yaml_snapshot!(chunks);
    }

    struct TestParams {
        columns: Vec<Column>,
        data: Vec<Vec<Arc<dyn Array>>>,
        chunk_size: Option<usize>,
    }

    async fn make_chunks(params: TestParams) -> Vec<SeriesChunk> {
        let TestParams {
            columns,
            data,
            chunk_size,
        } = params;

        let (schema, batches) = make_schema_and_batches(columns, data);

        let stream = MemoryStream::new_with_schema(batches, Arc::clone(&schema));
        let stream = SeriesChunkStream::try_new(stream, schema).unwrap();
        let stream = SeriesChunkMergeStream::new(stream, chunk_size.and_then(NonZeroUsize::new));

        let chunks: Result<Vec<SeriesChunk>> = stream.try_collect().await;
        chunks.unwrap()
    }
}
