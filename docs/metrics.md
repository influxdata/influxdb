# Metrics in IOx

IOx output metrics to Jaeger for distributed request correlation.

Here are useful metrics

### Requests to IOx Server including Routers and Query Servers
| Metric name |  Code Name | Description |
| --- | --- | --- |
| http_requests_total | http_requests | Total number of HTTP requests |
| gRPC_requests_total | requests | Total number of gROC requests |
| http_request_duration_seconds| ? | Time to finish a request  |
| http_mem_bytes | ? | ? |
| http_mem_bytes_total | ? | ? |


### Line Protocol Data ingested into Routers

| Metric name |  Code Name | Description |
| --- | --- | --- |
| ingest_points_total | ingest_lines_total | Total number of lines ingested |
| ingest_fields_total | ingest_fields_total | Total number of fields (columns) ingested |
| ingest_points_bytes_total | ingest_points_bytes_total | Total number of bytes ingested |
| ingest_entries_bytes_total |  ingest_entries_bytes_total | Total number of entry bytes ingested (Not sure what this means |

### Chunks
| Metric name |  Code Name | Description |
| --- | --- | --- |
| catalog_chunks_mem_usage_bytes | memory_metrics | Total memory usage by chunks |
| catalog_loaded_chunks | chunk_storage | Total number of chunks for each table |
| catalog_loaded_rows | row_count | Total number of rows for each table |
| catalog_lock_total | ? | ? |
| catalog_lock_wait_seconds_total | ? | ? |
| ? | partition_lock_tracker | ? |
| ? | chunk_lock_tracker | ? |
| ? | timestamp_histogram| Breakdown of timestamp distribution |
| catalog_chunks_total | chunk_state | Number of chunks in different states (open, closed, compacting, compacted, writing_os, rub_and_os, ...)) - might be removed in the future |

### Chunks and Rows Pruned by Queries
| Metric name |  Code Name | Description |
| --- | --- | --- |
| query_access_pruned_chunks_total | pruned_chunks | Number of chunks of a table pruned while running queries |
| query_access_pruned_rows_total  | pruned_rows | Number of chunks of a table pruned while running queries |

### jemalloc
| Metric name |  Code Name | Description |
| --- | --- | --- |
| jemalloc_memstats_bytes | ServerMetrics::jemalloc_domain | tracking jemalloc's active, alloc, metadata, mapped, resident, retained  |