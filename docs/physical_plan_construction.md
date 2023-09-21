# IOx Physical Plan Construction
This document describes how DataFusion physical plans should be constructed in IOx. As a reminder: Our logical plans
contain a `ChunkTableProvider` (implements [`TableProvider`]) that contains a set of `QueryChunk`s. The main entry point
for DataFusion is [`TableProvider::scan`] which receives the following information:

- context (unused)
- projection (optional, otherwise retrieve all column)
- filter expression
- limit (unused)

We want to design a system that is:

- correct (esp. it handles deduplication)
- efficient (esp. it should support [`TableProvider::supports_filter_pushdown`] = [`TableProviderFilterPushDown::Exact`])
- scalable (esp. it should avoid large fan-outs)
- easy to understand and extend
- work hand-in-hand w/ DataFusion's optimizer framework

We use the physical plan to fullfill the requirements instead of the logical planning system, because:

- it allows the querier to create a logical plan w/o contacting the ingesters (helpful for debugging and UX)
- currently (2023-02-14) the physical plan in DataFusion seems to be way more customizable than the logical plan
- it expresses the "what? scan a table" (logical) vs "how? perform dedup on this data" (physical) better

The overall strategy is the following:

1. **Initial Plan:** Construct a semantically correct, naive physical plan.
2. **IOx Optimizer Passes:** Apply IOx optimizer passes (in the order in which they occur in this document)].
3. **DataFusion Optimizer Passes:** Apply DataFusion's optimizer passes. This will esp. add all required sorts:
   - before `DeduplicateExec`
   - if output sorting is required (e.g. for SQL queries a la `SELECT ... FROM ... ORDER BY ...`)
   - if any group-by expression within the DataFusion plan can use sorting

This document uses [YAML] to illustrate the plans or plan transformations (in which case two plans are shown). Only the
relevant parameters are shown.

We assume that `QueryChunk`s can be transformed into `RecordBatchesExec`/[`ParquetExec`] and that we can always recover
the original `QueryChunk`s from these nodes. We use `ChunkExec` as a placeholder for `RecordBatchesExec`/[`ParquetExec`]
if the concrete node type is irrelevant.

## Initial Plan
The initial plan should be correct under [`TableProvider::supports_filter_pushdown`] =
[`TableProviderFilterPushDown::Exact`]. Hence it must capture the projection and filter parameters.

```yaml
---
ProjectionExec:  # optional
  FilterExec:
    DeduplicateExec:
      UnionExec:
        - RecordBatchesExec
        - ParquetExec:
            store: A
        # if there are multiple stores (unlikely)
        - ParquetExec:
            store: B
```

The created [`ParquetExec`] does NOT contain any predicates or projections. The files may be grouped into
[`target_partitions`] partitions.

## Union Handling
There are some essential transformations around [`UnionExec`] that always apply. They may be used at any at point (either
as an extra optimizer rule or built into some of the other rules). They are mentioned here once so the remaining
transformations are easier to follow.

### Union Un-nesting
```yaml
---
UnionExec:
  - UnionExec:
    - SomeExec1
    - SomeExec2
  - SomeExec3

---
UnionExec:
  - SomeExec1
  - SomeExec2
  - SomeExec3
```

### 1-Unions
```yaml
---
UnionExec:
 - SomeExec1

---
SomeExec1
```

## Empty Chunk Nodes
`RecordBatchesExec` w/o any `RecordBatch`es and [`ParquetExec`] w/o any files may just be removed from the plan if the
parent node is a [`UnionExec`].

## Deduplication Scope
Deduplication must only be performed if there are duplicate tuples (based on their primary key). This is the case if
either duplicates may occur within a chunk (e.g. freshly after ingest) or if the key space of chunks overlap. Since
deduplication potentially requires sorting and is a costly operation in itself, we may want to avoid it as good as
possible and also limit the scope (i.e. set of tuples) on which a deduplication acts on.

### Partition Split
```yaml
---
DeduplicateExec:
  UnionExec:  # optional, may only contain a single child node
    - ChunkExec:
        partition: A
    - ChunkExec:
        partition: B
    - ChunkExec:
        partition: A

---
Union:
  - DeduplicateExec:
      UnionExec:
        - ChunkExec:
            partition: A
        - ChunkExec:
            partition: A
  - DeduplicateExec:
      UnionExec:
        - ChunkExec:
            partition: B
```

### Time Split
From the `QueryChunk` statistics we always know the time ranges of a chunk. If chunks do NOT overlap in these ranges,
they also do NOT overlap in their key space. Hence we can use the time range to split `DeduplicateExec` nodes.

```yaml
---
DeduplicateExec:
  UnionExec:  # optional, may only contain a single child node
    - ChunkExec:
        ts_min: 1
        ts_max: 10
    - ChunkExec:
        ts_min: 2
        ts_max: 5
    - ChunkExec:
        ts_min: 5
        ts_max: 5
    - ChunkExec:
        ts_min: 8
        ts_max: 9
    - ChunkExec:
        ts_min: 11
        ts_max: 15
    - ChunkExec:
        ts_min: 16
        ts_max: 17
    - ChunkExec:
        ts_min: 17
        ts_max: 18

---
Union:
  - DeduplicateExec:
      UnionExec:
        - ChunkExec:
            ts_min: 1
            ts_max: 10
        - ChunkExec:
            ts_min: 2
            ts_max: 5
        - ChunkExec:
            ts_min: 5
            ts_max: 5
        - ChunkExec:
            ts_min: 8
            ts_max: 9
  - DeduplicateExec:
      UnionExec:
        - ChunkExec:
            ts_min: 11
            ts_max: 15
  - DeduplicateExec:
      UnionExec:
        - ChunkExec:
            ts_min: 16
            ts_max: 17
        - ChunkExec:
            ts_min: 17
            ts_max: 18
```

### No Duplicates
If a `DeduplicateExec` has a single child node and that node does NOT contain any duplicates (based on the primary key),
then we can remove the `DeduplicateExec`:

```yaml
---
DeduplicateExec:
  ChunkExec
    may_contain_pk_duplicates: false

---
ChunkExec
  may_contain_pk_duplicates: false
```

## Node Grouping
After the deduplication handling, chunks may or may not be contained in singular exec nodes. These transformations try
to reorganize them in a way that query execution is efficient.

### Type Grouping
`RecordBatchesExec`s can be grouped into a single node. [`ParquetExec`]s can be grouped by object store (this is a single)
store in most cases:

```yaml
---
UnionExec:
  - RecordBatchesExec:
      chunks: [C1, C2]
  - ParquetExec:
      chunks: [C4]
      store: A
  - ParquetExec:
      chunks: [C5]
      store: B
  - RecordBatchesExec
      chunks: [C6]
  - ParquetExec:
      chunks: [C7]
      store: A

---
UnionExec:
  - RecordBatchesExec:
      chunks: [C1, C2, C6]
  - ParquetExec:
      chunks: [C4, C7]
      store: A
  - ParquetExec:
      chunks: [C5]
      store: B
```

### Sort Grouping
Since DataFusion will insert the necessary [`SortExec`]s for us, it is important that we are able to tell it about already
sorted data. This only concerns [`ParquetExec`], since `RecordBatchesExec` are based on not-yet-sorted ingester data.
[`ParquetExec`] is able to express its sorting per partition, so we are NOT required a [`ParquetExec`] per existing sorting.
We just need to make sure that files/chunks with the same sorting end up in the same partition and that we make sure
that DataFusion knows about it ([`FileScanConfig::output_ordering`]).

This somewhat interferes with the [`target_partitions`] setting. We shall find a good balance between avoiding resorts and
"too wide" fan-outs.

## Predicate Pushdown
We may push down filters closer to the source under certain circumstances.

### Predicates & Unions
[`FilterExec`] can always be pushed through [`UnionExec`], since [`FilterExec`] only allows row-based operations:

```yaml
---
FilterExec:
  UnionExec:
    - SomeExec1
    - SomeExec2

---
UnionExec:
  - FilterExec:
      SomeExec1
  - FilterExec:
      SomeExec2
```

### Predicates & Projections
With the current "Initial Plan" and rule ordering, it should not be required to push predicates through projections. The
opposite however is the case, see "Projections & Predicates".

Note that we could add a transformation implementing this if we ever require it.

### Predicates & Dedup
[`FilterExec`]s contain a [`PhysicalExpr`]. If this is a AND-chain / logical conjunction, we can split it into
sub-expressions (otherwise we treat the whole expression as a single sub-expression). For each sub-expression, we can
tell which columns it uses. If it refers to only primary-key columns (i.e. no references to fields), we can push it through
`DeduplicateExec`:

```yaml
---
FilterExec:
  expr: (field = 1) AND (field=2 OR tag=3) AND (tag > 0)
  child:
    DeduplicateExec:
      SomeExec

---
FilterExec:
  expr: (field = 1) AND (field=2 OR tag=3)
  child:
    DeduplicateExec:
      FilterExec:
        expr: tag > 0
        child:
          SomeExec
```

Note that empty filters are removed during this process:

```yaml
---
FilterExec:
  expr: tag > 0
  child:
    DeduplicateExec:
      SomeExec

---
DeduplicateExec:
  FilterExec:
    expr: tag > 0
    child:
      SomeExec
```

### Predicates & Parquet
Predicates can be pushed down into [`ParquetExec`] and are partially evaluated there (depending on various other configs
and the complexity of the filter). Note that the [`ParquetExec`] itself decides when/how to evaluate predicates, so we are
note required to perform any predicate manipulation here:

```yaml
---
FilterExec:
  expr: (tag1 > 0) AND (some_fun(tag2) = 2)
  child:
    DeduplicateExec:
      ParquetExec:
        files: ...

---
FilterExec:
  expr: (tag1 > 0) AND (some_fun(tag2) = 2)
  child:
    DeduplicateExec:
      ParquetExec:
        predicate: (tag1 > 0) AND (some_fun(tag2) = 2)
        files: ...
```

### Predicates & Record Batches
`RecordBatchesExec` does not have any filter mechanism built in and hence relies on [`FilterExec`] to evaluate predicates.
We therefore do NOT push down any predicates into `RecordBatchesExec`.

## Projection Pushdown
This concerns the pushdown of columns selections only. Note that [`ProjectionExec`] may contain renaming columns or even
the calculation of new ones; these are NOT part of this rule and are never generated by the "Initial Plan".

### Projections & Unions
Projections can always be pushed through union operations since they are only column-based and all union inputs are
required to have the same schema:

```yaml
---
ProjectionExec:
  keep: tag1, field2, time
  child:
    UnionExec:
      - SomeExec1
      - SomeExec2

---
UnionExec:
  - ProjectionExec:
    keep: tag1, field2, time
    child:
      SomeExec1
  - ProjectionExec:
    keep: tag1, field2, time
    child:
      SomeExec2
```

### Projections & Predicates
Projections may be pushed through [`FilterExec`] if they keep all columns required to evaluate the filter expression:

```yaml
---
ProjectionExec:
  keep: tag1, field2, time
  child:
    FilterExec:
      predicate: field3 > 0
      child:
        SomeExec

---
ProjectionExec:
  keep: tag1, field2, time
  child:
    FilterExec:
      predicate: field3 > 0
      child:
        ProjectionExec:
          keep: tag1, field2, field3, time
          child:
            SomeExec
```

### Projections & Dedup
Projections that do NOT remove primary keys can be pushed through the deduplication. This is also compatible with the
[`SortExec`]s added by DataFusion, since these will only act on the primary keys:

```yaml
---
ProjectionExec:
  keep: tag1, field2, time
  child:
    DeduplicateExec:
      # We assume a primary key of [tag1, tag2, time] here,
      # but `SomeExec` may have more fields (e.g. [field1, field2]).
      SomeExec

---
ProjectionExec:
  keep: tag1, field2, time
  child:
    DeduplicateExec:
      ProjectionExec:
        keep: tag1, tag2, field2, time
        child:
        SomeExec
```

### Projections & Parquet
[`ParquetExec`] can be instructed to only deserialize required columns via [`FileScanConfig::projection`]. Note that we
shall not modify [`FileScanConfig::file_schema`] because we MUST NOT remove columns that are used for pushdown predicates.

```yaml
---
ProjectionExec:
  keep: tag1, field2, time
  child:
    ParquetExec:
      predicate: field1 > 0
      projection: null

---
ParquetExec:
  predicate: field1 > 0
  projection: tag1, field2, time
```

### Projections & Record Batches
While `RecordBatchesExec` does not implement any predicate evaluation, it implements projection (column selection). The
reason is that it creates NULL-columns for batches that do not contain the required output columns. Hence it is valuable
to push down projections into `RecordBatchesExec` so we can avoid creating columns that we would throw away anyways:

```yaml
---
ProjectionExec:
  keep: tag1, field2, time
  child:
    RecordBatchesExec:
      schema: tag1, tag2, field1, field2, time

---
RecordBatchesExec:
  schema: tag1, field2, time
```


[`FileScanConfig::file_schema`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/file_format/struct.FileScanConfig.html#structfield.file_schema
[`FileScanConfig::output_ordering`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/file_format/struct.FileScanConfig.html#structfield.output_ordering
[`FileScanConfig::projection`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/file_format/struct.FileScanConfig.html#structfield.projection
[`FilterExec`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/filter/struct.FilterExec.html
[`ParquetExec`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/file_format/struct.ParquetExec.html
[`PhysicalExpr`]:https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/trait.PhysicalExpr.html
[`ProjectionExec`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/projection/struct.ProjectionExec.html
[`SortExec`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/sorts/sort/struct.SortExec.html
[`TableProvider`]: https://docs.rs/datafusion/18.0.0/datafusion/datasource/datasource/trait.TableProvider.html
[`TableProvider::scan`]: https://docs.rs/datafusion/18.0.0/datafusion/datasource/datasource/trait.TableProvider.html#tymethod.scan
[`TableProvider::supports_filter_pushdown`]: https://docs.rs/datafusion/18.0.0/datafusion/datasource/datasource/trait.TableProvider.html#method.supports_filter_pushdown
[`TableProviderFilterPushDown::Exact`]: https://docs.rs/datafusion/18.0.0/datafusion/datasource/datasource/enum.TableProviderFilterPushDown.html#variant.Exact
[`target_partitions`]: https://docs.rs/datafusion/18.0.0/datafusion/config/struct.ExecutionOptions.html#structfield.target_partitions
[`UnionExec`]: https://docs.rs/datafusion/18.0.0/datafusion/physical_plan/union/struct.UnionExec.html
[YAML]: https://yaml.org/
