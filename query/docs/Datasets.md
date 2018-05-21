# Datasets

Datasets are the container for data between transformations of a query.
Datasets and transformations come in pairs.
A transformations receives data for processing and writes its results to its downstream dataset.
A dataset decides when data should be materialized and passed down to the next transformation dataset pair.

A dataset is made up of blocks.
A block is a time bounded set of data for a given grouping key.
Blocks are modeled as matrixes where rows labels are series keys and columns labels are timestamps.

The query engine is capable of processing data out of order and still providing complete and correct results.
This is accomplished through a mechanism of watermarks and triggers.
A watermark gives an indication that no data older than the mark is likely to arrive.
A trigger defines when a block of data should be materialized for processing.
Then if late data does arrive the block can be updated and rematerialized.
This mechanism allows for a trade off between three aspects of stream processing.

* Completeness - Did the query process all of the data?
* Latency - How quickly is a result produced once data is received?
* Cost - How much compute resources are used to process the pipeline?

Datasets cache active blocks and materialize them when ever they are triggered and remove then once they are finished.


## Resilience 

The plan is to implement datasets as resilient data stores like Spark's RDD, so that if a given dataset is lost, a replacement can be rebuilt quickly.

## Performance

The Dataset and Block interfaces are designed to allow different implementations to make various performance trade offs.

### Batching

Blocks represents time and group bounded data.
It is possible that data for a single block is too large to maintain in RAM.
Bounds on data indicate how aggregate transformations, etc. should behave.
Batching the data so that it can be processed with available resources is an orthogonal issue to the bounds of the data.
As such is not part of the Dataset or Block interfaces and is left up to the implementation of the interfaces as needed.

### Sparse vs. Dense

There will be three different implementations of the Block interface.

* Dense
* Sparse Row Optimized
* Sparse Column Optimized

A dense matrix implementation assumes that there is little to no missing data.
A dense matrix is typically "row-major" meaning its optimized for row based operations, at this point it doesn't seem helpful to have a column major dense implementation.
A sparse matrix implementation assumes that there is a significant amount of missing data.
Sparse implementations can be optimized for either row or column operations.

Since different processes access data in different patterns the planning step will be responsible for deciding which implementation is best at which steps in a query.
The planner will add transformations procedures for conversions between the different implementations.

