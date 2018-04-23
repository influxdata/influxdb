# TSM Series Cardinality Estimation Proposal

This document describes a proposal for providing future cardinality readings of series and measurement data in a database.

The existing approach of counting distinct series and measurement data will not scale to billions of series in a database, and regardless would be complicated by implementation of the new TSI proposal (#7174).

The rest of the proposal provides more background and a proposed approach for providing scalable cardinality readings.

## Background

Currently the TSM engine maintains in-memory inverted indices, which store measurement and series data for each database.
Each database's index is shared amongst the shards for that database.
As discussed in more detail in #7151, this presents a number of problems for databases with large numbers of series.
A proposal to solve these performance problems by implementing a new memory-mapped inverted persistent index has been made in #7174.
The in-memory shared indices would be replaced with these m-mapped time-series indices.

In the current shared in-memory index, we calculate the series and measurement cardinality for a database, by counting the number of distinct series and measurements, which only involves a single index, since all series and measurement data is held within the same shared index across shards.
However, with the new proposed approach, each shard would have a separate index, and with potentially different series and measurement values in it for a given database.

Therefore, we need to come up with a solution that gets cardinality readings for a database where:

 - We have billions of series values in the database; and
 - Series and measurement values are distributed across multiple indices.

The solution must not:

 - Take significant time to calculate the series/measurement cardinality.
 - Have significant storage requirements.
 - Be computationally intensive to run, since it will need to run often.


##### Exact Counting

The number of distinct series or measurements could be counted exactly by scanning all series/measurements in all indices relating to a database.
This could be achieved using a merge-type approach, where sorted streams of series/measurements are scanned in parallel and a counter is updated as distinct values are discovered.
It would not require significant memory because only the largest value seen so far, and the count would need to be stored.

However, this solution is completely unscalable because as soon as a series/measurement has been added or removed to the database, the entire calculation would need to be done again.
Further, to calculate the value all values need to be sent across a network, if indices are located on different hosts.


##### Probabilistic Cardinality Estimation

Cardinality estimation involves determining the number distinct elements in a stream, which in practice may be either a single stream, or a combined stream from multiple sources.

If it can be assumed that a stream of values are uniformly distributed (such as if the values have been hashed with a good hash function) then some assumptions can be made about the cardinality of the set of values by maintaining a simple summary ([sketch][3]) of the values seen so far.
This is the underpinnings of how probabilistic approaches to cardinality estimation work.

The only information this approach needs is the current value in the stream and some summary data on previously seen values. Therefore these approaches:

 - Are usually parallelisable;
 - Can be pre-computed and serialised if a complete stream is available, or updated on the fly for a continuous stream;
 - Can handle multiple streams;
 - Result in small storage footprints because they only summarised information about the values seen, rather than the values themselves.

### The HyperLogLog++ algorithm

The current industry standard for probabilistic cardinality estimation is the [HyperLogLog++ algorithm][1] (an extension to the original [HyperLogLog algorithm][2]).

It has a relative accuracy (estimation error) of:

```
1.02 / sqrt(M)
```

where `M` is related to the size of the sketch used to store frequencies of certain properties of input values.
In practice, `M` is typically in the range `1536–24576`, and so the accuracy is typically in the range `2.6%–0.65%`.
A lower error rate demands more memory to accommodate a bigger sketch, but in practice the space requirements are trivial (using the above ranges would result in disk requirements of `~256B - ~4KB` for the sketch).

To build a sketch, each value is hashed and the binary encoding of the hashed value is examined, to determine where in the sketch a counter should be incremented.
The computational requirements for building a sketch are linear to the size of the input since every value needs to be examined once, however, this process can be carried out in parallel because the operations necessary to build a sketch are commutative.

Sketches only need to be built once for each index; they can then be persisted, and consulted when a cardinality estimation needs to be made.
Multiple sketches can be combined if necessary, to estimate cardinality of series/measurements across multiple indices.
Further, combining sketches is an associative operation—it doesn't matter which order they're combined it.
Different sized sketches can be combined by folding the larger one into the smaller one.
Combining sketches to generate estimations is also _lossless_: that is, the accuracy of the estimate is no different when combining multiple sketches rather than a single sketch built from all the data.


## Implementation

This section gives a broad overview of how we might implement cardinality estimation.

On system startup we would need to consult each TSI index and extract the sketches contained in each one, storing them in the associated Shard types in the database.
Since the sketches are small, this should be quick to do.
Each TSI file contains the following sketches:

 - Series sketch
 - Measurement sketch
 - Tombstoned series sketch
 - Tombstoned measurements sketch

We would also need to scan all WAL files and, for each WAL file, update the sketches associated with the index.
Any series or measurements in the WAL files would be added to the series and measurement sketches, and any tombstoned series or measurement entries would be added to the tombstoned sketches.
The end result would be four in-memory sketches for each shard.
Persistent sketches in the TSI files would only be updated during compactions.

##### Building an index sketch

When a new TSI file is created as the result of a full compaction, a new sketches will be generated for both the series and measurements within the index.
This will involve scanning and hashing with a 64-bit hash function, every series and measurement value within the index.
A suitable hash function for this purpose would be [MurmurHash3][4], which is optimised to generate 128-bit hash values on 64-bit architectures (we can use the first 64 bits for the series/measurement hash).
Each hashed value will then contribute to the appropriate sketch.
Finally, the tombstone sketches will be reinitialised as empty sketches, since there will no longer be any tombstone entries in the index.

Some prototyping and testing may be needed, but I would suggest a starting point for the precision of the HLL++ sketch to be `14`, which will require a sketch size of around `16KB`.


##### Getting cardinality estimates

Cardinality estimates can be provided in constant time by consulting the in-memory sketches.
If series or measurements are distributed across multiple indices (likely), then those sketches can be combined in linear time, with respect to the size of the sketch, which typically will be small (`~4096`).
In summary, cardinality estimations will be fast, and will not be affected by the number of series or measurements in a database.

Calculating cardinality estimates will be a three step process, for example in the case of series:

 - 1. Combine all series sketches and generate a `series_estimate`;
 - 2. Combine all tombstone series sketches and generate a `tombstone_series_estimate`;
 - 3. Return `series_estimate - tombstone_series_estimate`.

If appropriate, we could maintain in memory the combined result sketches to avoid combining them across shards for each cardinality estimate.

##### Adding new series and measurements

Since TSI indices are immutable, we should not modify an index's sketch when a new series is added to the database.
Instead, we will update in the associated shards, the in-memory sketches with the series and measurement data that gets persisted to the associated WAL file.

##### Removing series and measurements

The HyperLogLog family of algorithms unfortunately do not support removing values from the sketch, since the sketch itself is lossy and does not contain enough information to support deletions.
However, we will improve the accuracy of estimations by maintaining estimates of the number of series and measurements deleted from a database.

In the case of series removal, a tombstone entry is added to the WAL.
When this occurs we will add the tombstoned series to the tombstone series sketch for that database.

In the case of measurement removal, we will maintain a similar tombstone sketch.
Further, because removal of a measurement involves the removal of all series under that measurement, we will also need to add all those series to the tombstone series sketches.

##### Fast compactions

During a fast compaction, WAL data is merged into the associated TSI index.
We can persist the in-memory series and measurement sketches to the TSI file since they will already represent the combined cardinality estimate for the TSI file once WAL data is merged in.
We will also persist the in-memory tombstone sketches to the TSI file because once the compaction completes we will be removing the associated WAL files.


##### Full compactions

During a full compaction, tombstoned series and measurements are removed from the TSI indices.
At this point all four sketches will need to be regenerated, though the tombstone sketches only need to be re-initialised.
These new sketches would replace the current in-memory sketches.

#### TSI file format

We could extend the TSI file format to include a sketches block for series, measurement and tombstone data, which could be located after the Series Dictionary.

```
╔═══════Inverted Index═══════╗
║ ┌────────────────────────┐ ║
║ │                        │ ║
║ │   Series Dictionary    │ ║
║ │                        │ ║
║ └────────────────────────┘ ║
║ ┌────────────────────────┐ ║
║ │                        │ ║
║ │        Sketches        │ ║
║ │                        │ ║
║ └────────────────────────┘ ║
║ ┌────────Tag Set─────────┐ ║
║ │┌──────────────────────┐│ ║
║ ││   Tag Values Block   ││ ║
║ │├──────────────────────┤│ ║
║ ││   Tag Values Block   ││ ║
║ │├──────────────────────┤│ ║
║ ││    Tag Keys Block    ││ ║
║ │└──────────────────────┘│ ║
║ └────────────────────────┘ ║
║ ┌────────Tag Set─────────┐ ║
║ │┌──────────────────────┐│ ║
║ ││   Tag Values Block   ││ ║
║ │├──────────────────────┤│ ║
║ ││   Tag Values Block   ││ ║
║ │├──────────────────────┤│ ║
║ ││   Tag Values Block   ││ ║
║ │├──────────────────────┤│ ║
║ ││    Tag Keys Block    ││ ║
║ │└──────────────────────┘│ ║
║ └────────────────────────┘ ║
║  ┌──────────────────────┐  ║
║  │  Measurements Block  │  ║
║  └──────────────────────┘  ║
╚════════════════════════════╝
```

The sketch block itself would contain a value indicating the number of sketches in the block.
Each sketch block would then contain the size of the sketch data, and the sketch itself.
The sketch may be a binary blob, perhaps generated by Go's `gob` package, or it may be a custom binary format of data comprising the HLL algorithm state and sketch.
The exact format will be decided during implementation.

```
╔══════════════Sketches═══════════════╗
║                                     ║
║┌───────── Series Sketch ───────────┐║
║│┌─────────────────────────────────┐│║
║││       len(Sketch) <uint32>      ││║
║│└─────────────────────────────────┘│║
║│┌─────────────────────────────────┐│║
║││         Sketch <byte...>        ││║
║│└─────────────────────────────────┘│║
║└───────────────────────────────────┘║
║┌──────Tombstoned Series Sketch ────┐║
║│┌─────────────────────────────────┐│║
║││      len(Sketch) <uint32>       ││║
║│└─────────────────────────────────┘│║
║│┌─────────────────────────────────┐│║
║││        Sketch <byte...>         ││║
║│└─────────────────────────────────┘│║
║└───────────────────────────────────┘║
║┌─────── Measurements Sketch ───────┐║
║│┌─────────────────────────────────┐│║
║││       len(Sketch) <uint32>      ││║
║│└─────────────────────────────────┘│║
║│┌─────────────────────────────────┐│║
║││         Sketch <byte...>        ││║
║│└─────────────────────────────────┘│║
║└───────────────────────────────────┘║
║┌──Tombstoned Measurements Sketch ──┐║
║│┌─────────────────────────────────┐│║
║││      len(Sketch) <uint32>       ││║
║│└─────────────────────────────────┘│║
║│┌─────────────────────────────────┐│║
║││        Sketch <byte...>         ││║
║│└─────────────────────────────────┘│║
║└───────────────────────────────────┘║
╚═════════════════════════════════════╝
```

<!-- References -->

 [1]: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf
 [2]: http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 [3]: https://en.wikipedia.org/wiki/Streaming_algorithm
 [4]: https://sites.google.com/site/murmurhash/
