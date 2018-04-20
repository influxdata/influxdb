# Overview

This document describes the intelligent rollup and querying of
aggregated data problem and requirements needed for a solution to the
problem.

## Problem Statement

When a user scales between different time ranges, it should be able to
automatically switch between different precision levels that are
precomputed to match the time range, the desired number of points,
and/or other potential factors that a user may want to provide that can
aid in finding an appropriate precision level. Using these rollups
should be automatic and performant.

## Requirements

### Functional

1. Must be capable of persisting the aggregate data for long term
   storage.
2. Must be capable of automatically updating the persisted data whenever
   the underlying shard is updated.
3. An automatic switch to querying an alternate retention policy must
   always return correct data.
4. Creating a rollup must be possible for an entire database and
   matching fields and measurements by regexes or wildcards so that a
   new rollup doesn't need to be created for each measurement/field
   combination.
5. Must be possible to list which aggregates are linked to a retention
   policy and which retention policy they are stored in. This way, a UI
   would be able to identify which aggregates have been setup so a user
   can get the most efficient groupings.
6. Data written to an old shard should be resampled into the appropriate
   retention policy before the data is reaped.

### Performance

1. Minimal to no impact on performance for any aggregated data.

### Reliability

## Use Cases

1. Aggregating data to a new retention policy for long term storage.
2. Calculating common aggregates to increase speed.
3. Easier usability when using retention policies.
4. Asking for a desired time range and/or number of points and
   automatically scaling the graph to the appropriate retention policy.

## Additional Considerations

1. Should writing to an old shard be reflected immediately for new
   queries? This is desirable so that we don't accidentally hide
   information written to an old shard, but it might be too hard as a
   requirement. If not immediately, when should the change to the shard
   data be reflected? Should we avoid using the aggregated data while
   it is out of date or continue to use it either way?
