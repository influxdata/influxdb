# Patterns for CPU usage in the Runtime

As discussed on https://github.com/influxdata/delorean/pull/221 and https://github.com/influxdata/delorean/pull/226, we needed a strategy for handling I/O as well as CPU heavy requests in a flexible manner that will allow making appropriate tradeoffs between resource utilization and availability under load.

## TLDR;

1. Use only async I/O via `tokio` -- do not use blocking I/O calls (in new code).

2. All CPU bound tasks should be scheduled on the separate application level `thread_pool` not with `tokio::task::spawn` nor `tokio::task::spawn_blocking` nor a new threadpool.

We will work, over time, to migrate the rest of the codebase to use these patterns.

## Background

At a high level there are two different kinds of work the server does:

1. Responding to external (I/O) requests (aka responding to `GET /ping`, `POST /api/v2/write`, or the various gRPC APIs).

2. CPU heavy tasks such data parsing, format conversion, or query processing

## Desired Properties / Rationale

We wish to have the following properties in the runtime:

1. Incoming requests are always processed and responded to in a "timely" manner, even while the server is load / overloaded. For example, even if the server is currently consuming 100% of the CPU doing useful work (converting data, running queries, etc), it still must be able to answer a `/ping` request to tell observers that it is still alive and not be taken out of a load balancer rotation or killed by kubernetes.

2. Ability to (eventually) control the priority of CPU heavy tasks, so we can prioritize certain tasks over others (e.g. query requests over background data rearrangement)

## Guidelines

The above desires, leads us to the following guidelines:

### Do not use blocking I/O.

**What**: All I/O should be done with `async` functions / tokio extensions (aka use `tokio::fs::File` rather than `std::fs::File`, etc.)

**Rationale**: This ensures that we do not tie up threads which are servicing I/O requests with blocking functions.

This can not always be done (e.g. with a library such as parquet writer which is not `async`).

### All CPU heavy work should be done on the single app level worker pool, separate from the tokio runtime

**What**: All CPU heavy work should be done on the single app level worker pool. We provide a `thread_pool` interface that interacts nicely with async tasks (e.g. that allows an async task to `await` for a CPU heavy task to complete).

**Rationale**: A single app level worker pool gives us a single place to control work priority, eventually, so that tasks such as compaction of large data files can have lower precedence than incoming queries. By using a different pool than the tokio runtime, with a limited number of threads, we avoid over-saturating the CPU with OS threads and thereby starving the limited number tokio I/O threads. A separate, single app level pool also limits the number of underlying OS CPU threads which are spawned, even under heavy load, keeping thread context switching overhead low.

There will, of course, always be a judgment call to be made of where "CPU bound work" starts and "work acceptable for I/O processing"  ends. A reasonable rule of thumb is if a job will *always* be completed in less than 100ms then that is probably fine for an I/O thread). This number may be revised as we tune the system.

## Examples

TODO: picture of how async + CPU heavy threads interact

The standard pattern to run CPU heavy tasks from async code is:
* Launch the cpu heavy tasks on the thread pool using `spawn_with_handle`
* `await` their completion using `join_all` or some other similar `Future` combinator.

## Alternatives Considered

###  Use tokio::task::spawn_blocking for all CPU heavy work
While this definitely avoids tying up all I/O handling threads, it can also result in a large number of threads when the system is under load and has more work coming in than it can complete.

### Use the main tokio::task::spawn for all  work
While this  likely results in the best possible efficiency, it can mean that I/O requests (such as responding to `/ping`) are not handled in a timely manner.
