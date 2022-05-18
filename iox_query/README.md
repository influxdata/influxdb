# IOx Query Layer

The IOx query layer is responsible for translating query requests from
different query languages and planning and executing them against
Chunks stored across various IOx storage systems.


Query Frontends
* SQL
* Storage gRPC
* Flux (possibly in the future)
* InfluxQL (possibly in the future)
* Others (possibly in the future)

Sources of Chunk data
* ReadBuffer
* MutableBuffer
* Parquet Files
* Others (possibly in the future, like Remote Chunk?)

The goal is to use the shared query / plan representation in order to
avoid N*M combinations of language and Chunk source.

Thus query planning is implemented in terms of traits, and those
traits are implemented by different chunk implementations.

Among other things, this means that this crate should not depend
directly on the ReadBuffer or the MutableBuffer.


```text
┌───────────────┐  ┌────────────────┐    ┌──────────────┐      ┌──────────────┐
│Mutable Buffer │  │  Read Buffer   │    │Parquet Files │  ... │Future Source │
│               │  │                │    │              │      │              │
└───────────────┘  └────────────────┘    └──────────────┘      └──────────────┘
        ▲                   ▲                    ▲                     ▲
        └───────────────────┴─────────┬──────────┴─────────────────────┘
                                      │
                                      │
                     ┌─────────────────────────────────┐
                     │          Shared Common          │
                     │   Predicate, Plans, Execution   │
                     └─────────────────────────────────┘
                                      ▲
                                      │
                                      │
               ┌──────────────────────┼─────────────────────────┐
               │                      │                         │
               │                      │                         │
               │                      │                         │
     ┌───────────────────┐  ┌──────────────────┐      ┌──────────────────┐
     │   SQL Frontend    │  │   gRPC Storage   │ ...  │ Future Frontend  │
     │                   │  │     Frontend     │      │ (e.g. InfluxQL)  │
     └───────────────────┘  └──────────────────┘      └──────────────────┘
```

We are trying to avoid ending up with something like this:


```
                          ┌─────────────────────────────────────────────────┐
                          │                                                 │
                          ▼                                                 │
                   ┌────────────┐                                           │
                   │Read Buffer │                  ┌────────────────────────┤
        ┌──────────┼────────────┼─────┬────────────┼────────────────────────┤
        │          └────────────┘     │            ▼                        │
        ▼                 ▲           │    ┌──────────────┐                 │
┌───────────────┐         │           │    │Parquet Files │                 │
│Mutable Buffer │         │           ├───▶│              │...              │
│               │◀────────┼───────────┤    └──────────────┘   ┌─────────────┼┐
└───────────────┘         │           │            ▲          │Future Source││
        ▲                 │           ├────────────┼─────────▶│             ││◀─┐
        │                 │           │            │          └─────────────┼┘  │
        │                 │           │            │                        │   │
        │                 │           │            │                        │   │
        │      ┌──────────┘           │            │                        │   │
        │      │                      │            │                        │   │
        │      ├──────────────────────┼────────────┘                        │   │
        └──────┤                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
               │                      │                                     │   │
     ┌───────────────────┐  ┌──────────────────┐      ┌──────────────────┐  │   │
     │   SQL Frontend    │  │   gRPC Storage   │ ...  │ Future Frontend  │  │   │
     │                   │  │     Frontend     │      │ (e.g. InfluxQL)  │──┴───┘
     └───────────────────┘  └──────────────────┘      └──────────────────┘
```
