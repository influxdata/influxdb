# IOx Data Organization and Lifecycle



## Data Organization
Figure 1 illustrates an `IOx Server` which is a columnar database management system (DBMS). An IOx Server includes many `databases`, each represents an isolated dataset from an organization or user. The IOx Server in Figure 1 consists of `p` databases. Each database has as many `tables` as needed. Data of each table is partitioned on a specified partition key which is an expression of the table column(s). In the example of Figure 1, `Table 1` is partitioned by date which is an expression on a time column of `Table 1`. `Partition` data is physically split into many chunks depending on the table's flow of ingested data which will be described in the next section, Data Life Cycle. Each chunk contains a subset of rows of its table partition on a subset of columns of the table. For example, `Chunk 1` has 2 rows of data on columns `col1`, `col2`, and `col3` while `Chunk 2` includes 3 rows on `col1` and `col4`. Since every chunk can consist of data of the same or different columns, a chunk has it own `schema` defined with it. `Chunk 1`'s schema is {`col1`, `col2`, `col3`} (and their corresponding data types) and `Chunk 2`'s schema is {`col1`, `col4`}. Columns with the same name, such as `col1`, represent the same column and must have the same data type across all chunks within the table. 

```text  
                                                                 ┌───────────┐                                    
                                                                 │IOx Server │                        IOx Server  
                                                                 └───────────┘                                    
                                                                       │                                          
                                                        ┌──────────────┼────────────────┐                         
                                                        ▼              ▼                ▼                         
                                              ┌───────────┐                     ┌────────────┐                    
                                              │Database 1 │           ...       │ Database p │        Databases   
                                              └───────────┘                     └────────────┘                    
                                                    │                                                             
                                     ┌──────────────┼─────────────┐                                               
                                     ▼              ▼             ▼                                               
                               ┌──────────┐                 ┌──────────┐                                          
                               │ Table 1  │       ...       │ Table n  │                                Tables    
                               └──────────┘                 └──────────┘                                          
                                     │                            │                                               
                      ┌──────────────┼──────────────┐             │                                               
                      ▼              ▼              ▼             ▼                                               
               ┌────────────┐                ┌────────────┐                                                       
               │Partition 1 │       ...      │Partition m │      ...                                 Partitions   
               │(2021-12-10)│                │(2021-12-20)│                                                       
               └────────────┘                └──────┬─────┘                                                       
                      │                             │                                                             
        ┌─────────────┼─────────────┐               │                                                             
        ▼             ▼             ▼               ▼                                                             
┌──────────────┐              ┌───────────┐                                                                       
│    Chunk 1   │     ...      │  Chunk 2  │        ...                                                 Chunks     
│              │              │           │                                                                       
│col1 col2 col3│              │ col1 col4 │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
└──────────────┘              │ ---- ---- │                                                                       
                              └───────────┘                                                                       
 
Figure 1: Data organization in an IOx Server
```

Chunk is considered the smallest block of data in IOx and the central discussion of the rest of this document. IOx does not (yet) support direct data modification but does allow deletion[^del] which means a modification can be done through a deletion and an ingestion. Another way to modify values of non-primary-key columns in IOx is to reload data of that row using same key values but different non-key ones. These duplicated rows will be deduplicated during compaction (see next section) and/or eliminated at Query time.[^dup]
[^del]: `Deletion` is a large topic that deserves its own document.
[^dup]: The detail of `duplication` and `deduplication` during compaction and query are parts of a large topic that deserve another document.

### Chunk Types
A `Chunk` in IOx is an abstract object defined in the code as a [DbChunk](https://github.com/influxdata/influxdb_iox/blob/12c40b0f0f93e94e483015f9104639a1f766d594/server/src/db/chunk.rs#L78). To optimize the Data LifeCycle and Query Performance, IOx implements these types of physical chunks for a DbChunk: O-MUB, F-MUB, RUB, OS.

1. O-MUB: **O**pen **MU**table **B**uffer chunk is optimized for writes and the only chunk type that accepts ingesting data. O-MUB is an in-memory chunk but its data is not sorted and not heavily compressed.[^type]
1. F-MUB: **F**rozen **MU**table **B**uffer chunk has the same format as O-MUB (in memory, not sorted, not encoded) but it no longer accepts writes. It is used as a transition chunk while its data is being moved from optimized-for-writes to optimized-for-reads.
1. RUB: **R**ead **B**uffer chunk is optimized for reads and does not accept writes. RUB is kept in memory and its data is sorted and encoded on the chunk's primary key. Note that since a chunk may store data of a subset of its table columns, Chunk's primary key can also be a subset of its table's primary key.
1. OS: **O**bject **S**tore chunk is a parquet file of a chunk stored in a durable cloud storage such as Amazon S3 (IOx also supports Azure and Google Clouds). Because an OS is always created from a RUB, it inherits all sorting and encoding properties of the corresponding RUB and Parquet.

[^type]: The detailed format of each chunk type is beyond the scope of this document

Depending on which stage of the lifecycle a chunk is in, it will be represented by one or a few physical chunk types above.

### Stages of a Chunk

Before digging into Data Lifecycle, let us look into the stages of a chunk implemented as [ChunkStage](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/server/src/db/catalog/chunk.rs#L130). A chunk goes through three stages demonstrated in Figure 2: `Open`, `Frozen`, and `Persisted`. 
* When data is ingested into IOx, it will be written into an open chunk which is an `O-MUB`.
* When triggered by some manual or automatic action of the lifecycle (described in next section), the open chunk will be frozen, first to `F-MUB` then transitioned to `RUB`.
* When the `RUB` is persisted to an `OS` chunk, its stage will be moved to persisted. Unlike the `Open` and `Frozen` stages that are represented by only one type of chunk at a moment in time, the `Persisted` stage can be represented by two chunk types at a time: `RUB` and `OS` that store the same data for the purpose of query performance. When a query needs to read data of a persisted chunk stage, it will first look for `RUB`, but, if not available, will look for `OS`. `RUB` will be unloaded from the persisted stage if IOx memory runs low, and reloaded if data of that chunk is queried a lot and IOx memory is underused.

```text
                                                       ┌───────────────────┐
                                                       │     Persisted     │
┌───────────┐       ┌──────────────────────────┐       │                   │
│   Open    │       │          Frozen          │       │ ┌───────────────┐ │
│           │       │                          │       │ │RUB (optional) │ │
│ ┌───────┐ │──────▶│┌───────┐        ┌───────┐│──────▶│ └───────────────┘ │
│ │ O-MUB │ │       ││ F-MUB ├───────▶│  RUB  ││       │ ┌───────────────┐ │
│ └───────┘ │       │└───────┘        └───────┘│       │ │      OS       │ │
└───────────┘       └──────────────────────────┘       │ └───────────────┘ │
                                                       └───────────────────┘

Figure 2: Stages of a C hunk
```

Now let us see how data of chunks are transformed in IOx's Data LifeCycle.

## Data Life Cycle

The IOx Data LifeCycle is the set of processes that transform chunks from one stage to another. These processes are triggered by some actions such as `compact`, `delete`, `persist`, `compact OS`, `load RUB`, and `cache OS` . These actions are invoked by either users manually or automatically by a `Data LifeCycle Policy`. In this document, we first define the actions listed above and the go over examples of those actions in Figure 3 to have basic understanding of IOx Data Life Cycle. The `Data LifeCycle Policy` is an advanced topic that will be discussed in a future document, it aims to maintain a smooth, uninterrupted flow of data from ingest to their final durable storage, all whilst keeping query performance high.

### Data LifeCycle Actions
To trigger chunks moving from one stage to another, IOx creates some actions. Since IOx keeps chunks inside their partition as defined in the previous Section, all chunks described in this section are in the context of one partition. By designed, there is at most one `O-MUB` at a time.
* **Compact**: is an action to compact eligible `O-MUB`, `F-MUBs`, and `RUBs` into one `RUB`. The eligibility is defined either automatically in the `Data LifeCycle Policy` which is outside the scope of this document or manually specified by users as parameters through that action's gRPC API.
* **Delete**: is an action issued by users (through gRPC API) to delete some data. When a delete is issued, all affected chunks are frozen and its delete predicates are attached to the chunks. So in this Data LifeCycle context, only O-MUBs are affected directly by delete actions.
* **Persist**: is an action to compact eligible `O-MUB`, `F-MUBs`, and `RUBs` into 2 chunks, an `OS` of time window t1-t2 and a `RUB` of time window t2-t3, in which t1 and t3 are the start and end of those chunks' data time respectively, and t2 is calculated based on the `Data LifeCycle Policy`. This chunk's data time is represented by the chunk's column named `time` which is a timestamp column either ingested explicitly with the chunk data or IOx will implicitly add it using the time when the data is ingested.
* **Compact OS**: is an action to compact specified OS chunks into one OS chunk.
* **Load RUB**: is an action to load OS chunks without RUB back in in-memory RUBs.

The `Compact`, `Persist` and `Compact OS` can only compact/persist contiguous chunks.  Contiguous chunks contain time-windows of *insert* time (not data timestamp) next to each other. They also eliminate permanently duplicated and deleted data. When a new chunk is created, the compacted/persisted ones will be dropped in the same transaction.


### Data LifeCycle Example
Figure 3 illustrates an example of IOx Data LifeCycle with 8 actions happening at time T1 to time T8. The number (1, 2, 3, ...) at the end of each chunk name is just for us to identify them easily. They are not the real UUID identifiers of the chunks.

* **Before T1**: There is only `O-MUB 1` in the system to accept eligible writes for its partition.
* **At T1**: a `Compact` action happens and triggers a process to compact the only `O-MUB 1` into `RUB 1`. In order to do that, `O-MUB 1` needs to get frozen to `F-MUB 1` first and then compacted into `RUB 1`. If more ingest data comes after T2, a new `O-MUB 2` is created to accept new writes.
* **At T2**: a `Delete` is issued that freezes `O-MUB 2` to `F-MUB 2`. Since new data keeps coming, `O-MUB 3` is created to accept new data.
* **At T3**: a `Compact` is requested on `RUB 1`, `F-MUB 2`, and `O-MUB 3`. As always, the `O-MUB 3` must be first frozen to `F-MUB 3` and then compacted with `RUB 1` and `F-MUB 2` to produce `RUB 2`, while `O-MUB 4` is created to accept ingesting data.
* **At T4**: a `Delete` triggers `O-MUB 4` frozen to `F-MUB 4` and `O-MUB 5` created.
* **At T5**: a `Compact` on `F-MUB 4` and `O-MUB 5` to produce `RUB 3` and triggers the creation of `O-MUB 6`.
* **At T6**: a `Persist` is issued on `RUB 2`, `RUB 3` and `O-MUB 6` to produce `OS 1` and `RUB 4`, and triggers the creation of `O-MUB 7`.
* **At T7**: a `Persist` is issued on `RUB 4` and `O-MUB 7` to produce `OS 2` and `RUB 5`, and triggers the creation of `O-MUB 8`.
* **AT T8**: a `Compact OS` is issued on `OS 1` and `OS 2` to produce `OS 3`.

```text
                                                                                                                                                                                           
                                                                                                                                                                                             
  Time                                                                                                                                                                                     
                                                                                                                                                                                           
  ─────────────────────────  T1  ──────────  T3  ────────────────  T3  ─────  T4  ────────────────  T5  ───────────────  T6  ────────────────────────  T7  ─────────────  T8  ────────▶    
                                                                                                                                                                                           
                                                                                                                                                                                           
                                                                                                                                                                                           
     Ingest    ┌───────┐    T1:   ┌───────┐ Continue ┌─────┐                                                                                                                               
  ────────────▶│O-MUB 1│─────────▶│F-MUB 1│─────────▶│RUB 1│──▶│                                                                                                                           
   before T1   └───────┘  Compact └───────┘compacting└─────┘   │                                                                                                                           
                                                               │                                                                                                                           
                                                               │                                                                                                                           
                                                               │                                                                                                                           
                                                               │                                                                                                                           
                      Ingest      ┌───────┐  T2:    ┌───────┐  │   T3:     ┌─────┐                                                                                                         
                 ────────────────▶│O-MUB 2│────────▶│F-MUB 2│──┼──────────▶│RUB 2│──────────────────────────────────▶│                                                                     
                between T1 and T2 └───────┘ Delete  └───────┘  │ Compact   └─────┘                                   │                                                                     
                                                               │      ▲                                              │                                                                     
                                                               │      │                                              │                                                                     
                                                               │      │                                              │                                                                     
                                       Ingest       ┌───────┐  │  ┌───────┐                                          │                                                                     
                                 ──────────────────▶│O-MUB 3│──┴─▶│F-MUB 3│                                          │                                                                     
                                 between T2 and T3  └───────┘     └───────┘                                          │                                                                     
                                                                                                                     │                                                                     
                                                                                                                     │                                                                     
                                                                                                                     │                 ┌─────┐                                             
                                                     Ingest       ┌───────┐  T4:    ┌───────┐                        │             ┌──▶│OS 1 │─────────────────────────▶│                  
                                               ──────────────────▶│O-MUB 4│────────▶│F-MUB 4│───▶│                   │             │   └─────┘                          │                  
                                               between T3 and T4  └───────┘ Delete  └───────┘    │   T5:    ┌─────┐  │    T6:      │                                    │                  
                                                                                                 ├─────────▶│RUB 3│──┼────────────▶│                                    │   T8:     ┌─────┐
                                                                                                 │ Compact  └─────┘  │  Persist    │                                    ├──────────▶│OS 3 │
                                                                                                 │    ▲              │      ▲      │   ┌─────┐                          │Compact OS └─────┘
                                                                                                 │    │              │      │      └──▶│RUB 4│───▶                      │                  
                                                                       Ingest       ┌───────┐    │┌───────┐          │      │          └─────┘   │              ┌─────┐ │                  
                                                                 ──────────────────▶│O-MUB 5│────▶│F-MUB 5│          │      │                    │           ┌─▶│OS 2 ├─▶                  
                                                                 between T4 and T5  └───────┘     └───────┘          │      │                    │           │  └─────┘                    
                                                                                                                     │      │                    │    T7:    │                             
                                                                                                                     │      │                    ├──────────▶│                             
                                                                                                                     │      │                    │  Persist  │                             
                                                                                     Ingest       ┌───────┐          │  ┌───────┐                │     ▲     │  ┌─────┐                    
                                                                               ──────────────────▶│O-MUB 6│──────────┴─▶│F-MUB 6│                │     │     └─▶│RUB 5│                    
                                                                               between T5 and T6  └───────┘             └───────┘                │     │        └─────┘                    
                                                                                                                                                 │     │                                   
                                                                                                                                                 │     │                                   
                                                                                                           Ingest       ┌───────┐                │ ┌───────┐                               
                                                                                                     ──────────────────▶│O-MUB 7│────────────────┴▶│F-MUB 7│                               
                                                                                                     between T6 and T7  └───────┘                  └───────┘                               
                                                                                                                                                                                           
                                                                                                                                      Ingest       ┌───────┐                               
                                                                                                                                ──────────────────▶│O-MUB 8│                               
                                                                                                                                     after T7      └───────┘                               
                                                                                                                                                                                           
Figure 3: Example of                                                                                                                                                                                                   
```

Now we understand chunks and their life cycle, let us move to next topic: [IOx Catalog: The Metadata for Operating a Database](catalogs.md)