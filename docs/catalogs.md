# IOx Catalogs:  The Metadata for Operating a Database

To continue our architecture documentation series, this document explains how IOx uses **metadata** to organize its physical chunks and operate its Data Lifecycle described in previous document, [IOx Data Organization and LifeCycle](link). We also shows how metadata is incrementally saved and used to rebuild the database, as well as used to find the right physical chunks quickly to run queries.

Figure 1 extracted from previous [document](link) illustrates IOx Data organization. Since actual data is only stored in the bottom layer, `Chunks`, the other information in other layers that is needed to access and manage the chunks are considered as `metadata` and, in IOx, managed by Catalogs.

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
Figure 1: Data Organization in an IOx Server

```


Each database of IOx owns three catalogs demonstrated in Figure 2. The `In-Memory Catalog` (or simply a `Catalog`), contains metadata to access all chunk types in both memory and Object Store described in [IOx Data Organization and LifeCycle](link). The `Preserved Catalog` is used to rebuild the database in case it is shutdown or destroyed by unexpected accidents. The `Query Catalog` includes extra information to access the right chunks and their statistics quickly for running queries, recording necessary log info, and reporting the results back to users. The `Other Properties` are a set of properties created/rebuilt with the database DB based on some default configuration parameters of the IOx Server or specified by the user who creates the database.

```text
                                     ┌───────────┐                   
                                     │IOx Server │                   
                                     └───────────┘                   
                                           │                         
                            ┌──────────────┼───────────────┐         
                            ▼              ▼               ▼         
                      ┌───────────┐                 ┌────────────┐   
                      │Database 1 │       ...       │ Database p │   
                      └───────────┘                 └────────────┘   
                            │                              │         
      ┌─────────────┬───────┴─────┬────────────┐           │         
      ▼             ▼             ▼            ▼           ▼         
┌──────────┐  ┌──────────┐  ┌──────────┐ ┌──────────┐                
│In-Memory │  │Preserved │  │  Query   │ │  Other   │      ...       
│ Catalog  │  │ Catalog  │  │ Catalog  │ │Properties│                
└──────────┘  └──────────┘  └──────────┘ └──────────┘                

Figure 2: Metadata of a Database
```
## In-memory Catalog

`In-Memory Catalog` shown in Figure 3 represents the structure of its actual `Data Organization` and hence looks similar to Figure 1

```text
                                                                                  ┌──────────────────┐                           
                                                                                  │In-Memory Catalog │                           
                                                                                  │----------------- │                           
                                                                                  │                  │                           
                                                                                  │. DB_name         │                           
                                                                                  │. CatalogMetrics  │                           
                                                                                  └──────────────────┘                           
                                                                                            │                                    
                                                                    ┌───────────────────────┼──────────────────────────┐         
                                                                    ▼                       │                          ▼         
                                                             ┌───────────────┐              ▼                ┌──────────────────┐
                                                             │     Table 1   │                               │     Table n      │
                                                             │---------------│            ...                │----------------- │
                                                             │               │                               │                  │
                                                             │. DB_name      │                               │       ...        │
                                                             │. Table_name   │                               │                  │
                                                             │. Table_schema │                               │                  │
                                                             └───────────────┘                               └──────────────────┘
                                                                     │                                                 │         
                                                                     │                                                 │         
                                           ┌─────────────────────────┼─────────────────────────┐                       │         
                                           │                         │                         │                       │         
                                           ▼                         │                         ▼                       ▼         
                       ┌──────────────────────────────────────┐      ▼           ┌──────────────────────────┐                    
                       │       Partition 1 (2021-12-12)       │                  │ Partition m (2021-12-20) │          ...       
                       │--------------------------------------│      ...         │ ------------------------ │                    
                       │                                      │                  │                          │                    
                       │. addr (db_name, table_name, part_key)│                  │           .......        │                    
                       │. PersistenceWindow                   │                  │                          │                    
                       │. CreatedTime & LastWriteTime         │                  │                          │                    
                       │. NextChunkOrder                      │                  │                          │                    
                       │. PartitionMetrics                    │                  │                          │                    
                       └──────────────────────────────────────┘                  └──────────────────────────┘                    
                                           │                                                   │                                 
                    ┌──────────────────────▼───────────────────┐                               │                                 
                    │                                          │                               │                                 
                    ▼                                          ▼                               ▼                                 
┌────────────────────────────────────────┐        ┌────────────────────────┐      ┌────────────────────────┐                     
│            Catalog Chunk 1             │        │    Catalog Chunk 2     │      │    Catalog Chunk 3     │                     
│--------------------------------------  │        │    ---------------     │      │    ---------------     │                     
│                                        │        │                        │      │                        │                     
│. addr (db, table, part_key, chunk_id)  │        │          ....          │      │          ....          │                     
│. FirstWriteTime & LastWrite_time       │        │                        │      │                        │                     
│. LifecycleAction                       │        │                        │      │                        │                     
│. ChunkMetrics                          │        │                        │      │                        │                     
│. AccessRecorder                        │        │                        │      │                        │                     
│. ......                                │        │                        │      │                        │                     
└────────────────────────────────────────┘        └────────────────────────┘      └────────────────────────┘                     
                       │                                      │                               │                                  
                       ▼                                      ▼                               ▼                                  
               ┌──────────────┐                       ┌──────────────┐                ┌──────────────┐                           
               │Chunk Stage 1 │                       │Chunk Stage 2 │                │Chunk Stage 3 │                           
               │    (Open)    │                       │   (Frozen)   │                │ (Persisted)  │                           
               └──────────────┘                       └──────────────┘                └───────┬──────┘                           
                       │                                      │                     ┌─────────▼─────────┐                        
                       ▼                                      ▼                     ▼                   ▼                        
               ┌──────────────┐                       ┌──────────────┐        ┌───────────┐       ┌───────────┐                  
               │    O-MUB 1   │                       │     RUB 2    │        │    RUB 3  │       │   OS 3    │                  
               │              │                       │              │        │           │       │           │                  
               │col1 col2 col3│                       │  col1 col4   │        │ col2 col3 │       │ col2 col3 │                  
               │---- ---- ----│                       │  ---- ----   │        │ ---- ---- │       │ ---- ---- │                  
               │---- ---- ----│                       │  ---- ----   │        │ ---- ---- │       │ ---- ---- │                  
               └──────────────┘                       │  ---- ----   │        │ ---- ---- │       │ ---- ---- │                  
                                                      └──────────────┘        └───────────┘       └───────────┘                  

Figure 3: In-Memory Catalog
```
## Preserved Catalog

## Query Catalog