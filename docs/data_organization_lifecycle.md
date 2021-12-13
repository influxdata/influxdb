# IOx Data Organization and Lifecycle



## Data Organization
`IOx Server` is an database management system (DBMS) that, as an example, can be briefly illustrated in Figure 1. An IOx server can include different isolated datasets from one or many organizations/users, each can be represented by a `database`. For example, the IOX Server in Figure 1 consists of `p` databases. Each database can have as many `tables` as needed. Data of each table can be split into many `partitions` based on a specified partition key which is an expression of the table column(s). In the example of Figure 1, `Table 1` is partitioned by date which is an expression on a time column of `Table 1`. Partition data can be further split into many chunks depending on the table's flow of ingested data which will be described in next section, Data Life Cycle. Each chunk contains a subset of rows of a table partition on a subset of columns of the table. For example, `Chunk 1` has 2 rows of data on columns `col1`, `col2`, and `col3` while `Chunk 2` includes 3 rows on `col1` and `col4`. Since every chunk can consist of data of the same or different columns, a chunk has it own `schema` defined with it. `Chunk 1`'s schema is {`col1`, `col2`, `col3`} (and their corresponding data types) and `Chunk 2`'s schema is {`col1`, `col4`}. Same name column, `col1`, represents the same column of the table and must have the same data type. 

```text                                                                                                                                
                                                                ┌───────────────┐                                   IOx Server  
                                                                │  IOx Server   │                                               
                                                                └───────────────┘                                               
                                                                        │                                                       
                                                   ┌────────────────────┼────────────────────────┐                              
                                                   │                    │                        │                              
                                                   ▼                    ▼                        ▼                              
                                           ┌───────────────┐                             ┌───────────────┐                      
                                           │  Database 1   │           ...               │  Database p   │          Databases   
                                           └───────────────┘                             └───────────────┘                      
                                                   │                                                                            
                              ┌────────────────────┼────────────────────┐                                                       
                              │                    │                    │                                                       
                              ▼                    ▼                    ▼                                                       
                      ┌───────────────┐                         ┌───────────────┐                                    Tables     
                      │    Table 1    │           ...           │    Table n    │                                               
                      └───────────────┘                         └───────────────┘                                               
                              │                                         │                                                       
        ┌─────────────────────┼────────────────────┐                    │                                                       
        │                     │                    │                    │                                                       
        ▼                     ▼                    ▼                    ▼                                                       
 ┌────────────┐                             ┌────────────┐                                                         Partitions   
 │Partition 1 │              ...            │Partition m │             ...                                                      
 │(2021-12-10)│                             │(2021-12-20)│                                                                      
 └────────────┘                             └────────────┘                                                                      
       │                                           │                                                                            
       │                          ┌────────────────┼────────────────┐                                                           
       │                          │                │                │                                                           
       ▼                          ▼                ▼                ▼                                                           
                          ┌──────────────┐                    ┌───────────┐                                         Chunks      
      ...                 │    Chunk 1   │        ...         │  Chunk 2  │                                                     
                          │              │                    │           │                                                     
                          │col1 col2 col3│                    │ col1 col4 │                                                     
                          │---- ---- ----│                    │ ---- ---- │                                                     
                          │---- ---- ----│                    │ ---- ---- │                                                     
                          └──────────────┘                    │ ---- ---- │                                                     
                                                              └───────────┘                                                     

Figure 1: Data organization in an IOx Server
```

Chunk is considered the smallest unit of blocks of data in IOx and the central discussion of the rest of this document. 

## Data Life Cycle