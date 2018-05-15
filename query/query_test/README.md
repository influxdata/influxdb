# Query Tests
This package simulates an end-to-end query test without requiring launching a full service.  This is currently achieved
by dynamically replacing instances of from()---which access a storage location---with instances of fromCSV() which is 
a data source that takes a single CSV text argument.

# Getting Started
You do not need to use fromCSV in your query.  You can write your query as you normally would, or copy it from an 
application as-is.  What you will need to provide, along with the query file, are two csv files that contain the input
and output data for the query.  The test framework will identify the ifql query by the file extension `.ifql` and 
then locate the required input/output files using the same root filename.  

As an example, consider the test `simple_max` which is defined by the file `simple_max.ifql`: 

### simple_max.ifql
```
from(db:"test") |> range(start:-5m)  |> group(by:["_measurement"]) |> max(useRowTime:true) |> map(fn: (r) => {max:r._value})
```

The test will read in this query, as well as the associated CSV files that are in the ifql-spec compatible CSV format: 

### simple_max_in.csv
```
#datatype,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,tag,tag,double
,blkid,_start,_stop,_time,_measurement,_field,_value
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,m1,f1,42.0
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,m1,f1,43.0
```

### simple_max_out.csv
```
#datatype,long,dateTime:RFC3339Nano,dateTime:RFC3339Nano,dateTime:RFC3339Nano,string,double
,blkid,_start,_stop,_time,_measurement,max
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,m1,43
```

Note that all of the test files are identified by the test name, `simple_max`.  So given a <FILENAME>, a new test must
provide the following files: 

- <FILENAME>.ifql: an ifql query file
- <FILENAME>_in.csv: input file for the query
- <FILENAME>_out.csv: output file for the query
- <FILENAME>.influxql: if present, this query will be transpiled and tested against the input and output csv files, 
but only if there is a corresponding `.ifql` query.  

## Notes about text formatting
Compliant to the HTTP spec for CSV data, the output produced by the ifqld process has some specific requirements: 
- UTF-8 normalized
- line endings are `\r\n`

The go program query/query_test/normalize_text/normalize.go can be run on a file to prepare the data in-place.  So you
may create a new file by copy/pasting some text, then prepare the fire for testing by: 
```go build ./query/query_test/normalize_text/normalize.go  && ./normalize query/query_test/test_cases/simple_max_out.csv```