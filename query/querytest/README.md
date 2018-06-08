# Query Tests
This package simulates an end-to-end query test without requiring launching a full service.  This is currently achieved
by dynamically replacing instances of from()---which access a storage location---with instances of fromCSV() which is
a data source that takes a single CSV text argument.

# Getting Started
You do not need to use fromCSV in your query.  You can write your query as you normally would, or copy it from an
application as-is.  What you will need to provide, along with the query file, are two csv files that contain the input
and output data for the query.  The test framework will identify the Flux query by the file extension `.flux` and
then locate the required input/output files using the same root filename.

As an example, consider the test `simple_max` which is defined by the file `simple_max.flux`:

### simple_max.flux
```
from(db:"test") |> range(start:-5m)  |> group(by:["_measurement"]) |> max(useRowTime:true) |> map(fn: (r) => {max:r._value})
```

The test will read in this query, as well as the associated CSV files that are in the flux-spec compatible CSV format:

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

- <FILENAME>.flux: a Flux query file
- <FILENAME>_in.csv: input file for the query
- <FILENAME>_out.csv: output file for the query
- <FILENAME>.influxql: if present, this query will be transpiled and tested against the input and output csv files,
but only if there is a corresponding `.flux` query.

## Notes about text formatting
Compliant to the HTTP spec for CSV data, the output produced by the fluxd process has some specific requirements:
- UTF-8 normalized
- line endings are `\r\n`

The go program query/querytest/prepcsvtests/prepcsvtests.go can be run to prepare tests.  A valid test case must have
<CASENAME>.flux  and <CASENAME>.in.csv   files.  The prepcsvtests executable will iterate over all such test cases in
the user-supplied directory, run the Flux query on the input and prompt the user to approve saving the result as
<CASENAME>.out.csv.
```go build ./querytest/prepcsvtests/prepcsvtests.go  && ./prepcsvtests query/querytests/test_cases```

Optionally, you can give a CASENAME to prep the output for a single case:
```go build ./querytest/prepcsvtests/prepcsvtests.go  && ./prepcsvtests query/querytests/test_cases CASENAME```
