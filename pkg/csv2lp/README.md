# CSV to Line Protocol
csv2lp library converts CSV (comma separated values) to InfluxDB Line Protocol.

  1. it can process CSV result of a (simple) flux query that exports data from a bucket
  2. it allows the processing of existing CSV files

## Usage
The entry point is the ``CsvToLineProtocol`` function that accepts a (utf8) reader with CSV data and returns a reader with line protocol data.

## Examples
#### Example 1 - Flux Query Result
csv:
```bash
#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:17:57Z,0,time_steal,cpu,cpu1,rsavage.prod
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:07Z,0,time_steal,cpu,cpu1,rsavage.prod

#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:01Z,2.7263631815907954,usage_user,cpu,cpu-total,tahoecity.prod
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:11Z,2.247752247752248,usage_user,cpu,cpu-total,tahoecity.prod
```

line protocol data:
```
cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669077000000000
cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669087000000000
cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.7263631815907954 1582669081000000000
cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.247752247752248 1582669091000000000
```
#### Example 2 - Simple CSV file

csv:
```bash
#datatype measurement,tag,tag,double,double,ignored,dateTime:number
m,cpu,host,time_steal,usage_user,nothing,time
cpu,cpu1,rsavage.prod,0,2.7,a,1482669077000000000
cpu,cpu1,rsavage.prod,0,2.2,b,1482669087000000000
```

line protocol data: 
```
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.7 1482669077000000000
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.2 1482669087000000000
```

Data type can be supplied in the column name, the CSV can be shortened to:

```
m|measurement,cpu|tag,host|tag,time_steal|double,usage_user|double,nothing|ignored,time|dateTime:number
cpu,cpu1,rsavage.prod,0,2.7,a,1482669077000000000
cpu,cpu1,rsavage.prod,0,2.2,b,1482669087000000000
```
#### Example 3 - Data Types with default values

csv:
```bash
#datatype measurement,tag,string,double,boolean,long,unsignedLong,duration,dateTime
#default test,annotatedDatatypes,,,,,,
m,name,s,d,b,l,ul,dur,time
,,str1,1.0,true,1,1,1ms,1
,,str2,2.0,false,2,2,2us,2020-01-11T10:10:10Z
```

line protocol data: 
```
test,name=annotatedDatatypes s="str1",d=1,b=true,l=1i,ul=1u,dur=1000000i 1
test,name=annotatedDatatypes s="str2",d=2,b=false,l=2i,ul=2u,dur=2000i 1578737410000000000
```

Default value can be supplied in the column label after data type, the CSV could be also:

```
m|measurement|test,name|tag|annotatedDatatypes,s|string,d|double,b|boolean,l|long,ul|unsignedLong,dur|duration,time|dateTime
,,str1,1.0,true,1,1,1ms,1
,,str2,2.0,false,2,2,2us,2020-01-11T10:10:10Z
```
#### Example 4 - Advanced usage
csv:
```
#constant measurement,test
#constant tag,name,datetypeFormats
#timezone -0500
t|dateTime:2006-01-02|1970-01-02,"d|double:,. ","b|boolean:y,Y:n,N|y"
1970-01-01,"123.456,78",
,"123 456,78",Y
```
   - measurement and extra tags is defined using the `#constant` annotation
   - timezone for dateTime is to `-0500` (EST)
   - `t` column is of `dateTime` data type of format is `2006-01-02`, default value is _January 2nd 1970_
   - `d` column is of `double` data type with `,` as a fraction delimiter and `. ` as ignored separators that  used to visually separate large numbers into groups
   - `b` column os of `boolean` data type that considers `y` or `Y` truthy, `n` or `N` falsy and empty column values as truthy 


line protocol data:
```
test,name=datetypeFormats d=123456.78,b=true 18000000000000
test,name=datetypeFormats d=123456.78,b=true 104400000000000
```

#### Example 5 - Custom column separator
```
sep=;
m|measurement;available|boolean:y,Y:|n;dt|dateTime:number
test;nil;1
test;N;2
test;";";3
test;;4
test;Y;5
```
   - the first line can define a column separator character for next lines, here: `;`
   - other lines use this separator, `available|boolean:y,Y` does not need to be wrapped in double quotes

line protocol data:
```
test available=false 1
test available=false 2
test available=false 3
test available=false 4
test available=true 5
```
## CSV Data On Input
This library supports all the concepts of [flux result annotated CSV](https://docs.influxdata.com/influxdb/latest/reference/syntax/annotated-csv/#tables) and provides a few extensions that allow to process existing/custom CSV files. The conversion to line protocol is driven by contents of annotation rows and layout of the header row.

#### New data types
Existing [data types](https://docs.influxdata.com/influxdb/latest/reference/syntax/annotated-csv/#data-types) are supported. The CSV input can also contain the following data types that are used to associate a column value to a part of a protocol line
 - `measurement` data type identifies a column that carries the measurement name
 - `tag` data type identifies a column with a tag value, column label (from the header row) is the tag name
 - `time` is an alias for existing `dateTime` type , there is at most one such column in a CSV row
 - `ignore` and `ignored` data types are used to identify columns that are ignored when creating a protocol line
 - `field` data type is used to copy the column data to a protocol line as-is

#### New CSV annotations
- `#constant` annotation adds a constant column to the data, so you can set measurement, time, field or tag of every row you import 
   - the format of a constant annotation row is `#constant,datatype,name,value`', it contains supported datatype, a column name, and a constant value
   - _column name_ can be omitted for _dateTime_ or _measurement_ columns, so the annotation can be simply `#constant,measurement,cpu`
- `#concat` annotation adds a new column that is concatenated from existing columns according to a template
   - the format of a concat annotation row is `#concat,datatype,name,template`', it contains supported datatype, a column name, and a template value
   - the `template` is a string with `${columnName}` placeholders, in which the placeholders are replaced by values of existing columns
      - for example: `#concat,string,fullName,${firstName} ${lastName}`
   - _column name_ can be omitted for _dateTime_ or _measurement_ columns
- `#timezone` annotation specifies the time zone of the data using an offset, which is either `+hhmm` or `-hhmm` or `Local` to use the local/computer time zone. Examples:  _#timezone,+0100_  _#timezone -0500_ _#timezone Local_

#### Data type with data format
All data types can include the format that is used to parse column data. It is then specified as `datatype:format`. The following data types support format:
- `dateTime:format` 
   - the following formats are predefined:
      - `dateTime:RFC3339` format is 2006-01-02T15:04:05Z07:00
      - `dateTime:RFC3339Nano` format is 2006-01-02T15:04:05.999999999Z07:00
      - `dateTime:number` represent UTCs time since epoch in nanoseconds
   - a custom layout as described in the [time](https://golang.org/pkg/time) package, for example `dateTime:2006-01-02` parses 4-digit-year , '-' , 2-digit month ,'-' , 2 digit day of the month
   - if the time format includes a time zone, the parsed date time respects the time zone; otherwise the timezone dependends on the presence of the new `#timezone` annotation; if there is no `#timezone` annotation, UTC is used
- `double:format`
   - the `format`'s first character is used to separate integer and fractional part (usually `.` or `,`), second and next format's characters (such as as `, _`) are removed from the column value, these removed characters are typically used to visually separate large numbers into groups
   - for example:
      - a Spanish locale value `3.494.826.157,123` is of `double:,.` type; the same `double` value is  _3494826157.123_
      - `1_000_000` is of `double:._` type to be a million `double`
   - note that you have to quote column delimiters whenever they appear in a CSV column value, for example:
      - `#constant,"double:,.",myColumn,"1.234,011"`
- `long:format` and `unsignedLong:format` support the same format as `double`, but everything after and including a fraction character is ignored
   - the format can be appended with `strict` to fail when a fraction digit is present, for example:
      - `1000.000` is `1000` when parsed as `long`, but fails when parsed as `long:strict`
      - `1_000,000` is `1000` when parsed as `long:,_`, but fails when parsed as `long:strict,_`
- `boolean:truthy:falsy`
   - `truthy` and `falsy` are comma-separated lists of values, they can be empty to assume all values as truthy/falsy; for example `boolean:sí,yes,ja,oui,ano,да:no,nein,non,ne,нет` 
   - a  `boolean` data type (without the format) parses column values that start with any of _tTyY1_ as `true` values, _fFnN0_ as `false` values and fails on other values
   - a column with an empty value is excluded in the protocol line unless a default value is supplied either using `#default` annotation or in a header line (see below)

#### Header row with data types and default values
The header row (i.e. the row that define column names) can also define column data types when supplied as `name|datatype`; for example `cpu|tag` defines a tag column named _cpu_ . Moreover, it can also specify a default value when supplied as `name|datatype|default`; for example, `count|long|0` defines a field column named _count_ of _long_ data type that will not skip the field if a column value is empty, but uses '0' as the column value.
  - this approach helps to easily specify column names, types and defaults in a single row
  - this is an alternative to using 3 lines being `#datatype` and `#default` annotations and a simple header row

#### Custom CSV column separator
A CSV file can start with a line `sep=;` to inform about a character that is used to separate columns, by default `,` is used as a column separator. This method is frequently used (Excel).

#### Error handling
The CSV conversion stops on the first error by default, line and column are reported together with the error. The CsvToLineReader's SkipRowOnError function can change it to skip error rows and log errors instead.

#### Support Existing CSV files
The majority of existing CSV files can be imported by skipping the first X lines of existing data (so that custom header line can be then provided) and prepending extra annotation/header lines to let this library know of how to convert the CSV to line protocol. The following functions helps to change the data on input
   - [csv2lp.SkipHeaderLinesReader](./skip_header_lines.go) returns a reader that skip the first x lines of the supplied reader
   - [io.MultiReader](https://golang.org/pkg/io/#MultiReader) joins multiple readers, custom header line(s) and new lines can be prepended as [strings.NewReader](https://golang.org/pkg/strings/#NewReader)s
   - [csv2lp.MultiCloser](./multi_closer.go) helps with closing multiple io.Closers (files) on input, [it is not available OOTB](https://github.com/golang/go/issues/20136)
