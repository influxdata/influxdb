import {FluxToolbarFunction} from 'src/types/shared'

export const FLUX_FUNCTIONS: FluxToolbarFunction[] = [
  {
    name: 'aggregateWindow',
    args: [
      {
        name: 'every',
        desc: 'The duration of windows.',
        type: 'Duration',
      },
      {
        name: 'fn',
        desc: 'The aggregate function used in the operation.',
        type: 'Unquoted String',
      },
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'timeSrc',
        desc:
          'The "time source" column from which time is copied for the aggregate record. Defaults to `"_stop"`.',
        type: 'String',
      },
      {
        name: 'timeDst',
        desc:
          'The "time destination" column to which time is copied for the aggregate record. Defaults to `"_time"`.',
        type: 'String',
      },
      {
        name: 'createEmpty',
        desc:
          'For windows without data, this will create an empty window and fill it with a `null` aggregate value.',
        type: 'Boolean',
      },
    ],
    desc: 'Applies an aggregate function to fixed windows of time.',
    example: 'aggregateWindow(every: 1m, fn: mean)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/aggregatewindow',
  },
  {
    name: 'assertEquals',
    args: [
      {
        name: 'name',
        desc: 'Unique name given to the assertion.',
        type: 'String',
      },
      {
        name: 'got',
        desc: 'The stream containing data to test.',
        type: 'Object',
      },
      {
        name: 'want',
        desc: 'The stream that contains the expected data to test against.',
        type: 'Object',
      },
    ],
    desc: 'Tests whether two streams have identical data.',
    example: 'assertEquals(got: got, want: want)',
    category: 'Test',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/tests/assertequals',
  },
  {
    name: 'bottom',
    args: [
      {
        name: 'n',
        desc: 'The number of rows to return.',
        type: 'Integer',
      },
      {
        name: 'cols',
        desc:
          'List of columns by which to sort. Sort precedence is determined by list order (left to right) .Default is `["_value"]`',
        type: 'Array of Strings',
      },
    ],
    desc: 'Sorts a table by columns and keeps only the bottom n rows.',
    example: 'bottom(n:10, cols: ["_value"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/bottom',
  },
  {
    name: 'buckets',
    args: [],
    desc: 'Returns a list of buckets in the organization.',
    example: 'buckets()',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/inputs/buckets',
  },
  {
    name: 'columns',
    args: [
      {
        name: 'column',
        desc:
          'The name of the output column in which to store the column labels.',
        type: 'String',
      },
    ],
    desc: 'Lists the column labels of input tables.',
    example: 'columns(column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/columns',
  },
  {
    name: 'count',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc: 'Outputs the number of records in each aggregated column.',
    example: 'count(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/count',
  },
  {
    name: 'cov',
    args: [
      {
        name: 'x',
        desc: 'One input stream used to calculate the covariance.',
        type: 'Object',
      },
      {
        name: 'y',
        desc: 'The other input table used to calculate the covariance.',
        type: 'Object',
      },
      {
        name: 'on',
        desc: 'The list of columns on which to join.',
        type: 'Array of Strings',
      },
      {
        name: 'pearsonr',
        desc:
          'Indicates whether the result should be normalized to be the Pearson R coefficient',
        type: 'Boolean',
      },
    ],
    desc:
      'Computes the covariance between two streams by first joining the streams, then performing the covariance operation.',
    example:
      'cov(x: table1, y: table2, on: ["_time", "_field"], pearsonr: false)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/cov',
  },
  {
    name: 'covariance',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Exactly two columns must be provided.',
        type: 'Array of Strings',
      },
      {
        name: 'pearsonr',
        desc:
          'Indicates whether the result should be normalized to be the Pearson R coefficient',
        type: 'Boolean',
      },
      {
        name: 'valueDst',
        desc:
          'The column into which the result will be placed. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    desc: 'Computes the covariance between two columns.',
    example:
      'covariance(columns: ["column_x", "column_y"], pearsonr: false, valueDst: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/covariance',
  },
  {
    name: 'cumulativeSum',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes a running sum for non-null records in the table. The output table schema will be the same as the input table.',
    example: 'cumulativeSum(columns: ["_value"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/cumulativesum',
  },
  {
    name: 'derivative',
    args: [
      {
        name: 'unit',
        desc: 'The time duration used when creating the derivative.',
        type: 'Duration',
      },
      {
        name: 'nonNegative',
        desc:
          'Indicates if the derivative is allowed to be negative. When set to `true`, if a value is less than the previous value, it is assumed the previous value should have been a zero.',
        type: 'Boolean',
      },
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'timeColumn',
        desc: 'The column name for the time values. Defaults to `"_time"`.',
        type: 'String',
      },
    ],
    desc:
      'Computes the rate of change per unit of time between subsequent non-null records. The output table schema will be the same as the input table.',
    example:
      'derivative(unit: 1s, nonNegative: true, columns: ["_value"], timeColumn: "_time")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/derivative',
  },
  {
    name: 'difference',
    args: [
      {
        name: 'nonNegative',
        desc:
          'Indicates if the derivative is allowed to be negative. When set to `true`, if a value is less than the previous value, it is assumed the previous value should have been a zero.',
        type: 'Boolean',
      },
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc: 'Computes the difference between subsequent records.',
    example: 'difference(nonNegative: false, columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/difference',
  },
  {
    name: 'distinct',
    args: [
      {
        name: 'column',
        desc: 'Column on which to track unique values.',
        type: 'String',
      },
    ],
    desc: 'Returns the unique values for a given column.',
    example: 'distinct(column: "host")',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/distinct',
  },
  {
    name: 'drop',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns to be removed from the table. Cannot be used with `fn`.',
        type: 'Array of Strings',
      },
      {
        name: 'fn',
        desc:
          'A function which takes a column name as a parameter and returns a boolean indicating whether or not the column should be removed from the table. Cannot be used with `columns`.',
        type: 'Function',
      },
    ],
    desc:
      'Removes specified columns from a table. Columns can be specified either through a list or a predicate function. When a dropped column is part of the group key, it will be removed from the key.',
    example: 'drop(columns: ["col1", "col2"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/drop',
  },
  {
    name: 'duplicate',
    args: [
      {
        name: 'column',
        desc: 'The column name to duplicate.',
        type: 'String',
      },
      {
        name: 'as',
        desc: 'The name assigned to the duplicate column.',
        type: 'String',
      },
    ],
    desc: 'Duplicates a specified column in a table.',
    example: 'duplicate(column: "column-name", as: "duplicate-name")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/duplicate',
  },
  {
    name: 'fill',
    args: [
      {
        name: 'column',
        desc:
          'The column in which to replace null values. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'value',
        desc: 'The constant value to use in place of nulls.',
        type: 'Value type of `column`',
      },
      {
        name: 'usePrevious',
        desc:
          'When `true`, assigns the value set in the previous non-null row.',
        type: 'Boolean',
      },
    ],
    desc:
      'replaces all null values in an input stream and replace them with a non-null value.',
    example: 'fill(column: "_value", usePrevious: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/fill',
  },
  {
    name: 'filter',
    args: [
      {
        name: 'fn',
        desc:
          'A single argument function that evaluates true or false. Records are passed to the function. Those that evaluate to true are included in the output tables.',
        type: 'Function',
      },
    ],
    desc:
      'Filters data based on conditions defined in the function. The output tables have the same schema as the corresponding input tables.',
    example: 'filter(fn: (r) => r._measurement == "cpu")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/filter',
  },
  {
    name: 'first',
    args: [],
    desc: 'Selects the first non-null record from an input table.',
    example: 'first()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/first',
  },
  {
    name: 'from',
    args: [
      {
        name: 'bucket',
        desc: 'The name of the bucket to query.',
        type: 'String',
      },
      {
        name: 'bucketID',
        desc: 'The string-encoded ID of the bucket to query.',
        type: 'String',
      },
    ],
    desc:
      'Used to retrieve data from an InfluxDB data source. It returns a stream of tables from the specified bucket. Each unique series is contained within its own table. Each record in the table represents a single point in the series.',
    example: 'from(bucket: "telegraf/autogen")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/inputs/from',
  },
  {
    name: 'fromCSV',
    args: [
      {
        name: 'file',
        desc: 'The file path of the CSV file to query.',
        type: 'String',
      },
      {
        name: 'csv',
        desc:
          'Raw CSV-formatted text. CSV data must be in the CSV format produced by the Flux HTTP response standard.',
        type: 'String',
      },
    ],
    desc: 'Retrieves data from a comma-separated value (CSV) data source.',
    example: 'from(file: "/path/to/data-file.csv")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/inputs/fromcsv',
  },
  {
    name: 'group',
    args: [
      {
        name: 'columns',
        desc:
          'List of columns to use in the grouping operation. Defaults to `[]`.',
        type: 'Array of Strings',
      },
      {
        name: 'mode',
        desc:
          'The mode used to group columns. The following options are available: by, except. Defaults to `"by"`.',
        type: 'String',
      },
    ],
    desc:
      'Groups records based on their values for specific columns. It produces tables with new group keys based on provided properties.',
    example: 'group(columns: ["host", "_measurement"], mode:"by")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/group',
  },
  {
    name: 'highestAverage',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the top `n` records from all groups using the average of each group.',
    example: 'highestAverage(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/highestaverage',
  },
  {
    name: 'highestCurrent',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the top `n` records from all groups using the last value of each group.',
    example: 'highestCurrent(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/highestcurrent',
  },
  {
    name: 'highestMax',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the top `n` records from all groups using the maximum of each group.',
    example: 'highestMax(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/highestmax',
  },
  {
    name: 'histogram',
    args: [
      {
        name: 'column',
        desc:
          'The name of a column containing input data values. The column type must be float. Defaults to `"_value"`.',
        type: 'Strings',
      },
      {
        name: 'upperBoundColumn',
        desc:
          'The name of the column in which to store the histogram\'s upper bounds. Defaults to `"le"`.',
        type: 'String',
      },
      {
        name: 'countColumn',
        desc:
          'The name of the column in which to store the histogram counts. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'bins',
        desc:
          'A list of upper bounds to use when computing the histogram frequencies. Each element in the array should contain a float value that represents the maximum value for a bin.',
        type: 'Array of Floats',
      },
      {
        name: 'normalize',
        desc:
          'When `true`, will convert the counts into frequency values between 0 and 1. Defaults to `false`.',
        type: 'Boolean',
      },
    ],
    desc:
      'Approximates the cumulative distribution function of a dataset by counting data frequencies for a list of buckets.',
    example:
      'histogram(column: "_value", upperBoundColumn: "le", countColumn: "_value", bins: [50.0, 75.0, 90.0], normalize: false)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/histogram',
  },
  {
    name: 'histogramQuantile',
    args: [
      {
        name: 'quantile',
        desc:
          'A value between 0 and 1 indicating the desired quantile to compute.',
        type: 'Float',
      },
      {
        name: 'countColumn',
        desc:
          'The name of the column in which to store the histogram counts. The count column type must be float. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'upperBoundColumn',
        desc:
          'The name of the column in which to store the histogram\'s upper bounds. The count column type must be float. Defaults to `"le"`.',
        type: 'String',
      },
      {
        name: 'valueColumn',
        desc:
          'The name of the output column which will contain the computed quantile. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'minValue',
        desc:
          'The assumed minimum value of the dataset. When the quantile falls below the lowest upper bound, interpolation is performed between `minValue` and the lowest upper bound. When `minValue` is equal to negative infinity, the lowest upper bound is used. Defaults to `0`.',
        type: 'Float',
      },
    ],
    desc:
      'Approximates a quantile given a histogram that approximates the cumulative distribution of the dataset.',
    example:
      'histogramQuantile(quantile: 0.5, countColumn: "_value", upperBoundColumn: "le", valueColumn: "_value", minValue: 0)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/histogramquantile',
  },
  {
    name: 'increase',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns for which the increase is calculated. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes the total non-negative difference between values in a table.',
    example: 'increase(columns: ["_values"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/increase',
  },
  {
    name: 'influxFieldsAsCols',
    args: [],
    desc:
      'A special application of the `pivot()` function that automatically aligns fields within each input table that have the same timestamp.',
    example: 'influxFieldsAsCols()',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/influxfieldsascols',
  },
  {
    name: 'integral',
    args: [
      {
        name: 'unit',
        desc: 'The time duration used when computing the integral.',
        type: 'Duration',
      },
      {
        name: 'columns',
        desc:
          'A list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes the area under the curve per unit of time of subsequent non-null records. The curve is defined using `_time` as the domain and record values as the range.',
    example: 'integral(unit: 10s, columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/integral',
  },
  {
    name: 'intervals',
    args: [
      {
        name: 'every',
        desc:
          'The duration between starts of each of the intervals. Defaults to the value of the `period` duration.',
        type: 'Duration',
      },
      {
        name: 'period',
        desc:
          'The length of each interval. Defaults to the value of the `every` duration.',
        type: 'Duration',
      },
      {
        name: 'offset',
        desc:
          'The offset duration relative to the location offset. Defaults to `0h`.',
        type: 'Duration',
      },
      {
        name: 'filter',
        desc:
          'A function that accepts an interval object and returns a boolean value. Each potential interval is passed to the filter function. When the function returns false, that interval is excluded from the set of intervals. Defaults to include all intervals.',
        type: 'Function',
      },
    ],
    desc: 'Generates a set of time intervals over a range of time.',
    example: 'intervals()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/misc/intervals',
  },
  {
    name: 'join',
    args: [
      {
        name: 'tables',
        desc: 'The map of streams to be joined.',
        type: 'Object',
      },
      {
        name: 'on',
        desc: 'The list of columns on which to join.',
        type: 'Array of Strings',
      },
      {
        name: 'method',
        desc:
          'The method used to join. Possible values are: `inner`, `cross`, `left`, `right`, or `full`. Defaults to `"inner"`.',
        type: 'String',
      },
    ],
    desc:
      'Merges two or more input streams, whose values are equal on a set of common columns, into a single output stream. The resulting schema is the union of the input schemas. The resulting group key is the union of the input group keys.',
    example:
      'join(tables: {key1: table1, key2: table2}, on: ["_time", "_field"], method: "inner")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/join',
  },
  {
    name: 'keep',
    args: [
      {
        name: 'columns',
        desc:
          'Columns that should be included in the resulting table. Cannot be used with `fn`.',
        type: 'Array of Strings',
      },
      {
        name: 'fn',
        desc:
          'A predicate function which takes a column name as a parameter and returns a boolean indicating whether or not the column should be removed from the table. Cannot be used with `columns`.',
        type: 'Function',
      },
    ],
    desc:
      'Returns a table containing only the specified columns, ignoring all others. Only columns in the group key that are also specified in the `keep()` function will be kept in the resulting group key. It is the inverse of `drop`.',
    example: 'keep(columns: ["col1", "col2"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/keep',
  },
  {
    name: 'keys',
    args: [
      {
        name: 'column',
        desc:
          'Column is the name of the output column to store the group key labels. Defaults to `_value`.',
        type: 'String',
      },
    ],
    desc:
      "Outputs the group key of input tables. For each input table, it outputs a table with the same group key columns, plus a _value column containing the labels of the input table's group key.",
    example: 'keys(column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/keys',
  },
  {
    name: 'keyValues',
    args: [
      {
        name: 'keyColumns',
        desc:
          'A list of columns from which values are extracted. All columns indicated must be of the same type.',
        type: 'Array of Strings',
      },
      {
        name: 'fn',
        desc:
          'Function used to identify a set of columns. All columns indicated must be of the same type.',
        type: 'Function',
      },
    ],
    desc:
      "Returns a table with the input table's group key plus two columns, `_key` and `_value`, that correspond to unique column + value pairs from the input table.",
    example: 'keyValues(keyColumns: ["usage_idle", "usage_user"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/keyvalues',
  },
  {
    name: 'last',
    args: [],
    desc: 'Selects the last non-null record from an input table.',
    example: 'last()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/last',
  },
  {
    name: 'limit',
    args: [
      {
        name: 'n',
        desc: 'The maximum number of records to output.',
        type: 'Integer',
      },
      {
        name: 'offset',
        desc:
          'The number of records to skip per table before limiting to n. Defaults to 0.',
        type: 'Integer',
      },
    ],
    desc:
      'Limits the number of records in output tables to a fixed number `n` records after the `offset`. If the input table has less than `n` records, all records are be output.',
    example: 'limit(n:10, offset: 0)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/limit',
  },
  {
    name: 'linearBins',
    args: [
      {
        name: 'start',
        desc: 'The first value in the returned list.',
        type: 'Float',
      },
      {
        name: 'width',
        desc: 'The distance between subsequent bin values.',
        type: 'Float',
      },
      {
        name: 'count',
        desc: 'The number of bins to create.',
        type: 'Integer',
      },
      {
        name: 'infinity',
        desc:
          'When `true`, adds an additional bin with a value of positive infinity. Defaults to `true`.',
        type: 'Boolean',
      },
    ],
    desc: 'Generates a list of linearly separated floats.',
    example: 'linearBins(start: 0.0, width: 5.0, count: 20, infinity: true)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/misc/linearbins',
  },
  {
    name: 'logarithmicBins',
    args: [
      {
        name: 'start',
        desc: 'The first value in the returned list.',
        type: 'Float',
      },
      {
        name: 'factor',
        desc: 'The multiplier applied to each subsequent bin.',
        type: 'Float',
      },
      {
        name: 'count',
        desc: 'The number of bins to create.',
        type: 'Integer',
      },
      {
        name: 'infinity',
        desc:
          'When `true`, adds an additional bin with a value of positive infinity. Defaults to `true`.',
        type: 'Boolean',
      },
    ],
    desc: 'Generates a list of exponentially separated floats.',
    example:
      'logarithmicBins(start: 1.0, factor: 2.0, count: 10, infinty: true)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/misc/logarithmicbins',
  },
  {
    name: 'lowestAverage',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the bottom `n` records from all groups using the average of each group.',
    example: 'lowestAverage(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/lowestaverage',
  },
  {
    name: 'lowestCurrent',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the bottom `n` records from all groups using the last value of each group.',
    example: 'lowestCurrent(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/lowestcurrent',
  },
  {
    name: 'lowestMin',
    args: [
      {
        name: 'n',
        desc: 'Number of records to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'List of columns by which to sort. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the bottom `n` records from all groups using the maximum of each group.',
    example: 'lowestMin(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/lowestmin',
  },
  {
    name: 'map',
    args: [
      {
        name: 'fn',
        desc:
          'A single argument function that to apply to each record. The return value must be an object.',
        type: 'Function',
      },
      {
        name: 'mergeKey',
        desc:
          'Indicates if the record returned from `fn` should be merged with the group key. Defaults to `true`.',
        type: 'Boolean',
      },
    ],
    desc: 'Applies a function to each record in the input tables.',
    example: 'map(fn: (r) => r._value * r._value, mergeKey: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/map',
  },
  {
    name: 'max',
    args: [],
    desc: 'Selects record with the highest `_value` from the input table.',
    example: 'max()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/max',
  },
  {
    name: 'mean',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to compute the mean. Defaults to `["_value"]`',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes the mean or average of non-null records in the input table.',
    example: 'mean(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/mean',
  },
  {
    name: 'median',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to compute the mean. Defaults to `["_value"]`',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Returns the median `_value` of an input table. The `median()` function can only be used with float value types.',
    example: 'median()',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/median',
  },
  {
    name: 'min',
    args: [],
    desc: 'Selects record with the lowest `_value` from the input table.',
    example: 'min()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/min',
  },
  {
    name: 'pearsonr',
    args: [
      {
        name: 'x',
        desc: 'First input stream used in the operation.',
        type: 'Object',
      },
      {
        name: 'y',
        desc: 'Second input stream used in the operation.',
        type: 'Object',
      },
      {
        name: 'on',
        desc: 'List of columns on which to join.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes the Pearson R correlation coefficient between two streams by first joining the streams, then performing the covariance operation normalized to compute R.',
    example: 'pearsonr(x: table1, y: table2, on: ["_time", "_field"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/pearsonr',
  },
  {
    name: 'percentile',
    args: [
      {
        name: 'columns',
        desc:
          'A list of columns on which to compute the percentile. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'percentile',
        desc: 'A value between 0 and 1 indicating the desired percentile.',
        type: 'Float',
      },
      {
        name: 'method',
        desc:
          'Defines the method of computation. The available options are: `estimate_tdigest`, `exact_mean`, or `exact_selector`.',
        type: 'String',
      },
      {
        name: 'compression',
        desc:
          'Indicates how many centroids to use when compressing the dataset. A larger number produces a more accurate result at the cost of increased memory requirements. Defaults to 1000.',
        type: 'Float',
      },
    ],
    desc:
      'This is both an aggregate and selector function depending on the `method` used. When using the `estimate_tdigest` or `exact_mean` methods, it outputs non-null records with values that fall within the specified percentile. When using the `exact_selector` method, it outputs the non-null record with the value that represents the specified percentile.',
    example:
      'percentile(columns: ["_value"], percentile: 0.99, method: "estimate_tdigest", compression: 1000)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/percentile',
  },
  {
    name: 'pivot',
    args: [
      {
        name: 'rowKey',
        desc: 'List of columns used to uniquely identify a row for the output.',
        type: 'Array of Strings',
      },
      {
        name: 'columnKey',
        desc:
          'List of columns used to pivot values onto each row identified by the rowKey.',
        type: 'Array of Strings',
      },
      {
        name: 'valueColumn',
        desc:
          'The single column that contains the value to be moved around the pivot.',
        type: 'String',
      },
    ],
    desc:
      'Collects values stored vertically (column-wise) in a table and aligns them horizontally (row-wise) into logical sets.',
    example:
      'pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/pivot',
  },
  {
    name: 'range',
    args: [
      {
        name: 'start',
        desc: 'Specifies the oldest time to be included in the results.',
        type: 'Duration',
      },
      {
        name: 'stop',
        desc:
          'Specifies the exclusive newest time to be included in the results. Defaults to `now`.',
        type: 'Duration',
      },
    ],
    desc:
      "Filters records based on time bounds. Each input table's records are filtered to contain only records that exist within the time bounds. Each input table's group key value is modified to fit within the time bounds. Tables where all records exists outside the time bounds are filtered entirely.",
    example: 'range(start: -15m, stop: now)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/range',
  },
  {
    name: 'rename',
    args: [
      {
        name: 'columns',
        desc:
          'A map of columns to rename and their corresponding new names. Cannot be used with `fn`.',
        type: 'Object',
      },
      {
        name: 'fn',
        desc:
          'A function mapping between old and new column names. Cannot be used with `columns`.',
        type: 'Function',
      },
    ],
    desc:
      'Renames specified columns in a table. If a column is renamed and is part of the group key, the column name in the group key will be updated.',
    example: 'rename(columns: {host: "server", facility: "datacenter"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/rename',
  },
  {
    name: 'sample',
    args: [
      {
        name: 'n',
        desc: 'Sample every Nth element.',
        type: 'Integer',
      },
      {
        name: 'pos',
        desc:
          'The position offset from the start of results where sampling begins. `pos` must be less than `n`. If `pos` is less than 0, a random offset is used. Defaults to `-1` (random offset).',
        type: 'Integer',
      },
    ],
    desc: 'Selects a subset of the records from the input table.',
    example: 'sample(n:5, pos: -1)',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/sample',
  },
  {
    name: 'set',
    args: [
      {
        name: 'key',
        desc: 'The label of the column to modify or set.',
        type: 'String',
      },
      {
        name: 'value',
        desc: 'The string value to set.',
        type: 'String',
      },
    ],
    desc:
      'Assigns a static value to each record in the input table. The key may modify an existing column or add a new column to the tables. If the modified column is part of the group key, the output tables are regrouped as needed.',
    example: 'set(key: "myKey", value: "myValue")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/set',
  },
  {
    name: 'shift',
    args: [
      {
        name: 'shift',
        desc:
          'The amount of time to add to each time value. The shift may be a negative duration.',
        type: 'String',
      },
      {
        name: 'columns',
        desc:
          'The list of all columns to be shifted. Defaults to `["_start", "_stop", "_time"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Adds a fixed duration to time columns. The output table schema is the same as the input table.',
    example: 'shift(shift: 10h, columns: ["_start", "_stop", "_time"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/shift',
  },
  {
    name: 'skew',
    args: [
      {
        name: 'columns',
        desc:
          'Specifies a list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc: 'Outputs the skew of non-null records as a float.',
    example: 'skew(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/skew',
  },
  {
    name: 'sort',
    args: [
      {
        name: 'columns',
        desc:
          'List of columns by which to sort. Sort precedence is determined by list order (left to right). Default is `["_value"]`.',
        type: 'Array of Strings',
      },
      {
        name: 'desc',
        desc: 'Sort results in descending order. Default is `false`.',
        type: 'Boolean',
      },
    ],
    desc:
      'Orders the records within each table. One output table is produced for each input table. The output tables will have the same schema as their corresponding input tables.',
    example: 'sort(columns: ["_value"], desc: false)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/sort',
  },
  {
    name: 'spread',
    args: [
      {
        name: 'columns',
        desc:
          'Specifies a list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Outputs the difference between the minimum and maximum values in each specified column. Only `uint`, `int`, and `float` column types can be used.',
    example: 'spread(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/spread',
  },
  {
    name: 'stateCount',
    args: [
      {
        name: 'fn',
        desc:
          'A single argument function that evaluates true or false to identify the state of the record.',
        type: 'Function',
      },
      {
        name: 'column',
        desc:
          'The name of the column added to each record that contains the incremented state count.',
        type: 'String',
      },
    ],
    desc:
      'Computes the number of consecutive records in a given state and stores the increment in a new column.',
    example: 'stateCount(fn: (r) => r._field == "state", column: "stateCount")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/statecount',
  },
  {
    name: 'stateDuration',
    args: [
      {
        name: 'fn',
        desc:
          'A single argument function that evaluates true or false to identify the state of the record.',
        type: 'Function',
      },
      {
        name: 'column',
        desc:
          'Name of the column added to each record that contains the incremented state duration.',
        type: 'String',
      },
      {
        name: 'unit',
        desc: 'Unit of time in which the state duration is incremented.',
        type: 'Duration',
      },
    ],
    desc:
      'Computes the duration of a given state and stores the increment in a new column.',
    example:
      'stateDuration(fn: (r) => r._measurement == "state", column: "stateDuration", unit: 1s)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/stateduration',
  },
  {
    name: 'stddev',
    args: [
      {
        name: 'columns',
        desc:
          'Specifies a list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Computes the standard deviation of non-null records in specified columns.',
    example: 'stddev(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/stddev',
  },
  {
    name: 'sum',
    args: [
      {
        name: 'columns',
        desc:
          'Specifies a list of columns on which to operate. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc: 'Computes the sum of non-null records in specified columns.',
    example: 'sum(columns: ["_value"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/aggregates/sum',
  },
  {
    name: 'systemTime',
    args: [],
    desc: 'Returns the current system time.',
    example: 'systemTime()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/misc/systemtime',
  },
  {
    name: 'to',
    args: [
      {
        name: 'bucket',
        desc:
          'The bucket to which data is written. Mutually exclusive with `bucketID`.',
        type: 'String',
      },
      {
        name: 'bucketID',
        desc:
          'The ID of the bucket to which data is written. Mutually exclusive with `bucket`.',
        type: 'String',
      },
      {
        name: 'org',
        desc:
          'The organization name of the specified `bucket`. Only required when writing to a remote host. Mutually exclusive with `orgID`.',
        type: 'String',
      },
      {
        name: 'orgID',
        desc:
          'The organization ID of the specified `bucket`. Only required when writing to a remote host. Mutually exclusive with `org`.',
        type: 'String',
      },
      {
        name: 'host',
        desc:
          'The remote InfluxDB host to which to write. If specified, a `token` is required.',
        type: 'String',
      },
      {
        name: 'token',
        desc:
          'The authorization token to use when writing to a remote host. Required when a `host` is specified.',
        type: 'String',
      },
      {
        name: 'timeColumn',
        desc: 'The time column of the output. Default is `"_time"`.',
        type: 'String',
      },
      {
        name: 'tagColumns',
        desc:
          'The tag columns of the output. Defaults to all columns with type `string`, excluding all value columns and the `_field` column if present.',
        type: 'Array of Strings',
      },
      {
        name: 'fieldFn',
        desc:
          'Function that takes a record from the input table and returns an object. For each record from the input table, `fieldFn` returns an object that maps output the field key to the output value. Default is `(r) => ({ [r._field]: r._value })`',
        type: 'Function',
      },
    ],
    desc: 'The `to()` function writes data to an InfluxDB v2.0 bucket.',
    example:
      'to(bucket: "my-bucket", org: "my-org", host: "http://example.com:8086", token: "xxxxxx", timeColumn: "_time", tagColumns: ["tag1", "tag2", "tag3"], fieldFn: (r) => ({ [r._field]: r._value }))',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/outputs/to',
  },
  {
    name: 'toBool',
    args: [],
    desc: 'Converts a value to a boolean.',
    example: 'toBool()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/tobool',
  },
  {
    name: 'toDuration',
    args: [],
    desc: 'Converts a value to a duration.',
    example: 'toDuration()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/toduration',
  },
  {
    name: 'toFloat',
    args: [],
    desc: 'Converts a value to a float.',
    example: 'toFloat()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/tofloat',
  },
  {
    name: 'toInt',
    args: [],
    desc: 'Converts a value to a integer.',
    example: 'toInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/toint',
  },
  {
    name: 'top',
    args: [
      {
        name: 'n',
        desc: 'Number of rows to return.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc:
          'List of columns by which to sort. Sort precedence is determined by list order (left to right). Default is `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    desc: 'Sorts a table by columns and keeps only the top n rows.',
    example: 'top(n:10, cols: ["_value"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/top',
  },
  {
    name: 'toString',
    args: [],
    desc: 'Converts a value to a string.',
    example: 'toString()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/tostring',
  },
  {
    name: 'toTime',
    args: [],
    desc: 'Converts a value to a time.',
    example: 'toTime()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/totime',
  },
  {
    name: 'toUInt',
    args: [],
    desc: 'Converts a value to an unsigned integer.',
    example: 'toUInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/type-conversions/touint',
  },
  {
    name: 'union',
    args: [
      {
        name: 'tables',
        desc:
          'Specifies the streams to union together. There must be at least two streams.',
        type: 'Array of Strings',
      },
    ],
    desc:
      'Concatenates two or more input streams into a single output stream. The output schemas of the `union()` function is the union of all input schemas. A sort operation may be added if a specific sort order is needed.',
    example: 'union(tables: ["table1", "table2"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/union',
  },
  {
    name: 'unique',
    args: [
      {
        name: 'column',
        desc: 'The column searched for unique values. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    desc: 'Returns all rows containing unique values in a specified column.',
    example: 'unique(column: "_value")',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/selectors/unique',
  },
  {
    name: 'window',
    args: [
      {
        name: 'every',
        desc: 'Duration of time between windows. Defaults to `period` value.',
        type: 'Duration',
      },
      {
        name: 'period',
        desc:
          'Duration of the window. Period is the length of each interval. It can be negative, indicating the start and stop boundaries are reversed. Defaults to `every` value.',
        type: 'Duration',
      },
      {
        name: 'offset',
        desc:
          'The offset duration relative to the `location` offset. It can be negative, indicating that the offset goes backwards in time. The default aligns the window boundaries with `now`.',
        type: 'Duration',
      },
      {
        name: 'intervals',
        desc:
          'A function that returns an interval generator, a set of intervals used as windows. See docs for an example.',
        type: 'Function',
      },
      {
        name: 'timeCol',
        desc: 'The column containing time. Defaults to `"_time"`.',
        type: 'String',
      },
      {
        name: 'startCol',
        desc:
          'The column containing the window start time. Defaults to `"_start"`.',
        type: 'String',
      },
      {
        name: 'stopCol',
        desc:
          'The column containing the window stop time. Defaults to `"_stop"`.',
        type: 'String',
      },
    ],
    desc:
      'Groups records based on a time value. New columns are added to uniquely identify each window. Those columns are added to the group key of the output tables. A single input record will be placed into zero or more output tables, depending on the specific windowing function.',
    example:
      'window(every: 5m, period: 5m, offset: 12h, timeCol: "_time", startCol: "_start", stopCol: "_stop")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/transformations/window',
  },
  {
    name: 'yield',
    args: [
      {
        name: 'name',
        desc: 'A unique name for the yielded results.',
        type: 'String',
      },
    ],
    desc:
      'Indicates the input tables received should be delivered as a result of the query. Yield outputs the input stream unmodified. A query may have multiple results, each identified by the name provided to the `yield()` function.',
    example: 'yield(name: "custom-name")',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/outputs/yield',
  },
]
