import {FluxToolbarFunction} from 'src/types/shared'

export const FROM: FluxToolbarFunction = {
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
  package: '',
  desc:
    'Used to retrieve data from an InfluxDB data source. It returns a stream of tables from the specified bucket. Each unique series is contained within its own table. Each record in the table represents a single point in the series.',
  example: 'from(bucket: "default")',
  category: 'Inputs',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/inputs/from/',
}

export const RANGE: FluxToolbarFunction = {
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
  package: '',
  desc:
    "Filters records based on time bounds. Each input table's records are filtered to contain only records that exist within the time bounds. Each input table's group key value is modified to fit within the time bounds. Tables where all records exists outside the time bounds are filtered entirely.",
  example: 'range(start: -15m, stop: now)',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/range/',
}

export const MEAN: FluxToolbarFunction = {
  name: 'mean',
  args: [
    {
      name: 'column',
      desc: 'The column on which to compute the mean. Defaults to `"_value"`',
      type: 'String',
    },
  ],
  package: '',
  desc: 'Computes the mean or average of non-null records in the input table.',
  example: 'mean(column: "_value")',
  category: 'Aggregates',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/mean/',
}

export const UNION: FluxToolbarFunction = {
  name: 'union',
  args: [
    {
      name: 'tables',
      desc:
        'Specifies the streams to union together. There must be at least two streams.',
      type: 'Array of Strings',
    },
  ],
  package: '',
  desc:
    'Concatenates two or more input streams into a single output stream. The output schemas of the `union()` function is the union of all input schemas. A sort operation may be added if a specific sort order is needed.',
  example: 'union(tables: [table1, table2])',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/union/',
}

export const MATH_ABS: FluxToolbarFunction = {
  name: 'math.abs',
  args: [
    {
      name: 'x',
      desc: 'The value used in the operation.',
      type: 'Float',
    },
  ],
  package: 'math',
  desc: 'Returns the absolute value of x.',
  example: 'math.abs(x: r._value)',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/abs/',
}

export const MATH_FLOOR: FluxToolbarFunction = {
  name: 'math.floor',
  args: [
    {
      name: 'x',
      desc: 'The value used in the operation.',
      type: 'Float',
    },
  ],
  package: 'math',
  desc: 'Returns the greatest integer value less than or equal to x.',
  example: 'math.floor(x: r._value)',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/floor/',
}

export const STRINGS_TITLE: FluxToolbarFunction = {
  name: 'strings.title',
  args: [
    {
      name: 'v',
      desc: 'The string value to convert.',
      type: 'String',
    },
  ],
  package: 'strings',
  desc: 'Converts a string to title case.',
  example: 'strings.title(v: r._value)',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/title/',
}

export const STRINGS_TRIM: FluxToolbarFunction = {
  name: 'strings.trim',
  args: [
    {
      name: 'v',
      desc: 'The string value to trim.',
      type: 'String',
    },
    {
      name: 'cutset',
      desc:
        'The leading and trailing characters to trim from the string value. Only characters that match exactly are trimmed.',
      type: 'String',
    },
  ],
  package: 'strings',
  desc: 'Removes specified leading and trailing characters from a string.',
  example: 'strings.trim(v: r._value, cutset: "_")',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/trim/',
}

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
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
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
    package: '',
    desc: 'Applies an aggregate function to fixed windows of time.',
    example: 'aggregateWindow(every: 1m, fn: mean)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/aggregatewindow/',
  },
  {
    name: 'bool',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, UInteger, Float',
      },
    ],
    package: '',
    desc: 'Converts a single value to a boolean.',
    example: 'bool(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/bool/',
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
        name: 'columns',
        desc:
          'List of columns by which to sort. Sort precedence is determined by list order (left to right) .Default is `["_value"]`',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc: 'Sorts a table by columns and keeps only the bottom n rows.',
    example: 'bottom(n:10, columns: ["_value"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/bottom/',
  },
  {
    name: 'buckets',
    args: [],
    package: '',
    desc: 'Returns a list of buckets in the organization.',
    example: 'buckets()',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/inputs/buckets/',
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
    package: '',
    desc: 'Lists the column labels of input tables.',
    example: 'columns(column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/columns/',
  },
  {
    name: 'contains',
    args: [
      {
        name: 'value',
        desc: 'The value to search for.',
        type: 'Boolean, Integer, UInteger, Float, String, Time',
      },
      {
        name: 'set',
        desc: 'The set of values in which to search.',
        type: 'Boolean, Integer, UInteger, Float, String, Time',
      },
    ],
    package: '',
    desc: 'Tests whether a value is a member of a set.',
    example: 'contains(value: 1, set: [1,2,3])',
    category: 'Test',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/tests/contains/',
  },
  {
    name: 'count',
    args: [
      {
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Outputs the number of records in the specified column.',
    example: 'count(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/count/',
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
    package: '',
    desc:
      'Computes the covariance between two streams by first joining the streams, then performing the covariance operation.',
    example:
      'cov(x: table1, y: table2, on: ["_time", "_field"], pearsonr: false)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/cov/',
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
    package: '',
    desc: 'Computes the covariance between two columns.',
    example:
      'covariance(columns: ["column_x", "column_y"], pearsonr: false, valueDst: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/covariance/',
  },
  {
    name: 'csv.from',
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
    package: 'csv',
    desc: 'Retrieves data from a comma-separated value (CSV) data source.',
    example: 'csv.from(file: "/path/to/data-file.csv")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/csv/from/',
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
    package: '',
    desc:
      'Computes a running sum for non-null records in the table. The output table schema will be the same as the input table.',
    example: 'cumulativeSum(columns: ["_value"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/cumulativesum/',
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
    package: '',
    desc:
      'Computes the rate of change per unit of time between subsequent non-null records. The output table schema will be the same as the input table.',
    example:
      'derivative(unit: 1s, nonNegative: true, columns: ["_value"], timeColumn: "_time")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/derivative/',
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
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Computes the difference between subsequent records in the specified column.',
    example: 'difference(nonNegative: false, column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/difference/',
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
    package: '',
    desc: 'Returns the unique values for a given column.',
    example: 'distinct(column: "host")',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/distinct/',
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
    package: '',
    desc:
      'Removes specified columns from a table. Columns can be specified either through a list or a predicate function. When a dropped column is part of the group key, it will be removed from the key.',
    example: 'drop(columns: ["col1", "col2"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/drop/',
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
    package: '',
    desc: 'Duplicates a specified column in a table.',
    example: 'duplicate(column: "column-name", as: "duplicate-name")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/duplicate/',
  },
  {
    name: 'duration',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Converts a single value to a duration.',
    example: 'duration(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/duration/',
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
    package: '',
    desc:
      'replaces all null values in an input stream and replace them with a non-null value.',
    example: 'fill(column: "_value", usePrevious: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/fill/',
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
    package: '',
    desc:
      'Filters data based on conditions defined in the function. The output tables have the same schema as the corresponding input tables.',
    example: 'filter(fn: (r) => r._measurement == "cpu")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/filter/',
  },
  {
    name: 'first',
    args: [],
    package: '',
    desc: 'Selects the first non-null record from an input table.',
    example: 'first()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/first/',
  },
  {
    name: 'float',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, UInteger, Boolean',
      },
    ],
    package: '',
    desc: 'Converts a single value to a float.',
    example: 'float(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/float/',
  },
  FROM,
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
    package: '',
    desc:
      'Groups records based on their values for specific columns. It produces tables with new group keys based on provided properties.',
    example: 'group(columns: ["host", "_measurement"], mode:"by")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/group/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the top `n` records from all groups using the average of each group.',
    example: 'highestAverage(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/highestaverage/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the top `n` records from all groups using the last value of each group.',
    example: 'highestCurrent(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/highestcurrent/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the top `n` records from all groups using the maximum of each group.',
    example: 'highestMax(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/highestmax/',
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
    package: '',
    desc:
      'Approximates the cumulative distribution function of a dataset by counting data frequencies for a list of buckets.',
    example:
      'histogram(column: "_value", upperBoundColumn: "le", countColumn: "_value", bins: [50.0, 75.0, 90.0], normalize: false)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/histogram/',
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
    package: '',
    desc:
      'Approximates a quantile given a histogram that approximates the cumulative distribution of the dataset.',
    example:
      'histogramQuantile(quantile: 0.5, countColumn: "_value", upperBoundColumn: "le", valueColumn: "_value", minValue: 0)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/histogramquantile/',
  },
  {
    name: 'increase',
    args: [
      {
        name: 'column',
        desc:
          'The column for which the increase is calculated. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Computes the total non-negative difference between values in a table.',
    example: 'increase(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/increase/',
  },
  {
    name: 'int',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, UInteger, Float, Boolean',
      },
    ],
    package: '',
    desc: 'Converts a single value to a integer.',
    example: 'int(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/int/',
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
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Computes the area under the curve per unit of time of subsequent non-null records. The curve is defined using `_time` as the domain and record values as the range.',
    example: 'integral(unit: 10s, column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/integral/',
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
    package: '',
    desc: 'Generates a set of time intervals over a range of time.',
    example: 'intervals()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/misc/intervals/',
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
    package: '',
    desc:
      'Merges two or more input streams, whose values are equal on a set of common columns, into a single output stream. The resulting schema is the union of the input schemas. The resulting group key is the union of the input group keys.',
    example:
      'join(tables: {key1: table1, key2: table2}, on: ["_time", "_field"], method: "inner")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/join/',
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
    package: '',
    desc:
      'Returns a table containing only the specified columns, ignoring all others. Only columns in the group key that are also specified in the `keep()` function will be kept in the resulting group key. It is the inverse of `drop`.',
    example: 'keep(columns: ["col1", "col2"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/keep/',
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
    package: '',
    desc:
      "Outputs the group key of input tables. For each input table, it outputs a table with the same group key columns, plus a _value column containing the labels of the input table's group key.",
    example: 'keys(column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/keys/',
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
    package: '',
    desc:
      "Returns a table with the input table's group key plus two columns, `_key` and `_value`, that correspond to unique column + value pairs from the input table.",
    example: 'keyValues(keyColumns: ["usage_idle", "usage_user"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/keyvalues/',
  },
  {
    name: 'last',
    args: [],
    package: '',
    desc: 'Selects the last non-null record from an input table.',
    example: 'last()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/last/',
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
    package: '',
    desc:
      'Limits the number of records in output tables to a fixed number `n` records after the `offset`. If the input table has less than `n` records, all records are be output.',
    example: 'limit(n:10, offset: 0)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/limit/',
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
    package: '',
    desc: 'Generates a list of linearly separated floats.',
    example: 'linearBins(start: 0.0, width: 5.0, count: 20, infinity: true)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/misc/linearbins/',
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
    package: '',
    desc: 'Generates a list of exponentially separated floats.',
    example:
      'logarithmicBins(start: 1.0, factor: 2.0, count: 10, infinty: true)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/misc/logarithmicbins/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the bottom `n` records from all groups using the average of each group.',
    example: 'lowestAverage(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/lowestaverage/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the bottom `n` records from all groups using the last value of each group.',
    example: 'lowestCurrent(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/lowestcurrent/',
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
        name: 'column',
        desc: 'Column by which to sort. Default is `"_value"`.',
        type: 'String',
      },
      {
        name: 'groupColumns',
        desc:
          'The columns on which to group before performing the aggregation. Default is `[]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Returns the bottom `n` records from all groups using the maximum of each group.',
    example: 'lowestMin(n:10, groupColumns: ["host"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/lowestmin/',
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
    package: '',
    desc: 'Applies a function to each record in the input tables.',
    example: 'map(fn: (r) => r._value * r._value, mergeKey: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/map/',
  },
  MATH_ABS,
  {
    name: 'math.acos',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the arccosine of x in radians.',
    example: 'math.acos(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/acos/',
  },
  {
    name: 'math.acosh',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation. Should be greater than 1.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the inverse hyperbolic cosine of x.',
    example: 'math.acosh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/acosh/',
  },
  {
    name: 'math.asin',
    args: [
      {
        name: 'x',
        desc:
          'The value used in the operation. Should be greater than -1 and less than 1.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the arcsine of x in radians.',
    example: 'math.asin(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/asin/',
  },
  {
    name: 'math.asinh',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the inverse hyperbolic sine of x.',
    example: 'math.asinh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/asinh/',
  },
  {
    name: 'math.atan',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the arctangent of x in radians.',
    example: 'math.atan(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/atan/',
  },
  {
    name: 'math.atan2',
    args: [
      {
        name: 'y',
        desc: 'The y coordinate used in the operation.',
        type: 'Float',
      },
      {
        name: 'x',
        desc: 'The x coordinate used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns the arc tangent of y/x, using the signs of the two to determine the quadrant of the return value.',
    example: 'math.atan2(y: r.y_coord, x: r.x_coord)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/atan2/',
  },
  {
    name: 'math.atanh',
    args: [
      {
        name: 'x',
        desc:
          'The value used in the operation. Should be greater than -1 and less than 1.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the inverse hyperbolic tangent of x.',
    example: 'math.atanh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/atanh/',
  },
  {
    name: 'math.cbrt',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the cube root of x.',
    example: 'math.cbrt(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/cbrt/',
  },
  {
    name: 'math.ceil',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the least integer value greater than or equal to x.',
    example: 'math.ceil(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/ceil/',
  },
  {
    name: 'math.copysign',
    args: [
      {
        name: 'x',
        desc: 'The magnitude used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The sign used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns a value with the magnitude of x and the sign of y.',
    example: 'math.copysign(x: r._magnitude, r._sign)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/copysign/',
  },
  {
    name: 'math.cos',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the cosine of the radian argument x.',
    example: 'math.cos(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/cos/',
  },
  {
    name: 'math.cosh',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the hyperbolic cosine of x.',
    example: 'math.cosh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/cosh/',
  },
  {
    name: 'math.dim',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the maximum of (x - y) or 0.',
    example: 'math.dim(x: r._value1, y: r._value2)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/dim/',
  },
  {
    name: 'math.erf',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the error function of x.',
    example: 'math.erf(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/erf/',
  },
  {
    name: 'math.erfc',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the complementary error function of x.',
    example: 'math.erfc(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/erfc/',
  },
  {
    name: 'math.erfcinv',
    args: [
      {
        name: 'x',
        desc:
          'The value used in the operation. Should be greater than 0 and less than 2.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the inverse of `math.erfc()`.',
    example: 'math.erfcinv(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/erfcinv/',
  },
  {
    name: 'math.erfinv',
    args: [
      {
        name: 'x',
        desc:
          'The value used in the operation. Should be greater than -1 and less than 1.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the inverse error function of x.',
    example: 'math.erfinv(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/erfinv/',
  },
  {
    name: 'math.exp',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the base-e exponential of x (`e**x`).',
    example: 'math.exp(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/exp/',
  },
  {
    name: 'math.exp2',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the base-2 exponential of x (`2**x`).',
    example: 'math.exp2(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/exp2/',
  },
  {
    name: 'math.expm1',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the base-e exponential of x minus 1 (`e**x - 1`).',
    example: 'math.expm1(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/expm1/',
  },
  {
    name: 'math.float64bits',
    args: [
      {
        name: 'f',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns the IEEE 754 binary representation of f, with the sign bit of f and the result in the same bit position.',
    example: 'math.float64bits(f: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/float64bits/',
  },
  MATH_FLOOR,
  {
    name: 'math.frexp',
    args: [
      {
        name: 'f',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Breaks f into a normalized fraction and an integral power of two.',
    example: 'math.frexp(f: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/frexp/',
  },
  {
    name: 'math.gamma',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the Gamma function of x.',
    example: 'math.gamma(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/gamma/',
  },
  {
    name: 'math.hypot',
    args: [
      {
        name: 'p',
        desc: 'The p value used in the operation.',
        type: 'Float',
      },
      {
        name: 'q',
        desc: 'The q value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns the square root of `p*p + q*q`, taking care to avoid overflow and underflow.',
    example: 'math.hypot(p: r.opp, p: r.adj)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/hypot/',
  },
  {
    name: 'math.ilogb',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the binary exponent of x as an integer.',
    example: 'math.ilogb(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/ilogb/',
  },
  {
    name: 'math.isInf',
    args: [
      {
        name: 'f',
        desc: 'The value used in the evaluation.',
        type: 'Float',
      },
      {
        name: 'sign',
        desc: 'The sign used in the evaluation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Reports whether f is an infinity, according to sign.',
    example: 'math.isInf(f: r._value, sign: r.sign)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/isinf/',
  },
  {
    name: 'math.isNaN',
    args: [
      {
        name: 'f',
        desc: 'The value used in the evaluation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Reports whether f is an IEEE 754 NaN value.',
    example: 'math.isNaN(f: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/isnan/',
  },
  {
    name: 'math.j0',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-zero Bessel function of the first kind.',
    example: 'math.j0(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/j0/',
  },
  {
    name: 'math.j1',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-one Bessel function of the first kind.',
    example: 'math.j1(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/j1/',
  },
  {
    name: 'math.jn',
    args: [
      {
        name: 'n',
        desc: 'The order number.',
        type: 'Float',
      },
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-n Bessel function of the first kind.',
    example: 'math.jn(n: 2, x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/jn/',
  },
  {
    name: 'math.ldexp',
    args: [
      {
        name: 'frac',
        desc: 'The fraction used in the operation.',
        type: 'Float',
      },
      {
        name: 'exp',
        desc: 'The exponent used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns `frac × 2**exp`. It is the inverse of `math.frexp()`.',
    example: 'math.ldexp(frac: r.frac, exp: r.exp)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/ldexp/',
  },
  {
    name: 'math.lgamma',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns the natural logarithm and sign (-1 or +1) of `math.gamma(x:x)`.',
    example: 'math.lgamma(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/lgamma/',
  },
  {
    name: 'math.log',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the natural logarithm of x.',
    example: 'math.log(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/log/',
  },
  {
    name: 'math.log1p',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the natural logarithm of 1 plus its argument x.',
    example: 'math.log1p(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/log1p/',
  },
  {
    name: 'math.log2',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the binary logarithm of x.',
    example: 'math.log2(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/log2/',
  },
  {
    name: 'math.logb',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the binary exponent of x.',
    example: 'math.logb(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/logb/',
  },
  {
    name: 'math.m_inf',
    args: [
      {
        name: 'sign',
        desc: 'The sign value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns positive infinity if `sign >= 0`, negative infinity if `sign < 0`.',
    example: 'math.m_inf(sign: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/m_inf/',
  },
  {
    name: 'math.m_max',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the larger of x or y.',
    example: 'math.m_max(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/m_max/',
  },
  {
    name: 'math.m_min',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the smaller of x or y.',
    example: 'math.m_min(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/m_min/',
  },
  {
    name: 'math.mod',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the floating-point remainder of x/y.',
    example: 'math.mod(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/mod/',
  },
  {
    name: 'math.modf',
    args: [
      {
        name: 'f',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc:
      'Returns integer and fractional floating-point numbers that sum to f. Both values have the same sign as f.',
    example: 'math.modf(f: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/modf/',
  },
  {
    name: 'math.NaN',
    args: [],
    package: 'math',
    desc: 'Returns an IEEE 754 NaN value.',
    example: 'math.NaN()',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/nan/',
  },
  {
    name: 'math.nextafter',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the next representable float value after x towards y.',
    example: 'math.nextafter(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/nextafter/',
  },
  {
    name: 'math.pow',
    args: [
      {
        name: 'x',
        desc: 'The X value used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The Y value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the base-x exponential of y, `x**y`.',
    example: 'math.pow(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/pow/',
  },
  {
    name: 'math.pow10',
    args: [
      {
        name: 'n',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the base-10 exponential of n, `10**n`.',
    example: 'math.pow10(n: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/pow10/',
  },
  {
    name: 'math.remainder',
    args: [
      {
        name: 'x',
        desc: 'The numerator used in the operation.',
        type: 'Float',
      },
      {
        name: 'y',
        desc: 'The denominator used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the IEEE 754 floating-point remainder of `x / y`.',
    example: 'math.remainder(x: r.numerator, y: r.denominator)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/remainder/',
  },
  {
    name: 'math.round',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the nearest integer, rounding half away from zero.',
    example: 'math.round(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/round/',
  },
  {
    name: 'math.roundtoeven',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the nearest integer, rounding ties to even.',
    example: 'math.roundtoeven(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/roundtoeven/',
  },
  {
    name: 'math.signbit',
    args: [
      {
        name: 'x',
        desc: 'The value used in the evaluation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Reports whether x is negative or negative zero.',
    example: 'math.signbit(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/signbit/',
  },
  {
    name: 'math.sin',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the sine of the radian argument x.',
    example: 'math.sin(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/sin/',
  },
  {
    name: 'math.sincos',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the values of `math.sin(x:x)` and `math.cos(x:x)`.',
    example: 'math.sincos(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/sincos/',
  },
  {
    name: 'math.sinh',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the hyperbolic sine of x.',
    example: 'math.sinh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/sinh/',
  },
  {
    name: 'math.sqrt',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the square root of x.',
    example: 'math.sqrt(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/sqrt/',
  },
  {
    name: 'math.tan',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the tangent of the radian argument x.',
    example: 'math.tan(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/tan/',
  },
  {
    name: 'math.tanh',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the hyperbolic tangent of x.',
    example: 'math.tanh(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/tanh/',
  },
  {
    name: 'math.trunc',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the integer value of x.',
    example: 'math.trunc(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/trunc/',
  },
  {
    name: 'math.y0',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-zero Bessel function of the second kind.',
    example: 'math.y0(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/y0/',
  },
  {
    name: 'math.y1',
    args: [
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-one Bessel function of the second kind.',
    example: 'math.y1(x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/y1/',
  },
  {
    name: 'math.yn',
    args: [
      {
        name: 'n',
        desc: 'The order number used in the operation.',
        type: 'Float',
      },
      {
        name: 'x',
        desc: 'The value used in the operation.',
        type: 'Float',
      },
    ],
    package: 'math',
    desc: 'Returns the order-n Bessel function of the second kind.',
    example: 'math.yn(n: 3, x: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/math/yn/',
  },
  {
    name: 'max',
    args: [],
    package: '',
    desc: 'Selects record with the highest `_value` from the input table.',
    example: 'max()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/max/',
  },
  MEAN,
  {
    name: 'median',
    args: [
      {
        name: 'column',
        desc: 'The column on which to compute the mean. Defaults to `"_value"`',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Returns the median `_value` of an input table. The `median()` function can only be used with float value types.',
    example: 'median(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/median/',
  },
  {
    name: 'min',
    args: [],
    package: '',
    desc: 'Selects record with the lowest `_value` from the input table.',
    example: 'min()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/min/',
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
    package: '',
    desc:
      'Computes the Pearson R correlation coefficient between two streams by first joining the streams, then performing the covariance operation normalized to compute R.',
    example: 'pearsonr(x: table1, y: table2, on: ["_time", "_field"])',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/pearsonr/',
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
    package: '',
    desc:
      'Collects values stored vertically (column-wise) in a table and aligns them horizontally (row-wise) into logical sets.',
    example:
      'pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/pivot/',
  },
  {
    name: 'quantile',
    args: [
      {
        name: 'column',
        desc:
          'The column on which to compute the quantile. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'quantile',
        desc: 'A value between 0 and 1 indicating the desired quantile.',
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
    package: '',
    desc:
      'This is both an aggregate and selector function depending on the `method` used. When using the `estimate_tdigest` or `exact_mean` methods, it outputs non-null records with values that fall within the specified quantile. When using the `exact_selector` method, it outputs the non-null record with the value that represents the specified quantile.',
    example:
      'quantile(column: "_value", quantile: 0.99, method: "estimate_tdigest", compression: 1000)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/quantile/',
  },
  RANGE,
  {
    name: 'reduce',
    args: [
      {
        name: 'fn',
        desc:
          'Function to apply to each record with a reducer object. The function expects two objects: `r` and `accumulator`.',
        type: 'Function',
      },
      {
        name: 'identity',
        desc:
          'Defines the reducer object and provides initial values to use when creating a reducer.',
        type: 'Object',
      },
    ],
    package: '',
    desc: 'Aggregates records in each table according to the reducer, `fn`',
    example:
      'reduce(fn: (r, accumulator) => ({ sum: r._value + accumulator.sum }), identity: {sum: 0.0})',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/reduce/',
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
    package: '',
    desc:
      'Renames specified columns in a table. If a column is renamed and is part of the group key, the column name in the group key will be updated.',
    example: 'rename(columns: {host: "server", facility: "datacenter"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/rename/',
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
    package: '',
    desc: 'Selects a subset of the records from the input table.',
    example: 'sample(n:5, pos: -1)',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/sample/',
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
    package: '',
    desc:
      'Assigns a static value to each record in the input table. The key may modify an existing column or add a new column to the tables. If the modified column is part of the group key, the output tables are regrouped as needed.',
    example: 'set(key: "myKey", value: "myValue")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/set/',
  },
  {
    name: 'skew',
    args: [
      {
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Outputs the skew of non-null records as a float.',
    example: 'skew(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/skew/',
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
    package: '',
    desc:
      'Orders the records within each table. One output table is produced for each input table. The output tables will have the same schema as their corresponding input tables.',
    example: 'sort(columns: ["_value"], desc: false)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/sort/',
  },
  {
    name: 'spread',
    args: [
      {
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Outputs the difference between the minimum and maximum values in the specified column. Only `uint`, `int`, and `float` column types can be used.',
    example: 'spread(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/spread/',
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
    package: '',
    desc:
      'Computes the number of consecutive records in a given state and stores the increment in a new column.',
    example: 'stateCount(fn: (r) => r._field == "state", column: "stateCount")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/statecount/',
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
    package: '',
    desc:
      'Computes the duration of a given state and stores the increment in a new column.',
    example:
      'stateDuration(fn: (r) => r._measurement == "state", column: "stateDuration", unit: 1s)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/stateduration/',
  },
  {
    name: 'stddev',
    args: [
      {
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
      {
        name: 'mode',
        desc:
          'The standard deviation mode (sample or population). Defaults to `"sample"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Computes the standard deviation of non-null records in specified column.',
    example: 'stddev(column: "_value", mode: "sample")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/stddev/',
  },
  {
    name: 'string',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'Integer, UInteger, Float, Boolean, Duration, Time',
      },
    ],
    package: '',
    desc: 'Converts a single value to a string.',
    example: 'string(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/string/',
  },
  STRINGS_TITLE,
  {
    name: 'strings.toLower',
    args: [
      {
        name: 'v',
        desc: 'The string value to convert.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Converts a string to lower case.',
    example: 'strings.toLower(v: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/tolower/',
  },
  {
    name: 'strings.toUpper',
    args: [
      {
        name: 'v',
        desc: 'The string value to convert.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Converts a string to upper case.',
    example: 'strings.toUpper(v: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/toupper/',
  },
  STRINGS_TRIM,
  {
    name: 'strings.trimPrefix',
    args: [
      {
        name: 'v',
        desc: 'The string value to trim.',
        type: 'String',
      },
      {
        name: 'prefix',
        desc: 'The prefix to remove.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Removes a prefix from a string. Strings that do not start with the prefix are returned unchanged.',
    example: 'strings.trimPrefix(v: r._value, prefix: "abc_")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/trimprefix/',
  },
  {
    name: 'strings.trimSpace',
    args: [
      {
        name: 'v',
        desc: 'The string value to trim.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Removes leading and trailing spaces from a string.',
    example: 'strings.trimSpace(v: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/trimspace/',
  },
  {
    name: 'strings.trimSuffix',
    args: [
      {
        name: 'v',
        desc: 'The string value to trim.',
        type: 'String',
      },
      {
        name: 'suffix',
        desc: 'The suffix to remove.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Removes a suffix from a string. Strings that do not end with the suffix are returned unchanged.',
    example: 'strings.trimSuffix(v: r._value, suffix: "_123")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/strings/trimsuffix/',
  },
  {
    name: 'sum',
    args: [
      {
        name: 'column',
        desc: 'The column on which to operate. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Computes the sum of non-null records in the specified column.',
    example: 'sum(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/aggregates/sum/',
  },
  {
    name: 'systemTime',
    args: [],
    package: '',
    desc: 'Returns the current system time.',
    example: 'systemTime()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/misc/systemtime/',
  },
  {
    name: 'testing.assertEmpty',
    args: [],
    package: 'testing',
    desc: 'Tests if an input stream is empty.',
    example: 'testing.assertEmpty()',
    category: 'Test',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/testing/assertempty/',
  },
  {
    name: 'testing.assertEquals',
    args: [
      {
        name: 'name',
        desc: 'Unique name given to the assertion.',
        type: 'String',
      },
      {
        name: 'got',
        desc: 'The stream containing data to test.',
        type: 'Obscflect',
      },
      {
        name: 'want',
        desc: 'The stream that contains the expected data to test against.',
        type: 'Object',
      },
    ],
    package: 'testing',
    desc: 'Tests whether two streams have identical data.',
    example: 'testing.assertEquals(got: got, want: want)',
    category: 'Test',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/testing/assertequals/',
  },
  {
    name: 'testing.diff',
    args: [
      {
        name: 'got',
        desc: 'The stream containing data to test.',
        type: 'Obscflect',
      },
      {
        name: 'want',
        desc: 'The stream that contains the expected data to test against.',
        type: 'Object',
      },
    ],
    package: 'testing',
    desc: 'Produces a diff between two streams.',
    example: 'testing.assertEquals(got: got, want: want)',
    category: 'Test',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/testing/diff/',
  },
  {
    name: 'time',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, UInteger',
      },
    ],
    package: '',
    desc: 'Converts a single value to a time.',
    example: 'time(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/time/',
  },
  {
    name: 'timeShift',
    args: [
      {
        name: 'duration',
        desc:
          'The amount of time to add to each time value. May be a negative duration.',
        type: 'String',
      },
      {
        name: 'columns',
        desc:
          'The list of all columns to be shifted. Defaults to `["_start", "_stop", "_time"]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Adds a fixed duration to time columns. The output table schema is the same as the input table.',
    example: 'timeShift(duration: 10h, columns: ["_start", "_stop", "_time"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/shift/',
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
    package: '',
    desc: 'The `to()` function writes data to an InfluxDB v2.0 bucket.',
    example:
      'to(bucket: "my-bucket", org: "my-org", host: "http://example.com:8086", token: "xxxxxx", timeColumn: "_time", tagColumns: ["tag1", "tag2", "tag3"], fieldFn: (r) => ({ [r._field]: r._value }))',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/outputs/to/',
  },
  {
    name: 'toBool',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a boolean.',
    example: 'toBool()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/tobool',
  },
  {
    name: 'toDuration',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a duration.',
    example: 'toDuration()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/toduration/',
  },
  {
    name: 'toFloat',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a float.',
    example: 'toFloat()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/tofloat/',
  },
  {
    name: 'toInt',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a integer.',
    example: 'toInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/toint/',
  },
  {
    name: 'toString',
    args: [],
    package: '',
    desc: 'Converts a value to a string.',
    example: 'toString()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/tostring/',
  },
  {
    name: 'toTime',
    args: [],
    package: '',
    desc: 'Converts a value to a time.',
    example: 'toTime()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/totime/',
  },
  {
    name: 'toUInt',
    args: [],
    package: '',
    desc: 'Converts a value to an unsigned integer.',
    example: 'toUInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/touint/',
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
    package: '',
    desc: 'Sorts a table by columns and keeps only the top n rows.',
    example: 'top(n:10, cols: ["_value"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/top/',
  },
  {
    name: 'uint',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, Boolean',
      },
    ],
    package: '',
    desc: 'Converts a single value to a uinteger.',
    example: 'uint(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/type-conversions/uint/',
  },
  UNION,
  {
    name: 'unique',
    args: [
      {
        name: 'column',
        desc: 'The column searched for unique values. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Returns all rows containing unique values in a specified column.',
    example: 'unique(column: "_value")',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/selectors/unique/',
  },
  {
    name: 'v1.fieldsAsCols',
    args: [],
    package: 'influxdata/influxdb/v1',
    desc: 'Aligns fields within each input table that have the same timestamp.',
    example: 'v1.fieldsAsCols()',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/fieldsascols/',
  },
  {
    name: 'v1.measurementTagKeys',
    args: [
      {
        name: 'bucket',
        desc:
          'The bucket from which to return tag keys for a specific measurement.',
        type: 'String',
      },
      {
        name: 'measurement',
        desc: 'The measurement from which to return tag keys.',
        type: 'String',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of tag keys for a specific measurement.',
    example: 'v1.measurementTagKeys(bucket: "default", measurement: "mem")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/measurementtagkeys/',
  },
  {
    name: 'v1.measurementTagValues',
    args: [
      {
        name: 'bucket',
        desc:
          'The bucket from which to return tag keys for a specific measurement.',
        type: 'String',
      },
      {
        name: 'measurement',
        desc: 'The measurement from which to return tag values.',
        type: 'String',
      },
      {
        name: 'tag',
        desc: 'The tag from which to return all unique values.',
        type: 'String',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of tag values for a specific measurement.',
    example:
      'v1.measurementTagValues(bucket: "default", measurement: "mem", tag: "host")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/measurementtagvalues/',
  },
  {
    name: 'v1.measurements',
    args: [
      {
        name: 'bucket',
        desc: 'The bucket from which to list measurements.',
        type: 'String',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of measurements in a specific bucket.',
    example: 'v1.measurements(bucket: "default")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/measurements/',
  },
  {
    name: 'v1.tagKeys',
    args: [
      {
        name: 'bucket',
        desc: 'The bucket from which to list tag keys.',
        type: 'String',
      },
      {
        name: 'predicate',
        desc:
          'The predicate function that filters tag keys. Defaults to `(r) => true.`',
        type: 'Function',
      },
      {
        name: 'start',
        desc:
          'Specifies the oldest time to be included in the results. Defaults to `-30d`.',
        type: 'Duration, Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of tag keys for all series that match the predicate.',
    example: 'v1.tagKeys(bucket: "default")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/tagkeys/',
  },
  {
    name: 'v1.tagValues',
    args: [
      {
        name: 'bucket',
        desc: 'The bucket from which to list tag values.',
        type: 'String',
      },
      {
        name: 'tag',
        desc: 'The tag for which to return unique values.',
        type: 'String',
      },
      {
        name: 'predicate',
        desc:
          'The predicate function that filters tag values. Defaults to `(r) => true.`',
        type: 'Function',
      },
      {
        name: 'start',
        desc:
          'Specifies the oldest time to be included in the results. Defaults to `-30d`.',
        type: 'Duration, Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of unique values for a given tag.',
    example: 'v1.tagValues(bucket: "default")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/influxdb-v1/tagvalues/',
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
        name: 'timeColumn',
        desc: 'The column containing time. Defaults to `"_time"`.',
        type: 'String',
      },
      {
        name: 'startColumn',
        desc:
          'The column containing the window start time. Defaults to `"_start"`.',
        type: 'String',
      },
      {
        name: 'stopColumn',
        desc:
          'The column containing the window stop time. Defaults to `"_stop"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Groups records based on a time value. New columns are added to uniquely identify each window. Those columns are added to the group key of the output tables. A single input record will be placed into zero or more output tables, depending on the specific windowing function.',
    example:
      'window(every: 5m, period: 5m, offset: 12h, timeColumn: "_time", startColumn: "_start", stopColumn: "_stop")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/transformations/window/',
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
    package: '',
    desc:
      'Indicates the input tables received should be delivered as a result of the query. Yield outputs the input stream unmodified. A query may have multiple results, each identified by the name provided to the `yield()` function.',
    example: 'yield(name: "custom-name")',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/functions/built-in/outputs/yield/',
  },
]
