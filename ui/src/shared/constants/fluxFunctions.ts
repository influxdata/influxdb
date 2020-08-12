import {FluxToolbarFunction} from 'src/types/shared'

export const FROM: FluxToolbarFunction = {
  name: 'from',
  args: [
    {
      name: 'bucket',
      desc: 'Name of the bucket to query.',
      type: 'String',
    },
    {
      name: 'bucketID',
      desc: 'String-encoded ID of the bucket to query.',
      type: 'String',
    },
    {
      name: 'host',
      desc:
        'URL of the InfluxDB instance to query (only required when querying a different organization or remote InfluxDB instance).',
      type: 'String',
    },
    {
      name: 'org',
      desc:
        'Organization name (only required when querying a different organization or remote InfluxDB instance).',
      type: 'String',
    },
    {
      name: 'orgID',
      desc:
        'String-encoded organization ID (only required when querying a different organization or remote InfluxDB instance).',
      type: 'String',
    },
    {
      name: 'token',
      desc:
        'InfluxDB authentication token (only required when querying a different organization or remote InfluxDB instance).',
      type: 'String',
    },
  ],
  package: '',
  desc:
    'Used to retrieve data from an InfluxDB data source. It returns a stream of tables from the specified bucket. Each unique series is contained within its own table. Each record in the table represents a single point in the series.',
  example: 'from(bucket: "example-bucket")',
  category: 'Inputs',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/inputs/from/',
}

export const RANGE: FluxToolbarFunction = {
  name: 'range',
  args: [
    {
      name: 'start',
      desc: 'The earliest time to include in results.',
      type: 'Duration | Time | Integer',
    },
    {
      name: 'stop',
      desc: 'The latest time to include in results. Defaults to `now()`.',
      type: 'Duration | Time | Integer',
    },
  ],
  package: '',
  desc:
    "Filters records based on time bounds. Each input table's records are filtered to contain only records that exist within the time bounds. Each input table's group key value is modified to fit within the time bounds. Tables where all records exists outside the time bounds are filtered entirely.",
  example: 'range(start: -15m, stop: now())',
  category: 'Transformations',
  link:
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/range/',
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
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/mean/',
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
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/union/',
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
  link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/abs/',
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
  link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/floor/',
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
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/title/',
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
    'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trim/',
}

export const FLUX_FUNCTIONS: FluxToolbarFunction[] = [
  {
    name: 'aggregate.rate',
    args: [
      {
        name: 'every',
        desc: 'Duration of windows.',
        type: 'Duration',
      },
      {
        name: 'groupColumns',
        desc: 'List of columns to group by. Defaults to [].',
        type: 'Array of Strings',
      },
      {
        name: 'unit',
        desc:
          'Time duration to use when calculating the rate. Defaults to `1s`.',
        type: 'Duration',
      },
    ],
    package: 'experimental/aggregate',
    desc: 'Calculates the rate of change per windows of time.',
    example: 'aggregate.rate(every: 1m, unit: 1s)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/aggregate/rate/',
  },
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
    example: 'aggregateWindow(every: v.windowPeriod, fn: mean)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/aggregatewindow/',
  },
  {
    name: 'array.from',
    args: [
      {
        name: 'rows',
        desc: 'Array of records to construct a table with.',
        type: 'Array of Objects',
      },
    ],
    package: 'experimental/array',
    desc: 'Constructs a table from an array of objects.',
    example:
      'array.from(rows: [{_time: 2020-01-01T00:00:00Z, _value: "foo"},{_time: 2020-01-02T00:00:00Z, _value: "bar"}])',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/array/from/',
  },
  {
    name: 'bigtable.from',
    args: [
      {
        name: 'token',
        desc:
          'The Google Cloud IAM token to use to access the Cloud Bigtable database.',
        type: 'String',
      },
      {
        name: 'project',
        desc:
          'The project ID of the Cloud Bigtable project to retrieve data from.',
        type: 'String',
      },
      {
        name: 'instance',
        desc:
          'The instance ID of the Cloud Bigtable instance to retrieve data from.',
        type: 'String',
      },
      {
        name: 'table',
        desc: 'The name of the Cloud Bigtable table to retrieve data from.',
        type: 'String',
      },
    ],
    package: 'experimental/bigtable',
    desc: 'Retrieves data from a Google Cloud Bigtable data source.',
    example:
      'bigtable.from(token: "mySuPeRseCretTokEn", project: "exampleProjectID", instance: "exampleInstanceID", table: "example-table")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/bigtable/from/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/bool/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/bottom/',
  },
  {
    name: 'buckets',
    args: [],
    package: '',
    desc: 'Returns a list of buckets in the organization.',
    example: 'buckets()',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/inputs/buckets/',
  },
  {
    name: 'bytes',
    args: [
      {
        name: 'v',
        desc: 'The value to convert.',
        type: 'String, Integer, UInteger, Float, Boolean',
      },
    ],
    package: '',
    desc: 'Converts a single value to bytes.',
    example: 'bytes(t: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/bytes/',
  },
  {
    name: 'chandeMomentumOscillator',
    args: [
      {
        name: 'n',
        desc: 'The period or number of points to use in the calculation.',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'Columns to operate on. Defaults to `["_value"]`.',
        type: 'Array of Strings`',
      },
    ],
    package: '',
    desc:
      'Applies the technical momentum indicator developed by Tushar Chande.',
    example: 'chandeMomentumOscillator(n: 10)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/chandemomentumoscillator/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/columns/',
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
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/tests/contains/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/count/',
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
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/cov/',
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
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/covariance/',
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
    example: 'csv.from(csv: csvData)',
    category: 'Inputs',
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/csv/from/',
  },
  {
    name: 'csv.from',
    args: [
      {
        name: 'url',
        desc: 'The URL to retrieve annotated CSV from.',
        type: 'String',
      },
    ],
    package: 'experimental/csv',
    desc: 'Retrieves annotated CSV data from a URL.',
    example: 'csv.from(url: "http://example.com/data.csv")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/csv/from/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/cumulativesum/',
  },
  {
    name: 'date.hour',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc: 'Returns the hour of a specified time. Results range from `[0-23]`.',
    example: 'date.hour(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/hour/',
  },
  {
    name: 'date.microsecond',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the microsecond of a specified time. Results range from `[1-999999]`.',
    example: 'date.microsecond(t: 2019-07-17T12:05:21.012934584Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/microsecond/',
  },
  {
    name: 'date.millisecond',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the millisecond of a specified time. Results range from `[1-999]`.',
    example: 'date.millisecond(t: 2019-07-17T12:05:21.012934584Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/millisecond/',
  },
  {
    name: 'date.minute',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the minute of a specified time. Results range from `[0-59]`.',
    example: 'date.minute(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/minute/',
  },
  {
    name: 'date.month',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc: 'Returns the month of a specified time. Results range from `[1-12]`.',
    example: 'date.month(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/month/',
  },
  {
    name: 'date.monthDay',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the day of the month for a specified time. Results range from `[1-31]`.',
    example: 'date.monthDay(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/monthday/',
  },
  {
    name: 'date.nanosecond',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the nanosecond of a specified time. Results range from `[1-999999999]`.',
    example: 'date.nanosecond(t: 2019-07-17T12:05:21.012934584Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/nanosecond/',
  },
  {
    name: 'date.quarter',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the quarter of the year for a specified time. Results range from `[1-4]`.',
    example: 'date.quarter(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/quarter/',
  },
  {
    name: 'date.second',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the second of a specified time. Results range from `[0-59]`.',
    example: 'date.second(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/second/',
  },
  {
    name: 'date.truncate',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
      {
        name: 'unit',
        desc:
          'The unit time to truncate to. Only use `1` and the unit of time to specify the `unit`. For example, `1s`, `1m`, `1h`.',
        type: 'Duration',
      },
    ],
    package: 'date',
    desc:
      'Truncates the time to a specified unit. Results range from `[0-59]`.',
    example: 'date.truncate(t: 2019-07-17T12:05:21.012Z, unit: 1s)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/truncate/',
  },
  {
    name: 'date.week',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the ISO week of the year for a specified time. Results range from `[1-53]`.',
    example: 'date.week(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/week/',
  },
  {
    name: 'date.weekDay',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the day of the week for a specified time. Results range from `[0-6]`.',
    example: 'date.weekDay(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/weekday/',
  },
  {
    name: 'date.year',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc: 'Returns the year of a specified time.',
    example: 'date.year(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/year/',
  },
  {
    name: 'date.yearDay',
    args: [
      {
        name: 't',
        desc: 'The time to operate on.',
        type: 'Time | Duration',
      },
    ],
    package: 'date',
    desc:
      'Returns the day of the year for a specified time. Results include leap days and range from `[1-366]`.',
    example: 'date.yearDay(t: 2019-07-17T12:05:21.012Z)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/date/yearday/',
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
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/derivative/',
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
          'The columns to use to compute the difference. Defaults to `"_value"`.',
        type: 'Array of Strings',
      },
      {
        name: 'keepFirst',
        desc:
          'Indicates the first row should be kept. If `true`, the difference will be `null`. Defaults to `false`.',
        type: 'Boolean',
      },
    ],
    package: '',
    desc:
      'Computes the difference between subsequent non-null records in the specified columns.',
    example: 'difference(nonNegative: false, columns: ["_value"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/difference/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/distinct/',
  },
  {
    name: 'doubleEMA',
    args: [
      {
        name: 'n',
        desc: 'The number of points to average.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Calculates the exponential moving average of values in the `_value` column grouped into `n` number of points, giving more weight to recent data at double the rate of `exponentialMovingAverage()`.',
    example: 'doubleEMA(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/doubleema/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/drop/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/duplicate/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/duration/',
  },
  {
    name: 'experimental.addDuration',
    args: [
      {
        name: 'd',
        desc: 'The duration to add.',
        type: 'Duration',
      },
      {
        name: 'to',
        desc: 'The time to add the duration to.',
        type: 'Time',
      },
    ],
    package: 'experimental',
    desc:
      'Adds a duration to a time value and returns the resulting time value.',
    example: 'experimental.addDuration(d: 12h, to: now())',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/addduration/',
  },
  {
    name: 'experimental.alignTime',
    args: [
      {
        name: 'alignTo',
        desc: 'UTC time to align tables to. Default is 1970-01-01T00:00:00Z.',
        type: 'Time',
      },
    ],
    package: 'experimental',
    desc: 'Aligns input tables to a common start time.',
    example: 'experimental.alignTime(alignTo: v.timeRangeStart)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/aligntime/',
  },
  {
    name: 'experimental.chain',
    args: [
      {
        name: 'first',
        desc: 'The first query to execute.',
        type: 'Stream of Tables',
      },
      {
        name: 'second',
        desc: 'The second query to execute.',
        type: 'Stream of Tables',
      },
    ],
    package: 'experimental',
    desc:
      'Executes two queries sequentially rather than in parallel and outputs the results of the second query.',
    example: 'experimental.chain(first: query1, second: query2)',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/chain/',
  },
  {
    name: 'experimental.group',
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
          'The mode used to group columns. Only the `extend` mode is available with this function.',
        type: 'String',
      },
    ],
    package: 'experimental',
    desc: 'Introduces an extend mode to the existing `group()` function.',
    example:
      'experimental.group(columns: ["host", "_measurement"], mode: "extend")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/group/',
  },
  {
    name: 'experimental.join',
    args: [
      {
        name: 'left',
        desc: 'First of two streams of tables to join.',
        type: 'Stream of tables',
      },
      {
        name: 'right',
        desc: 'Second of two streams of tables to join.',
        type: 'Stream of tables',
      },
      {
        name: 'fn',
        desc:
          'A function that maps new output rows using left and right input rows.',
        type: 'Function',
      },
    ],
    package: 'experimental',
    desc:
      'Joins two streams of tables on the group key and _time column. Use the fn parameter to map output tables.',
    example:
      'experimental.join(left: left, right: right, fn: (left, right) => ({left with lv: left._value, rv: right._value }))',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/join/',
  },
  {
    name: 'experimental.objectKeys',
    args: [
      {
        name: 'o',
        desc: 'The object to return keys from.',
        type: 'Object',
      },
    ],
    package: 'experimental',
    desc: 'Returns an array of keys in a specified object.',
    example: 'experimental.objectKeys(o: {key1: "value1", key2: "value2"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/objectkeys/',
  },
  {
    name: 'experimental.set',
    args: [
      {
        name: 'o',
        desc: 'An object that defines the columns and values to set.',
        type: 'Object',
      },
    ],
    package: 'experimental',
    desc: 'Sets multiple static column values on all records.',
    example: 'experimental.set(o: {column1: "value1", column2: "value2"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/set/',
  },
  {
    name: 'experimental.subDuration',
    args: [
      {
        name: 'd',
        desc: 'The duration to subtract.',
        type: 'Duration',
      },
      {
        name: 'from',
        desc: 'The time to subtract the duration from.',
        type: 'Time',
      },
    ],
    package: 'experimental',
    desc:
      'Subtracts a duration from a time value and returns the resulting time value.',
    example: 'experimental.subDuration(d: 12h, from: now())',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/subduration/',
  },
  {
    name: 'experimental.to',
    args: [
      {
        name: 'bucket',
        desc:
          'The bucket to write data to. `bucket` and `bucketID` are mutually exclusive.',
        type: 'String',
      },
      {
        name: 'bucketID',
        desc:
          'The ID of the bucket to write data to. `bucketID` and `bucket` are mutually exclusive.',
        type: 'String',
      },
      {
        name: 'org',
        desc:
          'The organization name of the specified bucket. `org` and `orgID` are mutually exclusive.',
        type: 'String',
      },
      {
        name: 'orgID',
        desc:
          'The organization ID of the specified bucket. `orgID` and `org` are mutually exclusive.',
        type: 'String',
      },
    ],
    package: 'experimental',
    desc:
      'Writes data to an InfluxDB v2.0 bucket, but in a different structure than the built-in `to()` function.',
    example: 'experimental.to(bucket: "example-bucket", org: "example-org")',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/to/',
  },
  {
    name: 'elapsed',
    args: [
      {
        name: 'unit',
        desc: 'The unit time to return. Defaults to `1s`.',
        type: 'Duration',
      },
      {
        name: 'timeColumn',
        desc:
          'The column to use to compute the elapsed time. Defaults to `"_time"`.',
        type: 'String`',
      },
      {
        name: 'columnName',
        desc: 'The column to store elapsed times. Defaults to `"elapsed"`.',
        type: 'String',
      },
    ],
    package: '',
    desc: 'Returns the time between subsequent records.',
    example: 'elapsed(unit: 1s)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/elapsed/',
  },
  {
    name: 'exponentialMovingAverage',
    args: [
      {
        name: 'n',
        desc: 'The number of points to average.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Calculates the exponential moving average of values in the `_value` column grouped into `n` number of points, giving more weight to recent data.',
    example: 'exponentialMovingAverage(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/exponentialmovingaverage/',
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
      'Replaces all null values in an input stream and replace them with a non-null value.',
    example: 'fill(column: "_value", usePrevious: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/fill/',
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
      {
        name: 'onEmpty',
        desc:
          'Defines the behavior for empty tables. Potential values are `keep` and `drop`. Defaults to `drop`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Filters data based on conditions defined in the function. The output tables have the same schema as the corresponding input tables.',
    example: 'filter(fn: (r) => r._measurement == "cpu")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/filter/',
  },
  {
    name: 'findColumn',
    args: [
      {
        name: 'fn',
        desc: 'Predicate function for matching keys in a table’s group key.',
        type: 'Function',
      },
      {
        name: 'column',
        desc: 'Name of the column to extract.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Returns an array of values in a specified column from the first table in a stream of tables where the group key values match the specified predicate.',
    example: 'findColumn(fn: (key) => key.host == "host1", column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stream-table/findcolumn/',
  },
  {
    name: 'findRecord',
    args: [
      {
        name: 'fn',
        desc: 'Predicate function for matching keys in a table’s group key.',
        type: 'Function',
      },
      {
        name: 'idx',
        desc: 'Index of the record to extract.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Returns a record at a specified index from the first table in a stream of tables where the group key values match the specified predicate.',
    example: 'findRecord(fn: (key) => key.host == "host1", idx: 0)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stream-table/findrecord/',
  },
  {
    name: 'first',
    args: [],
    package: '',
    desc: 'Selects the first non-null record from an input table.',
    example: 'first()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/first/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/float/',
  },
  FROM,
  {
    name: 'geo.asTracks',
    args: [
      {
        name: 'groupBy',
        desc: 'Columns to group by. They should uniquely identify each track.',
        type: 'Array of Strings',
      },
      {
        name: 'orderBy',
        desc: 'Columns to order results by.',
        type: 'Array of Strings',
      },
    ],
    package: 'experimental/geo',
    desc:
      'Groups geo-temporal data into tracks (sequential, related data points).',
    example: 'geo.asTracks(groupBy: ["id","tid"], orderBy: ["_time"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/astracks/',
  },
  {
    name: 'geo.filterRows',
    args: [
      {
        name: 'region',
        desc: 'Region containing the desired data points.',
        type: 'Object',
      },
      {
        name: 'minSize',
        desc:
          'Minimum number of cells that cover the specified region. Default is `24`',
        type: 'Integer',
      },
      {
        name: 'maxSize',
        desc:
          'Maximum number of cells that cover the specified region. Default is `-1`.',
        type: 'Object',
      },
      {
        name: 'level',
        desc: 'S2 cell level of grid cells. Default is `-1`',
        type: 'Integer',
      },
      {
        name: 's2CellIDLevel',
        desc: 'S2 Cell level used in `s2_cell_id` tag. Default is `-1`.',
        type: 'Integer',
      },
      {
        name: 'strict',
        desc: 'Enable strict geographic data filtering. Default is `true`',
        type: 'Boolean',
      },
    ],
    package: 'experimental/geo',
    desc:
      'Filters data by a specified geographic region with the option of strict filtering.',
    example:
      'geo.filterRows(region: {lat: 37.7858229, lon: -122.4058124, radius: 20.0}, strict: true)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/filterrows/',
  },
  {
    name: 'geo.gridFilter',
    args: [
      {
        name: 'region',
        desc: 'Region containing the desired data points.',
        type: 'Object',
      },
      {
        name: 'minSize',
        desc:
          'Minimum number of cells that cover the specified region. Default is `24`',
        type: 'Integer',
      },
      {
        name: 'maxSize',
        desc:
          'Maximum number of cells that cover the specified region. Default is `-1`.',
        type: 'Object',
      },
      {
        name: 'level',
        desc: 'S2 cell level of grid cells. Default is `-1`',
        type: 'Integer',
      },
      {
        name: 's2CellIDLevel',
        desc: 'S2 Cell level used in `s2_cell_id` tag. Default is `-1`.',
        type: 'Integer',
      },
    ],
    package: 'experimental/geo',
    desc:
      'Filters data by a specified geographic region using S2 geometry grid cells.',
    example:
      'geo.gridFilter(region: {lat: 37.7858229, lon: -122.4058124, radius: 20.0})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/gridfilter/',
  },
  {
    name: 'geo.groupByArea',
    args: [
      {
        name: 'newColumn',
        desc:
          'Name of the new column that stores the unique identifier for a geographic area.',
        type: 'String',
      },
      {
        name: 'level',
        desc:
          'S2 Cell level used to determine the size of each geographic area.',
        type: 'Integer',
      },
      {
        name: 's2cellIDLevel',
        desc: 'S2 Cell level used in `s2_cell_id` tag. Default is `-1`.',
        type: 'Integer',
      },
    ],
    package: 'experimental/geo',
    desc: 'Groups rows by geographic area using S2 geometry grid cells.',
    example: 'geo.groupByArea(newColumn: "geoArea", level: 10)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/groupbyarea/',
  },
  {
    name: 'geo.s2CellIDToken',
    args: [
      {
        name: 'point',
        desc:
          'Longitude and latitude in decimal degrees (WGS 84) to use when generating the S2 cell ID token. Object must contain `lat` and `lon` properties.',
        type: 'Object',
      },
      {
        name: 'token',
        desc: 'S2 cell ID token to update.',
        type: 'String',
      },
      {
        name: 'level',
        desc: 'S2 cell level to use when generating the S2 cell ID token.',
        type: 'Integer',
      },
    ],
    package: 'experimental/geo',
    desc: 'Returns an S2 cell ID token.',
    example:
      'geo.s2CellIDToken(point: {lat: 37.7858229, lon: -122.4058124}, level: 10)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/s2cellidtoken/',
  },
  {
    name: 'geo.s2CellLatLon',
    args: [
      {
        name: 'token',
        desc: 'S2 cell ID token.',
        type: 'String',
      },
    ],
    package: 'experimental/geo',
    desc: 'Returns the latitude and longitude of the center of an S2 cell.',
    example: 'geo.s2CellLatLon(token: "89c284")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/s2celllatlon/',
  },
  {
    name: 'geo.shapeData',
    args: [
      {
        name: 'latField',
        desc: 'Name of existing latitude field.',
        type: 'String',
      },
      {
        name: 'lonField',
        desc: 'Name of existing longitude field.',
        type: 'String',
      },
      {
        name: 'level',
        desc: 'S2 cell level to use when generating the S2 cell ID token.',
        type: 'Integer',
      },
    ],
    package: 'experimental/geo',
    desc:
      'Renames existing latitude and longitude fields to `lat` and `lon` and adds an `s2_cell_id` tag.',
    example:
      'geo.shapeData(latField: "latitude", lonField: "longitude", level: 10)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/shapedata/',
  },
  {
    name: 'geo.ST_Contains',
    args: [
      {
        name: 'region',
        desc: 'Region to test.',
        type: 'Object',
      },
      {
        name: 'geometry',
        desc:
          'GIS geometry to test. Can be either point or linestring geometry.',
        type: 'Object',
      },
    ],
    package: 'experimental/geo',
    desc: 'Tests if the region contains the GIS geometry.',
    example:
      'geo.ST_Contains(region: {lat: 40.7, lon: -73.3, radius: 20.0}, geometry: {lon: 39.7515, lat: 15.08433})',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_contains/',
  },
  {
    name: 'geo.ST_Distance',
    args: [
      {
        name: 'region',
        desc: 'Region to test.',
        type: 'Object',
      },
      {
        name: 'geometry',
        desc:
          'GIS geometry to test. Can be either point or linestring geometry.',
        type: 'Object',
      },
    ],
    package: 'experimental/geo',
    desc: 'Returns the distance between the regio and GIS geometry.',
    example:
      'geo.ST_Distance(region: {lat: 40.7, lon: -73.3, radius: 20.0}, geometry: {lon: 39.7515, lat: 15.08433})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_distance/',
  },
  {
    name: 'geo.ST_DWithin',
    args: [
      {
        name: 'region',
        desc: 'Region to test.',
        type: 'Object',
      },
      {
        name: 'geometry',
        desc:
          'GIS geometry to test. Can be either point or linestring geometry.',
        type: 'Object',
      },
      {
        name: 'distance',
        desc:
          'Maximum distance allowed between the region and geometry.',
        type: 'Float',
      },
    ],
    package: 'experimental/geo',
    desc: 'Tests if a region is within a specified distance from GIS geometry.',
    example:
      'geo.ST_DWithin(region: {lat: 40.7, lon: -73.3, radius: 20.0}, geometry: {lon: 39.7515, lat: 15.08433}, distance: 1000.0)',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_dwithin/',
  },
  {
    name: 'geo.ST_Intersects',
    args: [
      {
        name: 'region',
        desc: 'Region to test.',
        type: 'Object',
      },
      {
        name: 'geometry',
        desc:
          'GIS geometry to test. Can be either point or linestring geometry.',
        type: 'Object',
      },
    ],
    package: 'experimental/geo',
    desc: 'Tests if a region intersects with GIS geometry.',
    example:
      'geo.ST_Intersects(region: {lat: 40.7, lon: -73.3, radius: 20.0}, geometry: {linestring: "39.7515 14.01433, 38.3527 13.9228, 36.9978 15.08433"})',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_intersects/',
  },
  {
    name: 'geo.ST_Length',
    args: [
      {
        name: 'geometry',
        desc:
          'GIS geometry to test. Can be either point or linestring geometry.',
        type: 'Object',
      },
    ],
    package: 'experimental/geo',
    desc: 'Returns the spherical length of GIS geometry.',
    example:
      'geo.ST_Length(geometry: {linestring: "39.7515 14.01433, 38.3527 13.9228, 36.9978 15.08433"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_length/',
  },
  {
    name: 'geo.ST_LineString',
    args: [],
    package: 'experimental/geo',
    desc: 'Converts a series of geographic points into linestring',
    example:
      'geo.ST_LineString()',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/st_linestring/',
  },
  {
    name: 'geo.strictFilter',
    args: [
      {
        name: 'region',
        desc: 'Region containing the desired data points.',
        type: 'Object',
      },
    ],
    package: 'experimental/geo',
    desc: 'Filters data by latitude and longitude in a specified region.',
    example:
      'geo.strictFilter(region: {lat: 37.7858229, lon: -122.4058124, radius: 20.0})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/strictfilter/',
  },
  {
    name: 'geo.toRows',
    args: [],
    package: 'experimental/geo',
    desc:
      'Pivots geo-temporal data into row-wise sets based on time and other correlation columns.',
    example: 'geo.toRows(correlationKey: ["_time"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/geo/torows/',
  },
  {
    name: 'getColumn',
    args: [
      {
        name: 'column',
        desc: 'The name of the column to extract.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Extracts a column from a table given its label. If the label is not present in the set of columns, the function errors.',
    example: 'getColumn(column: "_value")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stream-table/getcolumn/',
  },
  {
    name: 'getRecord',
    args: [
      {
        name: 'idx',
        desc: 'The index of the record to extract.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Extracts a record from a table given the record’s index. If the index is out of bounds, the function errors.',
    example: 'getRecord(idx: 0)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stream-table/getrecord/',
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
    package: '',
    desc:
      'Groups records based on their values for specific columns. It produces tables with new group keys based on provided properties.',
    example: 'group(columns: ["host", "_measurement"], mode:"by")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/group/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/highestaverage/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/highestcurrent/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/highestmax/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/histogram/',
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
      'histogramQuantile(quantile: 0.5, countColumn: "_value", upperBoundColumn: "le", valueColumn: "_value", minValue: 0.0)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/histogramquantile/',
  },
  {
    name: 'holtWinters',
    args: [
      {
        name: 'n',
        desc: 'Number of values to predict.',
        type: 'Integer',
      },
      {
        name: 'seasonality',
        desc: 'Number of points in a season. Defaults to `0`.',
        type: 'Integer',
      },
      {
        name: 'interval',
        desc: 'The interval between two data points.',
        type: 'Duration',
      },
      {
        name: 'withFit',
        desc:
          'Returns "fitted" data points in results when `withFit` is set to `true`. Defaults to `false`.',
        type: 'Boolean',
      },
      {
        name: 'timeColumn',
        desc: 'The time column to use. Defaults to `"_time"`.',
        type: 'String',
      },
      {
        name: 'column',
        desc: 'The column to operate on. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Applies the Holt-Winters forecasting method to input tables. The Holt-Winters method predicts `n` seasonally-adjusted values for the specified `column` at the specified `interval`.',
    example: 'holtWinters(n: 10, interval: 1d)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/holtwinters/',
  },
  {
    name: 'hourSelection',
    args: [
      {
        name: 'start',
        desc:
          'The first hour of the hour range (inclusive). Hours range from `[0-23]`',
        type: 'Integer',
      },
      {
        name: 'stop',
        desc:
          'The last hour of the hour range (inclusive). Hours range from `[0-23]`.',
        type: 'Integer`',
      },
      {
        name: 'timeColumn',
        desc: 'The column that contains the time value. Default is `"_time"`.',
        type: 'String`',
      },
    ],
    package: '',
    desc:
      'Retains all rows with time values in a specified hour range. Hours are specified in military time.',
    example: 'hourSelection(start: 9, stop: 17)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/hourselection/',
  },
  {
    name: 'http.get',
    args: [
      {
        name: 'url',
        desc: 'The URL to send the GET request to.',
        type: 'String',
      },
      {
        name: 'headers',
        desc: 'Headers to include with the GET request.',
        type: 'Object',
      },
      {
        name: 'timeout',
        desc: 'Timeout for the GET request. Default is `30s`.',
        type: 'Duration',
      },
    ],
    package: 'experimental/http',
    desc:
      'Submits an HTTP GET request to the specified URL and returns the HTTP status code, response body, and response headers.',
    example:
      'http.get(url: "https://v2.docs.influxdata.com/v2.0/", headers: {foo: "bar"})',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/http/get/',
  },
  {
    name: 'http.pathEscape',
    args: [
      {
        name: 'inputString',
        desc: 'The string to escape.',
        type: 'String',
      },
    ],
    package: 'http',
    desc:
      'Escapes special characters in a string and replaces non-ASCII characters with hexadecimal representations (%XX).',
    example: 'http.pathEscape(inputString: "/this/is/an/example-path.html")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/http/pathescape/',
  },
  {
    name: 'http.post',
    args: [
      {
        name: 'url',
        desc: 'The URL to POST to.',
        type: 'String',
      },
      {
        name: 'headers',
        desc: 'Headers to include with the POST request.',
        type: 'Object',
      },
      {
        name: 'data',
        desc: 'The data body to include with the POST request.',
        type: 'Bytes',
      },
    ],
    package: 'http',
    desc:
      'Submits an HTTP POST request to the specified URL with headers and data and returns the HTTP status code.',
    example:
      'http.post(url: "http://localhost:9999/", headers: {x:"a", y:"b"}, data: bytes(v: "body"))',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/http/post/',
  },
  {
    name: 'increase',
    args: [
      {
        name: 'columns',
        desc:
          'List of columns to use in the operation. Defaults to `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    package: '',
    desc:
      'Computes the total non-negative difference between values in a table.',
    example: 'increase(columns: ["_value"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/increase/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/int/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/integral/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/join/',
  },
  {
    name: 'json.encode',
    args: [
      {
        name: 'v',
        desc: 'The value to encode.',
        type: 'Boolean, Duration, Float, Integer, String, Time, UInteger',
      },
    ],
    package: 'json',
    desc: 'Converts a value into JSON bytes.',
    example: 'json.encode(v: r._value)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/json/encode/',
  },
  {
    name: 'kaufmansAMA',
    args: [
      {
        name: 'n',
        desc: 'The period or number of points to use in the calculation.',
        type: 'Integer',
      },
      {
        name: 'column',
        desc: 'The column to operate on. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Calculates Kaufman’s Adaptive Moving Average (KAMA) using values in an input table.',
    example: 'kaufmansAMA(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/kaufmansama/',
  },
  {
    name: 'kaufmansER',
    args: [
      {
        name: 'n',
        desc: 'The period or number of points to use in the calculation.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Calculates the Kaufman’s Efficiency Ratio (KER) using values in an input table.',
    example: 'kaufmansER(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/kaufmanser/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/keep/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/keys/',
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
    ],
    package: '',
    desc:
      "Returns a table with the input table's group key plus two columns, `_key` and `_value`, that correspond to unique column + value pairs from the input table.",
    example: 'keyValues(keyColumns: ["usage_idle", "usage_user"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/keyvalues/',
  },
  {
    name: 'last',
    args: [],
    package: '',
    desc: 'Selects the last non-null record from an input table.',
    example: 'last()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/last/',
  },
  {
    name: 'length',
    args: [
      {
        name: 'arr',
        desc: 'The array to evaluate.',
        type: 'Array',
      },
    ],
    package: '',
    desc: 'Returns the number of items in an array.',
    example: 'length(arr: ["john"])',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/misc/length/',
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
          'The number of records to skip at the beginning of a table before limiting to `n`. Defaults to `0`.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Limits each output table to the first `n` records, excluding the offset.',
    example: 'limit(n:10, offset: 0)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/limit/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/misc/linearbins/',
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
      'logarithmicBins(start: 1.0, factor: 2.0, count: 10, infinity: true)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/misc/logarithmicbins/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/lowestaverage/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/lowestcurrent/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/lowestmin/',
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
    ],
    package: '',
    desc: 'Applies a function to each record in the input tables.',
    example: 'map(fn: (r) => ({ r with _value: r._value * r._value }))',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/map/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/acos/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/acosh/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/asin/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/asinh/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/atan/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/atan2/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/atanh/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/cbrt/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/ceil/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/copysign/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/cos/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/cosh/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/dim/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/erf/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/erfc/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/erfcinv/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/erfinv/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/exp/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/exp2/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/expm1/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/float64bits/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/frexp/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/gamma/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/hypot/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/ilogb/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/isinf/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/isnan/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/j0/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/j1/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/jn/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/ldexp/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/lgamma/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/log/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/log1p/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/log2/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/logb/',
  },
  {
    name: 'math.mInf',
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
    example: 'math.mInf(sign: r._value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/minf/',
  },
  {
    name: 'math.mMax',
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
    example: 'math.mMax(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/mmax/',
  },
  {
    name: 'math.mMin',
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
    example: 'math.mMin(x: r.x_value, y: r.y_value)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/mmin/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/mod/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/modf/',
  },
  {
    name: 'math.NaN',
    args: [],
    package: 'math',
    desc: 'Returns an IEEE 754 NaN value.',
    example: 'math.NaN()',
    category: 'Transformations',
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/nan/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/nextafter/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/pow/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/pow10/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/remainder/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/round/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/roundtoeven/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/signbit/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/sin/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/sincos/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/sinh/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/sqrt/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/tan/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/tanh/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/trunc/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/y0/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/y1/',
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
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/math/yn/',
  },
  {
    name: 'max',
    args: [],
    package: '',
    desc: 'Selects record with the highest `_value` from the input table.',
    example: 'max()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/max/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/median/',
  },
  {
    name: 'min',
    args: [],
    package: '',
    desc: 'Selects record with the lowest `_value` from the input table.',
    example: 'min()',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/min/',
  },
  {
    name: 'mode',
    args: [
      {
        name: 'column',
        desc: 'The column to use to compute the mode. Defaults to `"_value"`.',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Computes the mode or value that occurs most often in a specified column.',
    example: 'mode(column: "_value")',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/mode/',
  },
  {
    name: 'movingAverage',
    args: [
      {
        name: 'n',
        desc: 'The frequency of time windows.',
        type: 'Duration',
      },
    ],
    package: '',
    desc: 'Calculates the mean of values grouped into `n` number of points.',
    example: 'movingAverage(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/movingaverage/',
  },
  {
    name: 'mqtt.to',
    args: [
      {
        name: 'broker',
        desc: 'The MQTT broker connection string.',
        type: 'String',
      },
      {
        name: 'topic',
        desc: 'The MQTT topic to send data to.',
        type: 'String',
      },
      {
        name: 'message',
        desc:
          'The message or payload to send to the MQTT broker. The default payload is an output table.',
        type: 'String',
      },
      {
        name: 'qos',
        desc:
          'The MQTT Quality of Service (QoS) level. Values range from 0-2. Default is 0.',
        type: 'Integer',
      },
      {
        name: 'clientid',
        desc: 'The MQTT client ID.',
        type: 'String',
      },
      {
        name: 'username',
        desc: 'The username to send to the MQTT broker.',
        type: 'String',
      },
      {
        name: 'password',
        desc: 'The password to send to the MQTT broker.',
        type: 'String',
      },
      {
        name: 'name',
        desc: 'The name for the MQTT message.',
        type: 'String',
      },
      {
        name: 'timeout',
        desc: 'The MQTT connection timeout. Default is 1s.',
        type: 'Duration',
      },
      {
        name: 'timeColumn',
        desc:
          'The column to use as time values in the output line protocol. Default is `"_time"`.',
        type: 'String',
      },
      {
        name: 'tagColumns',
        desc:
          'The columns to use as tag sets in the output line protocol. Default is `[]`.',
        type: 'Array of Strings',
      },
      {
        name: 'valueColumns',
        desc:
          'The columns to use as field values in the output line protocol. Default is `["_value"]`.',
        type: 'Array of Strings',
      },
    ],
    package: 'experimental/mqtt',
    desc: 'Outputs data to an MQTT broker using MQTT protocol.',
    example:
      'mqtt.to(broker: "tcp://localhost:8883", topic: "example-topic", clientid: "exampleID", tagColumns: ["exampleTagKey"], valueColumns: ["_value"])',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/mqtt/to/',
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
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/pearsonr/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/pivot/',
  },
  {
    name: 'prometheus.histogramQuantile',
    args: [
      {
        name: 'quantile',
        desc: 'A value between 0.0 and 1.0 indicating the desired quantile.',
        type: 'Float',
      },
    ],
    package: 'experimental/prometheus',
    desc:
      'Calculates quantiles on a set of values assuming the histogram data is scraped or read from a Prometheus data source.',
    example: 'prometheus.histogramQuantile(quantile: 0.99)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/prometheus/histogramquantile/',
  },
  {
    name: 'prometheus.scrape',
    args: [
      {
        name: 'url',
        desc: 'The URL to scrape Prometheus-formatted metrics from.',
        type: 'String',
      },
    ],
    package: 'experimental/prometheus',
    desc: 'Retrieves Prometheus-formatted metrics from a specified URL.',
    example: 'prometheus.scrape(url: "http://localhost:9999/metrics")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/prometheus/scrape/',
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
        name: 'q',
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
      'quantile(column: "_value", q: 0.99, method: "estimate_tdigest", compression: 1000.0)',
    category: 'Aggregates',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/quantile/',
  },
  {
    name: 'query.filterFields',
    args: [
      {
        name: 'fields',
        desc: 'Fields to filter by.',
        type: 'Array of Strings',
      },
    ],
    package: 'experimental/query',
    desc: 'Filters input data by field.',
    example: 'query.filterFields(fields: ["field_name"])',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/query/filterfields/',
  },
  {
    name: 'query.filterMeasurement',
    args: [
      {
        name: 'measurement',
        desc: 'Measurement to filter by.',
        type: 'String',
      },
    ],
    package: 'experimental/query',
    desc: 'Filters input data by measurement.',
    example: 'query.filterMeasurement(measurement: "measurement_name")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/query/filtermeasurement/',
  },
  {
    name: 'query.fromRange',
    args: [
      {
        name: 'bucket',
        desc: 'Name of the bucket to query.',
        type: 'String',
      },
      {
        name: 'start',
        desc: 'The earliest time to include in results.',
        type: 'Duration | Time | Integer',
      },
      {
        name: 'stop',
        desc: 'The latest time to include in results. Defaults to `now()`.',
        type: 'Duration | Time | Integer',
      },
    ],
    package: 'experimental/query',
    desc: 'Returns all data from a specified bucket within given time bounds.',
    example:
      'query.fromRange(bucket: "example-bucket", start: v.timeRangeStart)',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/query/fromrange/',
  },
  {
    name: 'query.inBucket',
    args: [
      {
        name: 'bucket',
        desc: 'Name of the bucket to query.',
        type: 'String',
      },
      {
        name: 'start',
        desc: 'The earliest time to include in results.',
        type: 'Duration | Time | Integer',
      },
      {
        name: 'stop',
        desc: 'The latest time to include in results. Defaults to `now()`.',
        type: 'Duration | Time | Integer',
      },
      {
        name: 'measurement',
        desc: 'Measurement to filter by.',
        type: 'String',
      },
      {
        name: 'fields',
        desc: 'Fields to filter by.',
        type: 'Array of Strings',
      },
      {
        name: 'predicate',
        desc: 'A single argument function that evaluates true or false.',
        type: 'Function',
      },
    ],
    package: 'experimental/query',
    desc:
      'Queries data from a specified bucket within given time bounds, filters data by measurement, field, and optional predicate expressions.',
    example:
      'query.inBucket(bucket: "example-bucket", start: v.timeRangeStart, measurement: "measurement_name", fields: ["field_name"], predicate: (r) => r.host == "host1")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/experimental/query/inbucket/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/reduce/',
  },
  {
    name: 'regexp.compile',
    args: [
      {
        name: 'v',
        desc: 'The string value to parse into a regular expression.',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc:
      'Parses a string into a regular expression and returns a regexp object.',
    example: 'regexp.compile(v: "[a-zA-Z]")',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/compile/',
  },
  {
    name: 'regexp.findString',
    args: [
      {
        name: 'r',
        desc: 'The regular expression used to search `v`',
        type: 'Regexp',
      },
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc: 'Returns the left-most regular expression match in a string.',
    example: 'regexp.findString(r: /foo.?/, v: "seafood fool")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/findstring/',
  },
  {
    name: 'regexp.findStringIndex',
    args: [
      {
        name: 'r',
        desc: 'The regular expression used to search `v`',
        type: 'Regexp',
      },
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc:
      'Returns a two-element array of integers defining the beginning and ending indexes of the left-most regular expression match in a string.',
    example: 'regexp.findStringIndex(r: /ab?/, v: "tablet")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/findstringindex/',
  },
  {
    name: 'regexp.getString',
    args: [
      {
        name: 'r',
        desc: 'The regular expression object to convert to a string.',
        type: 'Regexp',
      },
    ],
    package: 'regexp',
    desc: 'Returns the source string used to compile a regular expression.',
    example: 'regexp.getString(r: /[a-zA-Z]/)',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/getstring/',
  },
  {
    name: 'regexp.matchRegexpString',
    args: [
      {
        name: 'r',
        desc: 'The regular expression used to search `v`',
        type: 'Regexp',
      },
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc: 'Tests if a string contains any match to a regular expression.',
    example: 'regexp.matchRegexpString(r: /(go){2}/, v: "gogogopher")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/matchregexpstring/',
  },
  {
    name: 'regexp.quoteMeta',
    args: [
      {
        name: 'v',
        desc:
          'String value containing regular expression metacharacters to escape.',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc: 'Escapes all regular expression metacharacters inside of a string.',
    example: 'regexp.quoteMeta(v: ".+*?()|[]{}^$")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/quotemeta/',
  },
  {
    name: 'regexp.replaceAllString',
    args: [
      {
        name: 'r',
        desc: 'The regular expression used to search `v`',
        type: 'Regexp',
      },
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The replacement for matches to `r`',
        type: 'String',
      },
    ],
    package: 'regexp',
    desc:
      'Replaces all regular expression matches in a string with a specified replacement.',
    example: 'regexp.replaceAllString(r: /a(x*)b/, v: "-ab-axxb-", t: "T")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/replaceallstring/',
  },
  {
    name: 'regexp.splitRegexp',
    args: [
      {
        name: 'r',
        desc: 'The regular expression used to search `v`',
        type: 'Regexp',
      },
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'i',
        desc: 'The number of substrings to return.',
        type: 'Integer',
      },
    ],
    package: 'regexp',
    desc:
      'Splits a string into substrings separated by regular expression matches and returns an array of `i` substrings between matches.',
    example: 'regexp.splitRegexp(r: /a*/, v: "abaabaccadaaae", i: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/regexp/splitregexp/',
  },
  {
    name: 'relativeStrengthIndex',
    args: [
      {
        name: 'n',
        desc:
          'The number of values to use to calculate the relative strength index (RSI).',
        type: 'Integer',
      },
      {
        name: 'columns',
        desc: 'Columns to operate on. Defaults to `["_value"]`.',
        type: 'Array of Strings`',
      },
    ],
    package: '',
    desc: 'Measures the relative speed and change of values in an input table.',
    example: 'relativeStrengthIndex(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/relativestrengthindex/',
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
    example: 'rename(columns: {host: "server", _field: "my_field"})',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/rename/',
  },
  {
    name: 'runtime.version',
    args: [],
    package: 'runtime',
    desc: 'Returns the current Flux version.',
    example: 'runtime.version()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/runtime/version/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/sample/',
  },
  {
    name: 'secrets.get',
    args: [
      {
        name: 'key',
        desc: 'The secret key to retrieve.',
        type: 'String',
      },
    ],
    package: 'influxdata/influxdb/secrets',
    desc: 'Retrieves a secret from the InfluxDB secret store.',
    example: 'secrets.get(key: "KEY_NAME")',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/secrets/get/',
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
    example: 'set(key: "_field", value: "my_field")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/set/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/skew/',
  },
  {
    name: 'sleep',
    args: [
      {
        name: 'v',
        desc: 'Defines input tables.',
        type: 'Object',
      },
      {
        name: 'duration',
        desc: 'The length of time to delay execution.',
        type: 'Duration',
      },
    ],
    package: '',
    desc: 'Delays execution by a specified duration.',
    example: 'sleep(duration: 5s)',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/misc/sleep/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/sort/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/spread/',
  },
  {
    name: 'sql.from',
    args: [
      {
        name: 'driverName',
        desc: 'The driver used to connect to the SQL database.',
        type: 'String',
      },
      {
        name: 'dataSourceName',
        desc:
          'The connection string used to connect to the SQL database. The string’s form and structure depend on the driver.',
        type: 'String',
      },
      {
        name: 'query',
        desc: 'The query to run against the SQL database.',
        type: 'String',
      },
    ],
    package: 'sql',
    desc: 'Retrieves data from a SQL data source.',
    example:
      'sql.from(driverName: "postgres", dataSourceName: "postgresql://user:password@localhost", query:"SELECT * FROM example_table")',
    category: 'Inputs',
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/sql/from/',
  },
  {
    name: 'sql.to',
    args: [
      {
        name: 'driverName',
        desc: 'The driver used to connect to the SQL database.',
        type: 'String',
      },
      {
        name: 'dataSourceName',
        desc:
          'The connection string used to connect to the SQL database. The string’s form and structure depend on the driver.',
        type: 'String',
      },
      {
        name: 'table',
        desc: 'The destination table.',
        type: 'String',
      },
      {
        name: 'batchSize',
        desc:
          'The number of parameters or columns that can be queued within each call to `Exec`. Defaults to `10000`.',
        type: 'Integer',
      },
    ],
    package: 'sql',
    desc: 'Writes data to a SQL database.',
    example:
      'sql.to(driverName: "postgres", dataSourceName: "postgresql://user:password@localhost", table: "example_table")',
    category: 'Outputs',
    link: 'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/sql/to/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/statecount/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stateduration/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/stddev/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/string/',
  },
  {
    name: 'strings.compare',
    args: [
      {
        name: 'v',
        desc: 'The string value to compare.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value to compare against.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Compares the lexicographical order of two strings.',
    example: 'strings.compare(v: "a", t: "b")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/compare/',
  },
  {
    name: 'strings.containsAny',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'chars',
        desc: 'Characters to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Reports whether a specified string contains characters from another string.',
    example: 'strings.containsAny(v: "abc", chars: "and")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/containsany/',
  },
  {
    name: 'strings.containsStr',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'substr',
        desc: 'The substring to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Reports whether a string contains a specified substring.',
    example: 'strings.containsStr(v: "This and that", substr: "and")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/containsstr/',
  },
  {
    name: 'strings.countStr',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'substr',
        desc: 'The substring count.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Counts the number of non-overlapping instances of a substring appears in a string.',
    example: 'strings.countStr(v: "Hello mellow fellow", substr: "ello")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/countstr/',
  },
  {
    name: 'strings.equalFold',
    args: [
      {
        name: 'v',
        desc: 'The string value to compare.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value to compare against.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Reports whether two UTF-8 strings are equal under Unicode case-folding.',
    example: 'strings.equalFold(v: "Go", t: "go")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/equalfold/',
  },
  {
    name: 'strings.hasPrefix',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'prefix',
        desc: 'The prefix to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Indicates if a string begins with a specified prefix.',
    example: 'strings.hasPrefix(v: "go gopher", prefix: "go")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/hasprefix/',
  },
  {
    name: 'strings.hasSuffix',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'suffix',
        desc: 'The suffix to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Indicates if a string ends with a specified suffix.',
    example: 'strings.hasSuffix(v: "gopher go", suffix: "go")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/hassuffix/',
  },
  {
    name: 'strings.index',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'substr',
        desc: 'The substring to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Returns the index of the first instance of a substring in a string. If the substring is not present, it returns `-1`.',
    example: 'strings.index(v: "go gopher", substr: "go")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/index-func/',
  },
  {
    name: 'strings.indexAny',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'chars',
        desc: 'Characters to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Returns the index of the first instance of specified characters in a string. If none of the specified characters are present, it returns -1.',
    example: 'strings.indexAny(v: "chicken", chars: "aeiouy")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/indexany/',
  },
  {
    name: 'strings.isDigit',
    args: [
      {
        name: 'v',
        desc: 'The single-character string to test.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Tests if a single-character string is a digit (0-9).',
    example: 'strings.isDigit(v: "7")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/isdigit/',
  },
  {
    name: 'strings.isLetter',
    args: [
      {
        name: 'v',
        desc: 'The single-character string to test.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Tests if a single-character string is a letter (a-z, A-Z).',
    example: 'strings.isLetter(v: "A")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/isletter/',
  },
  {
    name: 'strings.isLower',
    args: [
      {
        name: 'v',
        desc: 'The single-character string to test.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Tests if a single-character string is lowercase.',
    example: 'strings.isLower(v: "a")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/islower/',
  },
  {
    name: 'strings.isUpper',
    args: [
      {
        name: 'v',
        desc: 'The single-character string to test.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Tests if a single-character string is uppercase.',
    example: 'strings.isUpper(v: "A")',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/isupper/',
  },
  {
    name: 'strings.joinStr',
    args: [
      {
        name: 'arr',
        desc: 'The array of strings to concatenate.',
        type: 'Array of Strings',
      },
      {
        name: 'v',
        desc: 'The separator to use in the concatenated value.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Concatenates elements of a string array into a single string using a specified separator.',
    example: 'strings.joinStr(arr: ["a", "b", "c"], v: ",")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/joinstr/',
  },
  {
    name: 'strings.lastIndex',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'substr',
        desc: 'The substring to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Returns the index of the last instance of a substring in a string. If the substring is not present, the function returns -1.',
    example: 'strings.lastIndex(v: "go gopher", substr: "go")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/lastindex/',
  },
  {
    name: 'strings.lastIndexAny',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'chars',
        desc: 'Characters to search for.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Returns the index of the last instance of any specified characters in a string. If none of the specified characters are present, the function returns -1.',
    example: 'strings.lastIndexAny(v: "chicken", chars: "aeiouy")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/lastindexany/',
  },
  {
    name: 'strings.repeat',
    args: [
      {
        name: 'v',
        desc: 'The string value to repeat.',
        type: 'String',
      },
      {
        name: 'i',
        desc: 'The number of times to repeat `v`.',
        type: 'Integer',
      },
    ],
    package: 'strings',
    desc: 'Returns a string consisting of `i` copies of a specified string.',
    example: 'strings.repeat(v: "ha", i: 3)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/repeat/',
  },
  {
    name: 'strings.replace',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The substring to replace.',
        type: 'String',
      },
      {
        name: 'u',
        desc: 'The replacement for `i` instances of `t`.',
        type: 'String',
      },
      {
        name: 'i',
        desc: 'The number of non-overlapping `t` matches to replace.',
        type: 'Integer',
      },
    ],
    package: 'strings',
    desc:
      'Replaces the first `i` non-overlapping instances of a substring with a specified replacement.',
    example: 'strings.replace(v: "oink oink oink", t: "oink", u: "moo", i: 2)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/replace/',
  },
  {
    name: 'strings.replaceAll',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The substring to replace.',
        type: 'String',
      },
      {
        name: 'u',
        desc: 'The replacement for all instances of `t`.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Replaces all non-overlapping instances of a substring with a specified replacement.',
    example: 'strings.replaceAll(v: "oink oink oink", t: "oink", u: "moo")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/replaceall/',
  },
  {
    name: 'strings.split',
    args: [
      {
        name: 'v',
        desc: 'The string value to split.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value that acts as the separator.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Splits a string on a specified separator and returns an array of substrings.',
    example: 'strings.split(v: "a flux of foxes", t: " ")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/split/',
  },
  {
    name: 'strings.splitAfter',
    args: [
      {
        name: 'v',
        desc: 'The string value to split.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value that acts as the separator.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc:
      'Splits a string after a specified separator and returns an array of substrings.',
    example: 'strings.splitAfter(v: "a flux of foxes", t: " ")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/splitafter/',
  },
  {
    name: 'strings.splitAfterN',
    args: [
      {
        name: 'v',
        desc: 'The string value to split.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value that acts as the separator.',
        type: 'String',
      },
      {
        name: 'i',
        desc: 'The number of substrings to return.',
        type: 'Integer',
      },
    ],
    package: 'strings',
    desc:
      'Splits a string after a specified separator and returns an array of `i` substrings.',
    example: 'strings.splitAfterN(v: "a flux of foxes", t: " ", i: 2)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/splitaftern/',
  },
  {
    name: 'strings.splitN',
    args: [
      {
        name: 'v',
        desc: 'The string value to split.',
        type: 'String',
      },
      {
        name: 't',
        desc: 'The string value that acts as the separator.',
        type: 'String',
      },
      {
        name: 'i',
        desc: 'The number of substrings to return.',
        type: 'Integer',
      },
    ],
    package: 'strings',
    desc:
      'Splits a string on a specified separator and returns an array of `i` substrings.',
    example: 'strings.splitN(v: "a flux of foxes", t: " ", i: 2)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/splitn/',
  },
  {
    name: 'strings.strlen',
    args: [
      {
        name: 'v',
        desc: 'The string value to measure.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Returns the length of a string.',
    example: 'strings.strlen(v: "a flux of foxes")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/strlen/',
  },
  {
    name: 'strings.substring',
    args: [
      {
        name: 'v',
        desc: 'The string value to search.',
        type: 'String',
      },
      {
        name: 'start',
        desc: 'The starting index of the substring.',
        type: 'Integer',
      },
      {
        name: 'end',
        desc: 'The ending index of the substring.',
        type: 'Integer',
      },
    ],
    package: 'strings',
    desc: 'Returns a substring based on start and end parameters.',
    example: 'strings.substring(v: "influx", start: 0, end: 3)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/substring/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/tolower/',
  },
  {
    name: 'strings.toTitle',
    args: [
      {
        name: 'v',
        desc: 'The string value to convert.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Converts all characters in a string to title case.',
    example: 'strings.toTitle(v: "a flux of foxes")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/totitle/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/toupper/',
  },
  STRINGS_TRIM,
  {
    name: 'strings.trimLeft',
    args: [
      {
        name: 'v',
        desc: 'The string to remove characters from.',
        type: 'String',
      },
      {
        name: 'cutset',
        desc: 'The leading characters to remove from the string.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Removes specified leading characters from a string.',
    example: 'strings.trimLeft(v: ".abc", cutset: ".")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trimleft/',
  },
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trimprefix/',
  },
  {
    name: 'strings.trimRight',
    args: [
      {
        name: 'v',
        desc: 'The string to remove characters from.',
        type: 'String',
      },
      {
        name: 'cutset',
        desc: 'The trailing characters to remove from the string.',
        type: 'String',
      },
    ],
    package: 'strings',
    desc: 'Removes specified trailing characters from a string.',
    example: 'strings.trimRight(v: "abc.", cutset: ".")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trimright/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trimspace/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/strings/trimsuffix/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/aggregates/sum/',
  },
  {
    name: 'system.time',
    args: [],
    package: 'system',
    desc: 'Returns the current system time.',
    example: 'system.time()',
    category: 'Miscellaneous',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/system/time/',
  },
  {
    name: 'tableFind',
    args: [
      {
        name: 'fn',
        desc: 'A predicate function for matching keys in a table group key.',
        type: 'Function',
      },
    ],
    package: '',
    desc:
      'Extracts the first table in a stream of tables whose group key values match a predicate. If no table is found, the function errors.',
    example: 'tableFind(fn: (key) => key._field == "fieldName")',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/stream-table/tablefind/',
  },
  {
    name: 'tail',
    args: [
      {
        name: 'n',
        desc: 'The maximum number of records to output.',
        type: 'Integer',
      },
      {
        name: 'offset',
        desc:
          'The number of records to skip at the end of a table before limiting to `n`. Defaults to `0`.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Limits each output table to the last `n` records, excluding the offset.',
    example: 'tail(n: 10)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/tail/',
  },
  {
    name: 'testing.assertEmpty',
    args: [],
    package: 'testing',
    desc: 'Tests if an input stream is empty.',
    example: 'testing.assertEmpty()',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/testing/assertempty/',
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
        type: 'Stream of tables',
      },
      {
        name: 'want',
        desc: 'The stream that contains the expected data to test against.',
        type: 'Stream of tables',
      },
    ],
    package: 'testing',
    desc: 'Tests whether two streams have identical data.',
    example: 'testing.assertEquals(got: got, want: want)',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/testing/assertequals/',
  },
  {
    name: 'testing.diff',
    args: [
      {
        name: 'got',
        desc: 'The stream containing data to test.',
        type: 'Stream of tables',
      },
      {
        name: 'want',
        desc: 'The stream that contains the expected data to test against.',
        type: 'Stream of tables',
      },
      {
        name: 'epsilon',
        desc:
          'How far apart two float values can be, but still considered equal. Defaults to `0.000000001`.',
        type: 'Float',
      },
    ],
    package: 'testing',
    desc: 'Produces a diff between two streams.',
    example: 'testing.assertEquals(got: got, want: want)',
    category: 'Tests',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/testing/diff/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/time/',
  },
  {
    name: 'timedMovingAverage',
    args: [
      {
        name: 'every',
        desc: 'The frequency of time windows.',
        type: 'Duration',
      },
      {
        name: 'period',
        desc: 'The length of each averaged time window.',
        type: 'Duration',
      },
      {
        name: 'column',
        desc:
          'The column on which to compute the moving average. Defaults to `"_value"`',
        type: 'String',
      },
    ],
    package: '',
    desc:
      'Calculates the mean of values in a defined time range at a specified frequency.',
    example: 'timedMovingAverage(every: 1d, period: 5d)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/timedmovingaverage/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/shift/',
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
    example: 'to(bucket: "example-bucket", org: "example-org")',
    category: 'Outputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/outputs/to/',
  },
  {
    name: 'toBool',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a boolean.',
    example: 'toBool()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/tobool',
  },
  {
    name: 'toFloat',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a float.',
    example: 'toFloat()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/tofloat/',
  },
  {
    name: 'toInt',
    args: [],
    package: '',
    desc: 'Converts all values in the `_value` column to a integer.',
    example: 'toInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/toint/',
  },
  {
    name: 'toString',
    args: [],
    package: '',
    desc: 'Converts a value to a string.',
    example: 'toString()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/tostring/',
  },
  {
    name: 'toTime',
    args: [],
    package: '',
    desc: 'Converts a value to a time.',
    example: 'toTime()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/totime/',
  },
  {
    name: 'toUInt',
    args: [],
    package: '',
    desc: 'Converts a value to an unsigned integer.',
    example: 'toUInt()',
    category: 'Type Conversions',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/touint/',
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
    example: 'top(n:10, columns: ["_value"])',
    category: 'Selectors',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/top/',
  },
  {
    name: 'tripleEMA',
    args: [
      {
        name: 'n',
        desc: 'The number of points to average.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Calculates the exponential moving average of values in the `_value` column grouped into `n` number of points, giving more weight to recent data at triple the rate of `exponentialMovingAverage()`.',
    example: 'tripleEMA(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/tripleema/',
  },
  {
    name: 'tripleExponentialDerivative',
    args: [
      {
        name: 'n',
        desc: 'The number of points to use in the calculation.',
        type: 'Integer',
      },
    ],
    package: '',
    desc:
      'Calculates a triple exponential derivative (TRIX) of input tables using n points.',
    example: 'tripleExponentialDerivative(n: 5)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/tripleexponentialderivative/',
  },
  {
    name: 'truncateTimeColumn',
    args: [
      {
        name: 'unit',
        desc: 'The unit of time to truncate to.',
        type: 'Duration',
      },
    ],
    package: '',
    desc: 'Truncates all `_time` values to a specified unit.',
    example: 'truncateTimeColumn(unit: 1m)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/truncatetimecolumn/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/type-conversions/uint/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/selectors/unique/',
  },
  {
    name: 'v1.fieldKeys',
    args: [
      {
        name: 'bucket',
        desc: 'The bucket to list field keys from.',
        type: 'String',
      },
      {
        name: 'predicate',
        desc:
          'Predicate function that filters field keys. Defaults is (r) => true.',
        type: 'Function',
      },
      {
        name: 'start',
        desc: 'The oldest time to include in results. Defaults is `-30d`.',
        type: 'Duration | Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of fields in a bucket.',
    example: 'v1.fieldKeys(bucket: "example-bucket")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/measurementfieldkeys/',
  },
  {
    name: 'v1.fieldsAsCols',
    args: [],
    package: 'influxdata/influxdb/v1',
    desc: 'Aligns fields within each input table that have the same timestamp.',
    example: 'v1.fieldsAsCols()',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/fieldsascols/',
  },
  {
    name: 'v1.measurementFieldKeys',
    args: [
      {
        name: 'bucket',
        desc: 'The bucket to list field keys from.',
        type: 'String',
      },
      {
        name: 'measurement',
        desc: 'The measurement to list field keys from.',
        type: 'String',
      },
      {
        name: 'start',
        desc: 'The oldest time to include in results. Defaults is `-30d`.',
        type: 'Duration | Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of fields in a measurement.',
    example:
      'v1.measurementFieldKeys(bucket: "example-bucket", measurement: "example-measurement")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/measurementfieldkeys/',
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
    example:
      'v1.measurementTagKeys(bucket: "example-bucket", measurement: "mem")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/measurementtagkeys/',
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
      'v1.measurementTagValues(bucket: "example-bucket", measurement: "mem", tag: "host")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/measurementtagvalues/',
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
    example: 'v1.measurements(bucket: "example-bucket")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/measurements/',
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
        type: 'Duration | Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of tag keys for all series that match the predicate.',
    example: 'v1.tagKeys(bucket: "example-bucket")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/tagkeys/',
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
        type: 'Duration | Time',
      },
    ],
    package: 'influxdata/influxdb/v1',
    desc: 'Returns a list of unique values for a given tag.',
    example: 'v1.tagValues(bucket: "example-bucket", tag: "example-tag")',
    category: 'Inputs',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/influxdb-v1/tagvalues/',
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
      {
        name: 'createEmpty',
        desc:
          'Specifies whether empty tables should be created. Defaults to `false`.',
        type: 'Boolean',
      },
    ],
    package: '',
    desc:
      'Groups records based on a time value. New columns are added to uniquely identify each window. Those columns are added to the group key of the output tables. A single input record will be placed into zero or more output tables, depending on the specific windowing function.',
    example: 'window(every: v.windowPeriod)',
    category: 'Transformations',
    link:
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/transformations/window/',
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
      'https://v2.docs.influxdata.com/v2.0/reference/flux/stdlib/built-in/outputs/yield/',
  },
]
