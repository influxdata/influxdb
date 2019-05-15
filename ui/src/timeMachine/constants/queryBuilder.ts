export const AGG_WINDOW_AUTO = 'auto'
export const AGG_WINDOW_NONE = 'none'

export interface QueryFn {
  name: string
  flux: (period?: string) => string
}

export const FUNCTIONS: QueryFn[] = [
  {
    name: 'mean',
    flux: period => `|> aggregateWindow(every: ${period}, fn: mean)`,
  },
  {
    name: 'median',
    // TODO: https://github.com/influxdata/influxdb/issues/13806
    flux: period => `|> window(period: ${period})
  |> toFloat()
  |> median()
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")`,
  },
  {
    name: 'max',
    flux: period => `|> aggregateWindow(every: ${period}, fn: max)`,
  },
  {
    name: 'min',
    flux: period => `|> aggregateWindow(every: ${period}, fn: min)`,
  },
  {
    name: 'sum',
    flux: period => `|> aggregateWindow(every: ${period}, fn: sum)`,
  },
  {
    name: 'derivative',
    flux: period => `|> derivative(unit: ${period}, nonNegative: false)`,
  },
  {
    name: 'nonnegative derivative',
    flux: period => `|> derivative(unit: ${period}, nonNegative: true)`,
  },
  {
    name: 'distinct',
    flux: () => '|> distinct()',
  },
  {
    name: 'count',
    flux: () => '|> count()',
  },
  {
    name: 'increase',
    flux: () => '|> increase()',
  },
  {
    name: 'skew',
    flux: () => '|> skew()',
  },
  {
    name: 'spread',
    flux: () => '|> spread()',
  },
  {
    name: 'stddev',
    flux: period => `|> aggregateWindow(every: ${period}, fn: stddev)`,
  },
  {
    name: 'first',
    flux: period => `|> aggregateWindow(every: ${period}, fn: first)`,
  },
  {
    name: 'last',
    flux: period => `|> aggregateWindow(every: ${period}, fn: last)`,
  },
  {
    name: 'unique',
    flux: () => '|> unique()',
  },
  {
    name: 'sort',
    flux: () => '|> sort()',
  },
]
