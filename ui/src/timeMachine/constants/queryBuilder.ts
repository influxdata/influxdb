import {WINDOW_PERIOD, OPTION_NAME} from 'src/variables/constants'

export interface QueryFn {
  name: string
  flux: string
}

export const FUNCTIONS: QueryFn[] = [
  {
    name: 'mean',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: mean)`,
  },
  {
    name: 'median',
    // TODO: https://github.com/influxdata/influxdb/issues/13806
    flux: `|> window(period: ${OPTION_NAME}.${WINDOW_PERIOD})
  |> toFloat()
  |> median()
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")`,
  },
  {
    name: 'max',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: max)`,
  },
  {
    name: 'min',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: min)`,
  },
  {
    name: 'sum',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: sum)`,
  },
  {
    name: 'derivative',
    flux: `|> derivative(unit: ${OPTION_NAME}.${WINDOW_PERIOD}, nonNegative: false)`,
  },
  {
    name: 'distinct',
    flux: '|> distinct()',
  },
  {
    name: 'count',
    flux: '|> count()',
  },
  {
    name: 'increase',
    flux: '|> increase()',
  },
  {
    name: 'skew',
    flux: '|> skew()',
  },
  {
    name: 'spread',
    flux: '|> spread()',
  },
  {
    name: 'stddev',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: stddev)`,
  },
  {
    name: 'first',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: first)`,
  },
  {
    name: 'last',
    flux: `|> aggregateWindow(every: ${OPTION_NAME}.${WINDOW_PERIOD}, fn: last)`,
  },
  {
    name: 'unique',
    flux: '|> unique()',
  },
  {
    name: 'sort',
    flux: '|> sort()',
  },
]
