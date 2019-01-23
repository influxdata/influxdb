import {WINDOW_PERIOD} from 'src/shared/constants'

export interface QueryFn {
  name: string
  flux: string
  aggregate: boolean
}

export const FUNCTIONS: QueryFn[] = [
  {name: 'mean', flux: `|> mean()`, aggregate: true},
  {name: 'median', flux: '|> toFloat()\n  |> median()', aggregate: true},
  {name: 'max', flux: '|> max()', aggregate: true},
  {name: 'min', flux: '|> min()', aggregate: true},
  {name: 'sum', flux: '|> sum()', aggregate: true},
  {
    name: 'derivative',
    flux: `|> derivative(unit: ${WINDOW_PERIOD}, nonNegative: false)`,
    aggregate: false,
  },
  {name: 'distinct', flux: '|> distinct()', aggregate: false},
  {name: 'count', flux: '|> count()', aggregate: false},
  {name: 'increase', flux: '|> increase()', aggregate: false},
  {name: 'skew', flux: '|> skew()', aggregate: false},
  {name: 'spread', flux: '|> spread()', aggregate: false},
  {name: 'stddev', flux: '|> stddev()', aggregate: true},
  {name: 'first', flux: '|> first()', aggregate: true},
  {name: 'last', flux: '|> last()', aggregate: true},
  {name: 'unique', flux: '|> unique()', aggregate: false},
  {name: 'sort', flux: '|> sort()', aggregate: false},
]
