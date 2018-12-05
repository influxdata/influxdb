export interface QueryFn {
  name: string
  flux: string
}

export const FUNCTIONS: QueryFn[] = [
  {name: 'mean', flux: '|> mean()'},
  {name: 'median', flux: '|> toFloat()\n  |> median()'},
  {name: 'max', flux: '|> max()'},
  {name: 'min', flux: '|> min()'},
  {name: 'sum', flux: '|> sum()'},
  {name: 'distinct', flux: '|> distinct()'},
  {name: 'count', flux: '|> count()'},
  {name: 'increase', flux: '|> increase()'},
  {name: 'skew', flux: '|> skew()'},
  {name: 'spread', flux: '|> spread()'},
  {name: 'stddev', flux: '|> stddev()'},
  {name: 'first', flux: '|> first()'},
  {name: 'last', flux: '|> last()'},
  {name: 'unique', flux: '|> unique()'},
  {name: 'sort', flux: '|> sort()'},
]
