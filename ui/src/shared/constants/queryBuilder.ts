export interface QueryFn {
  name: string
}

export const FUNCTIONS: QueryFn[] = [
  {name: 'mean'},
  {name: 'median'},
  {name: 'max'},
  {name: 'min'},
  {name: 'sum'},
  {name: 'distinct'},
  {name: 'count'},
  {name: 'increase'},
  {name: 'skew'},
  {name: 'spread'},
  {name: 'stddev'},
  {name: 'first'},
  {name: 'last'},
  {name: 'unique'},
  {name: 'sort'},
]
