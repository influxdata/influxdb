import {sample} from 'lodash'

const emptyFunny = [
  'Looks like you don’t have any queries. Be a lot cooler if you did!',
  'Create a query. Go on!',
  'Create a query. Have fun!',
]

export const emptyGraphCopy = sample(emptyFunny)

export const INVALID_DATA_COPY =
  "The data returned from the query can't be visualized with this graph type."
