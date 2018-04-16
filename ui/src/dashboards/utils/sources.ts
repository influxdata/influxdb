import {Query} from 'src/types'

export const nextSource = (prevQuery: Query, nextQuery: Query): string => {
  if (nextQuery.source) {
    return nextQuery.source
  }

  return prevQuery.source
}
