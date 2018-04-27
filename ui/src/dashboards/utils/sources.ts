import {QueryConfig} from 'src/types'

export const nextSource = (
  prevQuery: QueryConfig,
  nextQuery: QueryConfig
): string => {
  if (nextQuery.source) {
    return nextQuery.source
  }

  return prevQuery.source
}
