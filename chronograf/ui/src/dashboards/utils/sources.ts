import {QueryConfig, Source} from 'src/types'

export const nextSource = (
  prevQuery: QueryConfig,
  nextQuery: QueryConfig
): Source => {
  if (nextQuery.source) {
    return nextQuery.source
  }

  return prevQuery.source
}
