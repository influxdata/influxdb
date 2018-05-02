import {QueryConfig} from 'src/types'

export const nextSource = (
  prevQuery: QueryConfig,
  nextQuery: QueryConfig
): string => {
  if (nextQuery.sourceLink) {
    return nextQuery.sourceLink
  }

  return prevQuery.sourceLink
}
