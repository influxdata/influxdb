// Types
import {CancelBox} from 'src/types/promises'
import {RunQueryResult} from 'src/shared/apis/query'

export type Action =
  | ReturnType<typeof resetCachedQueryResults>
  | ReturnType<typeof setQueryResultsByQueryID>

// Hashing function found here:
// https://jsperf.com/hashcodelordvlad
// Through this thread:
// https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript
export const hashCode = (queryText: string): string => {
  let hash = 0,
    char
  if (!queryText) {
    return `${hash}`
  }
  for (let i = 0; i < queryText.length; i++) {
    char = queryText.charCodeAt(i)
    hash = (hash << 5) - hash + char
    hash |= 0 // Convert to 32bit integer
  }
  return `${hash}`
}

export const setQueryResultsByQueryID = (
  queryID: string,
  queryPromise: CancelBox<RunQueryResult>
) =>
  ({
    type: 'SET_QUERY_RESULTS_BY_QUERY',
    queryID,
    queryPromise,
  } as const)

export const resetCachedQueryResults = () =>
  ({
    type: 'RESET_CACHED_QUERY_RESULTS',
  } as const)
