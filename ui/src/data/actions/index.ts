export type Action =
  | ReturnType<typeof resetCachedQueryResults>
  | ReturnType<typeof setQueryResultsByQueryID>

export const hashCode = s =>
  s.split('').reduce((a, b) => ((a << 5) - a + b.charCodeAt(0)) | 0, 0)

export const setQueryResultsByQueryID = (queryID: string, files: string[]) =>
  ({
    type: 'SET_QUERY_RESULTS_BY_QUERY',
    queryID: hashCode(queryID),
    files,
  } as const)

export const resetCachedQueryResults = () =>
  ({
    type: 'RESET_CACHED_QUERY_RESULTS',
  } as const)
