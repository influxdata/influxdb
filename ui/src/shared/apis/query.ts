// Libraries
import {sortBy} from 'lodash'

// Constants
import {FLUX_RESPONSE_BYTES_LIMIT, API_BASE_PATH} from 'src/shared/constants'
import {
  RATE_LIMIT_ERROR_STATUS,
  RATE_LIMIT_ERROR_TEXT,
} from 'src/cloud/constants'

// Utils
import {asAssignment, getAllVariables} from 'src/variables/selectors'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {filterUnusedVarsBasedOnQuery} from 'src/shared/utils/filterUnusedVars'

// Types
import {CancelBox} from 'src/types/promises'
import {AppState, File, Query, CancellationError, Variable} from 'src/types'

const TIME_INVALIDATION = 5000

export type RunQueryResult =
  | RunQuerySuccessResult
  | RunQueryLimitResult
  | RunQueryErrorResult

export interface RunQuerySuccessResult {
  type: 'SUCCESS'
  csv: string
  didTruncate: boolean
  bytesRead: number
}

export interface RunQueryLimitResult {
  type: 'RATE_LIMIT_ERROR'
  retryAfter: number
  message: string
}

export interface RunQueryErrorResult {
  type: 'UNKNOWN_ERROR'
  message: string
  code?: string
}

const asSimplyKeyValueVariables = (vari: Variable) => {
  return {
    [vari.name]: vari.selected || [],
  }
}

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

class QueryCache {
  cache = {}

  _cleanExpiredQueries = (): void => {
    const now = Date.now()
    for (let id in this.cache) {
      if (this.cache[id].isCustomTime) {
        continue
      }
      if (this.cache[id].dateSet > now - TIME_INVALIDATION) {
        this.resetCacheByID(id)
      }
    }
  }

  getFromCache = (id: string, variableID): RunQueryResult | null => {
    // no existing query match
    if (!this.cache[id]) {
      return null
    }
    // query match with no existing variable match
    if (this.cache[id].variables !== variableID) {
      this.resetCacheByID(id)
      return null
    }
    // query & variable match with an expired result
    if (this.cache[id].dateSet > Date.now() - TIME_INVALIDATION) {
      this.resetCacheByID(id)
      return null
    }
    return this.cache[id].values
  }

  resetCacheByID = (id: string): void => {
    if (!this.cache[id]) {
      return
    }
    delete this.cache[id]
  }

  resetCache = (): void => {
    this.cache = {}
  }

  setCacheByID = (
    queryID: string,
    variableID: string,
    values: RunQueryResult,
    isCustomTime: boolean = false
  ): void => {
    this.cache[queryID] = {
      dateSet: Date.now(),
      isCustomTime,
      values,
      variables: variableID,
    }
  }

  startWatchDog = () => {
    setInterval(() => {
      this._cleanExpiredQueries()
    }, TIME_INVALIDATION / 2)

    this._cleanExpiredQueries()
  }
}

const queryCache = new QueryCache()
// Set an interval to check for expired data to invalidate
queryCache.startWatchDog()

export const resetQueryCache = (): void => {
  queryCache.resetCache()
}

export const resetQueryCacheByID = (id: string): void => {
  queryCache.resetCacheByID(id)
}

export const resetQueryCacheByQuery = (query: string): void => {
  const queryID = `${hashCode(query)}`
  queryCache.resetCacheByID(queryID)
}

export const getRunQueryResults = (
  orgID: string,
  query: string,
  state: AppState,
  abortController?: AbortController
): CancelBox<RunQueryResult> => {
  const usedVars = filterUnusedVarsBasedOnQuery(getAllVariables(state), [query])
  const variables = sortBy(usedVars, ['name'])
  const simplifiedVariables = variables.map(v => asSimplyKeyValueVariables(v))
  const stringifiedVars = JSON.stringify(simplifiedVariables)
  // create the queryID based on the query & vars
  const queryID = `${hashCode(query)}}`
  const variableID = `${hashCode(stringifiedVars)}`

  const cacheResults: RunQueryResult = queryCache.getFromCache(
    query,
    variableID
  )
  // check the cache based on text & vars
  if (cacheResults) {
    const controller = abortController || new AbortController()
    return {
      promise: new Promise(resolve => resolve(cacheResults)),
      cancel: () => controller.abort(),
    }
  }
  const variableAssignments = variables
    .map(v => asAssignment(v))
    .filter(v => !!v)
  // otherwise query & set results
  const extern = buildVarsOption(variableAssignments)
  const results = runQuery(orgID, query, extern, abortController)
  results.promise = results.promise.then(res => {
    // if the timeRange is non-relative (i.e. a custom timeRange || the query text has a set time range)
    // we will need to pass an additional parameter to ensure that the cached data is treated differently
    // set the resolved promise results in the cache
    queryCache.setCacheByID(queryID, variableID, res)
    // non-variable start / stop should
    return res
  })

  return results
}

export const runQuery = (
  orgID: string,
  query: string,
  extern?: File,
  abortController?: AbortController
): CancelBox<RunQueryResult> => {
  const url = `${API_BASE_PATH}api/v2/query?${new URLSearchParams({orgID})}`

  const headers = {
    'Content-Type': 'application/json',
    'Accept-Encoding': 'gzip',
  }

  const body: Query = {
    query,
    extern,
    dialect: {annotations: ['group', 'datatype', 'default']},
  }

  const controller = abortController || new AbortController()

  const request = fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: controller?.signal,
  })

  const promise = request
    .then(processResponse)
    .catch(e =>
      e.name === 'AbortError'
        ? Promise.reject(new CancellationError())
        : Promise.reject(e)
    )

  return {
    promise,
    cancel: () => controller.abort(),
  }
}

const processResponse = async (response: Response): Promise<RunQueryResult> => {
  switch (response.status) {
    case 200:
      return processSuccessResponse(response)
    case RATE_LIMIT_ERROR_STATUS:
      return processRateLimitResponse(response)
    default:
      return processErrorResponse(response)
  }
}

const processSuccessResponse = async (
  response: Response
): Promise<RunQuerySuccessResult> => {
  const reader = response.body.getReader()
  const decoder = new TextDecoder()

  let csv = ''
  let bytesRead = 0
  let didTruncate = false

  let read = await reader.read()

  while (!read.done) {
    const text = decoder.decode(read.value)

    bytesRead += read.value.byteLength

    if (bytesRead > FLUX_RESPONSE_BYTES_LIMIT) {
      csv += trimPartialLines(text)
      didTruncate = true
      break
    } else {
      csv += text
      read = await reader.read()
    }
  }

  reader.cancel()

  return {
    type: 'SUCCESS',
    csv,
    bytesRead,
    didTruncate,
  }
}

const processRateLimitResponse = (response: Response): RunQueryLimitResult => {
  const retryAfter = response.headers.get('Retry-After')

  return {
    type: 'RATE_LIMIT_ERROR',
    retryAfter: retryAfter ? parseInt(retryAfter) : null,
    message: RATE_LIMIT_ERROR_TEXT,
  }
}

const processErrorResponse = async (
  response: Response
): Promise<RunQueryErrorResult> => {
  try {
    const body = await response.text()
    const json = JSON.parse(body)
    const message = json.message || json.error
    const code = json.code

    return {type: 'UNKNOWN_ERROR', message, code}
  } catch {
    return {type: 'UNKNOWN_ERROR', message: 'Failed to execute Flux query'}
  }
}

/*
  Given an arbitrary text chunk of a Flux CSV, trim partial lines off of the end
  of the text.

  For example, given the following partial Flux response,

            r,baz,3
      foo,bar,baz,2
      foo,bar,b

  we want to trim the last incomplete line, so that the result is

            r,baz,3
      foo,bar,baz,2

*/
const trimPartialLines = (partialResp: string): string => {
  let i = partialResp.length - 1

  while (partialResp[i] !== '\n') {
    if (i <= 0) {
      return partialResp
    }

    i -= 1
  }

  return partialResp.slice(0, i + 1)
}
