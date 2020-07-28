// Libraries
import {sortBy} from 'lodash'

// Utils
import {asAssignment, getAllVariables} from 'src/variables/selectors'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {filterUnusedVarsBasedOnQuery} from 'src/shared/utils/filterUnusedVars'
import {event} from 'src/cloud/utils/reporting'
import {getWindowVars} from 'src/variables/utils/getWindowVars'

// Types
import {RunQueryResult} from 'src/shared/apis/query'
import {CancelBox} from 'src/types/promises'
import {AppState, GetState, Variable} from 'src/types'
import {RunQueryPromiseMutex} from './singleQuery'

export const TIME_INVALIDATION = 1000 * 60 * 10 // 10 minutes

const asSimplyKeyValueVariables = (vari: Variable) => {
  return {
    [vari.name]: vari.selected || [],
  }
}

// Hashing function found here:
// https://jsperf.com/hashcodelordvlad
// Through this thread:
// https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript
export const hashCode = (rawText: string): string => {
  let hash = 0
  if (!rawText) {
    return `${hash}`
  }
  for (let i = 0; i < rawText.length; i++) {
    hash = (hash << 5) - hash + rawText.charCodeAt(i)
    hash |= 0 // Convert to 32bit integer
  }
  return `${hash}`
}

class QueryCache {
  cache = {}

  private cleanExpiredQueries = (): void => {
    const now = Date.now()
    for (const id in this.cache) {
      // TODO(ariel): need to implement specific rules for custom time ranges
      if (this.cache[id].isCustomTime) {
        continue
      }
      if (now - this.cache[id].dateSet > TIME_INVALIDATION) {
        this.resetCacheByID(id)
      }
    }
  }

  getFromCache = (
    id: string,
    hashedVariables: string
  ): RunQueryResult | null => {
    // no existing query match
    if (!this.cache[id]) {
      return null
    }
    // query match with no existing variable match
    if (this.cache[id].hashedVariables !== hashedVariables) {
      this.resetCacheByID(id)
      return null
    }
    // query has been initialized but the result has not been set
    if (this.cache[id].values === undefined) {
      return null
    }
    // query & variable match with an expired result
    if (Date.now() - this.cache[id].dateSet > TIME_INVALIDATION) {
      this.resetCacheByID(id)
      return null
    }
    event('Query Cache successful Get', {context: 'queryCache', queryID: id})
    return this.cache[id].values
  }

  initializeCacheByID = (
    queryID: string,
    hashedVariables: string,
    isCustomTime: boolean = false
  ) => {
    if (this.cache[queryID]) {
      return this.cache[queryID]
    }
    this.cache[queryID] = {
      dateSet: Date.now(),
      hashedVariables,
      isCustomTime,
      mutex: RunQueryPromiseMutex<RunQueryResult>(),
    }
    return this.cache[queryID]
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
    hashedVariables: string,
    values: RunQueryResult
  ): void => {
    event('Query Cache was Set', {context: 'queryCache', queryID})
    this.cache[queryID] = {
      ...this.initializeCacheByID(queryID, hashedVariables),
      dateSet: Date.now(),
      values,
    }
  }

  startWatchDog = () => {
    setInterval(() => {
      this.cleanExpiredQueries()
    }, TIME_INVALIDATION / 2)

    this.cleanExpiredQueries()
  }
}

const queryCache = new QueryCache()
// Set an interval to check for expired data to invalidate
queryCache.startWatchDog()

export const resetQueryCache = (): void => {
  queryCache.resetCache()
}

export const resetQueryCacheByQuery = (query: string): void => {
  const queryID = `${hashCode(query)}`
  queryCache.resetCacheByID(queryID)
}

export const getCachedResultsOrRunQuery = (
  orgID: string,
  query: string,
  state: AppState
): CancelBox<RunQueryResult> => {
  const queryID = `${hashCode(query)}`
  event('Starting Query Cache Process ', {context: 'queryCache', queryID})
  const allVars = getAllVariables(state)
  const usedVars = filterUnusedVarsBasedOnQuery(allVars, [query])
  const variables = sortBy(usedVars, ['name'])
  const simplifiedVariables = variables.map(v => asSimplyKeyValueVariables(v))
  const stringifiedVars = JSON.stringify(simplifiedVariables)
  // create the queryID based on the query & vars
  const hashedVariables = `${hashCode(stringifiedVars)}`

  const cacheResults: RunQueryResult | null = queryCache.getFromCache(
    queryID,
    hashedVariables
  )

  // check the cache based on text & vars
  if (cacheResults) {
    return {
      promise: new Promise(resolve => resolve(cacheResults)),
      cancel: () => {},
    }
  }
  const variableAssignments = variables
    .map(v => asAssignment(v))
    .filter(v => !!v)

  const windowVars = getWindowVars(query, allVars)

  // otherwise query & set results
  const extern = buildVarsOption([...variableAssignments, ...windowVars])
  const {mutex} = queryCache.initializeCacheByID(queryID, hashedVariables)
  const results = mutex.run(orgID, query, extern)
  results.promise = results.promise.then(res => {
    // TODO(ariel): handle custom time range
    // if the timeRange is non-relative (i.e. a custom timeRange or the query text has a set time range)
    // we will need to pass an additional parameter to ensure that the cached data is treated differently
    // set the resolved promise results in the cache
    queryCache.setCacheByID(queryID, hashedVariables, res)
    // non-variable start / stop should
    return res
  })

  return results
}

export const getCachedResultsThunk = (orgID: string, query: string) => (
  _,
  getState: GetState
): CancelBox<RunQueryResult> =>
  getCachedResultsOrRunQuery(orgID, query, getState())
