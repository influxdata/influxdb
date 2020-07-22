// Libraries
import {sortBy} from 'lodash'

// Utils
import {asAssignment, getAllVariables} from 'src/variables/selectors'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {filterUnusedVarsBasedOnQuery} from 'src/shared/utils/filterUnusedVars'
import {runQuery} from 'src/shared/apis/query'

// Types
import {RunQueryResult} from 'src/shared/apis/query'
import {CancelBox} from 'src/types/promises'
import {AppState, Variable} from 'src/types'

export const TIME_INVALIDATION = 5000

const asSimplyKeyValueVariables = (vari: Variable) => {
  return {
    [vari.name]: vari.selected || [],
  }
}

// Hashing function found here:
// https://jsperf.com/hashcodelordvlad
// Through this thread:
// https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript
const hashCode = (rawText: string): string => {
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

  private _cleanExpiredQueries = (): void => {
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

  getFromCache = (id: string, variableID: string): RunQueryResult | null => {
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
    if (Date.now() - this.cache[id].dateSet > TIME_INVALIDATION) {
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
  const queryID = `${hashCode(query)}`
  const variableID = `${hashCode(stringifiedVars)}`

  const cacheResults: RunQueryResult | null = queryCache.getFromCache(
    queryID,
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
    // TODO(ariel): handle custom time range
    // if the timeRange is non-relative (i.e. a custom timeRange or the query text has a set time range)
    // we will need to pass an additional parameter to ensure that the cached data is treated differently
    // set the resolved promise results in the cache
    queryCache.setCacheByID(queryID, variableID, res)
    // non-variable start / stop should
    return res
  })

  return results
}
