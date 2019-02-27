// Libraries
import {get} from 'lodash'

// APIs
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/query'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {InfluxLanguage, BuilderConfig} from 'src/types/v2'
import {WrappedCancelablePromise} from 'src/types/promises'

export const SEARCH_DURATION = '30d'
export const LIMIT = 200

type CancelableQuery = WrappedCancelablePromise<string[]>

export function findBuckets(url: string, orgID: string): CancelableQuery {
  const query = `buckets()
  |> sort(columns: ["name"])
  |> limit(n: ${LIMIT})`

  const {promise, cancel} = executeQuery(url, orgID, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, 'name')),
    cancel,
  }
}

export function findKeys(
  url: string,
  orgID: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  searchTerm: string = ''
): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)
  const previousKeyFilter = formatTagKeyFilterCall(tagsSelections)

  const query = `import "influxdata/influxdb/v1"

v1.tagKeys(bucket: "${bucket}", predicate: ${tagFilters}, start: -${SEARCH_DURATION})${searchFilter}${previousKeyFilter}
  |> filter(fn: (r) =>
    r._value != "_time" and
    r._value != "_start" and
    r._value !=  "_stop" and
    r._value != "_value")
  |> distinct()
  |> sort()
  |> limit(n: ${LIMIT})`

  const {promise, cancel} = executeQuery(url, orgID, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, '_value')),
    cancel,
  }
}

export function findValues(
  url: string,
  orgID: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  key: string,
  searchTerm: string = ''
): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)

  const query = `import "influxdata/influxdb/v1"

v1.tagValues(bucket: "${bucket}", tag: "${key}", predicate: ${tagFilters}, start: -${SEARCH_DURATION})${searchFilter}
  |> limit(n: ${LIMIT})
  |> sort()`

  const {promise, cancel} = executeQuery(url, orgID, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, '_value')),
    cancel,
  }
}

export function extractCol(
  resp: ExecuteFluxQueryResult,
  colName: string
): string[] {
  const tables = parseResponse(resp.csv)
  const data = get(tables, '0.data', [])

  if (!data.length) {
    return []
  }

  const colIndex = data[0].findIndex(d => d === colName)

  if (colIndex === -1) {
    throw new Error(`could not find column "${colName}" in response`)
  }

  const colValues = []

  for (let i = 1; i < data.length; i++) {
    colValues.push(data[i][colIndex])
  }

  return colValues
}

export function formatTagFilterPredicate(
  tagsSelections: BuilderConfig['tags']
) {
  const validSelections = tagsSelections.filter(
    ({key, values}) => key && values.length
  )

  if (!validSelections.length) {
    return '(r) => true'
  }

  const calls = validSelections
    .map(({key, values}) => {
      const body = values.map(value => `r.${key} == "${value}"`).join(' or ')

      return `(${body})`
    })
    .join(' and ')

  return `(r) => ${calls}`
}

export function formatTagKeyFilterCall(tagsSelections: BuilderConfig['tags']) {
  const keys = tagsSelections.map(({key}) => key)

  if (!keys.length) {
    return ''
  }

  const fnBody = keys.map(key => `r._value != "${key}"`).join(' and ')

  return `\n  |> filter(fn: (r) => ${fnBody})`
}

export function formatSearchFilterCall(searchTerm: string) {
  if (!searchTerm) {
    return ''
  }

  return `\n  |> filter(fn: (r) => r._value =~ /(?i:${searchTerm})/)`
}
