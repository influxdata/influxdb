// Libraries
import {get} from 'lodash'

// APIs
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/query'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Utils
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {formatExpression} from 'src/variables/utils/formatExpression'

// Types
import {BuilderConfig} from 'src/types/v2'
import {TimeRange} from 'src/types'
import {WrappedCancelablePromise} from 'src/types/promises'

const DEFAULT_TIME_RANGE: TimeRange = {lower: 'now() - 30d'}
const DEFAULT_LIMIT = 200

type CancelableQuery = WrappedCancelablePromise<string[]>

export interface FindBucketsOptions {
  url: string
  orgID: string
}

export function findBuckets({url, orgID}: FindBucketsOptions): CancelableQuery {
  const query = `buckets()
  |> sort(columns: ["name"])
  |> limit(n: ${DEFAULT_LIMIT})`

  const {promise, cancel} = executeQuery(url, orgID, query)

  return {
    promise: promise.then(resp => extractCol(resp, 'name')),
    cancel,
  }
}

export interface FindKeysOptions {
  url: string
  orgID: string
  bucket: string
  tagsSelections: BuilderConfig['tags']
  searchTerm?: string
  timeRange?: TimeRange
  limit?: number
}

export function findKeys({
  url,
  orgID,
  bucket,
  tagsSelections,
  searchTerm = '',
  timeRange = DEFAULT_TIME_RANGE,
  limit = DEFAULT_LIMIT,
}: FindKeysOptions): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)
  const previousKeyFilter = formatTagKeyFilterCall(tagsSelections)
  const timeRangeArguments = formatTimeRangeArguments(timeRange)

  // TODO: Use the `v1.tagKeys` function from the Flux standard library once
  // this issue is resolved: https://github.com/influxdata/flux/issues/1071
  const query = `from(bucket: "${bucket}")
  |> range(${timeRangeArguments})
  |> filter(fn: ${tagFilters})
  |> keys()
  |> keep(columns: ["_value"])${searchFilter}${previousKeyFilter}
  |> filter(fn: (r) => r._value != "_time" and r._value != "_start" and r._value !=  "_stop" and r._value != "_value")
  |> distinct()
  |> sort()
  |> limit(n: ${limit})`

  const {promise, cancel} = executeQuery(url, orgID, query)

  return {
    promise: promise.then(resp => extractCol(resp, '_value')),
    cancel,
  }
}

export interface FindValuesOptions {
  url: string
  orgID: string
  bucket: string
  tagsSelections: BuilderConfig['tags']
  key: string
  searchTerm?: string
  timeRange?: TimeRange
  limit?: number
}

export function findValues({
  url,
  orgID,
  bucket,
  tagsSelections,
  key,
  searchTerm = '',
  timeRange = DEFAULT_TIME_RANGE,
  limit = DEFAULT_LIMIT,
}: FindValuesOptions): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)
  const timeRangeArguments = formatTimeRangeArguments(timeRange)

  // TODO: Use the `v1.tagValues` function from the Flux standard library once
  // this issue is resolved: https://github.com/influxdata/flux/issues/1071
  const query = `from(bucket: "${bucket}")
  |> range(${timeRangeArguments})
  |> filter(fn: ${tagFilters})
  |> group(columns: ["${key}"])
  |> distinct(column: "${key}")
  |> keep(columns: ["_value"])${searchFilter}
  |> limit(n: ${limit})
  |> sort()`

  const {promise, cancel} = executeQuery(url, orgID, query)

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

export function formatTimeRangeArguments(timeRange: TimeRange): string {
  const [start, stop] = getTimeRangeVars(timeRange).map(assignment =>
    formatExpression(assignment.init)
  )

  return `start: ${start}, stop: ${stop}`
}
