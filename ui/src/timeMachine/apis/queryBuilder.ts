// Libraries
import {fromFlux} from '@influxdata/giraffe'

// APIs
import {runQuery, RunQueryResult} from 'src/shared/apis/query'

// Utils
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {formatExpression} from 'src/variables/utils/formatExpression'
import {tagToFlux} from 'src/timeMachine/utils/queryBuilder'
import {event} from 'src/cloud/utils/reporting'

// Types
import {TimeRange, BuilderConfig} from 'src/types'
import {CancelBox} from 'src/types/promises'
import {pastThirtyDaysTimeRange} from 'src/shared/constants/timeRanges'

const DEFAULT_TIME_RANGE: TimeRange = pastThirtyDaysTimeRange
const DEFAULT_LIMIT = 200

export interface FindBucketsOptions {
  url: string
  orgID: string
}

export function findBuckets({orgID}: FindBucketsOptions): CancelBox<string[]> {
  const query = `buckets()
  |> sort(columns: ["name"])
  |> limit(n: ${DEFAULT_LIMIT})`

  event('runQuery', {
    context: 'queryBuilder-findBuckets',
  })
  return extractBoxedCol(runQuery(orgID, query), 'name')
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
  orgID,
  bucket,
  tagsSelections,
  searchTerm = '',
  timeRange = DEFAULT_TIME_RANGE,
  limit = DEFAULT_LIMIT,
}: FindKeysOptions): CancelBox<string[]> {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const previousKeyFilter = formatTagKeyFilterCall(tagsSelections)
  const timeRangeArguments = formatTimeRangeArguments(timeRange)

  // requires Flux package to work which we will put in the query
  const searchFilter = !searchTerm
    ? ''
    : `\n  |> filter(fn: (r) => r._value =~ regexp.compile(v: "(?i:" + regexp.quoteMeta(v: "${searchTerm}") + ")"))`

  // TODO: Use the `v1.tagKeys` function from the Flux standard library once
  // this issue is resolved: https://github.com/influxdata/flux/issues/1071
  const query = `import "regexp"
  
  from(bucket: "${bucket}")
  |> range(${timeRangeArguments})
  |> filter(fn: ${tagFilters})
  |> keys()
  |> keep(columns: ["_value"])
  |> distinct()${searchFilter}${previousKeyFilter}
  |> filter(fn: (r) => r._value != "_time" and r._value != "_start" and r._value !=  "_stop" and r._value != "_value")
  |> sort()
  |> limit(n: ${limit})`

  event('runQuery', {
    context: 'queryBuilder-findKeys',
  })

  return extractBoxedCol(runQuery(orgID, query), '_value')
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
  orgID,
  bucket,
  tagsSelections,
  key,
  searchTerm = '',
  timeRange = DEFAULT_TIME_RANGE,
  limit = DEFAULT_LIMIT,
}: FindValuesOptions): CancelBox<string[]> {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const timeRangeArguments = formatTimeRangeArguments(timeRange)

  // requires Flux package to work which we will put in the query
  const searchFilter = !searchTerm
    ? ''
    : `\n  |> filter(fn: (r) => r._value =~ regexp.compile(v: "(?i:" + regexp.quoteMeta(v: "${searchTerm}") + ")"))`

  // TODO: Use the `v1.tagValues` function from the Flux standard library once
  // this issue is resolved: https://github.com/influxdata/flux/issues/1071
  const query = `import "regexp"
  
  from(bucket: "${bucket}")
  |> range(${timeRangeArguments})
  |> filter(fn: ${tagFilters})
  |> keep(columns: ["${key}"])
  |> group()
  |> distinct(column: "${key}")${searchFilter}
  |> limit(n: ${limit})
  |> sort()`

  event('runQuery', {
    context: 'queryBuilder-findValues',
  })

  return extractBoxedCol(runQuery(orgID, query), '_value')
}

export function extractBoxedCol(
  resp: CancelBox<RunQueryResult>,
  colName: string
): CancelBox<string[]> {
  const promise = resp.promise.then<string[]>(result => {
    if (result.type !== 'SUCCESS') {
      return Promise.reject(new Error(result.message))
    }

    return extractCol(result.csv, colName)
  })

  return {promise, cancel: resp.cancel}
}

export function extractCol(csv: string, colName: string): string[] {
  const {table} = fromFlux(csv)
  return table.getColumn(colName, 'string') || []
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

  const calls = validSelections.map(tag => `(${tagToFlux(tag)})`).join(' and ')

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

export function formatTimeRangeArguments(timeRange: TimeRange): string {
  const [start, stop] = getTimeRangeVars(timeRange).map(assignment =>
    formatExpression(assignment.init)
  )

  return `start: ${start}, stop: ${stop}`
}
