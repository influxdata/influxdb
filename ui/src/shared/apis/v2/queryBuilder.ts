// Libraries
import {get} from 'lodash'

// APIs
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/v2/query'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {InfluxLanguage, BuilderConfig} from 'src/types/v2'
import {WrappedCancelablePromise} from 'src/types/promises'

export const SEARCH_DURATION = '30d'
export const LIMIT = 200

type CancelableQuery = WrappedCancelablePromise<string[]>

function findBuckets(url: string): CancelableQuery {
  const query = `buckets()
  |> sort(columns: ["name"])
  |> limit(n: ${LIMIT})`

  const {promise, cancel} = executeQuery(url, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, 'name')),
    cancel,
  }
}

function findKeys(
  url: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  searchTerm: string = ''
): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)
  const previousKeyFilter = formatTagKeyFilterCall(tagsSelections)

  const query = `
  import "influxdata/influxdb/v1"

  v1.tagKeys(bucket: "${bucket}", predicate: ${tagFilters}, start: -${SEARCH_DURATION})${searchFilter}${previousKeyFilter}
  |> filter(fn: (r) =>
    r._value != "_time" and
    r._value != "_start" and
    r._value !=  "_stop" and
    r._value != "_value")
  |> distinct()
  |> limit(n: ${LIMIT})
  `

  const {promise, cancel} = executeQuery(url, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, '_value')),
    cancel,
  }
}

function findValues(
  url: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  key: string,
  searchTerm: string = ''
): CancelableQuery {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)

  const query = `
  import "influxdata/influxdb/v1"

  v1.tagValues(bucket: "${bucket}", tag: "${key}", predicate: ${tagFilters}, start: -${SEARCH_DURATION})${searchFilter}
  |> limit(n: ${LIMIT})
  `

  const {promise, cancel} = executeQuery(url, query, InfluxLanguage.Flux)

  return {
    promise: promise.then(resp => extractCol(resp, '_value')),
    cancel,
  }
}

function extractCol(resp: ExecuteFluxQueryResult, colName: string): string[] {
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

function formatTagKeyFilterCall(tagsSelections: BuilderConfig['tags']) {
  const keys = tagsSelections.map(({key}) => key)

  if (!keys.length) {
    return ''
  }

  const fnBody = keys.map(key => `r._value != "${key}"`).join(' and ')

  return `\n  |> filter(fn: (r) => ${fnBody})`
}

function formatSearchFilterCall(searchTerm: string) {
  if (!searchTerm) {
    return ''
  }

  return `\n  |> filter(fn: (r) => r._value =~ /(?i:${searchTerm})/)`
}

export class QueryBuilderFetcher {
  private findBucketsQuery: CancelableQuery
  private findKeysQueries: CancelableQuery[] = []
  private findValuesQueries: CancelableQuery[] = []

  public async findBuckets(url: string): Promise<string[]> {
    if (this.findBucketsQuery) {
      this.findBucketsQuery.cancel()
    }

    this.findBucketsQuery = findBuckets(url)

    return this.findBucketsQuery.promise
  }

  public async findKeys(
    index: number,
    url: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    searchTerm: string = ''
  ): Promise<string[]> {
    this.cancelFindKeys(index)

    this.findKeysQueries[index] = findKeys(
      url,
      bucket,
      tagsSelections,
      searchTerm
    )
    return this.findKeysQueries[index].promise
  }

  public cancelFindKeys(index) {
    if (this.findKeysQueries[index]) {
      this.findKeysQueries[index].cancel()
    }
  }

  public async findValues(
    index: number,
    url: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    key: string,
    searchTerm: string = ''
  ): Promise<string[]> {
    this.cancelFindValues(index)

    this.findValuesQueries[index] = findValues(
      url,
      bucket,
      tagsSelections,
      key,
      searchTerm
    )

    return this.findValuesQueries[index].promise
  }

  public cancelFindValues(index) {
    if (this.findValuesQueries[index]) {
      this.findValuesQueries[index].cancel()
    }
  }
}

function formatTagFilterPredicate(tagsSelections: BuilderConfig['tags']) {
  if (!tagsSelections.length) {
    return '(r) => true'
  }

  const calls = tagsSelections
    .filter(({key, values}) => key && values.length)
    .map(({key, values}) => {
      const body = values.map(value => `r.${key} == "${value}"`).join(' or ')

      return `(${body})`
    })
    .join(' and ')

  return `(r) => ${calls}`
}
