// Libraries
import {get} from 'lodash'
import uuid from 'uuid'

// APIs
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/v2/query'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {InfluxLanguage, BuilderConfig} from 'src/types/v2'

export const SEARCH_DURATION = '30d'
export const LIMIT = 200

async function findBuckets(url: string): Promise<string[]> {
  const query = `buckets()
  |> sort(columns: ["name"])
  |> limit(n: ${LIMIT})`

  const resp = await executeQuery(url, query, InfluxLanguage.Flux)
  const parsed = extractCol(resp, 'name')

  return parsed
}

async function findKeys(
  url: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  searchTerm: string = ''
): Promise<string[]> {
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

  const resp = await executeQuery(url, query, InfluxLanguage.Flux)
  const parsed = extractCol(resp, '_value')

  return parsed
}

async function findValues(
  url: string,
  bucket: string,
  tagsSelections: BuilderConfig['tags'],
  key: string,
  searchTerm: string = ''
): Promise<string[]> {
  const tagFilters = formatTagFilterPredicate(tagsSelections)
  const searchFilter = formatSearchFilterCall(searchTerm)

  const query = `
  import "influxdata/influxdb/v1"

  v1.tagValues(bucket: "${bucket}", tag: "${key}", predicate: ${tagFilters}, start: -${SEARCH_DURATION})${searchFilter}
  |> limit(n: ${LIMIT})
  `

  const resp = await executeQuery(url, query, InfluxLanguage.Flux)
  const parsed = extractCol(resp, '_value')

  return parsed
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

export class CancellationError extends Error {}

export class QueryBuilderFetcher {
  private findBucketsToken: string = ''
  private findKeysTokens = []
  private findValuesTokens = []

  public async findBuckets(url: string): Promise<string[]> {
    const token = uuid.v4()

    this.findBucketsToken = token

    const result = await findBuckets(url)

    if (token !== this.findBucketsToken) {
      throw new CancellationError()
    }

    return result
  }

  public async findKeys(
    index: number,
    url: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    searchTerm: string = ''
  ): Promise<string[]> {
    const token = uuid.v4()

    this.findKeysTokens[index] = token

    const result = await findKeys(url, bucket, tagsSelections, searchTerm)

    if (token !== this.findKeysTokens[index]) {
      throw new CancellationError()
    }

    return result
  }

  public cancelFindKeys(index) {
    this.findKeysTokens[index] = uuid.v4()
  }

  public async findValues(
    index: number,
    url: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    key: string,
    searchTerm: string = ''
  ): Promise<string[]> {
    const token = uuid.v4()

    this.findValuesTokens[index] = token

    const result = await findValues(
      url,
      bucket,
      tagsSelections,
      key,
      searchTerm
    )

    if (token !== this.findValuesTokens[index]) {
      throw new CancellationError()
    }

    return result
  }

  public cancelFindValues(index) {
    this.findValuesTokens[index] = uuid.v4()
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
