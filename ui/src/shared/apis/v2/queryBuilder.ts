// Libraries
import {get} from 'lodash'

// APIs
import {executeQuery, ExecuteFluxQueryResult} from 'src/shared/apis/v2/query'
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {SourceType, InfluxLanguage} from 'src/types/v2'

export const SEARCH_DURATION = '30d'
export const LIMIT = 200

export async function findBuckets(
  url: string,
  sourceType: SourceType,
  searchTerm?: string
) {
  if (sourceType === SourceType.V1) {
    throw new Error('metaqueries not yet implemented for SourceType.V1')
  }

  const resp = await findBucketsFlux(url, searchTerm)
  const parsed = extractCol(resp, 'name')

  return parsed
}

export async function findMeasurements(
  url: string,
  sourceType: SourceType,
  bucket: string,
  searchTerm: string = ''
): Promise<string[]> {
  if (sourceType === SourceType.V1) {
    throw new Error('metaqueries not yet implemented for SourceType.V1')
  }

  const resp = await findMeasurementsFlux(url, bucket, searchTerm)
  const parsed = extractCol(resp, '_measurement')

  return parsed
}

export async function findFields(
  url: string,
  sourceType: SourceType,
  bucket: string,
  measurements: string[],
  searchTerm: string = ''
): Promise<string[]> {
  if (sourceType === SourceType.V1) {
    throw new Error('metaqueries not yet implemented for SourceType.V1')
  }

  const resp = await findFieldsFlux(url, bucket, measurements, searchTerm)
  const parsed = extractCol(resp, '_field')

  return parsed
}

function findBucketsFlux(
  url: string,
  searchTerm: string
): Promise<ExecuteFluxQueryResult> {
  let query = 'buckets()'

  if (searchTerm) {
    query += `
  |> filter(fn: (r) => r.name =~ /(?i:${searchTerm})/)`
  }

  query += `
  |> sort(columns: ["name"])
  |> limit(n: ${LIMIT})`

  return executeQuery(url, query, InfluxLanguage.Flux)
}

function findMeasurementsFlux(
  url: string,
  bucket: string,
  searchTerm: string
): Promise<ExecuteFluxQueryResult> {
  let query = `from(bucket: "${bucket}")
  |> range(start: -${SEARCH_DURATION})`

  if (searchTerm) {
    query += `
  |> filter(fn: (r) => r._measurement =~ /(?i:${searchTerm})/)`
  }

  query += `
  |> group(by: ["_measurement"])
  |> distinct(column: "_measurement")
  |> group(none: true)
  |> sort(columns: ["_measurement"])
  |> limit(n: ${LIMIT})`

  return executeQuery(url, query, InfluxLanguage.Flux)
}

function findFieldsFlux(
  url: string,
  bucket: string,
  measurements: string[],
  searchTerm: string
): Promise<ExecuteFluxQueryResult> {
  const measurementPredicate = measurements
    .map(m => `r._measurement == "${m}"`)
    .join(' or ')

  let query = `from(bucket: "${bucket}")
  |> range(start: -${SEARCH_DURATION})
  |> filter(fn: (r) => ${measurementPredicate})`

  if (searchTerm) {
    query += `
  |> filter(fn: (r) => r._field =~ /(?i:${searchTerm})/)`
  }

  query += `
  |> group(by: ["_field"])
  |> distinct(column: "_field")
  |> group(none: true)
  |> sort(columns: ["_field"])
  |> limit(n: ${LIMIT})`

  return executeQuery(url, query, InfluxLanguage.Flux)
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
