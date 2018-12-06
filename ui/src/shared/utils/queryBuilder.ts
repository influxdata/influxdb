import {BuilderConfig} from 'src/types/v2'
import {FUNCTIONS} from 'src/shared/constants/queryBuilder'

const DEFAULT_WINDOW_INTERVAL = '10s'

const WINDOW_INTERVALS = {
  '5m': '10s',
  '15m': '10s',
  '1h': '1m',
  '6h': '5m',
  '12h': '10m',
  '24h': '20m',
  '2d': '1h',
  '7d': '1h',
  '30d': '6h',
}

export function isConfigValid(builderConfig: BuilderConfig): boolean {
  const {buckets, measurements} = builderConfig
  const isConfigValid = buckets.length >= 1 && measurements.length >= 1

  return isConfigValid
}

export function buildQuery(
  builderConfig: BuilderConfig,
  duration: string = '1h'
): string {
  const {buckets, measurements, fields, functions} = builderConfig

  let bucketFunctionPairs: Array<[string, string]>

  if (functions.length) {
    bucketFunctionPairs = [].concat(
      ...buckets.map(b => functions.map(f => [b, f]))
    )
  } else {
    bucketFunctionPairs = buckets.map(b => [b, null] as [string, string])
  }

  const query = bucketFunctionPairs
    .map(([b, f]) => buildQueryHelper(b, measurements, fields, f, duration))
    .join('\n\n')

  return query
}

function buildQueryHelper(
  bucket: string,
  measurements: string[],
  fields: string[],
  functionName: string,
  duration: string
): string {
  let query = `from(bucket: "${bucket}")
  |> range(start: -${duration})`

  if (measurements.length) {
    const measurementsPredicate = measurements
      .map(m => `r._measurement == "${m}"`)
      .join(' or ')

    query += `
  |> filter(fn: (r) => ${measurementsPredicate})`
  }

  if (fields.length) {
    const fieldsPredicate = fields.map(f => `r._field == "${f}"`).join(' or ')

    query += `
  |> filter(fn: (r) => ${fieldsPredicate})`
  }

  const fn = FUNCTIONS.find(f => f.name === functionName)

  if (fn && fn.aggregate) {
    query += `
  |> window(every: ${WINDOW_INTERVALS[duration] || DEFAULT_WINDOW_INTERVAL})
  ${fn.flux}
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")
  |> yield(name: "${fn.name}")`
  } else if (fn) {
    query += `
  ${fn.flux}
  |> yield(name: "${fn.name}")`
  }

  return query
}
