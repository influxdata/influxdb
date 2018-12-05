import {BuilderConfig} from 'src/types/v2/timeMachine'
import {QueryFn} from 'src/shared/constants/queryBuilder'

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

  let bucketFunctionPairs: Array<[string, QueryFn]>

  if (functions.length) {
    bucketFunctionPairs = [].concat(
      ...buckets.map(b => functions.map(f => [b, f]))
    )
  } else {
    bucketFunctionPairs = buckets.map(b => [b, null] as [string, QueryFn])
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
  fn: QueryFn,
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

  if (fn) {
    query += `\n  ${fn.flux}`
  }

  return query
}
